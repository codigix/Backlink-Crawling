"""Shared building blocks for the forward crawler: URL handling, SSRF guard, PostgreSQL store, matching, sitemaps.

The PostgreSQL database (DATABASE_URL) is the permanent link graph. Crawlee's RequestQueue is only a per-round work list fed from it.
"""

import atexit
import contextvars
import ipaddress
import logging
import os
import re
import random
import socket
import threading
import time
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
import psycopg
import tldextract
from psycopg_pool import ConnectionPool, PoolClosed, PoolTimeout
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")  # real environment variables still win
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/backlinks")
DB_SCHEMA = os.environ.get("DB_SCHEMA") or None  # test hook: search_path for an isolated schema (env so Celery worker processes share it)
TS = "(to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS'))"  # same text format the code compares as strings
# Honest identification; set CRAWLER_CONTACT_URL to a page describing who runs the crawler.
USER_AGENT = f"BacklinkGraphBot/1.0 (+{os.environ.get('CRAWLER_CONTACT_URL', 'https://example.com/bot')})"
_tld = tldextract.TLDExtract(suffix_list_urls=())  # bundled Public Suffix List snapshot, no network at import
NON_HTML = re.compile(r".*\.(pdf|jpe?g|png|gif|webp|svg|ico|zip|rar|gz|docx?|xlsx?|pptx?|mp4|mp3|css|js)(\?|$)", re.I)
UNSAFE_PATH = re.compile(r"(log-?out|sign-?out|delete|remove|unsubscribe|/cdn-cgi/)", re.I)
HOSTNAME = re.compile(r"^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}$|^[a-z0-9-]+\.localhost$")

# Test hook only: tests crawl 127.0.0.1 and must switch the SSRF guard off.
ALLOW_PRIVATE = os.environ.get("CRAWLER_ALLOW_PRIVATE") == "1"  # never set in production


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def later(**kw) -> str:
    return (datetime.now(timezone.utc) + timedelta(**kw)).strftime("%Y-%m-%d %H:%M:%S")


# ---------- URLs ----------

def normalize_url(url: str) -> str | None:
    """Canonical form for dedup: lowercase scheme/host, default port and fragment dropped. Query is kept as-is."""
    if len(url) > 1000:  # shortcut: a btree unique index rejects keys over ~2.7KB; such URLs are junk, upgrade to a hash index if needed
        return None
    try:
        p = urlsplit(url.strip())
        host = idna(p.hostname or "")
        if p.scheme not in ("http", "https") or not host:
            return None
        if not re.fullmatch(r"[a-z0-9._-]+|[0-9a-f:.]+", host):  # junk like "a.com|b.com" or "*.x.com" (IPv6 is the second form)
            return None
        port = p.port
    except ValueError:
        return None
    netloc = host if port in (None, 80 if p.scheme == "http" else 443) else f"{host}:{port}"
    if ":" in host:  # IPv6 literal
        netloc = f"[{host}]" + netloc[len(host):]
    return urlunsplit((p.scheme, netloc, p.path or "/", p.query, ""))


def resolve(base: str, href: str) -> str | None:
    """Absolute http(s) URL for an href found on page `base` (fragment kept), else None."""
    href = href.strip()
    if not href or href.lower().startswith(("javascript:", "mailto:", "tel:", "data:")):
        return None
    try:
        url = urljoin(base, href)
    except ValueError:
        return None
    return url if normalize_url(url) else None


def idna(host: str) -> str:
    """Lowercase, no trailing dot, internationalized names as punycode (so bücher.de == xn--bcher-kva.de)."""
    host = host.lower().rstrip(".")
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return host


def hostname(url: str) -> str:
    try:
        return idna(urlsplit(url).hostname or "")
    except ValueError:
        return ""


def registrable_domain(host: str) -> str:
    """Public-Suffix-aware registrable domain (example.co.uk); the host itself for IPs / localhost."""
    host = host.lower().rstrip(".")
    r = _tld(host)
    return f"{r.domain}.{r.suffix}" if r.suffix else host


def host_matches(host: str, target: str) -> bool:
    """True if host is target or a subdomain of it. Never substring matching."""
    target, host = idna(target.lstrip(".")), idna(host)  # Unicode and punycode spellings of one name must match
    return host == target or host.endswith("." + target)


def clean_target(domain: str) -> str:
    """'https://www.Codigix.co/x' -> 'codigix.co'. Raises ValueError for anything that is not a hostname."""
    d = domain.strip().lower()
    d = urlsplit(d if "://" in d else "//" + d).hostname or ""  # bare domains too, incl. codigix.co:443 / codigix.co?x=1
    d = idna(d.removeprefix("www."))
    if not HOSTNAME.match(d):
        raise ValueError(f"invalid domain: {domain!r}")
    return d


class UnsafeURL(ValueError):
    """A request was refused before it was sent (SSRF guard). Permanent: retrying the same URL cannot make it safe."""


def blocked_reason(url: str) -> str | None:
    """Why `url` must not be fetched, or None if it is safe. Checked for the first request AND for every redirect hop, right
    before it is sent (the DNS answer is looked up at that moment, which narrows the DNS-rebinding window to the connect itself).
    Every resolved address must be globally routable: loopback, private, link-local, CGNAT, reserved, multicast, unspecified and
    IPv4-mapped IPv6 forms of those are refused."""
    if ALLOW_PRIVATE:
        return None
    p = urlsplit(url)
    host = hostname(url)
    if p.scheme not in ("http", "https") or not host:
        return "scheme not http(s) or no host"
    if p.username or p.password:
        return "credentials in URL"
    if host == "localhost" or host.endswith(".localhost"):
        return "localhost"
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError as e:
        return f"DNS failure: {e}"
    for info in infos:
        if not ip_allowed(info[4][0]):
            return f"{host} resolves to non-public address {info[4][0]}"
    return None


def ip_allowed(addr: str) -> bool:
    """True if `addr` is globally routable (the egress proxy applies this to the address it actually connects to)."""
    if ALLOW_PRIVATE:
        return True
    ip = ipaddress.ip_address(addr.split("%")[0])
    ip = getattr(ip, "ipv4_mapped", None) or ip
    return ip.is_global and not ip.is_multicast


def log_blocked(why: str) -> None:
    import logging
    logging.getLogger("crawler.discovery").warning("EGRESS BLOCKED | %s", why)


def is_safe_url(url: str) -> bool:
    return blocked_reason(url) is None


_TEST_IPS: dict[str, str] = {}  # test hook: logical *.localhost name -> its own loopback address (Windows cannot resolve *.localhost)


def test_ip(host: str) -> str:
    return _TEST_IPS.setdefault(host, f"127.0.{len(_TEST_IPS) // 250}.{len(_TEST_IPS) % 250 + 1}")


def test_route(url: str) -> tuple[str, dict[str, str]]:
    """Only with CRAWLER_ALLOW_PRIVATE=1 (tests): fetch http://x.localhost:P/.. from 127.0.x.y:P with the original Host header."""
    host = hostname(url)
    if not ALLOW_PRIVATE or not host.endswith(".localhost"):
        return url, {}
    ip = test_ip(host)
    p = urlsplit(url)
    return urlunsplit((p.scheme, f"{ip}:{p.port}" if p.port else ip, p.path, p.query, p.fragment)), {"Host": p.netloc}


def test_unroute(url: str) -> str:
    for host, ip in _TEST_IPS.items():
        if (urlsplit(url).hostname or "") == ip:
            p = urlsplit(url)
            return urlunsplit((p.scheme, f"{host}:{p.port}" if p.port else host, p.path, p.query, p.fragment))
    return url


def crawlable(url: str) -> bool:
    return not NON_HTML.match(url) and not UNSAFE_PATH.search(urlsplit(url).path)


# ---------- PostgreSQL ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS discovered_urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE, normalized_url TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL, hostname TEXT NOT NULL,
    discovered_from TEXT, discovery_method TEXT,
    first_seen TEXT DEFAULT {TS}, last_seen TEXT DEFAULT {TS},
    crawl_status TEXT DEFAULT 'pending', crawl_depth INTEGER DEFAULT 0,
    is_internal INTEGER DEFAULT 0, is_external INTEGER DEFAULT 0,
    http_status INTEGER, content_type TEXT, last_crawled TEXT, crawl_error TEXT,
    next_crawl_at TEXT, retry_count INTEGER DEFAULT 0, sitemap_lastmod TEXT, canonical_url TEXT,
    last_successful_crawl TEXT, content_hash TEXT, title TEXT, lease_owner TEXT, lease_expires TEXT, skip_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_discovered_domain ON discovered_urls(domain);
CREATE INDEX IF NOT EXISTS idx_discovered_status ON discovered_urls(crawl_status);
CREATE INDEX IF NOT EXISTS idx_discovered_dom_status ON discovered_urls(domain, crawl_status);
CREATE TABLE IF NOT EXISTS discovered_domains (
    id BIGSERIAL PRIMARY KEY, domain TEXT NOT NULL UNIQUE,
    first_seen TEXT DEFAULT {TS}, last_seen TEXT DEFAULT {TS},
    discovery_source TEXT, discovery_status TEXT DEFAULT 'discovered',
    pages_discovered INTEGER DEFAULT 0, last_crawled TEXT, priority INTEGER DEFAULT 5
);
CREATE TABLE IF NOT EXISTS outgoing_links (
    id BIGSERIAL PRIMARY KEY,
    source_url TEXT NOT NULL, destination_url TEXT NOT NULL, destination_domain TEXT,
    anchor_text TEXT NOT NULL DEFAULT '', rel_attributes TEXT, link_type TEXT DEFAULT 'html',
    first_seen TEXT DEFAULT {TS}, last_seen TEXT DEFAULT {TS},
    is_active INTEGER DEFAULT 1,
    is_nofollow INTEGER DEFAULT 0, is_sponsored INTEGER DEFAULT 0, is_ugc INTEGER DEFAULT 0,
    img_alt TEXT, position TEXT, context TEXT, source_domain TEXT, lost_at TEXT,
    UNIQUE(source_url, destination_url, anchor_text)
);
CREATE INDEX IF NOT EXISTS idx_outgoing_destination ON outgoing_links(destination_domain);
CREATE INDEX IF NOT EXISTS idx_outgoing_source ON outgoing_links(source_url);
CREATE TABLE IF NOT EXISTS target_domains (
    id BIGSERIAL PRIMARY KEY, domain TEXT NOT NULL UNIQUE,
    created_at TEXT DEFAULT {TS}, is_active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS backlinks (
    id BIGSERIAL PRIMARY KEY,
    target_domain_id INTEGER NOT NULL, source_url TEXT NOT NULL, destination_url TEXT NOT NULL,
    anchor_text TEXT, rel_attributes TEXT, link_status TEXT DEFAULT 'discovered',
    first_seen TEXT DEFAULT {TS}, last_verified TEXT, is_active INTEGER DEFAULT 1,
    discovery_method TEXT DEFAULT 'forward_crawl',
    FOREIGN KEY(target_domain_id) REFERENCES target_domains(id),
    UNIQUE(target_domain_id, source_url, destination_url)
);
CREATE TABLE IF NOT EXISTS crawl_history (
    id BIGSERIAL PRIMARY KEY, source_url TEXT NOT NULL,
    crawl_started TEXT, crawl_completed TEXT, http_status INTEGER,
    links_found INTEGER DEFAULT 0, new_links_found INTEGER DEFAULT 0, error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_crawl_history_url ON crawl_history(source_url);
CREATE TABLE IF NOT EXISTS backlink_history (
    id BIGSERIAL PRIMARY KEY, backlink_id INTEGER NOT NULL, event_type TEXT NOT NULL,
    detected_at TEXT DEFAULT {TS}, details TEXT,
    FOREIGN KEY(backlink_id) REFERENCES backlinks(id)
);
CREATE TABLE IF NOT EXISTS discovery_sources (
    name TEXT PRIMARY KEY, enabled INTEGER DEFAULT 1, status TEXT DEFAULT 'idle', cursor TEXT,
    items_imported INTEGER DEFAULT 0, runs INTEGER DEFAULT 0, last_run TEXT, last_error TEXT
);
CREATE TABLE IF NOT EXISTS engine_state (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS worker_status (
    name TEXT PRIMARY KEY, status TEXT, heartbeat TEXT, runs INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, last_error TEXT,
    last_heartbeat_at TEXT, last_success_at TEXT, restart_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS discovery_jobs (
    id BIGSERIAL PRIMARY KEY, domain_id INTEGER NOT NULL, source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, last_error TEXT,
    created_at TEXT DEFAULT {TS}, updated_at TEXT DEFAULT {TS},
    UNIQUE(domain_id, source)
);
CREATE INDEX IF NOT EXISTS idx_discovery_jobs_pending ON discovery_jobs(source, status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_discovered_status_next ON discovered_urls(crawl_status, next_crawl_at);
CREATE INDEX IF NOT EXISTS idx_discovered_status_depth ON discovered_urls(crawl_status, crawl_depth);
CREATE TABLE IF NOT EXISTS crawl_jobs (
    job_id TEXT PRIMARY KEY, target_domain TEXT, status TEXT, config TEXT, error TEXT,
    pages_crawled INTEGER DEFAULT 0, pages_failed INTEGER DEFAULT 0, domains_admitted INTEGER DEFAULT 0,
    created_at TEXT, finished_at TEXT
);
""".replace("{TS}", TS)


class Row(dict):
    """Result row: access by column name or position, like sqlite3.Row."""
    def __getitem__(self, k):
        return list(self.values())[k] if isinstance(k, int) else super().__getitem__(k)


def _row_factory(cur):
    names = [c.name for c in cur.description or []]
    return lambda vals: Row(zip(names, vals))


def _sql(q: str, params) -> str:
    """The code base writes `?` placeholders and INSERT OR IGNORE; psycopg wants %s and ON CONFLICT."""
    if params is not None:  # without params psycopg leaves % alone, so only escape when it will be formatted
        q = q.replace("%", "%%").replace("?", "%s")
    if re.match(r"\s*INSERT OR IGNORE", q, re.I):
        q = re.sub(r"INSERT OR IGNORE", "INSERT", q, count=1, flags=re.I) + " ON CONFLICT DO NOTHING"
    return q


class Cursor(psycopg.Cursor):
    def execute(self, query, params=None, **kw):
        return super().execute(_sql(query, params), params, **kw)

    def executemany(self, query, params_seq, **kw):
        return super().executemany(_sql(query, ()), params_seq, **kw)


OPEN_CONNS = 0  # connections this process has checked out of the pool or opened unpooled right now (monitoring)
_count_lock = threading.Lock()

# Connection budget. DB_MAX_CONNECTIONS is the budget of the WHOLE deployment (API + engine + every Celery worker process + the dedicated
# unpooled connections), NOT a per-process cap: each process gets DB_MAX_CONNECTIONS // DB_APP_PROCESSES unless DB_POOL_MAX says otherwise.
# Keep  sum(pool max of every process) + one unpooled connection per engine  below PostgreSQL's max_connections minus
# superuser_reserved_connections and whatever else connects (psql, backups, monitoring). Pools are per process and never shared across fork().
DB_MAX_CONN = int(os.environ.get("DB_MAX_CONNECTIONS", 60))
DB_APP_PROCESSES = max(1, int(os.environ.get("DB_APP_PROCESSES", 6)))  # API 1 + engine 1 + Celery worker processes (4): how many processes share the budget
POOL_FLOOR = 6  # a process needs a few connections at once (a claim holds one while its frontier query takes a second), whatever the budget says
POOL_MAX = int(os.environ.get("DB_POOL_MAX") or max(POOL_FLOOR, DB_MAX_CONN // DB_APP_PROCESSES))
POOL_MIN = min(int(os.environ.get("DB_POOL_MIN", 1)), POOL_MAX)
class DatabaseUnavailable(TimeoutError):
    """No pooled connection within the wait (database down, overloaded, or the pool exhausted). A TimeoutError, so existing handlers still match."""


_WAIT = contextvars.ContextVar("db_wait", default=None)  # per-request override of DB_POOL_WAIT: the API sets it, workers and the engine never do
DB_POOL_WAIT = float(os.environ.get("DB_POOL_WAIT", 30))  # a caller waits this long for a free connection, then fails loudly with TimeoutError
POOL_MAX_IDLE = float(os.environ.get("DB_POOL_MAX_IDLE", 300))  # connections above POOL_MIN that stay unused this long are closed
POOL_MAX_LIFETIME = float(os.environ.get("DB_POOL_MAX_LIFETIME", 1800))  # a connection is replaced after this long (tunnels / proxies drop old ones)
POOL_CHECK_IDLE = float(os.environ.get("DB_POOL_CHECK_IDLE", 30))  # a connection idle longer than this is tested (one round trip) when it is handed out
logging.getLogger("psycopg.pool").setLevel(logging.WARNING)  # the pool logs every checkout at INFO


class Connection(psycopg.Connection):
    def executemany(self, query, params_seq):  # sqlite3 had it on the connection, psycopg only on cursors
        cur = self.cursor()
        cur.executemany(query, params_seq)
        return cur


def _opened(n: int) -> None:
    global OPEN_CONNS
    with _count_lock:
        OPEN_CONNS += n


class Unpooled(Connection):
    """A connection opened outside the pool (connect_unpooled). Closing it really closes it."""
    def close(self):
        if not self.closed:
            _opened(-1)
        super().close()


def _options() -> str:
    # Server-side limits turn "waits forever" into a loud error: a lock wait, a statement or an abandoned transaction ends after 60s / 5min.
    options = "-c lock_timeout=60000 -c statement_timeout=300000 -c idle_in_transaction_session_timeout=300000"
    return options + (f" -c search_path={DB_SCHEMA}" if DB_SCHEMA else "")  # DB_SCHEMA isolates tests in their own schema


def _conn_kwargs() -> dict:
    return dict(cursor_factory=Cursor, row_factory=_row_factory, application_name="backlink-crawler", connect_timeout=10, options=_options())


def connect_unpooled() -> Connection:
    """A dedicated connection that is never part of the pool, closed (really) by the caller. For connections whose SESSION state matters:
    a session-level advisory lock (autonomous.try_scheduler_lock) lives and dies with its connection, so that connection must never be handed to
    another caller. Counts against the connection budget (one per engine process), not against the pool's maximum."""
    conn = Unpooled.connect(DATABASE_URL, **_conn_kwargs())
    _opened(1)
    return conn


_pool: ConnectionPool | None = None
_pool_key: tuple | None = None
_pool_lock = threading.RLock()
_inherited_pools: list = []  # pools a forked child inherited: their sockets belong to the parent, so they are kept alive here, never closed or collected


def _check(conn) -> None:
    """Called by the pool whenever it hands a connection out: only a connection that sat idle for a while is tested (one cheap round trip)."""
    returned = getattr(conn, "_returned_at", None)
    if returned is not None and time.monotonic() - returned > POOL_CHECK_IDLE:
        ConnectionPool.check_connection(conn)


def _get_pool() -> ConnectionPool:
    """The process's pool, created on first use (never at import: a process that forks first must not own one). It is re-created when the
    connection settings change (tests switch DB_SCHEMA per test); the old one is closed, connections still out of it are closed on return."""
    global _pool, _pool_key
    key = (DATABASE_URL, DB_SCHEMA, POOL_MIN, POOL_MAX, POOL_MAX_IDLE, POOL_MAX_LIFETIME)
    pool = _pool
    if pool is not None and _pool_key == key and not pool.closed:
        return pool
    with _pool_lock:
        if _pool is not None and (_pool_key != key or _pool.closed):
            old, _pool, _pool_key = _pool, None, None
            old.close()
        if _pool is None:
            if POOL_MAX * DB_APP_PROCESSES > DB_MAX_CONN:
                logging.getLogger("crawler.db").warning("DB pool: %d connections x %d processes exceeds the DB_MAX_CONNECTIONS budget of %d", POOL_MAX, DB_APP_PROCESSES, DB_MAX_CONN)
            _pool = ConnectionPool(DATABASE_URL, connection_class=Connection, kwargs=_conn_kwargs() | {"prepare_threshold": None},
                                   min_size=POOL_MIN, max_size=POOL_MAX, timeout=DB_POOL_WAIT, max_idle=POOL_MAX_IDLE, max_lifetime=POOL_MAX_LIFETIME,
                                   check=_check, name="backlink-db", open=False)
            _pool.open(wait=False)  # prepare_threshold=None: no server-side prepared statements, so a long-lived connection never meets "cached plan must not change result type" after a migration
            _pool_key = key
            logging.getLogger("crawler.db").info("DB pool opened: min=%d max=%d wait=%.0fs (budget %d over %d processes)", POOL_MIN, POOL_MAX, DB_POOL_WAIT, DB_MAX_CONN, DB_APP_PROCESSES)
        return _pool


def close_pool(timeout: float = 10.0) -> None:
    """Close this process's pool (idempotent): idle connections are closed now, a connection still checked out is closed when it is returned.
    Called at interpreter exit, by the API's shutdown and by Celery's worker shutdown; the next connect() simply opens a new pool."""
    global _pool, _pool_key
    with _pool_lock:
        pool, _pool, _pool_key = _pool, None, None
    if pool is not None:
        pool.close(timeout)


def _forget_pool_after_fork() -> None:
    global _pool, _pool_key, _pool_lock, _count_lock, OPEN_CONNS
    if _pool is not None:
        _inherited_pools.append(_pool)
    _pool = _pool_key = None
    _pool_lock, _count_lock, OPEN_CONNS = threading.RLock(), threading.Lock(), 0  # a parent thread may have held a lock at the moment of fork


atexit.register(close_pool)
if hasattr(os, "register_at_fork"):  # Celery's prefork pool: each child builds its own pool on first use
    os.register_at_fork(after_in_child=_forget_pool_after_fork)


def pool_stats() -> dict:
    """Pool counters for diagnostics and measurements (requests, wait time, connection setup time, usage time, size, idle)."""
    pool = _pool
    base = {"pool_max": POOL_MAX, "pool_min": POOL_MIN, "checked_out": OPEN_CONNS}
    return base | (pool.get_stats() if pool is not None and not pool.closed else {})


class Lease:
    """One checkout from the pool. `with connect() as c:` yields the connection and, on exit, commits (rolls back on an exception) and returns it
    to the pool; used without `with`, it behaves like the connection and close() returns it. Releasing is idempotent per lease, so a repeated
    close() can never return a connection that has meanwhile been handed to somebody else, and a lease that is dropped without close() is
    returned when it is garbage collected."""
    __slots__ = ("_conn", "_pool", "_open")

    def __init__(self, conn, pool):
        self._conn, self._pool, self._open = conn, pool, True

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._open and not self._conn.closed:
                if exc_type:
                    self._rollback()
                else:
                    try:
                        self._conn.commit()
                    except BaseException:
                        self._rollback()
                        raise
        finally:
            self.release()

    def _rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception:  # noqa: BLE001 - a broken connection is discarded by the pool when it is returned
            pass

    def release(self) -> None:
        if not self._open:
            return
        self._open = False
        conn = self._conn
        conn.cursor_factory = Cursor  # api_timing swaps it for one checkout
        conn._returned_at = time.monotonic()
        _opened(-1)
        self._pool.putconn(conn)  # rolls back an open transaction, replaces a broken or expired connection

    def close(self) -> None:
        if self._open:
            self._rollback()  # like closing a plain connection: uncommitted work is discarded
            self.release()

    @property
    def closed(self) -> bool:
        return not self._open or self._conn.closed

    @property
    def cursor_factory(self):  # api_timing swaps it for one checkout; release() puts the plain Cursor back
        return self._conn.cursor_factory

    @cursor_factory.setter
    def cursor_factory(self, factory) -> None:
        self._conn.cursor_factory = factory

    def __getattr__(self, name):  # only reached for names Lease does not define itself
        if not self._open:
            raise psycopg.InterfaceError("the connection was already returned to the pool")
        return getattr(self._conn, name)

    def __del__(self):
        try:
            self.close()
        except Exception:  # noqa: BLE001 - interpreter shutdown
            pass


def connect() -> Lease:
    """A pooled connection (committed and returned by `with`). Waits up to DB_POOL_WAIT for a free one, then raises
    TimeoutError (database connection limit reached) instead of hanging or exhausting the server. DB_SCHEMA isolates tests in their own schema."""
    wait = _WAIT.get() or DB_POOL_WAIT
    for attempt in (0, 1):
        pool = _get_pool()
        try:
            conn = pool.getconn(timeout=wait)
        except PoolClosed:  # another thread replaced the pool (settings changed): use the new one
            if attempt:
                raise
            continue
        except PoolTimeout as e:
            raise DatabaseUnavailable(f"database connection limit reached ({pool.max_size} max per process, waited {wait}s): leak, overload or database unreachable") from e
        break
    _opened(1)
    return Lease(conn, pool)


BUSY_ERRORS = 0  # transient lock/deadlock errors seen by retry_locked in this process (monitoring)
TRANSIENT = (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure, psycopg.errors.LockNotAvailable)


def retry_locked(fn, tries: int = 4):
    """Run fn, retrying deadlocks/lock timeouts with bounded jittered backoff. Anything else (or the last failure) is raised."""
    global BUSY_ERRORS
    for i in range(tries):
        try:
            return fn()
        except TRANSIENT as e:
            BUSY_ERRORS += 1
            name = getattr(fn, "__qualname__", "operation")
            if i == tries - 1:
                logging.getLogger("crawler.db").error("%s: %s still failing after %d attempts, giving up: %s", name, type(e).__name__, tries, e)
                raise
            logging.getLogger("crawler.db").warning("%s: %s (attempt %d/%d), transaction rolled back, retrying", name, type(e).__name__, i + 1, tries)
            time.sleep(0.2 * 2 ** i * random.uniform(0.5, 1.5))


# Versioned migrations. Each version runs in its own transaction and is recorded in schema_migrations, so a failure leaves every
# earlier version applied and the failing one untouched. Append a new version; never edit an applied one. All statements are
# idempotent (IF NOT EXISTS), so a database created by an older build, or by the SQLite migration script, is upgraded in place.
_V1_COLUMNS = [
    ("discovered_urls", "lease_owner TEXT"), ("discovered_urls", "lease_expires TEXT"), ("discovered_urls", "skip_reason TEXT"),
    ("discovered_urls", "canonical_url TEXT"), ("discovered_urls", "last_successful_crawl TEXT"), ("discovered_urls", "content_hash TEXT"),
    ("discovered_urls", "title TEXT"), ("discovered_urls", "sitemap_lastmod TEXT"),
    ("outgoing_links", "img_alt TEXT"), ("outgoing_links", "position TEXT"), ("outgoing_links", "context TEXT"),
    ("outgoing_links", "source_domain TEXT"), ("outgoing_links", "lost_at TEXT"),
    ("worker_status", "last_heartbeat_at TEXT"), ("worker_status", "last_success_at TEXT"),
    ("worker_status", "restart_count INTEGER DEFAULT 0"),
]
MIGRATIONS: list[tuple[int, str, list[str]]] = [
    (1, "baseline schema and columns", [SCHEMA] + [f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c}" for t, c in _V1_COLUMNS]
     + ["CREATE INDEX IF NOT EXISTS idx_outgoing_source_domain ON outgoing_links(source_domain)"]),  # after the ALTERs: old tables lack the column
    (2, "link history, Common Crawl job cursor, API indexes", [
        "CREATE TABLE IF NOT EXISTS link_history (id BIGSERIAL PRIMARY KEY, source_url TEXT NOT NULL, destination_url TEXT NOT NULL,"
        " destination_domain TEXT, anchor_text TEXT NOT NULL DEFAULT '', event_type TEXT NOT NULL, detected_at TEXT DEFAULT " + TS + ")",
        "CREATE INDEX IF NOT EXISTS idx_link_history_dest ON link_history(destination_domain, detected_at)",
        "ALTER TABLE discovery_jobs ADD COLUMN IF NOT EXISTS page_cursor INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE discovery_jobs ADD COLUMN IF NOT EXISTS total_pages INTEGER",
        "ALTER TABLE discovery_jobs ADD COLUMN IF NOT EXISTS index_id TEXT",
        "CREATE INDEX IF NOT EXISTS idx_outgoing_dest_active ON outgoing_links(destination_domain, link_type, is_active)",
        "CREATE INDEX IF NOT EXISTS idx_backlink_history_bl ON backlink_history(backlink_id)",
        "CREATE INDEX IF NOT EXISTS idx_backlinks_source ON backlinks(source_url)",
        "CREATE INDEX IF NOT EXISTS idx_discovered_lease ON discovered_urls(lease_owner) WHERE lease_owner IS NOT NULL",
    ]),
    # 3 and 4 are taken by the 2-common_crawl branch in databases migrated from it: never reuse a version number across branches
    (5, "business onboarding jobs, per-job URL association and depth, lease fencing token", [
        # one row per onboarded business; the partial unique index makes a second onboarding of the same domain idempotent
        "CREATE TABLE IF NOT EXISTS business_jobs (id BIGSERIAL PRIMARY KEY, target_domain TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,"
        " status TEXT NOT NULL DEFAULT 'pending', max_depth INTEGER NOT NULL, max_pages INTEGER NOT NULL, search_results INTEGER NOT NULL DEFAULT 0,"
        " pages_discovered INTEGER NOT NULL DEFAULT 0, pages_crawled INTEGER NOT NULL DEFAULT 0, pages_failed INTEGER NOT NULL DEFAULT 0,"
        " backlinks_found INTEGER NOT NULL DEFAULT 0, retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, last_error TEXT,"
        " created_at TEXT DEFAULT " + TS + ", updated_at TEXT DEFAULT " + TS + ", finished_at TEXT)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_business_jobs_active ON business_jobs(target_domain) WHERE status IN ('pending','searching','crawling')",
        "CREATE INDEX IF NOT EXISTS idx_business_jobs_status ON business_jobs(status, priority, id)",
        # a URL may belong to many jobs; depth is per association, never a property of the shared URL row
        "CREATE TABLE IF NOT EXISTS job_urls (job_id BIGINT NOT NULL REFERENCES business_jobs(id) ON DELETE CASCADE,"
        " url_id BIGINT NOT NULL REFERENCES discovered_urls(id) ON DELETE CASCADE, depth INTEGER NOT NULL, expanded INTEGER NOT NULL DEFAULT 0,"
        " PRIMARY KEY (job_id, url_id))",
        "CREATE INDEX IF NOT EXISTS idx_job_urls_url ON job_urls(url_id)",
        "ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS lease_gen INTEGER NOT NULL DEFAULT 0",  # bumped by every claim: the fencing token
    ]),
    (6, "per-business recurring discovery schedule, run provenance", [
        # one row per onboarded business; the engine turns a due next_run_at into a (P2) business_jobs row
        "CREATE TABLE IF NOT EXISTS businesses (domain TEXT PRIMARY KEY, business_name TEXT, search_interval_seconds INTEGER NOT NULL,"
        " active INTEGER NOT NULL DEFAULT 1, next_run_at TEXT NOT NULL, last_run_at TEXT, last_success_at TEXT, created_at TEXT DEFAULT " + TS + ")",
        "CREATE INDEX IF NOT EXISTS idx_businesses_due ON businesses(next_run_at) WHERE active=1",
        "ALTER TABLE business_jobs ADD COLUMN IF NOT EXISTS duplicate_candidates INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE business_jobs ADD COLUMN IF NOT EXISTS started_at TEXT",
    ]),
    (7, "competitors: owner relation and target classification", [
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS target_type TEXT NOT NULL DEFAULT 'own_business'",  # own_business | competitor
        "CREATE TABLE IF NOT EXISTS business_competitors (id BIGSERIAL PRIMARY KEY, business_domain TEXT NOT NULL, competitor_domain TEXT NOT NULL,"
        " competitor_name TEXT, active INTEGER NOT NULL DEFAULT 1, created_at TEXT DEFAULT " + TS + ", updated_at TEXT DEFAULT " + TS + ","
        " UNIQUE (business_domain, competitor_domain))",
        "CREATE INDEX IF NOT EXISTS idx_business_competitors_target ON business_competitors(competitor_domain)",
    ]),
    (8, "worker_status: cycle outcomes (completed / completed_with_errors / failed / skipped)", [
        # last_success_at keeps meaning "a cycle with NO failed operation"; these say what the other cycles were
        "ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS last_cycle_at TEXT",
        "ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS last_cycle_status TEXT",
        "ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS last_partial_at TEXT",
        "ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS last_failed_at TEXT",
        "ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS last_cycle_detail TEXT",
    ]),
    (9, "business_jobs.search_gen: fencing token of a DuckDuckGo search", [
        # bumped by every claim of the search phase: a result from a search that was reset/re-claimed meanwhile (engine restart, second engine) is discarded
        "ALTER TABLE business_jobs ADD COLUMN IF NOT EXISTS search_gen INTEGER NOT NULL DEFAULT 0",
    ]),
    # 10 and 11 exist in databases migrated by other work (search_lease_expires, scheduler indexes): never reuse them
    (12, "continuous discovery: per-query pagination cursors, URL discovery provenance, fair-search order", [
        # one row per (target, query variation, backend): a backend's page offset is never reused by another backend
        "CREATE TABLE IF NOT EXISTS discovery_cursors (domain TEXT NOT NULL, query_hash TEXT NOT NULL, backend TEXT NOT NULL, query TEXT NOT NULL,"
        " next_page INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'active', dup_streak INTEGER NOT NULL DEFAULT 0,"
        " pages_done INTEGER NOT NULL DEFAULT 0, urls_found INTEGER NOT NULL DEFAULT 0, exhausted_at TEXT, rediscover_after TEXT, last_ok_at TEXT,"
        " PRIMARY KEY (domain, query_hash, backend))",
        # which target found which URL through which query: many targets and many queries per URL, never lost to global URL dedup
        "CREATE TABLE IF NOT EXISTS url_discoveries (url_id BIGINT NOT NULL REFERENCES discovered_urls(id) ON DELETE CASCADE, target_domain TEXT NOT NULL,"
        " query_hash TEXT NOT NULL, found_at TEXT DEFAULT " + TS + ", PRIMARY KEY (url_id, target_domain, query_hash))",
        "CREATE INDEX IF NOT EXISTS idx_url_discoveries_target ON url_discoveries(target_domain)",
        "CREATE INDEX IF NOT EXISTS idx_business_jobs_target ON business_jobs(target_domain, id)",  # outstanding-URL cap and latest-job lookups
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS last_search_at TEXT",  # set on claim/attempt, failures included: the least recently searched goes first
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS discovery_state TEXT NOT NULL DEFAULT 'active'",  # active|deferred_backlog|waiting_rediscovery|failed
        "CREATE INDEX IF NOT EXISTS idx_businesses_fair ON businesses(last_search_at NULLS FIRST, next_run_at) WHERE active=1",
        # candidates found before cursors existed count as already discovered by that target
        "INSERT INTO url_discoveries (url_id, target_domain, query_hash) SELECT DISTINCT ju.url_id, j.target_domain, 'legacy' FROM job_urls ju"
        " JOIN business_jobs j ON j.id=ju.job_id WHERE ju.depth=0 ON CONFLICT DO NOTHING",
    ]),
    (13, "hierarchical search fairness: the customer business a competitor is searched on behalf of; empty-page confirmation", [
        # level 1 of the rotation. NULL for a customer business (it is its own group); a shared competitor has ONE row and ONE search stream,
        # grouped under the lowest-sorting active owner
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS parent_domain TEXT",
        "UPDATE businesses b SET parent_domain=(SELECT MIN(c.business_domain) FROM business_competitors c WHERE c.competitor_domain=b.domain AND c.active=1)"
        " WHERE b.target_type='competitor' AND b.parent_domain IS NULL",
        "CREATE INDEX IF NOT EXISTS idx_businesses_parent ON businesses(parent_domain) WHERE parent_domain IS NOT NULL",
        # runs in a row that started on an empty page: a blocked client looks like the end of the results, so one such run does not exhaust a cursor
        "ALTER TABLE discovery_cursors ADD COLUMN IF NOT EXISTS empty_streak INTEGER NOT NULL DEFAULT 0",
    ]),
    (14, "discovery_cursors.head_checked_at: when pages 1-2 of a deep cursor were last re-read", [
        "ALTER TABLE discovery_cursors ADD COLUMN IF NOT EXISTS head_checked_at TEXT",
    ]),
    (15, "businesses.rediscovery_days: optional per-target override of DDGS_REDISCOVERY_DAYS", [
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS rediscovery_days REAL",  # NULL = the global default; set for no target by this migration
    ]),
]
SCHEMA_VERSION = MIGRATIONS[-1][0]


def migrate(conn, recover: bool = True) -> None:
    """Idempotent: applies pending schema versions, optionally recovers work a dead process left in flight."""
    conn.execute("SELECT pg_advisory_xact_lock(724001)")  # concurrent migrations from two processes would collide
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT, applied_at TEXT DEFAULT " + TS + ")")
    conn.commit()
    for version, name, statements in MIGRATIONS:
        conn.execute("SELECT pg_advisory_xact_lock(724001)")
        if conn.execute("SELECT 1 FROM schema_migrations WHERE version=?", (version,)).fetchone():
            conn.commit()
            continue
        try:
            for stmt in statements:
                conn.execute(stmt)
            conn.execute("INSERT INTO schema_migrations (version, name) VALUES (?,?)", (version, name))
            conn.commit()
        except BaseException:
            conn.rollback()  # this version is all-or-nothing; earlier versions stay applied
            raise
    conn.execute("SELECT pg_advisory_xact_lock(724001)")
    if recover:  # only the process that runs crawls may do this; the API must not reset a separate worker's in-flight work
        conn.execute("UPDATE discovered_urls SET crawl_status='pending', lease_owner=NULL, lease_expires=NULL WHERE crawl_status='crawling'")
        # interrupted recrawls go back to the schedule, never to 'pending' (they were crawled before) or 'completed' blindly
        conn.execute("UPDATE discovered_urls SET lease_owner=NULL, lease_expires=NULL, crawl_status=CASE WHEN last_successful_crawl IS NOT NULL"
                     " THEN 'completed' ELSE 'pending' END WHERE crawl_status IN ('recrawling','recrawl_due')")
        conn.execute("UPDATE discovery_jobs SET status='retry_pending' WHERE status='processing'")
        conn.execute("UPDATE crawl_jobs SET status='interrupted' WHERE status='running'")
    conn.commit()


def schema_problems(conn) -> list[str]:
    """What an up-to-date database must have and this one lacks (empty list = healthy). Used by tests and diagnostics."""
    have = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")} if conn.execute(
        "SELECT to_regclass('schema_migrations')").fetchone()[0] else set()
    out = [f"migration {v} not applied" for v, _, _ in MIGRATIONS if v not in have]
    cols = {(r[0], r[1]) for r in conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema=current_schema()")}
    for t, c in [(t, c.split()[0]) for t, c in _V1_COLUMNS] + [("discovery_jobs", "page_cursor"), ("discovery_jobs", "index_id")]:
        if (t, c) not in cols:
            out.append(f"missing column {t}.{c}")
    idx = {r[0] for r in conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname=current_schema()")}
    out += [f"missing index {i}" for i in ("idx_outgoing_dest_active", "idx_link_history_dest", "idx_backlink_history_bl") if i not in idx]
    unique = [r[0] for r in conn.execute("SELECT tablename||' '||indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexdef LIKE 'CREATE UNIQUE%'")]
    for table, columns in (("backlinks", "(target_domain_id, source_url, destination_url)"), ("outgoing_links", "(source_url, destination_url, anchor_text)"),
                           ("discovered_urls", "(normalized_url)"), ("discovered_domains", "(domain)"), ("discovery_jobs", "(domain_id, source)")):
        if not any(u.startswith(table + " ") and columns in u for u in unique):
            out.append(f"missing unique constraint {table}{columns} (existing duplicates must be removed by hand first)")
    fks = [r[0] for r in conn.execute("SELECT conrelid::regclass::text FROM pg_constraint WHERE contype='f' AND connamespace=current_schema()::regnamespace")]
    out += [f"missing foreign key on {t}" for t in ("backlinks", "backlink_history") if t not in fks]
    return out


# ---------- graph writes ----------

def save_url(conn, url: str, from_url: str | None, method: str, depth: int, internal: bool, status: str = "pending",
             lastmod: str | None = None, reason: str | None = None, touch_domain: bool = True) -> bool:
    """Record a discovered URL (never deletes). Returns True if it was new. process_page passes touch_domain=False and upserts each
    domain once per page instead (see upsert_domain), which keeps hot domain rows out of the per-link loop."""
    norm = normalize_url(url)
    if norm is None:
        return False
    host = hostname(norm)
    dom = registrable_domain(host)
    new = conn.execute(
        "INSERT OR IGNORE INTO discovered_urls (url, normalized_url, domain, hostname, discovered_from, discovery_method,"
        " crawl_depth, is_internal, is_external, crawl_status, sitemap_lastmod, skip_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (url, norm, dom, host, from_url, method, depth, int(internal), int(not internal), status, lastmod, reason if status == "skipped" else None),
    ).rowcount == 1
    if not new:
        # A URL skipped only for max_depth comes back as pending when it is found again within reach. robots / security / not_html skips stay.
        react = "(crawl_status='skipped' AND skip_reason='max_depth' AND ?::text='pending' AND ?<=crawl_depth)"
        conn.execute(f"UPDATE discovered_urls SET last_seen=?, crawl_status=CASE WHEN {react} THEN 'pending' ELSE crawl_status END,"
                     f" skip_reason=CASE WHEN {react} THEN NULL ELSE skip_reason END, crawl_depth=LEAST(crawl_depth, ?),"
                     " sitemap_lastmod=COALESCE(?, sitemap_lastmod) WHERE normalized_url=?",
                     (now(), status, depth, status, depth, depth, lastmod, norm))
    if touch_domain:
        upsert_domain(conn, dom, from_url or method, int(new))
    return new


def upsert_domain(conn, dom: str, source: str, new_pages: int = 0) -> None:
    conn.execute("INSERT INTO discovered_domains (domain, discovery_source, pages_discovered, last_seen) VALUES (?,?,?,?)"
                 " ON CONFLICT (domain) DO UPDATE SET last_seen=EXCLUDED.last_seen,"
                 " pages_discovered=discovered_domains.pages_discovered+EXCLUDED.pages_discovered", (dom, source, new_pages, now()))


def active_targets(conn) -> list[tuple[int, str]]:
    return [(r["id"], r["domain"]) for r in conn.execute("SELECT id, domain FROM target_domains WHERE is_active=1")]


def add_target(conn, domain: str) -> int:
    domain = clean_target(domain)
    conn.execute("INSERT OR IGNORE INTO target_domains (domain) VALUES (?)", (domain,))
    conn.execute("UPDATE target_domains SET is_active=1 WHERE domain=?", (domain,))
    return conn.execute("SELECT id FROM target_domains WHERE domain=?", (domain,)).fetchone()["id"]


def event(conn, backlink_id: int, kind: str, details: str | None = None) -> None:
    conn.execute("INSERT INTO backlink_history (backlink_id, event_type, details) VALUES (?,?,?)", (backlink_id, kind, details))


def record_backlink(conn, target_id: int, source: str, dest: str, anchor: str, rel: str | None, method: str = "forward_crawl",
                    status: str = "verified") -> None:
    """Create or refresh a backlink and log history. Only called after a live fetch of the source page saw the link."""
    row = conn.execute("SELECT id, is_active, link_status, anchor_text, rel_attributes FROM backlinks WHERE target_domain_id=? AND source_url=? AND destination_url=?",
                       (target_id, source, dest)).fetchone()
    stamp = now() if status == "verified" else None
    if row is None:
        bid = conn.execute(
            "INSERT INTO backlinks (target_domain_id, source_url, destination_url, anchor_text, rel_attributes,"
            " link_status, last_verified, discovery_method) VALUES (?,?,?,?,?,?,?,?) RETURNING id",
            (target_id, source, dest, anchor, rel, status, stamp, method)).fetchone()["id"]
        event(conn, bid, "first_discovered", method)
        if status == "verified":
            event(conn, bid, "verified")
        return
    if not row["is_active"] or row["link_status"] == "suspected_removed":
        event(conn, row["id"], "reappeared")
    elif (row["anchor_text"], row["rel_attributes"]) != (anchor, rel):
        event(conn, row["id"], "link_changed", f"anchor {row['anchor_text']!r}->{anchor!r}; rel {row['rel_attributes']!r}->{rel!r}")
    elif row["link_status"] != "verified":
        event(conn, row["id"], "verified")
    conn.execute("UPDATE backlinks SET is_active=1, link_status='verified', last_verified=?, anchor_text=?, rel_attributes=?,"
                 " discovery_method=CASE WHEN link_status='verified' THEN discovery_method ELSE ? END WHERE id=?",
                 (stamp, anchor, rel, method, row["id"]))


def match_target_from_graph(conn, target: str) -> int:
    """Backfill backlinks for a target from links already in the graph. Returns rows examined that matched."""
    target_id = add_target(conn, target)
    target = clean_target(target)
    n = 0
    for r in conn.execute("SELECT source_url, destination_url, anchor_text, rel_attributes FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1",
                          (registrable_domain(target),)).fetchall():
        if host_matches(hostname(r["destination_url"]), target) and not host_matches(hostname(r["source_url"]), target):
            record_backlink(conn, target_id, r["source_url"], r["destination_url"], r["anchor_text"], r["rel_attributes"])
            n += 1
    conn.commit()
    return n


# ---------- sitemaps ----------

def _locs(xml: bytes) -> tuple[bool, list[tuple[str, str | None]]]:
    root = ET.fromstring(xml)
    ns = lambda e: e.tag.rsplit("}", 1)[-1]  # noqa: E731 - tag without namespace
    items = []
    for el in root:
        loc = next((c.text.strip() for c in el if ns(c) == "loc" and c.text), None)
        mod = next((c.text.strip() for c in el if ns(c) == "lastmod" and c.text), None)
        if loc:
            items.append((loc, mod))
    return ns(root) == "sitemapindex", items


async def discover_sitemaps(client: httpx.AsyncClient, base_url: str, max_urls: int = 5000) -> list[tuple[str, str | None]]:
    """robots.txt Sitemap: lines plus /sitemap.xml and /sitemap_index.xml; follows nested indexes. -> [(url, lastmod)]."""
    origin = "{0.scheme}://{0.netloc}".format(urlsplit(base_url))
    todo = [urljoin(origin, "/sitemap.xml"), urljoin(origin, "/sitemap_index.xml")]
    try:
        robots = await client.get(urljoin(origin, "/robots.txt"))
        if robots.status_code == 200:
            todo = re.findall(r"(?im)^\s*sitemap:\s*(\S+)", robots.text) + todo
    except httpx.HTTPError:
        pass
    seen, urls = set(), {}
    while todo and len(urls) < max_urls and len(seen) < 50:  # 50 sitemap files per domain: runaway guard
        sm = todo.pop(0)
        if sm in seen or not is_safe_url(sm) or not host_matches(hostname(sm), registrable_domain(hostname(origin))):
            continue
        seen.add(sm)
        try:
            r = await client.get(sm)
            if r.status_code != 200 or len(r.content) > 20_000_000:
                continue
            body = r.content
            if sm.endswith(".gz") or body[:2] == b"\x1f\x8b":
                import gzip
                body = gzip.decompress(body)
            is_index, items = _locs(body)
        except (httpx.HTTPError, ET.ParseError, OSError, ValueError):
            continue
        if is_index:
            todo += [u for u, _ in items]
        else:
            for u, mod in items:
                urls.setdefault(u, mod)
    return list(urls.items())[:max_urls]

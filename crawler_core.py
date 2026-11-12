"""Shared building blocks for the forward crawler: URL handling, SSRF guard, PostgreSQL store, matching, sitemaps.

The PostgreSQL database (DATABASE_URL) is the permanent link graph. Crawlee's RequestQueue is only a per-round work list fed from it.
"""

import ipaddress
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
    d = urlsplit(d).hostname or "" if "://" in d else d.split("/")[0]
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


OPEN_CONNS = 0  # connections this process has open right now
# Per-process cap on simultaneously open connections (several workers x threads would otherwise exceed PostgreSQL's max_connections).
# Callers over the cap wait up to DB_POOL_WAIT seconds, then fail loudly instead of hanging or exhausting the server.
DB_MAX_CONN = int(os.environ.get("DB_MAX_CONNECTIONS", 20))
DB_POOL_WAIT = float(os.environ.get("DB_POOL_WAIT", 30))
_SLOTS = threading.BoundedSemaphore(DB_MAX_CONN)


class Connection(psycopg.Connection):
    def close(self):
        global OPEN_CONNS
        if not self.closed:
            OPEN_CONNS -= 1
            _SLOTS.release()  # exactly once per opened connection
        super().close()

    def executemany(self, query, params_seq):  # sqlite3 had it on the connection, psycopg only on cursors
        cur = self.cursor()
        cur.executemany(query, params_seq)
        return cur


def connect() -> Connection:
    """New connection (committed and closed by `with`). DB_SCHEMA isolates tests in their own schema."""
    global OPEN_CONNS
    # Server-side limits turn "waits forever" into a loud error: a lock wait, a statement or an abandoned transaction ends after 60s / 5min.
    options = "-c lock_timeout=60000 -c statement_timeout=300000 -c idle_in_transaction_session_timeout=300000"
    if not _SLOTS.acquire(timeout=DB_POOL_WAIT):
        raise TimeoutError(f"database connection limit reached ({DB_MAX_CONN} open, waited {DB_POOL_WAIT}s): leak or overload")
    try:
        conn = Connection.connect(DATABASE_URL, cursor_factory=Cursor, row_factory=_row_factory, application_name="backlink-crawler",
                                  connect_timeout=10, options=options + (f" -c search_path={DB_SCHEMA}" if DB_SCHEMA else ""))
    except BaseException:
        _SLOTS.release()
        raise
    OPEN_CONNS += 1
    return conn


BUSY_ERRORS = 0  # transient lock/deadlock errors seen by retry_locked in this process (monitoring)
TRANSIENT = (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure, psycopg.errors.LockNotAvailable)


def retry_locked(fn, tries: int = 4):
    """Run fn, retrying deadlocks/lock timeouts with bounded jittered backoff. Anything else (or the last failure) is raised."""
    global BUSY_ERRORS
    for i in range(tries):
        try:
            return fn()
        except TRANSIENT:
            BUSY_ERRORS += 1
            if i == tries - 1:
                raise
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
    row = conn.execute("SELECT * FROM backlinks WHERE target_domain_id=? AND source_url=? AND destination_url=?",
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
    for r in conn.execute("SELECT * FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1",
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

"""Shared building blocks for the forward crawler: URL handling, SSRF guard, SQLite store, matching, sitemaps.

The SQLite file is the permanent link graph. Crawlee's RequestQueue is only a per-round work list fed from it.
"""

import ipaddress
import os
import re
import random
import socket
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
import tldextract

DB_FILE = Path(__file__).parent / "backlinks.db"
# Honest identification; set CRAWLER_CONTACT_URL to a page describing who runs the crawler.
USER_AGENT = f"BacklinkGraphBot/1.0 (+{os.environ.get('CRAWLER_CONTACT_URL', 'https://example.com/bot')})"
_tld = tldextract.TLDExtract(suffix_list_urls=())  # bundled Public Suffix List snapshot, no network at import
NON_HTML = re.compile(r".*\.(pdf|jpe?g|png|gif|webp|svg|ico|zip|rar|gz|docx?|xlsx?|pptx?|mp4|mp3|css|js)(\?|$)", re.I)
UNSAFE_PATH = re.compile(r"(log-?out|sign-?out|delete|remove|unsubscribe|/cdn-cgi/)", re.I)
HOSTNAME = re.compile(r"^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}$|^[a-z0-9-]+\.localhost$")

# Test hook only: tests crawl 127.0.0.1 and must switch the SSRF guard off.
ALLOW_PRIVATE = False


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def later(**kw) -> str:
    return (datetime.now(timezone.utc) + timedelta(**kw)).strftime("%Y-%m-%d %H:%M:%S")


# ---------- URLs ----------

def normalize_url(url: str) -> str | None:
    """Canonical form for dedup: lowercase scheme/host, default port and fragment dropped. Query is kept as-is."""
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
    target = target.lower().lstrip(".")
    return host == target or host.endswith("." + target)


def clean_target(domain: str) -> str:
    """'https://www.Codigix.co/x' -> 'codigix.co'. Raises ValueError for anything that is not a hostname."""
    d = domain.strip().lower()
    d = urlsplit(d).hostname or "" if "://" in d else d.split("/")[0]
    d = idna(d.removeprefix("www."))
    if not HOSTNAME.match(d):
        raise ValueError(f"invalid domain: {domain!r}")
    return d


def is_safe_url(url: str) -> bool:
    """SSRF guard: http(s) only, no credentials, no localhost/private/link-local/reserved addresses."""
    if ALLOW_PRIVATE:
        return True
    p = urlsplit(url)
    host = hostname(url)
    if p.scheme not in ("http", "https") or not host or p.username or host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            return False
    return True


def crawlable(url: str) -> bool:
    return not NON_HTML.match(url) and not UNSAFE_PATH.search(urlsplit(url).path)


# ---------- SQLite ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS discovered_urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE, normalized_url TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL, hostname TEXT NOT NULL,
    discovered_from TEXT, discovery_method TEXT,
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP, last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    crawl_status TEXT DEFAULT 'pending', crawl_depth INTEGER DEFAULT 0,
    is_internal INTEGER DEFAULT 0, is_external INTEGER DEFAULT 0,
    http_status INTEGER, content_type TEXT, last_crawled DATETIME, crawl_error TEXT,
    next_crawl_at DATETIME, retry_count INTEGER DEFAULT 0, sitemap_lastmod TEXT, canonical_url TEXT,
    last_successful_crawl DATETIME, content_hash TEXT, title TEXT, lease_owner TEXT, lease_expires DATETIME, skip_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_discovered_domain ON discovered_urls(domain);
CREATE INDEX IF NOT EXISTS idx_discovered_status ON discovered_urls(crawl_status);
CREATE INDEX IF NOT EXISTS idx_discovered_dom_status ON discovered_urls(domain, crawl_status);
CREATE TABLE IF NOT EXISTS discovered_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT, domain TEXT NOT NULL UNIQUE,
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP, last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    discovery_source TEXT, discovery_status TEXT DEFAULT 'discovered',
    pages_discovered INTEGER DEFAULT 0, last_crawled DATETIME, priority INTEGER DEFAULT 5
);
CREATE TABLE IF NOT EXISTS outgoing_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL, destination_url TEXT NOT NULL, destination_domain TEXT,
    anchor_text TEXT NOT NULL DEFAULT '', rel_attributes TEXT, link_type TEXT DEFAULT 'html',
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP, last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1,
    is_nofollow INTEGER DEFAULT 0, is_sponsored INTEGER DEFAULT 0, is_ugc INTEGER DEFAULT 0,
    img_alt TEXT, position TEXT, context TEXT, source_domain TEXT, lost_at DATETIME,
    UNIQUE(source_url, destination_url, anchor_text)
);
CREATE INDEX IF NOT EXISTS idx_outgoing_destination ON outgoing_links(destination_domain);
CREATE INDEX IF NOT EXISTS idx_outgoing_source ON outgoing_links(source_url);
CREATE TABLE IF NOT EXISTS target_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT, domain TEXT NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, is_active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS backlinks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_domain_id INTEGER NOT NULL, source_url TEXT NOT NULL, destination_url TEXT NOT NULL,
    anchor_text TEXT, rel_attributes TEXT, link_status TEXT DEFAULT 'discovered',
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP, last_verified DATETIME, is_active INTEGER DEFAULT 1,
    discovery_method TEXT DEFAULT 'forward_crawl',
    FOREIGN KEY(target_domain_id) REFERENCES target_domains(id),
    UNIQUE(target_domain_id, source_url, destination_url)
);
CREATE TABLE IF NOT EXISTS crawl_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, source_url TEXT NOT NULL,
    crawl_started DATETIME, crawl_completed DATETIME, http_status INTEGER,
    links_found INTEGER DEFAULT 0, new_links_found INTEGER DEFAULT 0, error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_crawl_history_url ON crawl_history(source_url);
CREATE TABLE IF NOT EXISTS backlink_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, backlink_id INTEGER NOT NULL, event_type TEXT NOT NULL,
    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP, details TEXT,
    FOREIGN KEY(backlink_id) REFERENCES backlinks(id)
);
CREATE TABLE IF NOT EXISTS discovery_sources (
    name TEXT PRIMARY KEY, enabled INTEGER DEFAULT 1, status TEXT DEFAULT 'idle', cursor TEXT,
    items_imported INTEGER DEFAULT 0, runs INTEGER DEFAULT 0, last_run DATETIME, last_error TEXT
);
CREATE TABLE IF NOT EXISTS engine_state (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS worker_status (
    name TEXT PRIMARY KEY, status TEXT, heartbeat DATETIME, runs INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, last_error TEXT,
    last_heartbeat_at DATETIME, last_success_at DATETIME, restart_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS discovery_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, domain_id INTEGER NOT NULL, source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_retry_at DATETIME, last_error TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
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
"""


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


BUSY_ERRORS = 0  # transient lock errors seen by retry_locked in this process (monitoring)


def retry_locked(fn, tries: int = 4):
    """Run fn, retrying SQLITE_BUSY/LOCKED with bounded jittered backoff. Anything else (or the last failure) is raised."""
    global BUSY_ERRORS
    for i in range(tries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if not any(w in str(e).lower() for w in ("locked", "busy")):
                raise
            BUSY_ERRORS += 1
            if i == tries - 1:
                raise
            time.sleep(0.2 * 2 ** i * random.uniform(0.5, 1.5))


def migrate(conn: sqlite3.Connection, recover: bool = True) -> None:
    """Idempotent. The pre-existing `backlinks` table (per-page job results) clashes with the new
    backlinks table, so it is renamed to legacy_backlinks with all rows kept."""
    cols = {c["name"] for c in conn.execute("PRAGMA table_info(backlinks)")}
    if cols and "target_domain_id" not in cols:
        conn.execute("DROP INDEX IF EXISTS idx_backlinks_domain")
        conn.execute("DROP INDEX IF EXISTS idx_backlinks_job")
        conn.execute("ALTER TABLE backlinks RENAME TO legacy_backlinks")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_legacy_backlinks_domain ON legacy_backlinks(target_domain)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_legacy_backlinks_job ON legacy_backlinks(job_id)")
    conn.executescript(SCHEMA)
    for table, adds in (("discovered_urls", ("last_successful_crawl DATETIME", "content_hash TEXT", "title TEXT",
                                             "lease_owner TEXT", "lease_expires DATETIME", "skip_reason TEXT")),
                        ("worker_status", ("last_heartbeat_at DATETIME", "last_success_at DATETIME", "restart_count INTEGER DEFAULT 0")),
                        ("outgoing_links", ("is_nofollow INTEGER DEFAULT 0", "is_sponsored INTEGER DEFAULT 0",
                                            "is_ugc INTEGER DEFAULT 0", "img_alt TEXT", "position TEXT", "context TEXT",
                                            "source_domain TEXT", "lost_at DATETIME"))):
        have = {c["name"] for c in conn.execute(f"PRAGMA table_info({table})")}  # tables from an earlier build
        for col in adds:
            if col.split()[0] not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_outgoing_source_domain ON outgoing_links(source_domain)")
    for (u,) in conn.execute("SELECT DISTINCT source_url FROM outgoing_links WHERE source_domain IS NULL").fetchall():  # rows from older builds
        conn.execute("UPDATE outgoing_links SET source_domain=? WHERE source_url=? AND source_domain IS NULL",
                     (registrable_domain(hostname(u)), u))
    if recover:  # only the process that runs crawls may do this; the API must not reset a separate worker's in-flight work
        conn.execute("UPDATE discovered_urls SET crawl_status='pending', lease_owner=NULL, lease_expires=NULL WHERE crawl_status='crawling'")
        # interrupted recrawls go back to the schedule, never to 'pending' (they were crawled before) or 'completed' blindly
        conn.execute("UPDATE discovered_urls SET lease_owner=NULL, lease_expires=NULL, crawl_status=CASE WHEN last_successful_crawl IS NOT NULL"
                     " THEN 'completed' ELSE 'pending' END WHERE crawl_status IN ('recrawling','recrawl_due')")
        conn.execute("UPDATE discovery_jobs SET status='retry_pending' WHERE status='processing'")
        conn.execute("UPDATE crawl_jobs SET status='interrupted' WHERE status='running'")
    conn.commit()


# ---------- graph writes ----------

def save_url(conn, url: str, from_url: str | None, method: str, depth: int, internal: bool, status: str = "pending",
             lastmod: str | None = None) -> bool:
    """Record a discovered URL (never deletes). Returns True if it was new."""
    norm = normalize_url(url)
    if norm is None:
        return False
    host = hostname(norm)
    dom = registrable_domain(host)
    new = conn.execute(
        "INSERT OR IGNORE INTO discovered_urls (url, normalized_url, domain, hostname, discovered_from, discovery_method,"
        " crawl_depth, is_internal, is_external, crawl_status, sitemap_lastmod) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (url, norm, dom, host, from_url, method, depth, int(internal), int(not internal), status, lastmod),
    ).rowcount == 1
    if not new:
        conn.execute("UPDATE discovered_urls SET last_seen=?, crawl_depth=MIN(crawl_depth, ?),"
                     " sitemap_lastmod=COALESCE(?, sitemap_lastmod) WHERE normalized_url=?", (now(), depth, lastmod, norm))
    conn.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_source) VALUES (?,?)", (dom, from_url or method))
    conn.execute("UPDATE discovered_domains SET last_seen=?, pages_discovered=pages_discovered+? WHERE domain=?",
                 (now(), int(new), dom))
    return new


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
            " link_status, last_verified, discovery_method) VALUES (?,?,?,?,?,?,?,?)",
            (target_id, source, dest, anchor, rel, status, stamp, method)).lastrowid
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

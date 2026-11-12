"""Forward crawler: crawls approved source domains, stores every URL and link, admits new domains, recrawls.

Crawlee's RequestQueue is a throwaway per-round work list. Everything permanent (what was found, what is
crawled, when to recrawl) lives in PostgreSQL via crawler_core, so a restart loses nothing.
"""

import asyncio
import hashlib
import logging
import re
import warnings
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import timedelta
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from crawlee import ConcurrencySettings, Request, SkippedReason
from crawlee.crawlers import (
    BeautifulSoupCrawler, BeautifulSoupCrawlingContext, PlaywrightCrawler, PlaywrightCrawlingContext,
)
from crawlee._utils.robots import RobotsTxtFile
from crawlee.errors import PersistentRateLimitError, RequestThrottledError
from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.http_clients import ImpitHttpClient
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

import crawler_core as core
import egress_proxy

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)  # XML pages are skipped by content type anyway

# Dead or forbidden pages: retrying will not help and we must not push past access controls.
PERMANENT_HTTP = [400, 401, 403, 404, 405, 406, 410, 451]
log = logging.getLogger("crawler.discovery")
vlog = logging.getLogger("crawler.verification")
xlog = logging.getLogger("crawler.extraction")

# Hosts with millions of pages that block crawlers: store their URLs but never crawl inside them.
NO_CRAWL_DOMAINS = {"linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com",
                    "pinterest.com", "tiktok.com", "reddit.com", "quora.com", "medium.com", "google.com"}


@dataclass
class Config:
    target_domain: str | None = None
    max_pages: int = 10_000
    max_depth: int = 5
    concurrency: int = 10
    use_playwright: bool = False
    enable_sitemap_discovery: bool = True
    enable_external_domain_discovery: bool = False
    max_domains: int = 20  # cap on admitted source domains per job (controlled expansion)
    admit_batch: int = 5  # domains admitted per expansion step
    timeout: int = 30  # seconds per request
    max_retries: int = 3
    domain_delay: float = 0.2  # min seconds between requests to one domain (politeness)
    recrawl_days: int = 7
    max_response_bytes: int = 2_000_000
    max_redirects: int = 5
    js_fallback: bool = True  # re-fetch JavaScript-looking pages in a browser (never every page)
    rate_limit_base: float = 2.0  # first backoff after an HTTP 429, doubling up to 60s
    rate_limit_stall: float = 300.0  # a host rate limited this long ends the round
    max_js_pages: int = 20  # per round: browsers are expensive


HTTP = {"started": 0, "done": 0, "total": 0.0, "retries": 0}  # fetch counters for the diagnostics log (single event loop, no lock needed)
_T0: dict[str, float] = {}


def _http_done(url: str, failed: bool = False) -> None:
    t = _T0.pop(url, None)
    if t is not None:
        HTTP["done"] += 1
        HTTP["total"] += time.monotonic() - t
        HTTP["retries"] += failed


_PL: dict = {}
PERSIST_QUEUE_SECONDS = 300  # extra handler time for waiting in line behind pages of the same domain


def _persist_lock(domain: str) -> asyncio.Lock:
    """In-process queue per source domain (the cross-process guard is the advisory lock inside process_page). Waiting here holds
    no thread and no database connection. Different domains never wait for each other."""
    loop = asyncio.get_running_loop()
    if _PL.get("loop") is not loop:
        _PL.clear()
        _PL.update(loop=loop, locks={})
    return _PL["locks"].setdefault(domain, asyncio.Lock())


def _job_add(job_id: str | None, col: str, n: int = 1) -> None:
    if job_id and col in ("pages_crawled", "pages_failed", "domains_admitted"):
        with core.connect() as c:
            c.execute(f"UPDATE crawl_jobs SET {col}={col}+? WHERE job_id=?", (n, job_id))


def _position(a) -> str:
    names = {p.name for p in a.parents}
    return next((n for n in ("nav", "header", "footer", "aside") if n in names), "content")


def extract_links(soup, base: str) -> list[tuple[str, str, str | None, str, dict]]:
    """(destination, anchor_text, rel, link_type, extra) for every <a href> and the canonical URL. Query strings kept.
    extra: nofollow/sponsored/ugc flags, img_alt (image links), position (nav/header/footer/aside/content), context."""
    out = []
    for a in soup.find_all("a", href=True):
        dest = core.resolve(base, a["href"])
        if dest:
            tokens = [t.lower() for t in (a.get("rel") or [])]
            img = a.find("img")
            extra = {"nofollow": "nofollow" in tokens, "sponsored": "sponsored" in tokens, "ugc": "ugc" in tokens,
                     "img_alt": (img.get("alt") or "").strip()[:200] if img else None, "position": _position(a),
                     "context": (a.parent.get_text(" ", strip=True) if a.parent else "")[:300]}
            out.append((dest, a.get_text(" ", strip=True)[:200], " ".join(tokens) or None, "html", extra))
    for ln in soup.find_all("link", href=True):
        if "canonical" in [r.lower() for r in (ln.get("rel") or [])]:
            dest = core.resolve(base, ln["href"])
            if dest:
                out.append((dest, "", None, "canonical", {}))
    return out


def link_event(conn, source: str, dest: str, ddom: str, anchor: str, kind: str) -> None:
    """Append-only record of a link changing state (suspected_removed / lost / reappeared). Never updated or deleted, so repeated
    removal and reappearance all stay visible; outgoing_links only holds the current state."""
    conn.execute("INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type) VALUES (?,?,?,?,?)",
                 (source, dest, ddom, anchor, kind))


def process_page(conn, key_url: str, base_url: str, depth: int, soup, status: int | None, ctype: str | None,
                 cfg: Config, approved: set[str], follow: bool, started: str) -> list[tuple[str, int]]:
    """Persist one successfully fetched page in ONE transaction (all of it or none of it). Returns internal (url, depth) to crawl next.

    Concurrency: pages of the same source domain are serialized by a per-domain advisory lock (they share hot rows: the domain row,
    sibling URLs). Pages of different domains run in parallel; their writes are made in one global order (links sorted by registrable
    domain, then URL; one upsert per domain, also sorted), which is what prevents lock cycles. A rare remaining deadlock is detected by
    PostgreSQL and the whole transaction is retried by the caller (core.retry_locked), so nothing is half-written."""
    src_dom = core.registrable_domain(core.hostname(key_url))
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(?))", ("page:" + src_dom,))
    links = extract_links(soup, base_url)
    if base_url != key_url:  # redirect
        links.append((base_url, "", None, "redirect", {}))
    prepared = {}
    for dest, anchor, rel, ltype, ex in links:  # one row per (destination, anchor); the order below is the global lock order
        prepared.setdefault((dest, anchor), (core.registrable_domain(core.hostname(dest)), core.normalize_url(dest) or dest, rel, ltype, ex))
    ordered = sorted(prepared.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0][1]))
    targets = core.active_targets(conn)
    seen_out: set[tuple[str, str]] = set()
    found_backlinks: set[tuple[int, str]] = set()
    domains: dict[str, int] = {src_dom: 0}
    to_queue, new_links, canonical = [], 0, None
    for (dest, anchor), (ddom, ndest, rel, ltype, ex) in ordered:
        internal = ddom == src_dom
        ok = core.crawlable(dest) and ddom not in NO_CRAWL_DOMAINS
        too_deep = internal and depth + 1 > cfg.max_depth  # claim_urls never leases these, so "pending" would be forever
        is_new = core.save_url(conn, dest, key_url, "forward_crawl", depth + 1 if internal else 0, internal,
                               "pending" if ok and not too_deep else "skipped", reason="max_depth" if ok and too_deep else None,
                               touch_domain=False)
        domains[ddom] = domains.get(ddom, 0) + int(is_new)
        flags = (int(ex.get("nofollow", 0)), int(ex.get("sponsored", 0)), int(ex.get("ugc", 0)), ex.get("img_alt"), ex.get("position"), ex.get("context"))
        fresh = conn.execute(
            "INSERT OR IGNORE INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text,"
            " rel_attributes, link_type, is_nofollow, is_sponsored, is_ugc, img_alt, position, context, source_domain)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (key_url, dest, ddom, anchor, rel, ltype, *flags, src_dom)).rowcount
        new_links += fresh
        if not fresh:  # existing row: refresh it and learn whether it was suspected/removed (one statement, row locked)
            was = conn.execute(
                "WITH o AS (SELECT id, is_active, lost_at FROM outgoing_links WHERE source_url=? AND destination_url=? AND anchor_text=? FOR UPDATE)"
                " UPDATE outgoing_links l SET last_seen=?, is_active=1, lost_at=NULL, rel_attributes=?, is_nofollow=?, is_sponsored=?, is_ugc=?,"
                " img_alt=?, position=?, context=? FROM o WHERE l.id=o.id RETURNING o.is_active AS was_active, o.lost_at AS was_lost",
                (key_url, dest, anchor, core.now(), rel, *flags)).fetchone()
            if was and ltype == "html" and (not was["was_active"] or was["was_lost"]):
                link_event(conn, key_url, dest, ddom, anchor, "reappeared")
        seen_out.add((dest, anchor))
        if ltype == "canonical":
            canonical = dest
        if ltype == "html":
            host = core.hostname(dest)
            for tid, tdom in targets:
                if (tid, dest) not in found_backlinks and core.host_matches(host, tdom) \
                        and not core.host_matches(core.hostname(key_url), tdom):
                    core.record_backlink(conn, tid, key_url, dest, anchor, rel)
                    found_backlinks.add((tid, dest))
                    vlog.info("backlink verified %s -> %s", key_url, dest)
        if follow and internal and ok and ltype != "redirect" and depth + 1 <= cfg.max_depth and ddom in approved:
            row = conn.execute("SELECT url, crawl_status, crawl_depth FROM discovered_urls WHERE normalized_url=?", (ndest,)).fetchone()
            if row["crawl_status"] == "pending":
                conn.execute("UPDATE discovered_urls SET crawl_status='crawling' WHERE normalized_url=?", (ndest,))
                to_queue.append((row["url"], row["crawl_depth"]))
    for dom in sorted(domains):  # one upsert per domain, in sorted order
        core.upsert_domain(conn, dom, key_url, domains[dom])
    # Same two-miss rule as backlinks, so the graph and the backlinks table never disagree: first successful miss sets lost_at
    # (suspected, still active); a second one deactivates. Seeing the link again clears lost_at (above). Rows are never deleted.
    for r in conn.execute("SELECT id, destination_url, destination_domain, anchor_text, link_type, lost_at FROM outgoing_links"
                          " WHERE source_url=? AND is_active=1 ORDER BY id FOR UPDATE", (key_url,)).fetchall():
        if (r["destination_url"], r["anchor_text"]) not in seen_out:
            conn.execute("UPDATE outgoing_links SET is_active=?, lost_at=? WHERE id=?", (int(not r["lost_at"]), core.now(), r["id"]))
            if r["link_type"] == "html":
                link_event(conn, key_url, r["destination_url"], r["destination_domain"], r["anchor_text"], "lost" if r["lost_at"] else "suspected_removed")
    suspected = False  # one miss is only a suspicion (soft blocks, layout changes); a second consecutive miss confirms
    for b in conn.execute("SELECT id, target_domain_id, destination_url, link_status FROM backlinks WHERE source_url=? AND"
                          " is_active=1 AND link_status IN ('verified','fetch_failed','suspected_removed') ORDER BY id FOR UPDATE", (key_url,)).fetchall():
        if (b["target_domain_id"], b["destination_url"]) in found_backlinks:
            continue
        if b["link_status"] == "suspected_removed":
            conn.execute("UPDATE backlinks SET is_active=0, link_status='removed', last_verified=? WHERE id=?", (core.now(), b["id"]))
            core.event(conn, b["id"], "confirmed_removed", "absent on two consecutive successful fetches")
            vlog.info("backlink removed %s -> %s", key_url, b["destination_url"])
        else:
            conn.execute("UPDATE backlinks SET link_status='suspected_removed', last_verified=? WHERE id=?", (core.now(), b["id"]))
            core.event(conn, b["id"], "suspected_removed", "absent after successful fetch; recheck scheduled")
            vlog.info("backlink suspected removed %s -> %s", key_url, b["destination_url"])
            suspected = True
    conn.execute(
        "UPDATE discovered_urls SET crawl_status='completed', http_status=?, content_type=?, last_crawled=?, crawl_error=NULL,"
        " retry_count=0, next_crawl_at=?, canonical_url=?, last_successful_crawl=?, content_hash=?, title=?, lease_owner=NULL,"
        " lease_expires=NULL WHERE normalized_url=?",
        (status, ctype, core.now(), core.later(days=1 if suspected else cfg.recrawl_days), canonical, core.now(),
         hashlib.sha256(str(soup).encode()).hexdigest(), (soup.title.get_text(strip=True)[:300] if soup.title else None),
         core.normalize_url(key_url)))
    conn.execute("UPDATE discovered_domains SET last_crawled=? WHERE domain=?", (core.now(), src_dom))
    conn.execute("INSERT INTO crawl_history (source_url, crawl_started, crawl_completed, http_status, links_found,"
                 " new_links_found) VALUES (?,?,?,?,?,?)", (key_url, started, core.now(), status, len(links), new_links))
    return to_queue


def mark_failed(conn, url: str, error: str, status: int | None, cfg: Config, started: str, permanent: bool = False) -> None:
    """A failed fetch changes nothing about links or backlinks; it only schedules a retry with backoff."""
    norm = core.normalize_url(url)
    row = conn.execute("SELECT retry_count FROM discovered_urls WHERE normalized_url=?", (norm,)).fetchone()
    tries = (row["retry_count"] if row else 0) + 1
    if permanent:
        state, nxt = "failed", core.later(days=30)
    elif tries >= cfg.max_retries:
        state, nxt = "failed", core.later(days=1)
    else:
        state, nxt = "retry_pending", core.later(minutes=5 * 2 ** tries)
    conn.execute("UPDATE discovered_urls SET crawl_status=?, retry_count=?, next_crawl_at=?, crawl_error=?, http_status=?,"
                 " lease_owner=NULL, lease_expires=NULL WHERE normalized_url=?", (state, tries, nxt, error[:500], status, norm))
    for b in conn.execute("UPDATE backlinks SET link_status='fetch_failed' WHERE source_url=? AND link_status='verified' RETURNING id", (url,)).fetchall():
        core.event(conn, b["id"], "fetch_failed", error[:200])  # not removed: one failed fetch proves nothing about the link
    conn.execute("INSERT INTO crawl_history (source_url, crawl_started, crawl_completed, http_status, error_message)"
                 " VALUES (?,?,?,?,?)", (url, started, core.now(), status, error[:500]))


def concurrent_robots(crawler, timeout: float = 10) -> None:
    """Crawlee fetches robots.txt files one at a time under a single crawler-wide lock, so one slow or dead host stalls every
    other fetch (a round of 100 sites sat idle for minutes). This replaces that lookup with one fetch per site, run in parallel,
    each capped at `timeout`. An unreachable robots.txt counts as 'no rules', same as Crawlee's own behaviour.
    shortcut: overrides a private Crawlee method (crawlee is pinned in requirements.txt); re-check it when upgrading."""
    cache: dict[str, asyncio.Future] = {}

    async def get(url: str):
        p = urlsplit(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in cache:
            cache[origin] = asyncio.ensure_future(asyncio.wait_for(RobotsTxtFile.find(url, crawler._http_client), timeout))
        try:
            return await asyncio.shield(cache[origin])
        except Exception:  # noqa: BLE001 - timeout / network error: crawl as if there were no robots.txt
            return None

    crawler._get_robots_txt_file_for_url = get


class ResponseTooLarge(ValueError):
    """The body exceeded Config.max_response_bytes while it was being downloaded (the download is cut off, not just flagged)."""


class _Buffered:
    """A fully read, size-checked response with the surface that Crawlee's redirect loop and adapter use."""

    def __init__(self, response, body: bytes) -> None:
        self._r, self.content, self.is_closed, self.is_stream_consumed = response, body, True, True

    @property
    def url(self) -> str:
        return core.test_unroute(str(self._r.url))

    def __getattr__(self, name):
        return getattr(self._r, name)


class _Guarded:
    """Wraps impit's AsyncClient. Crawlee follows redirects hop by hop, calling this for the first request AND for every hop, so
    each destination is validated right before it is sent. Bodies are streamed with a hard byte cap."""

    def __init__(self, inner, max_bytes: int) -> None:
        self._inner, self._max = inner, max_bytes

    async def request(self, *, url: str, headers=None, stream: bool = False, **kw):
        why = await asyncio.to_thread(core.blocked_reason, url)
        if why:
            log.warning("BLOCKED %s | %s", url, why)
            raise core.UnsafeURL(f"blocked before fetch ({why}): {url}")
        real, extra = core.test_route(url)
        resp = await self._inner.request(url=real, headers={**(headers or {}), **extra} or None, stream=True, **kw)
        if stream:
            return resp
        try:
            if int(resp.headers.get("content-length") or 0) > self._max:
                raise ResponseTooLarge(f"Content-Length over {self._max} bytes")
            chunks, n = [], 0
            async for chunk in resp.aiter_bytes():
                n += len(chunk)
                if n > self._max:
                    raise ResponseTooLarge(f"body over {self._max} bytes")
                chunks.append(chunk)
        finally:
            resp.close()
        return _Buffered(resp, b"".join(chunks))


class SafeImpitClient(ImpitHttpClient):
    """ImpitHttpClient whose every outgoing request (pages, redirect hops, robots.txt) goes through _Guarded.
    shortcut: overrides the private _get_client / relies on Crawlee's hop-by-hop redirect loop (crawlee is pinned in requirements.txt); re-check on upgrade."""

    def __init__(self, *, max_response_bytes: int, **kw) -> None:
        super().__init__(**kw)
        self._max_bytes, self._guards = max_response_bytes, {}

    def _get_client(self, proxy_url):
        inner = super()._get_client(egress_proxy.ensure())  # every connection is re-validated and pinned by the egress proxy
        return self._guards.setdefault(id(inner), _Guarded(inner, self._max_bytes))

    async def crawl(self, request, **kw):
        try:
            return await super().crawl(request, **kw)
        except (core.UnsafeURL, ResponseTooLarge):
            request.no_retry = True  # the same URL will be refused / too large again
            raise


def needs_js(soup) -> bool:
    """Cheap static heuristic: a near-empty page shell (few links, JS app root / noscript notice / script-heavy with little text)."""
    if soup.body is None or len(soup.find_all("a", href=True)) >= 3:
        return False
    text = len(soup.body.get_text(" ", strip=True))
    shell = soup.find(id=re.compile(r"^(root|app|__next|__nuxt|svelte)$")) is not None
    noscript = any("javascript" in n.get_text().lower() for n in soup.find_all("noscript"))
    return noscript or (shell and text < 300) or (len(soup.find_all("script")) >= 3 and text < 200)


async def crawl_round(cfg: Config, job_id: str | None, work: list[tuple[str, int]], approved: set[str], follow: bool,
                      limit: int) -> None:
    """Run one Crawlee crawl over `work` [(url, depth)]; internal links found are queued dynamically if `follow`.
    Static HTML first; pages that look JavaScript-rendered are re-fetched in a browser (Playwright) in a second pass of the
    same round and persisted through the same process_page."""
    if not work or limit <= 0:
        return
    slots: dict[str, float] = {}
    slot_lock = asyncio.Lock()
    started: dict[str, str] = {}
    deferred: dict[str, tuple] = {}  # url -> (soup, base, status, ctype, depth, t0): static fetch awaiting its rendered version
    done_render: set[str] = set()
    finished: set[str] = set()  # URLs whose outcome is recorded (persisted, failed or skipped)

    def req(url: str, depth: int) -> Request:
        return Request.from_url(url, unique_key=core.normalize_url(url) or url, user_data={"depth": depth})

    async def slot_wait(url: str) -> None:  # one shared per-domain schedule for static fetches, browser navigations and redirect hops
        dom = core.registrable_domain(core.hostname(url))
        async with slot_lock:
            wait = slots.get(dom, 0) - time.monotonic()
            slots[dom] = max(time.monotonic(), slots.get(dom, 0)) + cfg.domain_delay
        if wait > 0:
            await asyncio.sleep(wait)

    async def polite(context) -> None:  # SSRF check before the request, then per-domain rate limit
        why = await asyncio.to_thread(core.blocked_reason, context.request.url)
        if why:
            context.request.no_retry = True
            raise core.UnsafeURL(f"blocked before fetch ({why}): {context.request.url}")
        await slot_wait(context.request.url)
        started[context.request.url] = core.now()
        if context.request.url not in _T0:
            HTTP["started"] += 1
        _T0[context.request.url] = time.monotonic()

    async def persist_page(url, base, depth, soup, status, ctype, t0):
        def persist():
            def once():
                with core.connect() as c:  # one transaction per page; a deadlock victim is rolled back whole and retried
                    return process_page(c, url, base, depth, soup, status, ctype, cfg, approved, follow, t0)
            return core.retry_locked(once)
        async with _persist_lock(core.registrable_domain(core.hostname(url))):  # waiting here holds no thread or connection
            nxt = await asyncio.to_thread(persist)
        finished.add(url)
        _job_add(job_id, "pages_crawled")
        def last():
            with core.connect() as c:
                return c.execute("SELECT links_found, new_links_found FROM crawl_history WHERE source_url=? ORDER BY id DESC LIMIT 1", (url,)).fetchone()
        n = await asyncio.to_thread(last)
        xlog.info("crawled %s | status %s | links %s (new %s) | queued %d", url, status, n["links_found"], n["new_links_found"], len(nxt))
        return nxt

    async def handle(context, soup, status, ctype, defer: bool = False) -> None:
        url, base = context.request.url, context.request.loaded_url or context.request.url
        _http_done(url)  # response received (body parsing and database work after this are not HTTP time)
        t0 = started.get(url, core.now())
        if status is not None and status >= 400:  # ignored by Crawlee so it does not retry; recorded as a permanent failure
            def fail():
                with core.connect() as c:
                    mark_failed(c, url, f"HTTP {status}", status, cfg, t0, permanent=True)
            await asyncio.to_thread(fail)
            finished.add(url)
            _job_add(job_id, "pages_failed")
            log.info("skipped %s | HTTP %s", url, status)
            return
        if base != url and not await asyncio.to_thread(core.is_safe_url, base):
            raise core.UnsafeURL(f"redirect to unsafe URL blocked: {base}")
        if ctype and "html" not in ctype.lower():
            with core.connect() as c:
                c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='not_html', content_type=?, http_status=?,"
                          " lease_owner=NULL, lease_expires=NULL WHERE normalized_url=?", (ctype, status, core.normalize_url(url)))
            finished.add(url)
            return
        depth = context.request.user_data["depth"]
        if defer and cfg.js_fallback and len(deferred) < cfg.max_js_pages and needs_js(soup):
            deferred[url] = (soup, base, status, ctype, depth, t0)  # persisted after the rendered pass (or, if that fails, as fetched)
            return
        nxt = await persist_page(url, base, depth, soup, status, ctype, t0)
        if nxt:
            await context.add_requests([req(u, d) for u, d in nxt])

    async def failed(context, error: Exception) -> None:
        log.warning("FAILED %s | %s", context.request.url, error)
        _http_done(context.request.url, failed=True)
        permanent = isinstance(error, (core.UnsafeURL, ResponseTooLarge))  # blocked / oversized: retrying cannot help
        def fail():
            with core.connect() as c:
                mark_failed(c, context.request.url, f"{type(error).__name__}: {error}" if permanent else repr(error),
                            getattr(error, "status_code", None) or (429 if isinstance(error, RequestThrottledError) else None), cfg, started.get(context.request.url, core.now()), permanent=permanent)
        await asyncio.to_thread(fail)
        finished.add(context.request.url)
        _job_add(job_id, "pages_failed")

    def common(**extra) -> dict:
        return dict(
            max_requests_per_crawl=limit, max_request_retries=cfg.max_retries,
            concurrency_settings=ConcurrencySettings(desired_concurrency=cfg.concurrency, max_concurrency=cfg.concurrency),
            request_handler_timeout=timedelta(seconds=cfg.timeout * 2 + PERSIST_QUEUE_SECONDS), respect_robots_txt_file=True,
            ignore_http_error_status_codes=PERMANENT_HTTP, http_client=SafeImpitClient(
                max_response_bytes=cfg.max_response_bytes, timeout=cfg.timeout, browser=None, max_redirects=cfg.max_redirects,
                headers={"User-Agent": core.USER_AGENT}), **extra)

    async def run(crawler, items) -> None:
        @crawler.on_skipped_request
        async def skipped(url: str, reason: SkippedReason) -> None:
            if reason != "robots_txt":
                return
            # terminal state: without this the URL stays 'crawling', is released to 'pending' and is claimed again forever
            with core.connect() as c:
                c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='robots_txt', lease_owner=NULL, lease_expires=NULL"
                          " WHERE normalized_url=? AND crawl_status IN ('crawling','recrawling')", (core.normalize_url(url),))
            finished.add(url)
            log.info("skipped %s | robots.txt", url)

        crawler.pre_navigation_hook(polite)
        concurrent_robots(crawler)
        await asyncio.gather(*(crawler._get_robots_txt_file_for_url(u) for u, _ in items))  # all sites at once, before add_requests checks them one by one
        try:
            await crawler.run([req(u, d) for u, d in items])
        except PersistentRateLimitError as e:  # a host answered 429 for the whole stall window: reschedule what never finished
            log.warning("rate limited: %s", e)
            def resched():
                with core.connect() as c:
                    for u, _ in items:
                        if u not in finished:
                            mark_failed(c, u, "HTTP 429 (persistent rate limit)", 429, cfg, started.get(u, core.now()))
            await asyncio.to_thread(resched)

    async def queue(*, alias=None, storage_client=None, configuration=None):  # also the opener for the per-host sub-queues
        # always a fresh alias: the throttler's own per-host aliases are fixed names, and a reused queue remembers URLs as already handled
        return await RequestQueue.open(alias=uuid.uuid4().hex, storage_client=MemoryStorageClient())

    async def queue_for(items):
        """Per-host throttling manager: an HTTP 429 holds that host back with exponential backoff (cfg.rate_limit_base .. 60s) and
        robots.txt Crawl-delay is honoured; other hosts keep going. A host that stays rate limited for cfg.rate_limit_stall seconds
        ends the round (caught below) and its unfinished URLs are rescheduled."""
        hosts = sorted({core.hostname(u) for u, _ in items if core.hostname(u)})
        return ThrottlingRequestManager(await queue(), domains=hosts, request_manager_opener=queue,
                                        base_delay=timedelta(seconds=cfg.rate_limit_base), max_delay=timedelta(seconds=60),
                                        max_domain_stall=timedelta(seconds=cfg.rate_limit_stall))

    async def playwright_crawler(handler_failed, items):
        crawler = PlaywrightCrawler(
            request_manager=await queue(),  # plain queue: the throttling manager never dispatched requests to Playwright (verified)
            browser_new_context_options={"user_agent": core.USER_AGENT},
            # all browser traffic through the egress proxy; <-loopback> stops Chromium from bypassing it for 127.0.0.1 / localhost
            browser_launch_options={"proxy": {"server": egress_proxy.ensure()}, "args": ["--proxy-bypass-list=<-loopback>"]},
            navigation_timeout=timedelta(seconds=cfg.timeout), **common())
        safe: dict[str, bool] = {}

        @crawler.pre_navigation_hook
        async def guard(context: PlaywrightCrawlingContext) -> None:  # every browser request, including redirects and sub-resources
            async def route(r) -> None:
                u = r.request.url
                if not u.startswith(("http://", "https://")):
                    return await r.continue_()
                host = core.hostname(u)
                if host not in safe:
                    safe[host] = await asyncio.to_thread(core.is_safe_url, u)
                if safe[host]:
                    return await r.continue_()
                log.warning("BLOCKED (browser) %s", u)
                await r.abort("blockedbyclient")
            await context.page.route("**/*", route)

        @crawler.router.default_handler
        async def pw(context: PlaywrightCrawlingContext) -> None:
            try:  # let scripts add links; busy pages never go idle, so cap the wait
                await context.page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:  # noqa: BLE001
                pass
            html = await context.page.content()
            resp = context.response
            await handle(context, BeautifulSoup(html, "html.parser"), resp.status if resp else None,
                         (resp.headers.get("content-type") if resp else None))
            done_render.add(context.request.url)
        crawler.failed_request_handler(handler_failed)
        return crawler

    if cfg.use_playwright:  # everything rendered (explicit opt-in)
        await run(await playwright_crawler(failed, work), work)
        return

    crawler = BeautifulSoupCrawler(request_manager=await queue_for(work), navigation_timeout=timedelta(seconds=cfg.timeout), **common())

    @crawler.router.default_handler
    async def bs(context: BeautifulSoupCrawlingContext) -> None:
        r = context.http_response
        await handle(context, context.soup, r.status_code, r.headers.get("content-type"), defer=True)

    crawler.failed_request_handler(failed)
    await run(crawler, work)
    if not deferred:
        return
    log.info("rendering %d page(s) that look JavaScript-driven", len(deferred))

    async def render_failed(context, error: Exception) -> None:  # browser could not render it: keep the static result, never lose the page
        log.warning("render failed for %s (%s); using the static HTML", context.request.url, error)
        _http_done(context.request.url, failed=True)
    try:
        items = [(u, v[4]) for u, v in deferred.items()]
        await run(await playwright_crawler(render_failed, items), items)
    except Exception:  # noqa: BLE001 - e.g. browsers not installed: fall back to static for all
        log.exception("Playwright pass failed; persisting static HTML")
    for url, (soup, base, status, ctype, depth, t0) in deferred.items():
        if url not in done_render:
            await persist_page(url, base, depth, soup, status, ctype, t0)


async def _guard(request: httpx.Request) -> None:
    why = await asyncio.to_thread(core.blocked_reason, str(request.url))
    if why:
        log.warning("BLOCKED %s | %s", request.url, why)
        raise httpx.RequestError(f"unsafe URL blocked ({why}): {request.url}")
    real, extra = core.test_route(str(request.url))  # tests only
    if extra:
        request.url = httpx.URL(real)
        request.headers["Host"] = extra["Host"]


async def admit_domain(cfg: Config, job_id: str | None, domain: str, seed_url: str | None = None) -> bool:
    """Approve a domain for crawling: seed its start URL and (optionally) its sitemap URLs."""
    with core.connect() as c:
        row = c.execute("SELECT url FROM discovered_urls WHERE domain=? ORDER BY id LIMIT 1", (domain,)).fetchone()
    origin = "{0.scheme}://{0.netloc}".format(urlsplit(seed_url or (row["url"] if row else f"https://{domain}/")))
    if not await asyncio.to_thread(core.is_safe_url, origin + "/"):
        with core.connect() as c:
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain) VALUES (?)", (domain,))
            c.execute("UPDATE discovered_domains SET discovery_status='blocked' WHERE domain=?", (domain,))
        return False
    sitemap = []
    if cfg.enable_sitemap_discovery:
        async with httpx.AsyncClient(timeout=cfg.timeout, follow_redirects=True, headers={"User-Agent": core.USER_AGENT},
                                     proxy=egress_proxy.ensure(), event_hooks={"request": [_guard]}) as client:  # the hook also runs for every redirect hop
            sitemap = await core.discover_sitemaps(client, origin + "/")
    def save() -> None:
        with core.connect() as c:
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_source) VALUES (?, 'seed')", (domain,))
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (domain,))
            c.execute("INSERT OR IGNORE INTO discovery_jobs (domain_id, source) SELECT id, 'commoncrawl' FROM discovered_domains WHERE domain=?", (domain,))
            core.save_url(c, seed_url or origin + "/", None, "seed", 0, True)
            for u, mod in sitemap:
                if core.registrable_domain(core.hostname(u)) == domain:
                    core.save_url(c, u, origin + "/sitemap.xml", "sitemap", 1, True, "pending" if core.crawlable(u) else "skipped", mod)
    await asyncio.to_thread(save)  # up to 5000 urls: 2-3s of database work that must not block crawling
    _job_add(job_id, "domains_admitted")
    log.info("admitted %s (%d sitemap urls)", domain, len(sitemap))
    return True


def _pending(cfg: Config, approved: set[str], limit: int) -> list[tuple[str, int]]:
    with core.connect() as c:
        marks = ",".join("?" * len(approved)) or "''"
        rows = c.execute(f"SELECT url, crawl_depth FROM discovered_urls WHERE crawl_status='pending' AND domain IN ({marks})"
                         " AND crawl_depth<=? ORDER BY crawl_depth, id LIMIT ?", (*approved, cfg.max_depth, limit)).fetchall()
        work = [(r["url"], r["crawl_depth"]) for r in rows]
        for u, _ in work:
            c.execute("UPDATE discovered_urls SET crawl_status='crawling' WHERE url=?", (u,))
    return work


def _approved() -> set[str]:
    with core.connect() as c:
        return {r["domain"] for r in c.execute("SELECT domain FROM discovered_domains WHERE discovery_status='admitted'")}


async def run_crawl(cfg: Config, source_urls: list[str], job_id: str | None = None) -> None:
    """Seed -> crawl approved domains -> admit new external domains (bounded) -> repeat. Resumes from SQLite."""
    with core.connect() as c:
        if cfg.target_domain:
            core.add_target(c, cfg.target_domain)
            core.match_target_from_graph(c, cfg.target_domain)  # results already in the graph show up immediately
    log.info("job %s started | target=%s | %d seed urls | max_pages=%d max_depth=%d", job_id, cfg.target_domain, len(source_urls), cfg.max_pages, cfg.max_depth)
    for u in source_urls:
        if core.normalize_url(u):
            await admit_domain(cfg, job_id, core.registrable_domain(core.hostname(u)), u)
    done = 0
    while done < cfg.max_pages:
        approved = _approved()
        work = _pending(cfg, approved, cfg.max_pages - done)
        if work:
            with core.connect() as c:
                before = c.execute("SELECT COUNT(*) n FROM crawl_history").fetchone()["n"]
            await crawl_round(cfg, job_id, work, approved, True, cfg.max_pages - done)
            with core.connect() as c:
                done += c.execute("SELECT COUNT(*) n FROM crawl_history").fetchone()["n"] - before
                c.execute("UPDATE discovered_urls SET crawl_status='pending' WHERE crawl_status='crawling'")  # cut off by the page cap
            if done >= cfg.max_pages:
                break
            continue
        if not cfg.enable_external_domain_discovery or len(approved) >= cfg.max_domains:
            break
        with core.connect() as c:
            skip = cfg.target_domain and core.registrable_domain(cfg.target_domain)
            cand = [r["domain"] for r in c.execute(
                "SELECT domain FROM discovered_domains WHERE discovery_status='discovered' AND domain!=? ORDER BY priority, id"
                " LIMIT ?", (skip or "", min(cfg.admit_batch, cfg.max_domains - len(approved))))]
        if not cand:
            break
        for d in cand:
            await admit_domain(cfg, job_id, d)
    log.info("job %s crawl loop finished | pages crawled this run: %d", job_id, done)
    with core.connect() as c:
        c.execute("UPDATE discovered_domains SET discovery_status='crawled' WHERE discovery_status='admitted' AND domain NOT IN"
                  " (SELECT domain FROM discovered_urls WHERE crawl_status IN ('pending','crawling'))")


def due_urls(limit: int = 1000) -> list[tuple[str, int]]:
    """Successfully crawled pages whose recrawl time has come. failed / retry_pending are never recrawl work:
    retry_pending is retried by the forward crawler (claim_urls), failed is terminal."""
    with core.connect() as c:
        rows = c.execute("SELECT url, crawl_depth FROM discovered_urls WHERE crawl_status='completed'"
                         " AND next_crawl_at IS NOT NULL AND next_crawl_at<=? ORDER BY next_crawl_at LIMIT ?",
                         (core.now(), limit)).fetchall()
    return [(r["url"], r["crawl_depth"]) for r in rows]


def claim_recrawl(limit: int, urls: list[str] | None = None, owner: str = "recrawl", lease_seconds: int = 900) -> list[tuple[str, int, str]]:
    """Atomically move due pages (or an explicit manual list, any non-skipped state) to 'recrawling' under a lease.
    Returns (url, depth, previous_status) so unreached pages go back exactly where they were."""
    with core.connect() as c:
        if urls is not None:
            norms = [n for n in map(core.normalize_url, urls) if n]
            rows = c.execute(f"SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE normalized_url IN"
                             f" ({','.join('?' * len(norms)) or 'NULL'}) AND crawl_status IN ('completed','retry_pending','failed')"
                             " FOR UPDATE SKIP LOCKED", norms).fetchall()
        else:
            rows = c.execute("SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE crawl_status='completed'"
                             " AND next_crawl_at IS NOT NULL AND next_crawl_at<=? ORDER BY next_crawl_at LIMIT ? FOR UPDATE SKIP LOCKED",
                             (core.now(), limit)).fetchall()
        c.executemany("UPDATE discovered_urls SET crawl_status='recrawling', lease_owner=?, lease_expires=? WHERE id=?",
                      [(owner, core.later(seconds=lease_seconds), r["id"]) for r in rows])
    return [(r["url"], r["crawl_depth"], r["crawl_status"]) for r in rows]


async def recrawl_due(cfg: Config, job_id: str | None = None, urls: list[str] | None = None, limit: int | None = None,
                      owner: str = "recrawl", lease_seconds: int = 900) -> int:
    """Re-fetch up to `limit` due pages (or the given URLs). Crawlee never revisits completed requests on its own, so each
    recrawl uses a fresh queue fed from SQLite. Success -> completed, failure -> mark_failed (retry_pending/failed);
    pages the round never reached return to their previous status. Newly found internal URLs are stored as pending."""
    claimed = claim_recrawl(limit or cfg.max_pages, urls, owner, lease_seconds)
    try:
        await crawl_round(cfg, job_id, [(u, d) for u, d, _ in claimed], set(), False, max(len(claimed), 1))
    finally:  # also runs on cancellation: nothing stays 'recrawling' because of a shutdown
        with core.connect() as c:
            c.executemany("UPDATE discovered_urls SET crawl_status=?, lease_owner=NULL, lease_expires=NULL WHERE url=? AND crawl_status='recrawling'",
                          [(prev, u) for u, _, prev in claimed])
    return len(claimed)


async def scheduler_loop(cfg: Config, every: int = 3600, stop: asyncio.Event | None = None) -> None:
    """Continuous recrawling: wake up regularly and refetch whatever is due."""
    while not (stop and stop.is_set()):
        try:
            await recrawl_due(cfg)
        except Exception:  # noqa: BLE001 - keep the scheduler alive
            log.exception("recrawl failed")
        await asyncio.sleep(every)


def config_dict(cfg: Config) -> dict:
    return asdict(cfg)

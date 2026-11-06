"""Forward crawler: crawls approved source domains, stores every URL and link, admits new domains, recrawls.

Crawlee's RequestQueue is a throwaway per-round work list. Everything permanent (what was found, what is
crawled, when to recrawl) lives in SQLite via crawler_core, so a restart loses nothing.
"""

import asyncio
import hashlib
import logging
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
from crawlee.http_clients import ImpitHttpClient
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

import crawler_core as core

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


def process_page(conn, key_url: str, base_url: str, depth: int, soup, status: int | None, ctype: str | None,
                 cfg: Config, approved: set[str], follow: bool, started: str) -> list[tuple[str, int]]:
    """Persist one successfully fetched page. Returns internal (url, depth) pairs to crawl next."""
    src_dom = core.registrable_domain(core.hostname(key_url))
    links = extract_links(soup, base_url)
    if base_url != key_url:  # redirect
        links.append((base_url, "", None, "redirect", {}))
    targets = core.active_targets(conn)
    seen_out: set[tuple[str, str]] = set()
    found_backlinks: set[tuple[int, str]] = set()
    to_queue, new_links, canonical = [], 0, None
    for dest, anchor, rel, ltype, ex in links:
        ddom = core.registrable_domain(core.hostname(dest))
        internal = ddom == src_dom
        ok = core.crawlable(dest) and ddom not in NO_CRAWL_DOMAINS
        core.save_url(conn, dest, key_url, "forward_crawl", depth + 1 if internal else 0, internal,
                      "pending" if ok else "skipped")
        new_links += conn.execute(
            "INSERT OR IGNORE INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text,"
            " rel_attributes, link_type, is_nofollow, is_sponsored, is_ugc, img_alt, position, context, source_domain)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (key_url, dest, ddom, anchor, rel, ltype, ex.get("nofollow", 0), ex.get("sponsored", 0), ex.get("ugc", 0),
             ex.get("img_alt"), ex.get("position"), ex.get("context"), src_dom)).rowcount
        conn.execute("UPDATE outgoing_links SET last_seen=?, is_active=1, lost_at=NULL, rel_attributes=?, is_nofollow=?, is_sponsored=?,"
                     " is_ugc=?, img_alt=?, position=?, context=? WHERE source_url=? AND destination_url=? AND anchor_text=?",
                     (core.now(), rel, ex.get("nofollow", 0), ex.get("sponsored", 0), ex.get("ugc", 0), ex.get("img_alt"),
                      ex.get("position"), ex.get("context"), key_url, dest, anchor))
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
            row = conn.execute("SELECT url, crawl_status, crawl_depth FROM discovered_urls WHERE normalized_url=?",
                               (core.normalize_url(dest),)).fetchone()
            if row["crawl_status"] == "pending":
                conn.execute("UPDATE discovered_urls SET crawl_status='crawling' WHERE normalized_url=?",
                             (core.normalize_url(dest),))
                to_queue.append((row["url"], row["crawl_depth"]))
    # Fetch succeeded, so absence is now real: deactivate links that vanished (history is kept, rows are not deleted).
    for r in conn.execute("SELECT id, destination_url, anchor_text FROM outgoing_links WHERE source_url=? AND is_active=1",
                          (key_url,)).fetchall():
        if (r["destination_url"], r["anchor_text"]) not in seen_out:
            conn.execute("UPDATE outgoing_links SET is_active=0, lost_at=? WHERE id=?", (core.now(), r["id"]))
    suspected = False  # one miss is only a suspicion (soft blocks, layout changes); a second consecutive miss confirms
    for b in conn.execute("SELECT id, target_domain_id, destination_url, link_status FROM backlinks WHERE source_url=? AND"
                          " is_active=1 AND link_status IN ('verified','fetch_failed','suspected_removed')", (key_url,)).fetchall():
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
    conn.execute("UPDATE backlinks SET link_status='fetch_failed' WHERE source_url=? AND link_status='verified'", (url,))
    conn.execute("INSERT INTO crawl_history (source_url, crawl_started, crawl_completed, http_status, error_message)"
                 " VALUES (?,?,?,?,?)", (url, started, core.now(), status, error[:500]))


async def crawl_round(cfg: Config, job_id: str | None, work: list[tuple[str, int]], approved: set[str], follow: bool,
                      limit: int) -> None:
    """Run one Crawlee crawl over `work` [(url, depth)]; internal links found are queued dynamically if `follow`."""
    if not work or limit <= 0:
        return
    queue = await RequestQueue.open(alias=uuid.uuid4().hex, storage_client=MemoryStorageClient())
    slots: dict[str, float] = {}
    slot_lock = asyncio.Lock()
    started: dict[str, str] = {}

    def req(url: str, depth: int) -> Request:
        return Request.from_url(url, unique_key=core.normalize_url(url) or url, user_data={"depth": depth})

    async def polite(context) -> None:  # per-domain rate limit
        dom = core.registrable_domain(core.hostname(context.request.url))
        async with slot_lock:
            wait = slots.get(dom, 0) - time.monotonic()
            slots[dom] = max(time.monotonic(), slots.get(dom, 0)) + cfg.domain_delay
        if wait > 0:
            await asyncio.sleep(wait)
        started[context.request.url] = core.now()

    async def handle(context, soup, status, ctype) -> None:
        url, base = context.request.url, context.request.loaded_url or context.request.url
        t0 = started.get(url, core.now())
        if status is not None and status >= 400:  # ignored by Crawlee so it does not retry; recorded as a permanent failure
            with core.connect() as c:
                mark_failed(c, url, f"HTTP {status}", status, cfg, t0, permanent=True)
            _job_add(job_id, "pages_failed")
            log.info("skipped %s | HTTP %s", url, status)
            return
        if base != url and not await asyncio.to_thread(core.is_safe_url, base):
            raise ValueError(f"redirect to unsafe URL blocked: {base}")
        if ctype and "html" not in ctype.lower():
            with core.connect() as c:
                c.execute("UPDATE discovered_urls SET crawl_status='skipped', content_type=?, http_status=? WHERE normalized_url=?",
                          (ctype, status, core.normalize_url(url)))
            return
        with core.connect() as c:  # one transaction per page
            nxt = process_page(c, url, base, context.request.user_data["depth"], soup, status, ctype, cfg, approved, follow, t0)
        _job_add(job_id, "pages_crawled")
        with core.connect() as c:
            n = c.execute("SELECT links_found, new_links_found FROM crawl_history WHERE source_url=? ORDER BY id DESC LIMIT 1", (url,)).fetchone()
        xlog.info("crawled %s | status %s | links %s (new %s) | queued %d", url, status, n["links_found"], n["new_links_found"], len(nxt))
        if nxt:
            await context.add_requests([req(u, d) for u, d in nxt])

    async def failed(context, error: Exception) -> None:
        log.warning("FAILED %s | %s", context.request.url, error)
        with core.connect() as c:
            mark_failed(c, context.request.url, repr(error), getattr(error, "status_code", None), cfg,
                        started.get(context.request.url, core.now()))
        _job_add(job_id, "pages_failed")

    common = dict(
        request_manager=queue, max_requests_per_crawl=limit, max_request_retries=cfg.max_retries,
        concurrency_settings=ConcurrencySettings(desired_concurrency=cfg.concurrency, max_concurrency=cfg.concurrency),
        request_handler_timeout=timedelta(seconds=cfg.timeout * 2), respect_robots_txt_file=True,
        ignore_http_error_status_codes=PERMANENT_HTTP,
    )
    if cfg.use_playwright:
        crawler = PlaywrightCrawler(browser_new_context_options={"user_agent": core.USER_AGENT}, **common)

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
    else:
        crawler = BeautifulSoupCrawler(http_client=ImpitHttpClient(timeout=cfg.timeout, browser=None, max_redirects=cfg.max_redirects,
                                                        headers={"User-Agent": core.USER_AGENT}),
                                   navigation_timeout=timedelta(seconds=cfg.timeout), **common)  # the client's own timeout is not what Crawlee applies

        @crawler.router.default_handler
        async def bs(context: BeautifulSoupCrawlingContext) -> None:
            r = context.http_response
            ctype = r.headers.get("content-type")
            if int(r.headers.get("content-length") or 0) > cfg.max_response_bytes:
                raise ValueError("response too large")
            await handle(context, context.soup, r.status_code, ctype)

    @crawler.on_skipped_request
    async def skipped(url: str, reason: SkippedReason) -> None:
        if reason != "robots_txt":
            return
        # terminal state: without this the URL stays 'crawling', is released to 'pending' and is claimed again forever
        with core.connect() as c:
            c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='robots_txt', lease_owner=NULL, lease_expires=NULL"
                      " WHERE normalized_url=? AND crawl_status IN ('crawling','recrawling')", (core.normalize_url(url),))
        log.info("skipped %s | robots.txt", url)

    crawler.pre_navigation_hook(polite)
    crawler.failed_request_handler(failed)
    await crawler.run([req(u, d) for u, d in work])


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
        async with httpx.AsyncClient(timeout=cfg.timeout, follow_redirects=True, headers={"User-Agent": core.USER_AGENT}) as client:
            sitemap = await core.discover_sitemaps(client, origin + "/")
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_source) VALUES (?, 'seed')", (domain,))
        c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (domain,))
        core.save_url(c, seed_url or origin + "/", None, "seed", 0, True)
        for u, mod in sitemap:
            if core.registrable_domain(core.hostname(u)) == domain:
                core.save_url(c, u, origin + "/sitemap.xml", "sitemap", 1, True, "pending" if core.crawlable(u) else "skipped", mod)
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
        c.execute("BEGIN IMMEDIATE")  # select + update as one unit: two claimers can never get the same row
        if urls is not None:
            norms = [n for n in map(core.normalize_url, urls) if n]
            rows = c.execute(f"SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE normalized_url IN"
                             f" ({','.join('?' * len(norms)) or 'NULL'}) AND crawl_status IN ('completed','retry_pending','failed')", norms).fetchall()
        else:
            rows = c.execute("SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE crawl_status='completed'"
                             " AND next_crawl_at IS NOT NULL AND next_crawl_at<=? ORDER BY next_crawl_at LIMIT ?",
                             (core.now(), limit)).fetchall()
        c.executemany("UPDATE discovered_urls SET crawl_status='recrawling', lease_owner=?, lease_expires=? WHERE id=?",
                      [(owner, core.later(seconds=lease_seconds), r["id"]) for r in rows])
    return [(r["url"], r["crawl_depth"], r["crawl_status"]) for r in rows]


async def recrawl_due(cfg: Config, job_id: str | None = None, urls: list[str] | None = None, limit: int | None = None,
                      owner: str = "recrawl") -> int:
    """Re-fetch up to `limit` due pages (or the given URLs). Crawlee never revisits completed requests on its own, so each
    recrawl uses a fresh queue fed from SQLite. Success -> completed, failure -> mark_failed (retry_pending/failed);
    pages the round never reached return to their previous status. Newly found internal URLs are stored as pending."""
    claimed = claim_recrawl(limit or cfg.max_pages, urls, owner)
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

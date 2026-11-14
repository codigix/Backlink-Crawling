"""Autonomous engine: bootstraps its own discovery sources, then runs five supervised workers over the PostgreSQL graph.

  python autonomous.py            # no seeds needed; Ctrl+C shuts down gracefully
  python autonomous.py seed example.com --directories   # optional: crawl these first (high priority)
  uvicorn backlink_api:app        # separate process: only reads the database and sets pause/bootstrap flags

Workers: discovery (public sources) | scheduler (leases, domain admission) | crawler (claims frontier URLs)
         | indexer (tracked-target backlinks) | verifier (recrawls due pages). PostgreSQL is the only shared state, so
pause/resume and status work from any process. Archived (Common Crawl) URLs only enter the frontier; a link becomes
a backlink only after OUR live fetch of the source page saw it.
"""

import asyncio
import json
import logging
import math
import os
import random
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import httpx

import crawler_core as core
import crawler_engine as engine

log = logging.getLogger("crawler.autonomous")
DEFAULT_FEEDS = ("https://news.ycombinator.com/rss", "https://lobste.rs/rss", "https://hnrss.org/newest")


@dataclass
class AutoConfig:
    sources: tuple = tuple(os.environ.get("AUTONOMOUS_SOURCES", "tranco,feeds,commoncrawl").split(","))
    tranco_limit: int = 10_000  # top-N ranks ever imported, in tranco_batch steps with a persisted cursor
    tranco_batch: int = 200  # ranks imported per run (incremental)
    feeds: tuple = DEFAULT_FEEDS
    cc_domains_per_run: int = 10  # was 3; jobs are claimed in one statement, the cost is CDX requests (cc_delay apart), not DB contention
    cc_urls_per_domain: int = 100
    cc_pages_per_run: int = 5  # CDX pages fetched per job and run (pagination); was 3
    cc_max_pages: int = 20  # pages per domain and index; a deliberate ceiling
    cc_failed_reset: int = 86400  # a failed job is retried from scratch after this long
    max_admitted: int = 500  # total domains allowed to be crawled
    admit_per_cycle: int = 5
    domain_page_budget: int = 200  # pages crawled per domain, ever (breadth over depth)
    per_batch: int = 25  # URLs of one domain per claim (was 10); at domain_delay 1.0s a domain takes ~25s per round, well inside lease_seconds
    claim_batch: int = 500  # was 100; 500 URLs / 25 concurrent fetches ~ a few minutes per round, so also inside lease_seconds
    seed_batch: int = 100  # max URLs per claim reserved for seeded (priority 0) domains; the rest stays for everyone else
    recrawl_batch: int = 10  # due pages per recrawl round: a big backlog is worked off in small slices
    cc_delay: float = 1.0  # seconds between Common Crawl requests (their guidance: do not hammer the index server)
    heartbeat_every: float = 15
    monitor_every: float = 60
    lease_seconds: int = 900
    source_interval: dict = field(default_factory=lambda: {"tranco": 3600, "feeds": 3600, "commoncrawl": 60})
    # Fetching is network-bound (real sites average ~10s per URL), so many fetches in flight; set CRAWL_CONCURRENCY to tune
    crawl: engine.Config = field(default_factory=lambda: engine.Config(
        max_depth=3, concurrency=int(os.environ.get("CRAWL_CONCURRENCY", 25)), domain_delay=1.0, timeout=20, max_retries=2))


# ---------- discovery sources: each returns ([(url, priority|None, depth)], new_cursor) ----------

async def src_tranco(client, cfg, cursor):
    start = int(cursor or 0)
    if start >= cfg.tranco_limit:
        return [], cursor
    list_id = (await client.get("https://tranco-list.eu/api/lists/date/latest")).raise_for_status().json()["list_id"]
    end = min(start + cfg.tranco_batch, cfg.tranco_limit)
    text = (await client.get(f"https://tranco-list.eu/download/{list_id}/{end}")).raise_for_status().text
    out = []
    for line in text.splitlines()[start:end]:
        rank, _, dom = line.partition(",")
        if rank.isdigit() and dom:
            out.append((f"https://{dom.strip()}/", 1 if int(rank) <= 1000 else 2 if int(rank) <= 10_000 else 3, 0))
    return out, str(end)


async def src_feeds(client, cfg, cursor):
    out, err = [], None
    for feed in cfg.feeds:
        try:
            root = ET.fromstring((await client.get(feed)).raise_for_status().content)
        except (httpx.HTTPError, ET.ParseError) as e:
            err = e
            continue
        for el in root.iter():
            if el.tag.rsplit("}", 1)[-1] == "link":
                out.append(((el.text or el.get("href") or "").strip(), None, 0))
    if not out and err:
        raise err
    return [o for o in out if o[0]], cursor


RETRY_DELAYS = (60, 300, 900, 3600, 21600)  # per Common Crawl attempt; after the last one the job is 'failed' (kept, with last_error)


def _cc_finish(jid: int, status: str, attempts: int = 0, err: str | None = None, retry_after: float = 0) -> None:
    """pending -> processing -> completed | retry_pending (backoff + jitter, honoring Retry-After) | failed."""
    nxt = None
    if status == "retry_pending":
        if attempts > len(RETRY_DELAYS):
            status = "failed"
        else:
            nxt = core.later(seconds=max(RETRY_DELAYS[attempts - 1], retry_after) * random.uniform(0.8, 1.2))
    with core.connect() as c:
        c.execute("UPDATE discovery_jobs SET status=?, next_retry_at=?, last_error=?, updated_at=? WHERE id=?", (status, nxt, err, core.now(), jid))


def _cc_update(jid: int, **cols) -> None:
    sets = ", ".join(f"{k}=?" for k in cols)
    with core.connect() as c:
        c.execute(f"UPDATE discovery_jobs SET {sets}, updated_at=? WHERE id=?", (*cols.values(), core.now(), jid))


async def _cc_get(client, api: str, params: dict, jid: int, att: int, rest: list):
    """One CDX request. Returns (response | None, abort_run). On any problem the job is already moved to its next state
    (retry_pending with backoff / failed) and None is returned; 429/503 additionally hand the not-yet-started jobs back."""
    try:
        resp = await client.get(api, params=params)
    except httpx.HTTPError as e:
        _cc_finish(jid, "retry_pending", att, repr(e)[:300])
        return None, False
    code = resp.status_code
    if code in (200, 404):  # CDX answers 404 "No Captures found" for a domain it has never seen: a finished, empty result
        return resp, False
    if code in (429, 503):  # slow down: this job backs off (Retry-After wins if longer), the rest wait for a later run
        ra = resp.headers.get("Retry-After", "")
        _cc_finish(jid, "retry_pending", att, f"HTTP {code}", float(ra) if ra.isdigit() else 0)
        with core.connect() as c:
            c.executemany("UPDATE discovery_jobs SET status='pending', attempts=attempts-1 WHERE id=?", [(x["id"],) for x in rest])
        return None, True
    if code >= 500:
        _cc_finish(jid, "retry_pending", att, f"HTTP {code}")
    else:  # other 4xx: our query is wrong; retrying cannot help
        _cc_finish(jid, "failed", att, f"HTTP {code}: {resp.text[:200]}")
    return None, False


async def src_commoncrawl(client, cfg, cursor):
    """Historical URLs per domain from the Common Crawl CDX index, driven by the persistent discovery_jobs queue.

    - A job exists for every admitted AND retired ('crawled') domain, created at admission (admit_domain) or here as a safety net.
    - Pagination: showNumPages gives the page count; each run fetches up to cc_pages_per_run pages per job and stores page_cursor,
      so a job resumes where it stopped (capped at cc_max_pages per domain and index).
    - The job records index_id. When a newer index appears, completed jobs go back to 'pending' (periodic rediscovery).
    - Errors keep attempts and last_error; 429/503 honour Retry-After; 'failed' jobs get a fresh start after cc_failed_reset seconds.
    Results only enter the frontier as discovered URLs; they are never reported as backlinks until our own live fetch sees the link."""
    idx_err = None
    with core.connect() as c:
        c.execute("UPDATE discovery_jobs SET status='retry_pending' WHERE status='processing' AND updated_at<?",
                  (core.later(seconds=-cfg.lease_seconds),))  # a worker died mid-query
        c.execute("UPDATE discovery_jobs SET status='retry_pending', attempts=0 WHERE source='commoncrawl' AND status='failed' AND updated_at<?",
                  (core.later(seconds=-cfg.cc_failed_reset),))  # failed jobs are not dead forever
        c.execute("INSERT OR IGNORE INTO discovery_jobs (domain_id, source) SELECT id, 'commoncrawl' FROM discovered_domains"
                  " WHERE discovery_status IN ('admitted','crawled')")  # one job per domain; retired domains are not excluded
        has_jobs = c.execute("SELECT 1 FROM discovery_jobs WHERE source='commoncrawl' LIMIT 1").fetchone()
    if has_jobs and getattr(cfg, "_cc_api", (0, None, None))[0] < time.monotonic():  # at most one collinfo request per hour
        try:
            latest = (await client.get("https://index.commoncrawl.org/collinfo.json")).raise_for_status().json()[0]
            cfg._cc_api = (time.monotonic() + 3600, latest["cdx-api"], latest.get("id") or latest["cdx-api"])  # the index changes monthly
        except (httpx.HTTPError, ValueError, KeyError, IndexError) as e:
            idx_err = e
    with core.connect() as c:
        if not idx_err and has_jobs:  # a newer index exists: finished jobs are due again (their page cursor restarts)
            c.execute("UPDATE discovery_jobs SET status='pending', page_cursor=0, total_pages=NULL, attempts=0 WHERE source='commoncrawl'"
                      " AND status='completed' AND index_id IS DISTINCT FROM ?", (cfg._cc_api[2],))
        jobs = c.execute(
            "UPDATE discovery_jobs SET status='processing', attempts=attempts+1, updated_at=? WHERE id IN (SELECT id FROM discovery_jobs"
            " WHERE source='commoncrawl' AND status IN ('pending','retry_pending') AND (next_retry_at IS NULL OR next_retry_at<=?)"
            " ORDER BY id LIMIT ?) AND status IN ('pending','retry_pending') RETURNING id, attempts, page_cursor, total_pages, index_id,"
            " (SELECT domain FROM discovered_domains WHERE id=domain_id) AS domain",
            (core.now(), core.now(), cfg.cc_domains_per_run)).fetchall()
    out = []
    if not jobs:
        return out, cursor
    if idx_err:
        for r in jobs:
            _cc_finish(r["id"], "retry_pending", r["attempts"], repr(idx_err)[:300])
        return out, cursor
    api, index_id = cfg._cc_api[1], cfg._cc_api[2]
    for n, r in enumerate(jobs):
        jid, att, dom = r["id"], r["attempts"], r["domain"]
        page, total = (r["page_cursor"], r["total_pages"]) if r["index_id"] == index_id else (0, None)  # a half-done job on an old index restarts
        base = {"url": f"{dom}/*", "output": "json", "fl": "url", "filter": ["=status:200", "=mime:text/html"]}
        got, ok = [], True
        for _ in range(cfg.cc_pages_per_run + 1):  # +1: the page-count probe
            if total is None:  # first contact: ask how many pages there are
                resp, abort = await _cc_get(client, api, {**base, "showNumPages": "true"}, jid, att, jobs[n + 1:])
                if resp is None:
                    ok = False
                    if abort:
                        return out + got, cursor
                    break
                lines = [ln for ln in resp.text.splitlines() if ln.startswith("{")] if resp.status_code == 200 else []
                try:
                    first = json.loads(lines[0]) if lines else {}
                    if "pages" in first:
                        total = min(int(first["pages"]), cfg.cc_max_pages)
                        _cc_update(jid, total_pages=total, index_id=index_id)
                        continue
                    got += [(json.loads(ln)["url"], None, 1) for ln in lines]  # server without paging: everything came in one answer
                except (ValueError, KeyError, TypeError) as e:
                    _cc_finish(jid, "retry_pending", att, f"bad response: {e!r}"[:300])
                    ok = False
                    break
                total, page = 1, 1
                break
            if page >= total:
                break
            resp, abort = await _cc_get(client, api, {**base, "page": page}, jid, att, jobs[n + 1:])
            if resp is None:
                ok = False
                if abort:
                    return out + got, cursor
                break
            try:
                got += [(json.loads(ln)["url"], None, 1) for ln in resp.text.splitlines() if ln.startswith("{")] if resp.status_code == 200 else []
            except (ValueError, KeyError) as e:
                _cc_finish(jid, "retry_pending", att, f"bad response: {e!r}"[:300])
                ok = False
                break
            page += 1
            _cc_update(jid, page_cursor=page, index_id=index_id)
            await asyncio.sleep(cfg.cc_delay)
        out += [(u, None, 1) for u, _, _ in got if core.registrable_domain(core.hostname(u)) == dom]
        if ok:
            if total is not None and page >= total:
                _cc_update(jid, status="completed", index_id=index_id, page_cursor=page, total_pages=total, last_error=None, next_retry_at=None)
            else:  # more pages remain: back in the queue for the next run (not a failure, attempts reset)
                _cc_update(jid, status="pending", attempts=0, last_error=None, next_retry_at=None)
        await asyncio.sleep(cfg.cc_delay)
    return out, cursor


SOURCES = {"tranco": src_tranco, "feeds": src_feeds, "commoncrawl": src_commoncrawl}


async def run_source(name: str, cfg: AutoConfig, client, force: bool = False) -> int:
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO discovery_sources (name) VALUES (?)", (name,))
        c.execute("UPDATE discovery_sources SET enabled=? WHERE name=?", (int(name in cfg.sources), name))
        row = c.execute("SELECT * FROM discovery_sources WHERE name=?", (name,)).fetchone()
    if not row["enabled"]:
        return 0
    if not force and row["last_run"] and row["last_run"] > core.later(seconds=-cfg.source_interval.get(name, 3600)):
        return 0
    new = 0
    try:
        urls, cursor = await SOURCES[name](client, cfg, row["cursor"])
        err = None
        def save() -> int:
            n = 0
            with core.connect() as c:
                for url, prio, depth in urls:
                    if core.normalize_url(url) and core.crawlable(url):
                        n += core.save_url(c, url, None, name, depth, True)
                        if prio:
                            c.execute("UPDATE discovered_domains SET priority=? WHERE priority>0 AND domain=?", (prio, core.registrable_domain(core.hostname(url))))
            return n
        new = await asyncio.to_thread(save)
    except Exception as e:  # noqa: BLE001 - an unavailable source must never stop the engine
        cursor, err = row["cursor"], repr(e)[:300]
        log.warning("source %s failed: %s", name, err)
    with core.connect() as c:
        c.execute("UPDATE discovery_sources SET status=?, cursor=?, items_imported=items_imported+?, runs=runs+1, last_run=?,"
                  " last_error=? WHERE name=?", ("error" if err else "ok", cursor, new, core.now(), err, name))
    log.info("source %s: %d new urls%s", name, new, f" (error: {err})" if err else "")
    return new


async def bootstrap(cfg: AutoConfig, force: bool = False) -> dict:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers={"User-Agent": core.USER_AGENT}) as client:
        return {n: await run_source(n, cfg, client, force) for n in SOURCES}


# ---------- frontier ----------

def claim_urls(owner: str, cfg: AutoConfig) -> list[tuple[str, int]]:
    """Atomically lease up to claim_batch pending URLs of admitted domains: one UPDATE ... RETURNING, so two workers
    can never get the same URL. Round-robin across domains (rn), then domain priority, then shallow-first."""
    with core.connect() as c:
        rows = c.execute(
            "UPDATE discovered_urls SET crawl_status='crawling', lease_owner=?, lease_expires=? WHERE id IN ("
            " SELECT id FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending') AND id IN ("  # SKIP LOCKED: concurrent claimers never wait on, or share, a row
            " SELECT id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY priority=0 ORDER BY rn, id) sr FROM ("
            " SELECT u.id, d.priority, COALESCE(k.n, 0) used,"
            "  ROW_NUMBER() OVER (PARTITION BY u.domain ORDER BY u.crawl_depth, u.id) rn"
            "  FROM discovered_urls u JOIN discovered_domains d ON d.domain=u.domain"
            "  LEFT JOIN (SELECT domain, COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('completed','crawling')"
            "   GROUP BY domain) k ON k.domain=u.domain"  # counted once per domain, not once per pending URL
            "  WHERE (u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?))"
            "   AND d.discovery_status='admitted' AND u.crawl_depth<=?) t1"
            "  WHERE rn<=? AND rn+used<=?) t2"  # seeded domains (priority 0) first, at most seed_batch URLs; the rest round-robin
            " ORDER BY CASE WHEN priority=0 AND sr<=? THEN 0 ELSE 1 END, rn, priority, id LIMIT ?) FOR UPDATE SKIP LOCKED)"
            " AND crawl_status IN ('pending','retry_pending') RETURNING url, crawl_depth,"
            " (SELECT priority FROM discovered_domains d WHERE d.domain=discovered_urls.domain) AS prio",
            (owner, core.later(seconds=cfg.lease_seconds), core.now(), cfg.crawl.max_depth, cfg.per_batch, cfg.domain_page_budget,
             cfg.seed_batch, cfg.claim_batch)).fetchall()
    rows.sort(key=lambda r: 99 if r["prio"] is None else r["prio"])  # seeded (priority 0) URLs first in this round's queue, not just in the claim
    seeded = [r["url"] for r in rows if r["prio"] == 0]
    if seeded:
        log.info(">>> SEED-PRIORITY: claimed %d seeded URL(s) ahead of the backlog (of %d claimed), e.g. %s", len(seeded), len(rows), seeded[:3])
    return [(r["url"], r["crawl_depth"]) for r in rows]


def release(owner: str | None = None) -> int:
    """Return leased URLs to where they came from: crawling -> pending, recrawling -> completed (crawled before).
    This owner's leases (after a round/shutdown) or any expired lease (dead worker)."""
    with core.connect() as c:
        who, arg = ("lease_owner=?", (owner,)) if owner else ("lease_expires<?", (core.now(),))
        n = c.execute("UPDATE discovered_urls SET crawl_status='pending', lease_owner=NULL, lease_expires=NULL"
                      " WHERE crawl_status='crawling' AND " + who, arg).rowcount
        c.execute("UPDATE discovered_urls SET lease_owner=NULL, lease_expires=NULL, crawl_status=CASE WHEN last_successful_crawl IS NOT NULL"
                  " THEN 'completed' ELSE 'pending' END WHERE crawl_status='recrawling' AND " + who, arg)
        return n


def renew(owner: str, seconds: int) -> int:
    with core.connect() as c:
        return c.execute("UPDATE discovered_urls SET lease_expires=? WHERE lease_owner=? AND crawl_status IN ('crawling','recrawling')",
                         (core.later(seconds=seconds), owner)).rowcount


async def with_lease(owner: str, cfg: AutoConfig, work):
    """Await `work` while a side task keeps extending this owner's leases, so a slow round is never released as 'dead'."""
    async def keep():
        while True:
            await asyncio.sleep(cfg.lease_seconds / 3)
            try:
                await asyncio.to_thread(renew, owner, cfg.lease_seconds)
            except Exception:  # noqa: BLE001 - a missed renewal only matters if it repeats for a whole lease
                log.warning("lease renewal failed for %s", owner, exc_info=True)
    t = asyncio.create_task(keep())
    try:
        return await work
    finally:
        t.cancel()


async def admit_batch(cfg: AutoConfig) -> int:
    """Admit the best-scoring discovered domains, within the global cap. Score = stored priority (lower is better) minus
    log2 of distinct referring domains, so widely linked sites go first."""
    with core.connect() as c:
        room = min(cfg.admit_per_cycle, cfg.max_admitted - c.execute(
            "SELECT COUNT(*) FROM discovered_domains WHERE discovery_status='admitted'").fetchone()[0])
        if room <= 0:
            return 0
        cands = [dict(r) for r in c.execute(
            "SELECT domain, priority, (SELECT COUNT(DISTINCT source_domain) FROM outgoing_links WHERE destination_domain=d.domain"
            " AND source_domain!=d.domain) inbound FROM (SELECT * FROM discovered_domains WHERE discovery_status='discovered'"
            " ORDER BY priority, id LIMIT 200) d")]
    score = lambda r: r["priority"] - min(4, math.log2(1 + r["inbound"]))  # noqa: E731
    admitted = 0
    for r in sorted(cands, key=score):
        if admitted >= room:
            break
        with core.connect() as c:
            c.execute("UPDATE discovered_domains SET priority=? WHERE domain=?", (max(1, round(score(r))), r["domain"]))
            if r["domain"] in engine.NO_CRAWL_DOMAINS:
                c.execute("UPDATE discovered_domains SET discovery_status='skipped' WHERE domain=?", (r["domain"],))
                continue
        admitted += await engine.admit_domain(cfg.crawl, None, r["domain"])
    return admitted


def retire_unreachable() -> int:
    """Domain-level failure handling: 5+ failed URLs and nothing ever crawled -> stop spending budget on it."""
    with core.connect() as c:
        return c.execute(
            "UPDATE discovered_domains SET discovery_status='unreachable' WHERE discovery_status='admitted'"
            " AND (SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_status='failed')>=5"
            " AND NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_status='completed')").rowcount


def retire_exhausted(cfg: AutoConfig) -> int:
    """Rolling admission: free the slot of an admitted domain whose forward work is done or page budget spent. The row stays
    (status 'crawled', so it is never re-admitted). Never while a URL is leased or still pending/retrying within max_depth.
    Pending URLs that can never be crawled are marked skipped with a reason instead of staying pending forever."""
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='max_depth', lease_owner=NULL, lease_expires=NULL"
                  " WHERE crawl_status IN ('pending','retry_pending') AND crawl_depth>?", (cfg.crawl.max_depth,))
        c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='domain_page_budget_exhausted' WHERE crawl_status IN"
                  " ('pending','retry_pending') AND domain IN (SELECT domain FROM discovered_domains WHERE discovery_status IN ('admitted','crawled'))"
                  " AND (SELECT COUNT(*) FROM discovered_urls k WHERE k.domain=discovered_urls.domain AND k.crawl_status='completed')>=?",
                  (cfg.domain_page_budget,))
        return c.execute(
            "UPDATE discovered_domains SET discovery_status='crawled' WHERE discovery_status='admitted'"
            " AND NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_status='crawling')"
            " AND ((SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_status='completed')>=?"
            "  OR NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_depth<=?"
            "   AND u.crawl_status IN ('pending','retry_pending')))", (cfg.domain_page_budget, cfg.crawl.max_depth)).rowcount


def reactivate_domains(cfg: AutoConfig) -> int:
    """A finished domain that has crawlable pending URLs again (found later, e.g. linked from another site) goes back to
    'discovered' so admit_batch re-admits it. Blocked/skipped/unreachable domains and spent page budgets stay as they are."""
    with core.connect() as c:
        return c.execute(
            "UPDATE discovered_domains SET discovery_status='discovered' WHERE discovery_status='crawled'"
            " AND EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_depth<=?"
            "  AND u.crawl_status IN ('pending','retry_pending'))"
            " AND (SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=discovered_domains.domain AND u.crawl_status='completed')<?",
            (cfg.crawl.max_depth, cfg.domain_page_budget)).rowcount


# ---------- optional targeted seeding (python autonomous.py seed ...) ----------

DIRECTORIES = ("clutch.co", "goodfirms.co", "designrush.com", "sortlist.com", "themanifest.com", "upcity.com",
               "agencyspotter.com", "topdigital.agency", "digitalagencynetwork.com", "expertise.com")


async def seed(urls: list[str], cfg: AutoConfig | None = None) -> dict[str, bool]:
    """Admit domains at top priority so the crawler visits them first. Same SSRF guard and robots rules as everything else."""
    cfg, out = cfg or AutoConfig(), {}
    for u in urls:
        u = u if "://" in u else f"https://{u}/"
        dom = core.registrable_domain(core.hostname(u))
        out[u] = await engine.admit_domain(cfg.crawl, None, dom, u)
        with core.connect() as c:
            c.execute("UPDATE discovered_domains SET priority=0 WHERE domain=?", (dom,))
        log.info(">>> SEED-PRIORITY: %s %s at priority 0 (top)", u, "admitted" if out[u] else "NOT admitted (blocked/unsafe)")
    return out


# ---------- engine state ----------

def set_paused(paused: bool) -> None:
    with core.connect() as c:
        c.execute("INSERT INTO engine_state (key, value) VALUES ('paused', ?) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                  ("1" if paused else "0",))


def is_paused() -> bool:
    with core.connect() as c:
        r = c.execute("SELECT value FROM engine_state WHERE key='paused'").fetchone()
    return bool(r and r["value"] == "1")


def _beat(name: str, status: str, error: str | None = None) -> None:
    """Record a state change. last_heartbeat_at is written only by the separate heartbeat loop (see Engine._heartbeat)."""
    ok = int(status == "idle")
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO worker_status (name) VALUES (?)", (name,))
        c.execute("UPDATE worker_status SET status=?, heartbeat=?, runs=runs+?, errors=errors+?, restart_count=restart_count+?,"
                  " last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END, last_error=COALESCE(?, last_error) WHERE name=?",
                  (status, core.now(), ok, int(error is not None), int(error is not None), bool(ok), core.now(), error, name))


def _pulse(name: str) -> None:
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO worker_status (name) VALUES (?)", (name,))
        c.execute("UPDATE worker_status SET last_heartbeat_at=? WHERE name=?", (core.now(), name))


def worker_health(row: dict, max_age: int = 60) -> bool:
    """Healthy = not in error AND its heartbeat is fresh. A worker that cannot report in is not healthy, whatever its status says."""
    return row["status"] in ("running", "idle", "paused") and bool(row["last_heartbeat_at"]) and row["last_heartbeat_at"] > core.later(seconds=-max_age)


class Engine:
    def __init__(self, cfg: AutoConfig | None = None):
        self.cfg = cfg or AutoConfig()
        self.forward_owner = uuid.uuid4().hex  # separate owners: release() of one must never reset the other's leases
        self.verify_owner = uuid.uuid4().hex
        self.stop = asyncio.Event()
        self.forward_lock = asyncio.Lock()  # forward crawling and recrawling are independent: neither waits for the other's round
        self.recrawl_lock = asyncio.Lock()
        self.tasks: dict[str, asyncio.Task] = {}  # strong references: an unreferenced task can be garbage collected mid-run
        self.specs: dict[str, tuple] = {}

    def _status(self, name: str, status: str, error: str | None = None) -> None:
        try:
            _beat(name, status, error)
        except Exception:  # noqa: BLE001 - an unavailable database must not kill a worker; the heartbeat age will show the problem
            core.BUSY_ERRORS += 1
            log.warning("could not record %s status %s", name, status, exc_info=True)

    async def _supervise(self, name: str, fn, every: float, busy_every: float | None = None) -> None:
        """Keep one worker alive: any exception (including from status writes) is logged, counted and retried with
        exponential backoff + jitter; the backoff resets after a clean run. Cancellation always propagates."""
        fails = 0
        try:
            while not self.stop.is_set():
                wait = every
                try:
                    if is_paused():
                        self._status(name, "paused")
                    else:
                        self._status(name, "running")
                        busy = await fn()
                        self._status(name, "idle")
                        fails = 0
                        wait = busy_every if (busy and busy_every is not None) else every
                except Exception as e:  # noqa: BLE001 - supervised: record, back off, keep going
                    fails += 1
                    log.exception("worker %s failed (restart #%d)", name, fails)
                    self._status(name, "error", repr(e)[:300])
                    wait = min(300, max(every, 1) * 2 ** min(fails, 6)) * random.uniform(0.8, 1.2)
                try:
                    await asyncio.wait_for(self.stop.wait(), wait)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._status(name, "stopped")

    async def _heartbeat(self, name: str) -> None:
        """Independent of the worker: reports liveness only while the worker's supervisor task is really running, and
        restarts a supervisor that died unexpectedly. A failing database write is retried and logged, never fatal."""
        while not self.stop.is_set():
            t = self.tasks.get(name)
            if t is not None and t.done() and not self.stop.is_set():
                log.error("supervisor %s ended unexpectedly (%r); restarting", name, t.exception() if not t.cancelled() else "cancelled")
                self._spawn(name)
            else:
                try:
                    await asyncio.to_thread(core.retry_locked, lambda: _pulse(name))
                except Exception:  # noqa: BLE001
                    log.exception("heartbeat failed for %s", name)
            try:
                await asyncio.wait_for(self.stop.wait(), self.cfg.heartbeat_every)
            except asyncio.TimeoutError:
                pass

    def _spawn(self, name: str) -> None:
        self.tasks[name] = asyncio.create_task(self._supervise(name, *self.specs[name]), name=f"worker-{name}")

    async def monitor(self) -> None:
        import engine_stats
        while not self.stop.is_set():
            try:
                log.info("metrics %s", json.dumps(await asyncio.to_thread(engine_stats.metrics), default=str))
            except Exception:  # noqa: BLE001
                log.exception("metrics failed")
            try:
                await asyncio.wait_for(self.stop.wait(), self.cfg.monitor_every)
            except asyncio.TimeoutError:
                pass

    async def diagnostics(self) -> None:
        """Every 10s: one log line of frontier/connection/lock/HTTP state. Own single thread, so a starved default thread pool or an
        exhausted database cannot silence it; if the snapshot itself hangs, that is logged instead."""
        import concurrent.futures
        import engine_stats
        dlog, pool = logging.getLogger("crawler.diag"), concurrent.futures.ThreadPoolExecutor(1, "diag")
        while not self.stop.is_set():
            try:
                snap = await asyncio.wait_for(asyncio.wrap_future(pool.submit(engine_stats.diagnostics, engine.HTTP)), 8)
                if os.environ.get("USE_CELERY") == "1":
                    import tasks
                    snap["celery"] = await asyncio.wait_for(asyncio.wrap_future(pool.submit(tasks.celery_stats)), 5)
                snap["workers"] = {n: ("done" if t.done() else "alive") for n, t in self.tasks.items()}
                snap["active_workers"] = sum(not t.done() for t in self.tasks.values())
                snap["asyncio_tasks"] = len(asyncio.all_tasks())
                dlog.info("DIAG %s", json.dumps(snap, default=str))
            except Exception as e:  # noqa: BLE001
                dlog.error("DIAG snapshot failed/hung: %r | open_conns=%d", e, core.OPEN_CONNS)
            try:
                await asyncio.wait_for(self.stop.wait(), 10)
            except asyncio.TimeoutError:
                pass

    async def discovery(self) -> bool:
        with core.connect() as c:  # POST /discovery/bootstrap leaves a flag here; only this process ever fetches sources
            asked = c.execute("DELETE FROM engine_state WHERE key='bootstrap_requested'").rowcount
        await bootstrap(self.cfg, force=bool(asked))
        return False

    async def scheduler(self) -> bool:
        with core.connect() as c:  # POST /crawler/seed leaves seed:<url> flags; admit them at top priority, then clear them
            keys = [r["key"] for r in c.execute("SELECT key FROM engine_state WHERE key LIKE 'seed:%'")]
        if keys:
            log.info(">>> SEED-PRIORITY: received %d seed(s) from the API: %s", len(keys), [k.removeprefix("seed:") for k in keys])
            await seed([k.removeprefix("seed:") for k in keys], self.cfg)
            with core.connect() as c:
                c.executemany("DELETE FROM engine_state WHERE key=?", [(k,) for k in keys])
        await asyncio.to_thread(lambda: (release(), retire_unreachable(), retire_exhausted(self.cfg), reactivate_domains(self.cfg)))
        return bool(await admit_batch(self.cfg))

    async def crawler(self) -> bool:
        if self.stop.is_set():
            return False
        if os.environ.get("USE_CELERY") == "1":  # Celery workers crawl; this loop keeps their queue fed and recovers dead leases
            import tasks
            await asyncio.to_thread(tasks.recover)
            return bool(await asyncio.to_thread(tasks.dispatch))
        work = await asyncio.to_thread(claim_urls, self.forward_owner, self.cfg)
        if not work:
            return False
        try:
            async with self.forward_lock:
                # follow=False: new internal URLs stay in the frontier and are claimed fairly next cycle
                await with_lease(self.forward_owner, self.cfg,
                                 engine.crawl_round(self.cfg.crawl, None, work, engine._approved(), False, len(work)))
        finally:
            release(self.forward_owner)
        return True

    async def indexer(self) -> bool:
        with core.connect() as c:
            for _, domain in core.active_targets(c):
                core.match_target_from_graph(c, domain)
        return False

    async def verifier(self) -> bool:
        if self.stop.is_set():
            return False
        async with self.recrawl_lock:  # never shared with the forward crawl, and only a small slice per round
            return bool(await with_lease(self.verify_owner, self.cfg, engine.recrawl_due(
                self.cfg.crawl, limit=self.cfg.recrawl_batch, owner=self.verify_owner, lease_seconds=self.cfg.lease_seconds)))

    async def run(self) -> None:
        with core.connect() as c:
            core.migrate(c)  # also resets URLs a dead process left 'crawling'/'recrawling'
        with core.connect() as c:
            n = {k: c.execute(q).fetchone()[0] for k, q in (
                ("crawled", "SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='completed'"),
                ("queued", "SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='pending'"),
                ("domains", "SELECT COUNT(*) FROM discovered_domains"), ("links", "SELECT COUNT(*) FROM outgoing_links"))}
        log.info("autonomous engine started, resuming from saved state: %s | sources: %s", n, ",".join(self.cfg.sources))
        self.specs = {"discovery": (self.discovery, 30), "scheduler": (self.scheduler, 30, 1), "crawler": (self.crawler, 15, 1),
                      "indexer": (self.indexer, 300), "verifier": (self.verifier, 60, 5)}
        for name in self.specs:
            self._spawn(name)
        side = [asyncio.create_task(self._heartbeat(n), name=f"heartbeat-{n}") for n in self.specs] + [asyncio.create_task(self.monitor()), asyncio.create_task(self.diagnostics())]
        try:
            await self.stop.wait()
        finally:  # normal stop or cancellation (Ctrl+C): give workers a moment, then stop everything and return all leases
            self.stop.set()
            for t in side:
                t.cancel()
            _, pending = await asyncio.wait(list(self.tasks.values()), timeout=10)
            for t in pending:
                t.cancel()
            await asyncio.gather(*side, *self.tasks.values(), return_exceptions=True)
            release(self.forward_owner)
            release(self.verify_owner)


if __name__ == "__main__":
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%H:%M:%S"))
    logging.getLogger("crawler").addHandler(h)
    logging.getLogger("crawler").setLevel(logging.INFO)
    logging.getLogger("crawler").propagate = False
    for n in ("BeautifulSoupCrawler", "PlaywrightCrawler"):  # we already log one FAILED line per URL; drop Crawlee's duplicate traceback
        logging.getLogger(n).addFilter(lambda r: "reached maximum retries" not in r.getMessage())
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one INFO line per sitemap request buries real problems
    logging.getLogger("crawlee._utils.robots").setLevel(logging.ERROR)  # unreachable hosts reset robots.txt fetches constantly
    if sys.argv[1:2] == ["seed"]:  # python autonomous.py seed growthpixelagency.com [--directories] [more domains/urls]
        args = [a for a in sys.argv[2:] if a != "--directories"]
        with core.connect() as c:
            core.migrate(c, recover=False)  # the engine may be running in another terminal
        print(asyncio.run(seed(args + (list(DIRECTORIES) if "--directories" in sys.argv else []))))
        raise SystemExit
    try:
        asyncio.run(Engine().run())
    except KeyboardInterrupt:
        log.info("shut down; leased URLs returned to the frontier")

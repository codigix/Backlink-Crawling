"""Autonomous engine: bootstraps its own discovery sources, then runs five supervised workers over the PostgreSQL graph.

  python autonomous.py            # no seeds needed; Ctrl+C shuts down gracefully
  python autonomous.py seed example.com --directories   # optional: crawl these first (high priority)
  uvicorn backlink_api:app        # separate process: only reads the database and sets pause/bootstrap flags

Workers: discovery (public sources) | scheduler (leases, domain admission) | crawler (claims frontier URLs)
         | indexer (tracked-target backlinks) | verifier (recrawls due pages). PostgreSQL is the only shared state, so
pause/resume and status work from any process. A link becomes a backlink only after OUR live fetch of the source page saw it.
"""

import asyncio
import contextlib
import json
import logging
import math
import os
import random
import socket
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import httpx

import business_jobs
import ddg_lookup  # noqa: F401 - the job search phase calls ddg_lookup.search_urls; kept importable as autonomous.ddg_lookup
import crawler_core as core
import crawler_engine as engine

log = logging.getLogger("crawler.autonomous")
NORMAL_PRIORITY = 5

DEFAULT_FEEDS = ("https://news.ycombinator.com/rss", "https://lobste.rs/rss", "https://hnrss.org/newest")


@dataclass
class AutoConfig:
    sources: tuple = tuple(x for x in os.environ.get("AUTONOMOUS_SOURCES", "").split(",") if x)  # business-driven by default; "tranco,feeds" re-enables generic discovery
    tranco_limit: int = 10_000  # top-N ranks ever imported, in tranco_batch steps with a persisted cursor
    tranco_batch: int = 200  # ranks imported per run (incremental)
    feeds: tuple = DEFAULT_FEEDS
    max_admitted: int = 500  # total domains allowed to be crawled
    admit_per_cycle: int = 5
    domain_page_budget: int = 200  # pages crawled per domain, ever (breadth over depth)
    per_batch: int = 25  # URLs of one domain per claim (was 10); at domain_delay 1.0s a domain takes ~25s per round, well inside lease_seconds
    claim_batch: int = int(os.environ.get("CRAWL_CLAIM_BATCH", 500))  # URLs one worker leases per round; smaller = faster warm shutdown (a worker finishes its batch) but more claims. was 100; 500 URLs / 25 concurrent fetches ~ a few minutes per round, so also inside lease_seconds
    seed_batch: int = 100  # max URLs per claim reserved for seeded (priority 0) domains; the rest stays for everyone else
    recrawl_batch: int = 10  # due pages per recrawl round: a big backlog is worked off in small slices
    heartbeat_every: float = 15
    monitor_every: float = 60
    lease_seconds: int = 900
    # business onboarding capacity (see business_jobs.py); env defaults, validated below
    max_crawl_workers: int = business_jobs.MAX_CRAWL_WORKERS
    business_reserved_workers: int = business_jobs.BUSINESS_RESERVED_WORKERS
    max_jobs_per_business: int = business_jobs.MAX_CONCURRENT_JOBS_PER_BUSINESS
    source_interval: dict = field(default_factory=lambda: {"tranco": 3600, "feeds": 3600})
    # Fetching is network-bound (real sites average ~10s per URL), so many fetches in flight; set CRAWL_CONCURRENCY to tune
    crawl: engine.Config = field(default_factory=lambda: engine.Config(
        max_depth=3, concurrency=int(os.environ.get("CRAWL_CONCURRENCY", 25)), domain_delay=1.0, timeout=20, max_retries=2))

    def __post_init__(self):
        business_jobs.validate(self.max_crawl_workers, self.business_reserved_workers, self.max_jobs_per_business)


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


SOURCES = {"tranco": src_tranco, "feeds": src_feeds}


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

def claim_urls(owner: str, cfg: AutoConfig, conn=None) -> list[tuple[str, int]]:
    """Atomically lease up to claim_batch pending URLs of admitted domains: one UPDATE ... RETURNING, so two workers
    can never get the same URL. Round-robin across domains (rn), then domain priority, then shallow-first."""
    with core.connect() as c:
        rows = c.execute(
            # lease_gen+1 = the fencing token: without it a stale worker and the worker that re-claimed the URL hold the same token
            "UPDATE discovered_urls SET crawl_status='crawling', lease_owner=?, lease_expires=?, lease_gen=lease_gen+1 WHERE id IN ("
            " SELECT id FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending') AND id IN ("  # SKIP LOCKED: concurrent claimers never wait on, or share, a row
            " SELECT id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY priority=0 ORDER BY rn, id) sr FROM ("
            " SELECT u.id, d.priority, COALESCE(k.n, 0) used,"
            "  ROW_NUMBER() OVER (PARTITION BY u.domain ORDER BY u.crawl_depth, u.id) rn"
            "  FROM discovered_urls u JOIN discovered_domains d ON d.domain=u.domain"
            "  LEFT JOIN (SELECT domain, COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('completed','crawling')"
            "   GROUP BY domain) k ON k.domain=u.domain"  # counted once per domain, not once per pending URL
            "  WHERE (u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?))"
            "   AND d.discovery_status='admitted' AND d.priority>=0 AND u.crawl_depth<=?) t1"
            "  WHERE rn<=? AND rn+used<=?) t2"  # seeded domains (priority 0) first, at most seed_batch URLs; the rest round-robin
            " ORDER BY CASE WHEN priority=0 AND sr<=? THEN 0 ELSE 1 END, rn, priority, id LIMIT ?) FOR UPDATE SKIP LOCKED)"
            " AND crawl_status IN ('pending','retry_pending') RETURNING url, normalized_url, lease_gen, crawl_depth,"
            " (SELECT priority FROM discovered_domains d WHERE d.domain=discovered_urls.domain) AS prio",
            (owner, core.later(seconds=cfg.lease_seconds), core.now(), cfg.crawl.max_depth, cfg.per_batch, cfg.domain_page_budget,
             cfg.seed_batch, cfg.claim_batch)).fetchall()
    engine.FENCE.update({r["normalized_url"]: r["lease_gen"] for r in rows})
    rows.sort(key=lambda r: 99 if r["prio"] is None else r["prio"])  # seeded (priority 0) URLs first in this round's queue, not just in the claim
    seeded = [r["url"] for r in rows if r["prio"] == 0]
    if seeded:
        log.info(">>> SEED-PRIORITY: claimed %d seeded URL(s) ahead of the backlog (of %d claimed), e.g. %s", len(seeded), len(rows), seeded[:3])
    return [(r["url"], r["crawl_depth"]) for r in rows]


def claim_work(owner: str, cfg: AutoConfig) -> list[tuple[str, int]]:
    """The one scheduler entry point for crawl workers: business onboarding URLs first (fair between businesses), otherwise
    autonomous URLs if the reserved business capacity allows. One transaction under an advisory lock, so concurrent workers in any
    process see each other's leases and the capacity rules cannot be raced."""
    with core.connect() as c:
        c.execute("SELECT pg_advisory_xact_lock(?)", (business_jobs.LOCK,))
        work = business_jobs.claim(c, owner, cfg)
        if not work and business_jobs.autonomous_allowed(c, owner, cfg.max_crawl_workers, cfg.business_reserved_workers):
            work = claim_urls(owner, cfg, c)
    return work


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


SCHED_LOCK = 724003  # advisory-lock class; the 2nd key is the schema, so staging/test schemas never contend with production
retire_stats: dict = {}  # last EXHAUSTED-RETIREMENT numbers, read by the cycle metrics


def try_scheduler_lock(owner: str):
    """(connection, owner) when this engine now owns the scheduler, else (None, current owner). The lock is a session-level advisory lock on a
    dedicated idle connection (autocommit: no transaction stays open for the whole cycle). It disappears with the connection, so a crashed or
    killed engine can never leave the scheduler locked."""
    c = core.connect_unpooled()  # session-level advisory lock: this connection must never be handed to anybody else, so it is not from the pool
    try:
        c.autocommit = True
        if c.execute("SELECT pg_try_advisory_lock(?, hashtext(current_schema()))", (SCHED_LOCK,)).fetchone()[0]:
            c.execute("INSERT INTO engine_state (key, value) VALUES ('scheduler_owner', ?) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (owner,))
            return c, owner
        holder = c.execute("SELECT value FROM engine_state WHERE key='scheduler_owner'").fetchone()
        c.close()
        return None, holder[0] if holder else "unknown"
    except BaseException:
        c.close()
        raise


def release_scheduler_lock(c) -> None:
    try:
        c.execute("SELECT pg_advisory_unlock(?, hashtext(current_schema()))", (SCHED_LOCK,))
    except Exception:  # noqa: BLE001 - closing the connection releases it anyway
        log.warning("could not unlock the scheduler explicitly; closing the connection releases it", exc_info=True)
    finally:
        c.close()


def _bump(key: str) -> int:
    with core.connect() as c:
        return int(c.execute("INSERT INTO engine_state (key, value) VALUES (?, '1') ON CONFLICT (key) DO UPDATE SET value=(engine_state.value::int+1)::text RETURNING value", (key,)).fetchone()[0])


def record_cycle(status: str, detail: dict) -> None:
    """Persist what the last scheduler cycle was: completed (all operations ok), completed_with_errors, failed or skipped."""
    t = core.now()
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO worker_status (name) VALUES ('scheduler')")
        c.execute("UPDATE worker_status SET last_cycle_at=?, last_cycle_status=?, last_cycle_detail=?,"
                  " last_partial_at=CASE WHEN ?='completed_with_errors' THEN ? ELSE last_partial_at END,"
                  " last_failed_at=CASE WHEN ?='failed' THEN ? ELSE last_failed_at END WHERE name='scheduler'",
                  (t, status, json.dumps(detail, default=str)[:4000], status, t, status, t))


ADMIT_TIMEOUT = float(os.environ.get("ADMIT_TIMEOUT_SECONDS", 30))  # one domain's admission (robots, sitemaps: many requests); a slow host must not stall the cycle
RETIRE_BATCH = int(os.environ.get("RETIRE_BATCH_ROWS", 5000))  # rows per retirement transaction
RETIRE_BUDGET_S = float(os.environ.get("RETIRE_BUDGET_SECONDS", 120))  # retirement stops starting batches after this; the rest resumes next cycle
ADMIT_CYCLE_BUDGET =float(os.environ.get("ADMIT_CYCLE_BUDGET_SECONDS", 60))  # no new admission is started after this much time in one cycle
ADMIT_MAX_FAILURES = 3  # consecutive failed/timed-out admissions after which a domain is marked unreachable instead of blocking the head of the queue again


SEED_TIMEOUT = float(os.environ.get("SEED_TIMEOUT_SECONDS", 120))  # one unreachable host must not hold the whole scheduler cycle


def timed_op(name: str, fn, *args):
    """Run one scheduler operation: always logs its duration; a failure is logged with the operation's name and returned (never raised), so the
    independent operations after it still run. Returns (ok, result)."""
    t0 = time.monotonic()
    try:
        r = fn(*args)
    except Exception as e:  # noqa: BLE001 - isolation between independent operations; the caller reports the failures
        log.error("SCHEDULER-OP %s FAILED duration=%.2fs error=%r", name, time.monotonic() - t0, e)
        log.debug("SCHEDULER-OP %s traceback", name, exc_info=True)
        return False, None
    log.info("SCHEDULER-OP %s duration=%.2fs result=%s", name, time.monotonic() - t0, r)
    return True, r


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
    admitted, t_end = 0, time.monotonic() + ADMIT_CYCLE_BUDGET
    for r in sorted(cands, key=score):
        if admitted >= room or time.monotonic() > t_end:
            break
        try:
            with core.connect() as c:
                c.execute("SET LOCAL lock_timeout = '3s'")  # the domain row may be held by a crawl's long page transaction: skip it now, retry next cycle
                c.execute("UPDATE discovered_domains SET priority=? WHERE domain=?", (max(1, round(score(r))), r["domain"]))
                if r["domain"] in engine.NO_CRAWL_DOMAINS:
                    c.execute("UPDATE discovered_domains SET discovery_status='skipped' WHERE domain=?", (r["domain"],))
                    continue
        except core.psycopg.errors.LockNotAvailable:
            log.warning("DOMAIN-ADMISSION %s: its row is locked by a crawl, skipped until the next cycle", r["domain"])
            continue
        try:
            admitted += await asyncio.wait_for(engine.admit_domain(cfg.crawl, None, r["domain"]), ADMIT_TIMEOUT)
        except Exception as e:  # noqa: BLE001 - includes TimeoutError: this domain failed, the others are still admitted
            n = await asyncio.to_thread(_bump, "admit_fail:" + r["domain"])
            log.error("DOMAIN-ADMISSION %s failed after the %.0fs limit (%d/%d): %r", r["domain"], ADMIT_TIMEOUT, n, ADMIT_MAX_FAILURES, e)
            if n >= ADMIT_MAX_FAILURES:
                with core.connect() as c:
                    c.execute("UPDATE discovered_domains SET discovery_status='unreachable' WHERE domain=?", (r["domain"],))
    return admitted


def retire_unreachable() -> int:
    """Domain-level failure handling: 5+ failed URLs and nothing ever crawled -> stop spending budget on it."""
    with core.connect() as c:
        # domain rows are hot (every crawled page upserts its domain inside a long transaction): SKIP LOCKED = try again next cycle, never wait for the lock timeout
        return c.execute(
            "UPDATE discovered_domains SET discovery_status='unreachable' WHERE id IN (SELECT d.id FROM discovered_domains d WHERE d.discovery_status='admitted'"
            " AND (SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_status='failed')>=5"
            " AND NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_status='completed') FOR UPDATE OF d SKIP LOCKED)").rowcount


def retire_exhausted(cfg: AutoConfig) -> int:
    """Rolling admission: free the slot of an admitted domain whose forward work is done or page budget spent. The row stays
    (status 'crawled', so it is never re-admitted). Never while a URL is leased or still pending/retrying within max_depth.
    Pending URLs that can never be crawled are marked skipped with a reason instead of staying pending forever.
    Set-based: over-budget domains are found once (GROUP BY the completed rows; a domain's budget is spent when it has >= domain_page_budget
    'completed' URLs, exactly the old per-row COUNT(*) rule) and only their pending rows are touched. Row updates use FOR UPDATE SKIP LOCKED:
    a URL a Celery worker is claiming right now is left for the next cycle instead of making this statement wait for the lock timeout."""
    t0 = time.monotonic()
    deadline = t0 + RETIRE_BUDGET_S
    # Bounded batches, one transaction (commit) each: a 300 s statement used to be cancelled and rolled back, so it never progressed. Now every
    # committed batch is permanent progress; the same eligibility rules apply and a skipped row leaves the predicate, so a re-run or restart resumes.
    def batched(sql: str, args: tuple) -> int:
        total = 0
        while time.monotonic() < deadline:
            with core.connect() as c:
                n = c.execute(sql, (*args, RETIRE_BATCH)).rowcount
            total += n
            if n < RETIRE_BATCH:  # drained (or the rest is row-locked by crawls: next cycle)
                break
        return total

    t1 = time.monotonic()
    depth = batched("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='max_depth', lease_owner=NULL, lease_expires=NULL WHERE id IN ("
                    " SELECT id FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending') AND crawl_depth>? ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED)",
                    (cfg.crawl.max_depth,))
    t2 = time.monotonic()
    budget = 0
    if t2 < deadline:
        with core.connect() as c:  # over-budget domains found once per cycle (read only), then their pending rows are retired in batches
            over = [r[0] for r in c.execute("SELECT k.domain FROM discovered_urls k WHERE k.crawl_status='completed' GROUP BY k.domain HAVING COUNT(*)>=?",
                                            (cfg.domain_page_budget,)).fetchall()]
        budget = batched("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='domain_page_budget_exhausted' WHERE id IN ("
                         " SELECT id FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending') AND domain = ANY(?::text[])"
                         "  AND domain IN (SELECT domain FROM discovered_domains WHERE discovery_status IN ('admitted','crawled')) ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED)",
                         (over,)) if over else 0
    t3 = time.monotonic()
    with core.connect() as c:
        retired = c.execute(
            "UPDATE discovered_domains SET discovery_status='crawled' WHERE id IN (SELECT d.id FROM discovered_domains d WHERE d.discovery_status='admitted'"
            " AND NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_status='crawling')"
            " AND ((SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_status='completed')>=?"
            "  OR NOT EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_depth<=?"
            "   AND u.crawl_status IN ('pending','retry_pending'))) FOR UPDATE OF d SKIP LOCKED)", (cfg.domain_page_budget, cfg.crawl.max_depth)).rowcount
        t4 = time.monotonic()
    retire_stats.update(urls_retired=depth + budget, domains_retired=retired)
    log.info("EXHAUSTED-RETIREMENT depth_skipped=%d budget_skipped=%d domains_retired=%d sql_s=[depth %.2f, budget %.2f, domains %.2f] txn_s=%.2f",
             depth, budget, retired, t2 - t1, t3 - t2, t4 - t3, t4 - t0)
    return retired


def reactivate_domains(cfg: AutoConfig) -> int:
    """A finished domain that has crawlable pending URLs again (found later, e.g. linked from another site) goes back to
    'discovered' so admit_batch re-admits it. Blocked/skipped/unreachable domains and spent page budgets stay as they are."""
    with core.connect() as c:
        return c.execute(
            "UPDATE discovered_domains SET discovery_status='discovered' WHERE id IN (SELECT d.id FROM discovered_domains d WHERE d.discovery_status='crawled'"
            " AND EXISTS (SELECT 1 FROM discovered_urls u WHERE u.domain=d.domain AND u.crawl_depth<=?"
            "  AND u.crawl_status IN ('pending','retry_pending'))"
            # budget not spent <=> not among the domains with >= budget completed URLs (found once; a domain with none counts 0 < budget)
            " AND NOT EXISTS (SELECT 1 FROM (SELECT k.domain FROM discovered_urls k WHERE k.crawl_status='completed' GROUP BY k.domain HAVING COUNT(*)>=?) o"
            "  WHERE o.domain=d.domain) FOR UPDATE OF d SKIP LOCKED)",
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


class PartialCycle(Exception):
    """A scheduler cycle that ran to the end but had failing operations: recorded as a cycle (runs+1) with its error, but it is NOT a success
    (last_success_at stays) and NOT a crash (no restart, no backoff)."""


class CycleSkipped(Exception):
    """Another engine owns the scheduler right now: nothing ran, nothing failed."""


def _beat(name: str, status: str, error: str | None = None, partial: str | None = None, skipped: bool = False) -> None:
    """Record a state change. last_heartbeat_at is written only by the separate heartbeat loop (see Engine._heartbeat)."""
    ok = status == "idle" and partial is None and not skipped  # only a fully successful run advances last_success_at
    counts = status == "idle" and not skipped
    err = error if error is not None else partial
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO worker_status (name) VALUES (?)", (name,))
        c.execute("UPDATE worker_status SET status=?, heartbeat=?, runs=runs+?, errors=errors+?, restart_count=restart_count+?,"
                  " last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END, last_error=COALESCE(?, last_error) WHERE name=?",
                  (status, core.now(), int(counts), int(err is not None), int(error is not None), ok, core.now(), err, name))


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
        self.sched_owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"  # who holds the scheduler lock, shown to a second engine
        self.tasks: dict[str, asyncio.Task] = {}  # strong references: an unreferenced task can be garbage collected mid-run
        self.specs: dict[str, tuple] = {}

    def _status(self, name: str, status: str, error: str | None = None, partial: str | None = None, skipped: bool = False) -> None:
        try:
            _beat(name, status, error, partial, skipped)
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
                        try:
                            busy = await fn()
                            self._status(name, "idle")
                        except PartialCycle as e:  # ran to the end with failed operations: visible, but not a crash
                            busy = False
                            self._status(name, "idle", partial=str(e)[:300])
                        except CycleSkipped:
                            busy = False
                            self._status(name, "idle", skipped=True)
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

    async def lookups(self) -> bool:
        """Business jobs waiting for their DuckDuckGo search (bounded concurrency, provider cooldown): see business_jobs.search_pending."""
        return await business_jobs.search_pending()

    async def business(self) -> bool:
        """Advance business jobs: expand crawled pages one level, refresh counters, close finished jobs (freeing their capacity)."""
        return bool(await asyncio.to_thread(business_jobs.tick))

    async def _seeds(self) -> tuple[list[str], int]:
        """POST /crawler/seed leaves seed:<url> flags. Each is processed on its own and its flag is deleted only after that seed succeeded
        (admitted or refused as unsafe: both are final answers); a failure or timeout keeps the flag, so the next cycle retries just that one."""
        with core.connect() as c:
            keys = [r["key"] for r in c.execute("SELECT key FROM engine_state WHERE key LIKE 'seed:%' ORDER BY key")]
        failed = []
        total = len(keys)
        if keys:
            log.info(">>> SEED-PRIORITY: received %d seed(s) from the API: %s", len(keys), [k.removeprefix("seed:") for k in keys])
        for k in keys:
            url, t0 = k.removeprefix("seed:"), time.monotonic()
            try:
                out = await asyncio.wait_for(seed([url], self.cfg), SEED_TIMEOUT)
            except Exception as e:  # noqa: BLE001 - includes TimeoutError
                log.error("SEED-FAILED %s after %.1fs: %r (flag kept, retried next cycle)", url, time.monotonic() - t0, e)
                failed.append(url)
                continue
            with core.connect() as c:
                c.execute("DELETE FROM engine_state WHERE key=?", (k,))
            log.info("SEED-DONE %s admitted=%s duration=%.1fs (flag cleared)", url, next(iter(out.values()), None), time.monotonic() - t0)
        return failed, total

    async def scheduler(self) -> bool:
        """One scheduler cycle, owned by exactly one engine at a time (PostgreSQL advisory lock, see try_scheduler_lock). Raises CycleSkipped when
        another engine owns it, PartialCycle when some operations failed, RuntimeError when everything failed."""
        t0 = time.monotonic()
        lock, holder = await asyncio.to_thread(try_scheduler_lock, self.sched_owner)
        if lock is None:
            n = await asyncio.to_thread(_bump, "scheduler_lock_contention")
            log.info("SCHEDULER-LOCK busy owner=%s", holder)
            log.info("SCHEDULER-CYCLE-SKIPPED reason=lock_busy contention_total=%d", n)
            await asyncio.to_thread(record_cycle, "skipped", {"owner": holder, "scheduler_lock_contention": n})
            raise CycleSkipped(holder)
        log.info("SCHEDULER-LOCK acquired owner=%s", self.sched_owner)
        try:
            return await self._cycle(t0)
        finally:
            await asyncio.to_thread(release_scheduler_lock, lock)

    async def _cycle(self, t0: float) -> bool:
        """The operations are independent, each is timed and isolated: a failing one is logged by name and the rest still run."""
        log.info("SCHEDULER-CYCLE-START")
        retire_stats.clear()  # a failed retirement must not report the previous cycle's numbers
        ops: dict[str, dict] = {}

        def done(name: str, ok: bool, dur: float, result=None, error=None) -> None:
            ops[name] = {"status": "SUCCESS" if ok else "FAILED", "duration_s": round(dur, 2), "result": result, "error": error}

        t1, pending_seeds, failed_seeds = time.monotonic(), 0, []
        try:
            failed_seeds, pending_seeds = await self._seeds()
            done("SEEDS", not failed_seeds, time.monotonic() - t1, {"pending": pending_seeds, "failed": failed_seeds}, f"seed(s) failed: {failed_seeds}" if failed_seeds else None)
        except Exception as e:  # noqa: BLE001 - e.g. the flag query itself
            done("SEEDS", False, time.monotonic() - t1, error=repr(e))
        for name, fn in (("LEASE-RECOVERY", release), ("UNREACHABLE-RETIREMENT", retire_unreachable),
                         ("EXHAUSTED-RETIREMENT", lambda: retire_exhausted(self.cfg)), ("DOMAIN-REACTIVATION", lambda: reactivate_domains(self.cfg))):
            t1 = time.monotonic()
            ok, res = await asyncio.to_thread(timed_op, name, fn)
            done(name, ok, time.monotonic() - t1, res, None if ok else "see log")
        t1, admitted = time.monotonic(), 0
        try:
            admitted = await admit_batch(self.cfg)
            done("DOMAIN-ADMISSION", True, time.monotonic() - t1, admitted)
        except Exception as e:  # noqa: BLE001
            log.error("SCHEDULER-OP DOMAIN-ADMISSION FAILED duration=%.2fs error=%r", time.monotonic() - t1, e)
            done("DOMAIN-ADMISSION", False, time.monotonic() - t1, error=repr(e))
        failed = [n for n, o in ops.items() if o["status"] == "FAILED"]
        worked = [n for n in ops if not (n == "SEEDS" and pending_seeds == 0)]  # an idle seed step is no evidence that anything worked
        status = "completed" if not failed else "failed" if all(n in failed for n in worked) else "completed_with_errors"
        dur = time.monotonic() - t0
        for n, o in ops.items():
            log.info("SCHEDULER-OP %-22s %-7s duration=%.2fs%s", n, o["status"], o["duration_s"], f" error={o['error']}" if o["error"] else "")
        log.info("SCHEDULER-CYCLE-%s duration=%.1fs failed=%s", status.upper(), dur, failed or "none")
        stats = {**retire_stats}
        metrics = {"scheduler_cycle_duration_seconds": round(dur, 2), "scheduler_cycle_status": status,
                   "scheduler_operation_duration_seconds": {n: o["duration_s"] for n, o in ops.items()},
                   "scheduler_operation_failures": {n: o["error"] for n, o in ops.items() if o["status"] == "FAILED"},
                   "exhausted_domains_found": stats.get("domains_retired", 0), "urls_retired": stats.get("urls_retired", 0),
                   "domains_admitted": admitted if isinstance(admitted, int) else 0, "domains_reactivated": ops["DOMAIN-REACTIVATION"]["result"] or 0,
                   "expired_leases_recovered": ops["LEASE-RECOVERY"]["result"] or 0, "pending_seed_count": pending_seeds}
        await asyncio.to_thread(record_cycle, status, metrics)
        log.info("SCHEDULER-METRICS %s", json.dumps(metrics, default=str))
        if status == "failed":
            raise RuntimeError(f"every scheduler operation failed: {failed}")  # the supervisor backs off and counts a restart
        if status == "completed_with_errors":
            raise PartialCycle(f"failed operations: {failed}")
        return bool(admitted)

    async def crawler(self) -> bool:
        if self.stop.is_set():
            return False
        if os.environ.get("USE_CELERY") == "1":  # Celery workers crawl; this loop keeps their queue fed and recovers dead leases
            import tasks
            await asyncio.to_thread(tasks.recover)
            return bool(await asyncio.to_thread(tasks.dispatch))
        await asyncio.to_thread(release)  # leases of a dead engine process: without this they wait for a restart and its job never closes
        work = await asyncio.to_thread(claim_work, self.forward_owner, self.cfg)
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

    @staticmethod
    def _business_waiting() -> bool:
        with core.connect() as c:
            return business_jobs.waiting(c)

    @staticmethod
    def _index() -> None:
        with core.connect() as c:
            for _, domain in core.active_targets(c):
                core.match_target_from_graph(c, domain)

    async def indexer(self) -> bool:
        # blocking DB work (one query per link, minutes for a big target): off the event loop, or every other worker freezes with it
        await asyncio.to_thread(self._index)
        return False

    async def verifier(self) -> bool:
        if self.stop.is_set():
            return False
        if await asyncio.to_thread(self._business_waiting):  # P3 yields to onboarding; shortcut: a permanently busy business queue defers all rechecks
            return False
        async with self.recrawl_lock:  # never shared with the forward crawl, and only a small slice per round
            return bool(await with_lease(self.verify_owner, self.cfg, engine.recrawl_due(
                self.cfg.crawl, limit=self.cfg.recrawl_batch, owner=self.verify_owner, lease_seconds=self.cfg.lease_seconds)))

    async def run(self) -> None:
        with core.connect() as c:
            core.migrate(c)  # also resets URLs a dead process left 'crawling'/'recrawling'
        business_jobs.reset_interrupted()  # a search in flight died with the old process; crawling jobs resume from job_urls
        with core.connect() as c:
            n = {k: c.execute(q).fetchone()[0] for k, q in (
                ("crawled", "SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='completed'"),
                ("queued", "SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='pending'"),
                ("domains", "SELECT COUNT(*) FROM discovered_domains"), ("links", "SELECT COUNT(*) FROM outgoing_links"))}
        log.info("autonomous engine started, resuming from saved state: %s | sources: %s", n, ",".join(self.cfg.sources))
        self.specs = {"discovery": (self.discovery, 30), "scheduler": (self.scheduler, 30, 1), "crawler": (self.crawler, 15, 1),
                      "indexer": (self.indexer, 300), "verifier": (self.verifier, 60, 5),
                      "lookups": (self.lookups, 5), "business": (self.business, 5, 1)}  # own workers: a slow search or admission round never delays the others
        log.info("BUSINESS-WORKER-STARTED workers=%s ddgs_enabled=%s", ",".join(self.specs), business_jobs.DDGS_ENABLED)
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

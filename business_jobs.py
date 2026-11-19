"""Business onboarding: one discovery job per business, scheduled ahead of autonomous crawling, fairly, on the shared link graph.

Lifecycle of a job (PostgreSQL is the only state; nothing here lives in memory):
  pending -> searching (DuckDuckGo, provider-wide cooldown, bounded concurrency) -> crawling -> done | failed
  crawling: job_urls holds the job's pages with THEIR OWN depth (a shared URL can sit at depth 0 for one business and 2 for another).
  A crawled page is fetched once for everybody (discovered_urls / outgoing_links are global); `expand` then walks that page's stored links
  into the job at depth+1, so a page that was already crawled for another business is reused, not refetched.
Capacity: MAX_CRAWL_WORKERS logical workers (= distinct lease owners). Business work may use all of them; autonomous crawling may use
only MAX - BUSINESS_RESERVED while business work is waiting, and everything when none is.
"""

import asyncio
import logging
import os
import random
import time

import crawler_core as core
import crawler_engine as engine
import ddg_lookup

log = logging.getLogger("crawler.business")


class CrawlPriority:  # lower number = served first
    BUSINESS_ONBOARDING = 0  # P0: a new business's search (and its job until its first page is crawled)
    BUSINESS_EXISTING = 1  # P1: that job's candidate crawling once it is making progress
    RECURRING = 2  # P2: periodic re-discovery of an existing business
    COMPETITOR_INITIAL = 3  # a newly registered competitor: after every customer-business job, before routine work
    COMPETITOR_RECURRING = 4
    AUTONOMOUS = 5  # autonomous frontier expansion (label: claim_work gives it capacity separately)
    VERIFICATION = 6  # routine backlink re-verification (label, see Engine.verifier)


def _env(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


MAX_CRAWL_WORKERS = _env("MAX_CRAWL_WORKERS", 4)
BUSINESS_RESERVED_WORKERS = _env("BUSINESS_RESERVED_WORKERS", 3)
MAX_CONCURRENT_JOBS_PER_BUSINESS = _env("MAX_ACTIVE_TASKS_PER_BUSINESS", 1)  # workers one business may hold at once
MAX_PAGES_PER_BUSINESS_JOB = _env("MAX_PAGES_PER_BUSINESS_RUN", 150)
MAX_DISCOVERY_DEPTH = _env("MAX_DEPTH", 3)
FOLLOW_EXTERNAL_DOMAINS = os.environ.get("FOLLOW_EXTERNAL_DOMAINS", "false").lower() in ("1", "true", "yes")  # off: never crawl past a candidate's own site
DDGS_ENABLED = os.environ.get("DDGS_ENABLED", "true").lower() in ("1", "true", "yes")
DDG_CONCURRENCY = _env("DDGS_SEARCH_CONCURRENCY", 1)  # simultaneous searches (more triggers rate limits)
DDG_RESULTS = min(_env("DDGS_RESULTS_PER_RUN", 50), _env("DDGS_MAX_RESULTS", 60))
SEARCH_RETRIES = _env("DDGS_MAX_RETRIES", 3)
SEARCH_INTERVAL = int(float(os.environ.get("DDGS_INTERVAL_HOURS", 6)) * 3600)
COMPETITOR_ENABLED = os.environ.get("COMPETITOR_DISCOVERY_ENABLED", "true").lower() in ("1", "true", "yes")
COMPETITOR_INTERVAL = int(float(os.environ.get("COMPETITOR_DDGS_INTERVAL_HOURS", 24)) * 3600)
COMPETITOR_RESULTS = min(_env("COMPETITOR_DDGS_RESULTS", 50), _env("DDGS_MAX_RESULTS", 60))
MAX_ACTIVE_COMPETITOR_JOBS = _env("MAX_ACTIVE_COMPETITOR_JOBS", 1)  # competitor jobs past 'pending' at once (searching or crawling)
MAX_OUTSTANDING_URLS = _env("MAX_OUTSTANDING_URLS_PER_TARGET", 300)  # open (not yet crawled) URLs one target may hold; above it its next discovery is deferred
COMPETITOR_STARVATION_SECONDS = _env("DDGS_COMPETITOR_MAX_WAIT_SECONDS", 1800)  # a competitor job waiting this long is claimed before customer re-runs
FAIR_POLICY = os.environ.get("DDGS_FAIR_POLICY", "hierarchical")  # hierarchical: parent business first, then its own targets | target: every target equal
FAIR_WINDOW = 50  # due targets looked at per scheduling pass (bounded, so a backlog of deferred targets cannot hide a due one behind it)
JITTER = 0.1  # next run lands 0-10% after the interval, so businesses onboarded together drift apart
DDG_COOLDOWN = _env("DDG_COOLDOWN_SECONDS", 60)  # first provider-wide pause after a failed search; doubles per failure of the same job
LOCK = 724002  # advisory lock serializing every capacity decision (claim) across processes
ACTIVE = ("pending", "searching", "crawling")
OPEN_URL = "('pending','crawling','recrawling','retry_pending')"


def _group() -> str:
    """Level 1 of the search rotation: the customer business a target belongs to (a competitor belongs to the owner that registered it)."""
    if FAIR_POLICY not in ("hierarchical", "target"):
        raise ValueError(f"DDGS_FAIR_POLICY must be 'hierarchical' or 'target', not {FAIR_POLICY!r}")
    return "COALESCE(parent_domain, domain)" if FAIR_POLICY == "hierarchical" else "domain"


def validate(total: int, reserved: int, per_business: int) -> None:
    if total < 1 or reserved < 0 or per_business < 1:
        raise ValueError("MAX_CRAWL_WORKERS and MAX_CONCURRENT_JOBS_PER_BUSINESS must be >= 1, BUSINESS_RESERVED_WORKERS >= 0")
    if reserved > total:
        raise ValueError(f"BUSINESS_RESERVED_WORKERS ({reserved}) exceeds MAX_CRAWL_WORKERS ({total})")


validate(MAX_CRAWL_WORKERS, BUSINESS_RESERVED_WORKERS, MAX_CONCURRENT_JOBS_PER_BUSINESS)


# ---------- onboarding ----------

def create_job(conn, domain: str, max_depth: int | None = None, max_pages: int | None = None, name: str | None = None,
               target_type: str = "own_business") -> tuple[dict, bool]:
    """(job, created). Registers the business (its recurring schedule) and queues a P0 job now. Idempotent: a domain with an active
    job returns that job (the partial unique index decides, so concurrent requests cannot create two)."""
    reg = core.registrable_domain(core.clean_target(domain))
    # a customer business is never downgraded to a competitor: onboarding upgrades a competitor row, registering a competitor leaves an own row alone
    conn.execute("INSERT INTO businesses (domain, business_name, search_interval_seconds, next_run_at, target_type) VALUES (?,?,?,?,?)"
                 " ON CONFLICT (domain) DO UPDATE SET business_name=COALESCE(EXCLUDED.business_name, businesses.business_name), active=1,"
                 " target_type=CASE WHEN EXCLUDED.target_type='own_business' THEN 'own_business' ELSE businesses.target_type END",
                 (reg, name, COMPETITOR_INTERVAL if target_type == "competitor" else SEARCH_INTERVAL, core.now(), target_type))
    typ = conn.execute("SELECT target_type FROM businesses WHERE domain=?", (reg,)).fetchone()[0]
    row = conn.execute("INSERT INTO business_jobs (target_domain, priority, max_depth, max_pages) VALUES (?,?,?,?) ON CONFLICT DO NOTHING RETURNING *",
                       (reg, CrawlPriority.COMPETITOR_INITIAL if typ == "competitor" else CrawlPriority.BUSINESS_ONBOARDING,
                        max_depth or MAX_DISCOVERY_DEPTH, max_pages or MAX_PAGES_PER_BUSINESS_JOB)).fetchone()
    if row:
        log.info(">>> BUSINESS-JOB %s: created (id %s)", reg, row["id"])
        return dict(row), True
    return dict(conn.execute("SELECT * FROM business_jobs WHERE target_domain=? AND status IN ('pending','searching','crawling')", (reg,)).fetchone()), False


def latest(conn, domain: str) -> dict | None:
    r = conn.execute("SELECT * FROM business_jobs WHERE target_domain=? ORDER BY id DESC LIMIT 1", (core.registrable_domain(domain),)).fetchone()
    return dict(r) if r else None


def api_state(job: dict | None) -> str:
    """The status vocabulary of the API."""
    if not job:
        return "not_started"
    s = job["status"]
    if s == "pending":
        return "retry_wait" if job["retry_count"] else "discovering"
    return {"searching": "discovering", "crawling": "crawling", "failed": "failed"}.get(s) or ("completed" if job["backlinks_found"] else "completed_no_results")


def _reschedule(conn, domain: str, ok: bool) -> None:
    """A run ended: the business's next run is one interval (+jitter) away. A failed run is retried at the next interval, not immediately."""
    b = conn.execute("SELECT search_interval_seconds s FROM businesses WHERE domain=?", (domain,)).fetchone()
    if b:
        conn.execute("UPDATE businesses SET last_run_at=?, last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END, next_run_at=? WHERE domain=?",
                     (core.now(), ok, core.now(), core.later(seconds=b["s"] * (1 + random.random() * JITTER)), domain))
        # every query exhausted: nothing to read until the earliest rediscovery time, so do not schedule a pointless run before it
        conn.execute("UPDATE businesses SET next_run_at=GREATEST(next_run_at, (SELECT MIN(rediscover_after) FROM discovery_cursors c"
                     " WHERE c.domain=businesses.domain AND c.status='exhausted')) WHERE domain=? AND discovery_state='waiting_rediscovery'", (domain,))


def schedule_due() -> int:
    """Turn every due business schedule into a P2 job (only while fewer than DDG_CONCURRENCY jobs wait for a search, so a restart after downtime does not fire all
    overdue businesses together). A business with an active job is skipped; a missed schedule fires once."""
    if not DDGS_ENABLED:
        return 0
    with core.connect() as c:
        slots = DDG_CONCURRENCY - c.execute("SELECT COUNT(*) FROM business_jobs WHERE status IN ('pending','searching')").fetchone()[0]
        if slots <= 0:
            return 0
        # least recently searched first (never searched = new targets first); the window is bounded, the cap check only runs on these rows
        due = c.execute(  # glast: when the business's group (itself + its competitors) last had a turn; the group served longest ago goes first
            f"SELECT domain, target_type FROM (SELECT domain, target_type, active, next_run_at, last_search_at, MAX(last_search_at) OVER (PARTITION BY {_group()}) glast"
            " FROM businesses WHERE active=1) b WHERE next_run_at<=? AND (target_type<>'competitor' OR ?::boolean)"
            " AND NOT EXISTS (SELECT 1 FROM business_jobs j WHERE j.target_domain=b.domain AND j.status IN ('pending','searching','crawling'))"
            " ORDER BY glast NULLS FIRST, last_search_at NULLS FIRST, next_run_at LIMIT ?", (core.now(), COMPETITOR_ENABLED, FAIR_WINDOW)).fetchall()
        take, deferred = [], []
        for r in due:
            if len(take) >= slots:
                break
            (deferred if outstanding(c, r["domain"]) >= MAX_OUTSTANDING_URLS else take).append(r)
        if deferred:  # stays due: the first pass after its backlog drops below the cap picks it up again, no other trigger needed
            c.execute("UPDATE businesses SET discovery_state='deferred_backlog' WHERE domain = ANY(?::text[])", ([r["domain"] for r in deferred],))
        return sum(1 for r in take if c.execute(
            "INSERT INTO business_jobs (target_domain, priority, max_depth, max_pages) VALUES (?,?,?,?) ON CONFLICT DO NOTHING RETURNING id",
            (r["domain"], CrawlPriority.COMPETITOR_RECURRING if r["target_type"] == "competitor" else CrawlPriority.RECURRING,
             MAX_DISCOVERY_DEPTH, MAX_PAGES_PER_BUSINESS_JOB)).fetchone())


def outstanding(conn, domain: str) -> int:
    """Open (waiting or being crawled) URLs of the target's jobs, counted only up to the cap."""
    return conn.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM job_urls ju JOIN business_jobs j ON j.id=ju.job_id AND j.target_domain=? JOIN discovered_urls u ON u.id=ju.url_id"
        f" WHERE u.crawl_status IN {OPEN_URL} LIMIT ?) x", (domain, MAX_OUTSTANDING_URLS)).fetchone()[0]


def discovery_status(conn, domain: str) -> dict:
    """Where discovery of one target stands. exhausted = cursors that read past their last page; waiting_rediscovery = ALL of them, until their
    rediscovery time; deferred_backlog = too many open URLs; failed = the last run ran out of retries; paused = the target is switched off."""
    b = conn.execute("SELECT active, discovery_state, last_search_at, next_run_at FROM businesses WHERE domain=?", (domain,)).fetchone()
    if not b:
        return {"state": "not_started"}
    cur = conn.execute("SELECT COUNT(*) total, COUNT(*) FILTER (WHERE status='exhausted') exhausted, COALESCE(SUM(urls_found),0) urls, MIN(rediscover_after) FILTER"
                       " (WHERE status='exhausted') rediscover_at FROM discovery_cursors WHERE domain=?", (domain,)).fetchone()
    return {"state": "paused" if not b["active"] else b["discovery_state"], "last_search_at": b["last_search_at"], "next_run_at": b["next_run_at"],
            "cursors": cur["total"], "exhausted_cursors": cur["exhausted"], "urls_found": int(cur["urls"]), "rediscover_at": cur["rediscover_at"]}


# ---------- search phase ----------

def _claim_kind(c, limit: int, competitor: bool, aged: bool = False) -> list[dict]:
    """Claim pending searches of one kind, least recently searched first inside a priority, and stamp last_search_at NOW (the attempt, success or
    not, is the target's turn). aged: only competitor jobs that have waited past COMPETITOR_STARVATION_SECONDS."""
    got = [dict(r) for r in c.execute(
        "UPDATE business_jobs SET status='searching', search_gen=search_gen+1, started_at=COALESCE(started_at, ?), updated_at=? WHERE id IN (SELECT j.id FROM business_jobs j LEFT JOIN (SELECT domain, last_search_at tlast, MAX(last_search_at) OVER (PARTITION BY " + _group() + ") glast"
        " FROM businesses WHERE active=1) f ON f.domain=j.target_domain WHERE j.status='pending'"
        " AND (j.next_retry_at IS NULL OR j.next_retry_at<=?)"
        " AND EXISTS (SELECT 1 FROM businesses b WHERE b.domain=j.target_domain AND b.target_type='competitor') = ?::boolean AND (NOT ?::boolean OR j.created_at<=?)"
        " ORDER BY j.priority, f.glast NULLS FIRST, f.tlast NULLS FIRST, j.id LIMIT ? FOR UPDATE OF j SKIP LOCKED) RETURNING id, target_domain, retry_count, search_gen,"
        " (SELECT business_name FROM businesses WHERE domain=business_jobs.target_domain) AS name,"
        " (SELECT target_type FROM businesses WHERE domain=business_jobs.target_domain) AS target_type",
        (core.now(), core.now(), core.now(), competitor, aged, core.later(seconds=-COMPETITOR_STARVATION_SECONDS), limit))]
    if got:
        c.execute("UPDATE businesses SET last_search_at=to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') WHERE domain = ANY(?::text[])",
                  ([j["target_domain"] for j in got],))  # microseconds: two targets claimed in one second still have an order
    return got


def _claim_search(limit: int) -> list[dict]:
    """Customer-business jobs first (P0-P2), then competitor jobs only into the free MAX_ACTIVE_COMPETITOR_JOBS slots (jobs past 'pending').
    shortcut: the slot count is read, not locked; exact because one engine process claims searches serially."""
    with core.connect() as c:
        got = _claim_kind(c, 1, True, aged=True) if limit > 0 and COMPETITOR_STARVATION_SECONDS > 0 else []  # an aged competitor first: no starvation by customer re-runs
        got += _claim_kind(c, limit - len(got), False) if limit > len(got) else []
        active = c.execute("SELECT COUNT(*) FROM business_jobs j JOIN businesses b ON b.domain=j.target_domain AND b.target_type='competitor'"
                           " WHERE j.status IN ('searching','crawling')").fetchone()[0]
        room = min(limit - len(got), MAX_ACTIVE_COMPETITOR_JOBS - active - sum(1 for j in got if j["target_type"] == "competitor"))
        return got + (_claim_kind(c, room, True) if room > 0 else [])


def _search_failed(job: dict, err: Exception) -> None:
    tries = job["retry_count"] + 1
    wait = int(DDG_COOLDOWN * 2 ** (tries - 1) * (1 + random.random() * JITTER))
    final = tries >= SEARCH_RETRIES
    with core.connect() as c:
        if not c.execute("UPDATE business_jobs SET status=?, retry_count=?, last_error=?, next_retry_at=?, updated_at=?, finished_at=? WHERE id=?"
                         " AND status='searching' AND search_gen=? RETURNING id",
                         ("failed" if final else "pending", tries, repr(err)[:300], None if final else core.later(seconds=wait), core.now(),
                          core.now() if final else None, job["id"], job["search_gen"])).fetchone():
            log.warning("BUSINESS-JOB %s: failure of a superseded search ignored (%r)", job["target_domain"], err)
            return
        if final:
            c.execute("UPDATE businesses SET discovery_state='failed' WHERE domain=?", (job["target_domain"],))
            _reschedule(c, job["target_domain"], False)
        c.execute("INSERT INTO engine_state (key, value) VALUES ('ddg_cooldown_until', ?) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                  (core.later(seconds=wait),))  # provider-level: every job waits, not just this one
    log.error("BUSINESS-JOB-ERROR >>> BUSINESS-JOB %s: search failed (%d/%d)%s: %r", job["target_domain"], tries, SEARCH_RETRIES, "" if final else f", retry in {wait}s", err)


def _search_done(job: dict, urls: list[str], run: "ddg_lookup.Run | None" = None) -> None:
    """Store a finished search. Fenced: only the search that currently owns the job (status 'searching', same search_gen) may write; a result
    from a search that was reset and re-claimed meanwhile (engine restart, a second engine) is discarded, so one job never gets two result
    sets. Candidates are capped by the job's page budget. With a Run, the pagination cursors and the query provenance of every hit are stored in
    this same transaction: a cursor never moves past a page whose URLs were not stored, and a URL already known globally is still associated."""
    reg = job["target_domain"]
    ids: list[int] = []
    dup = 0
    done: set[str] = set()
    with core.connect() as c:
        own = c.execute("UPDATE business_jobs SET status='crawling', last_error=NULL, updated_at=? WHERE id=? AND status='searching' AND search_gen=? RETURNING max_pages",
                        (core.now(), job["id"], job["search_gen"])).fetchone()
        if not own:
            log.warning("BUSINESS-JOB %s: result of a superseded search discarded (%d url(s))", reg, len(urls))
            return
        room = own["max_pages"] - c.execute("SELECT COUNT(*) FROM job_urls WHERE job_id=?", (job["id"],)).fetchone()[0]
        for u in urls:
            if len(ids) >= max(room, 0):
                break
            norm = core.normalize_url(u)
            dom = core.registrable_domain(core.hostname(u))
            if not norm or dom == reg:  # the business's own pages are not backlinks
                continue
            dup += bool(c.execute("SELECT 1 FROM discovered_urls WHERE normalized_url=?", (norm,)).fetchone())  # already known: another run or business
            ok = core.crawlable(u) and dom not in engine.NO_CRAWL_DOMAINS
            core.save_url(c, u, None, "ddg", 0, False, "pending" if ok else "skipped", None, None if ok else "no_crawl")
            ids.append(c.execute("SELECT id FROM discovered_urls WHERE normalized_url=?", (norm,)).fetchone()["id"])
            done.add(norm)
        if run:
            _store_run(c, reg, run, done, cut=len(done) < len({core.normalize_url(u) for u in urls} - {None}))
        if ids:
            c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, unnest(?::bigint[]), 0 ON CONFLICT DO NOTHING", (job["id"], ids))
        c.execute("UPDATE business_jobs SET search_results=(SELECT COUNT(*) FROM job_urls WHERE job_id=? AND depth=0), duplicate_candidates=? WHERE id=?",
                  (job["id"], dup, job["id"]))
    log.info(">>> BUSINESS-JOB %s: DuckDuckGo gave %d candidate page(s)", reg, len(ids))


def _load_run(domain: str) -> "ddg_lookup.Run":
    """The target's saved pagination cursors and the URLs it already discovered, two indexed reads."""
    with core.connect() as c:
        cursors = {(r["query_hash"], r["backend"]): dict(r) for r in c.execute("SELECT * FROM discovery_cursors WHERE domain=?", (domain,))}
        known = {r["normalized_url"] for r in c.execute("SELECT u.normalized_url FROM url_discoveries d JOIN discovered_urls u ON u.id=d.url_id"
                                                        " WHERE d.target_domain=?", (domain,))}
    return ddg_lookup.Run(cursors, known)


def _store_run(c, domain: str, run: "ddg_lookup.Run", done: set[str], cut: bool) -> None:
    """Cursors, query provenance and the target's discovery state, atomically with the candidate URLs. `cut`: the job's page budget dropped some
    new URLs, so cursors stay where they were (those pages are read again; what was stored is deduplicated)."""
    keep = [(h, qh) for h, qh, new in run.hits if not new or core.normalize_url(h) in done]
    if keep:
        ids = {r["normalized_url"]: r["id"] for r in c.execute("SELECT id, normalized_url FROM discovered_urls WHERE normalized_url = ANY(?::text[])",
                                                               (sorted({core.normalize_url(h) for h, _ in keep}),))}
        pairs = sorted({(ids[n], qh) for h, qh in keep if (n := core.normalize_url(h)) in ids})
        if pairs:  # a URL found by several targets or several queries keeps every association
            c.execute("INSERT INTO url_discoveries (url_id, target_domain, query_hash) SELECT unnest(?::bigint[]), ?, unnest(?::text[]) ON CONFLICT DO NOTHING",
                      ([p[0] for p in pairs], domain, [p[1] for p in pairs]))
    if not cut:
        for (qh, backend), k in run.updates.items():
            c.execute("INSERT INTO discovery_cursors (domain, query_hash, backend, query, next_page, status, dup_streak, pages_done, urls_found, exhausted_at,"
                      " rediscover_after, last_ok_at, empty_streak, head_checked_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (domain, query_hash, backend) DO UPDATE SET next_page=EXCLUDED.next_page,"
                      " empty_streak=EXCLUDED.empty_streak, status=EXCLUDED.status, dup_streak=EXCLUDED.dup_streak, pages_done=EXCLUDED.pages_done, urls_found=EXCLUDED.urls_found,"
                      " exhausted_at=EXCLUDED.exhausted_at, rediscover_after=EXCLUDED.rediscover_after, last_ok_at=EXCLUDED.last_ok_at, head_checked_at=EXCLUDED.head_checked_at",
                      (domain, qh, backend, k["query"], k["next_page"], k["status"], k["dup_streak"], k["pages_done"], k["urls_found"], k["exhausted_at"],
                       k["rediscover_after"], k["last_ok_at"], k.get("empty_streak", 0), k.get("head_checked_at")))
    c.execute("UPDATE businesses SET discovery_state=? WHERE domain=?", ("waiting_rediscovery" if run.waiting_until else "active", domain))


async def _search_one(job: dict) -> None:
    try:
        run = await asyncio.to_thread(_load_run, job["target_domain"])
        urls = await asyncio.to_thread(ddg_lookup.search_urls, job["target_domain"], COMPETITOR_RESULTS if job.get("target_type") == "competitor" else DDG_RESULTS,
                                       name=job.get("name"), run=run)
    except Exception as e:  # noqa: BLE001 - one business's failed search must never touch another job
        return await asyncio.to_thread(_search_failed, job, e)
    await asyncio.to_thread(_search_done, job, urls, run)


def poll_state() -> dict:
    """One poll's view: pending/eligible/searching counts, free search slots and, when none, why."""
    with core.connect() as c:
        pend = c.execute("SELECT COUNT(*) n, COUNT(*) FILTER (WHERE next_retry_at IS NULL OR next_retry_at<=?) AS eligible, MIN(next_retry_at) AS next_retry"
                         " FROM business_jobs WHERE status='pending'", (core.now(),)).fetchone()
        searching = c.execute("SELECT COUNT(*) FROM business_jobs WHERE status='searching'").fetchone()[0]
        cd = c.execute("SELECT value FROM engine_state WHERE key='ddg_cooldown_until'").fetchone()
    reason = ("ddgs_disabled" if not DDGS_ENABLED else f"provider_cooldown_until={cd['value']}" if cd and cd["value"] > core.now()
              else f"search_slots_full({searching}/{DDG_CONCURRENCY})" if searching >= DDG_CONCURRENCY else None)
    return {"pending": pend["n"], "eligible": pend["eligible"], "next_retry_at": pend["next_retry"], "searching": searching,
            "free": 0 if reason else DDG_CONCURRENCY - searching, "blocked_by": reason}


_last_beat = 0.0


async def search_pending() -> bool:
    """Start searches for pending jobs: at most DDG_CONCURRENCY at once, none during the provider cooldown. True if any ran."""
    global _last_beat
    st = await asyncio.to_thread(poll_state)
    busy = st["pending"] or st["searching"]
    log.log(logging.INFO if busy else logging.DEBUG, "BUSINESS-WORKER-POLL pending=%d eligible=%d searching=%d free_slots=%d next_retry_at=%s blocked_by=%s",
            st["pending"], st["eligible"], st["searching"], st["free"], st["next_retry_at"], st["blocked_by"])
    if time.monotonic() - _last_beat > 60:
        _last_beat = time.monotonic()
        log.info("BUSINESS-WORKER-HEARTBEAT enabled=%s concurrency=%d interval_h=%.2f", DDGS_ENABLED, DDG_CONCURRENCY, SEARCH_INTERVAL / 3600)
    jobs = await asyncio.to_thread(_claim_search, st["free"]) if st["free"] > 0 and st["eligible"] else []
    for j in jobs:
        log.info("BUSINESS-JOB-CLAIMED job=%s domain=%s retry=%d | BUSINESS-JOB-SEARCHING", j["id"], j["target_domain"], j["retry_count"])
    await asyncio.gather(*(_search_one(j) for j in jobs))
    return bool(jobs)


def reset_interrupted() -> None:
    """Engine restart: a search that was in flight went down with the old process; run it again."""
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='pending' WHERE status='searching'")


# ---------- crawl phase: expansion, progress, completion ----------

def expand(conn, job: dict) -> int:
    """Move the job one level deeper through pages already crawled (by anyone): their stored html links to the SAME site become job URLs
    at depth+1 (external ones stay in the graph but are not followed unless FOLLOW_EXTERNAL_DOMAINS), within the job's depth and page budget. Returns pages examined."""
    ready = conn.execute(
        "SELECT ju.url_id, ju.depth, u.url, u.domain, u.crawl_status FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id"
        " WHERE ju.job_id=? AND ju.expanded=0 AND u.crawl_status IN ('completed','failed','skipped') ORDER BY ju.depth, ju.url_id LIMIT 200", (job["id"],)).fetchall()
    for r in ready:
        room = job["max_pages"] - conn.execute("SELECT COUNT(*) FROM job_urls WHERE job_id=?", (job["id"],)).fetchone()[0]
        if r["crawl_status"] == "completed" and r["depth"] < job["max_depth"] and room > 0:
            norms = {n for n in (core.normalize_url(x["destination_url"]) for x in conn.execute(
                "SELECT destination_url FROM outgoing_links WHERE source_url=? AND link_type='html' AND is_active=1", (r["url"],)).fetchall()) if n}
            if norms:
                new = [x["id"] for x in conn.execute(
                    "SELECT id FROM discovered_urls d WHERE normalized_url = ANY(?::text[]) AND domain<>? AND (?::boolean OR domain=?) AND NOT EXISTS"
                    " (SELECT 1 FROM job_urls j WHERE j.job_id=? AND j.url_id=d.id) ORDER BY id LIMIT ?",
                    (sorted(norms), job["target_domain"], FOLLOW_EXTERNAL_DOMAINS, r["domain"], job["id"], room)).fetchall()]
                if new:
                    conn.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, unnest(?::bigint[]), ? ON CONFLICT DO NOTHING", (job["id"], new, r["depth"] + 1))
                    # skipped only because ANOTHER crawl reached it too deep: within this job's reach it is crawlable again
                    conn.execute("UPDATE discovered_urls SET crawl_status='pending', skip_reason=NULL, crawl_depth=LEAST(crawl_depth, ?) WHERE id = ANY(?::bigint[])"
                                 " AND crawl_status='skipped' AND skip_reason='max_depth'", (r["depth"] + 1, new))
        conn.execute("UPDATE job_urls SET expanded=1 WHERE job_id=? AND url_id=?", (job["id"], r["url_id"]))
    return len(ready)


def _progress(conn, job: dict) -> bool:
    """Refresh the job's counters; close it when nothing is open or unexpanded. True if the job finished."""
    one = lambda sql, *a: conn.execute(sql, (job["id"], *a)).fetchone()[0]  # noqa: E731
    base = "SELECT COUNT(*) FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE ju.job_id=?"
    crawled, failed = one(base + " AND u.crawl_status='completed'"), one(base + " AND u.crawl_status='failed'")
    open_ = one(base + f" AND (u.crawl_status IN {OPEN_URL} OR (ju.expanded=0 AND u.crawl_status IN ('completed','failed','skipped')))")
    found = one("SELECT COUNT(DISTINCT l.source_url) FROM outgoing_links l JOIN discovered_urls u ON u.url=l.source_url JOIN job_urls ju ON ju.url_id=u.id"
                " AND ju.job_id=? WHERE l.destination_domain=? AND l.link_type='html' AND l.is_active=1 AND l.source_domain<>?", job["target_domain"], job["target_domain"])
    prio = CrawlPriority.BUSINESS_EXISTING if crawled and job["priority"] == CrawlPriority.BUSINESS_ONBOARDING else job["priority"]
    conn.execute("UPDATE business_jobs SET pages_discovered=?, pages_crawled=?, pages_failed=?, backlinks_found=?, priority=?, updated_at=? WHERE id=?",
                 (one("SELECT COUNT(*) FROM job_urls WHERE job_id=?"), crawled, failed, found, prio, core.now(), job["id"]))
    if open_ == 0:
        if conn.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE id=? AND status='crawling'", (core.now(), job["id"])).rowcount:
            _reschedule(conn, job["target_domain"], True)
        log.info(">>> BUSINESS-JOB %s: done | %d page(s) crawled, %d failed | %d linking page(s)", job["target_domain"], crawled, failed, found)
        return True
    return False


def tick() -> int:
    """Queue due recurring runs, then advance every crawling job. A job that raises is recorded and skipped; the others carry on. Returns jobs that moved."""
    schedule_due()
    with core.connect() as c:
        jobs = [dict(r) for r in c.execute("SELECT * FROM business_jobs WHERE status='crawling' ORDER BY id")]
    moved = 0
    for job in jobs:
        try:
            with core.connect() as c:
                for _ in range(50):  # levels already crawled elsewhere cascade within one tick
                    if not expand(c, job):
                        break
                    moved += 1
                _progress(c, job)
        except Exception as e:  # noqa: BLE001 - isolation: one job's error is data on that job, not a crash
            log.exception(">>> BUSINESS-JOB %s: tick failed", job["target_domain"])
            with core.connect() as c:
                c.execute("UPDATE business_jobs SET last_error=?, updated_at=? WHERE id=?", (repr(e)[:300], core.now(), job["id"]))
    return moved


# ---------- capacity and claiming ----------

def waiting(conn) -> bool:
    """Business work that needs workers now: a job still searching/queued, or a crawling job with a claimable URL."""
    return bool(conn.execute(
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM business_jobs WHERE status IN ('pending','searching')) OR EXISTS ("
        " SELECT 1 FROM job_urls ju JOIN business_jobs j ON j.id=ju.job_id AND j.status='crawling' JOIN discovered_urls u ON u.id=ju.url_id"
        " WHERE u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?))", (core.now(),)).fetchone())


def _owners(conn, owner: str, business: bool | None = None) -> int:
    """Workers busy right now: distinct owners holding unexpired leases, excluding the asker. business=True/False restricts to URLs that
    do / do not belong to a crawling job."""
    inb = "u.id IN (SELECT ju.url_id FROM job_urls ju JOIN business_jobs j ON j.id=ju.job_id AND j.status='crawling')"
    extra = "" if business is None else (f" AND {inb}" if business else f" AND NOT {inb}")
    return conn.execute("SELECT COUNT(DISTINCT u.lease_owner) FROM discovered_urls u WHERE u.crawl_status='crawling' AND u.lease_expires>?"
                        " AND u.lease_owner<>?" + extra, (core.now(), owner)).fetchone()[0]


def autonomous_allowed(conn, owner: str, total: int, reserved: int) -> bool:
    busy = _owners(conn, owner)
    if busy >= total:
        return False
    return not waiting(conn) or _owners(conn, owner, False) < total - reserved


def claim(conn, owner: str, cfg) -> list[tuple[str, int]]:
    """Lease up to cfg.claim_batch business URLs [(url, job depth)]: globally capped by MAX_CRAWL_WORKERS, at most
    MAX_CONCURRENT_JOBS_PER_BUSINESS workers per business, round-robin between businesses (P0 before P1), one lease per URL even when
    several jobs want it. Call under LOCK, in the transaction that also decides autonomous capacity."""
    if _owners(conn, owner) >= cfg.max_crawl_workers:
        return []
    full = [r[0] for r in conn.execute(
        "SELECT ju.job_id FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE u.crawl_status='crawling' AND u.lease_expires>?"
        " AND u.lease_owner<>? GROUP BY ju.job_id HAVING COUNT(DISTINCT u.lease_owner)>=?", (core.now(), owner, cfg.max_jobs_per_business))]
    cands = conn.execute(
        "SELECT u.id, u.domain, ju.job_id, ju.depth, j.priority, ROW_NUMBER() OVER (PARTITION BY ju.job_id ORDER BY ju.depth, u.id) rn"
        " FROM job_urls ju JOIN business_jobs j ON j.id=ju.job_id AND j.status='crawling' JOIN discovered_urls u ON u.id=ju.url_id"
        " WHERE (u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?)) AND ju.job_id<>ALL(?::bigint[])",
        (core.now(), full)).fetchall()
    picked: dict[int, int] = {}
    per_domain: dict[str, int] = {}
    for r in sorted(cands, key=lambda r: (r["priority"], r["rn"], r["job_id"])):
        if len(picked) >= cfg.claim_batch:
            break
        if r["id"] in picked or per_domain.get(r["domain"], 0) >= cfg.per_batch:
            continue
        picked[r["id"]] = r["depth"]
        per_domain[r["domain"]] = per_domain.get(r["domain"], 0) + 1
    if not picked:
        return []
    got = conn.execute(
        "UPDATE discovered_urls SET crawl_status='crawling', lease_owner=?, lease_expires=?, lease_gen=lease_gen+1 WHERE id IN ("
        " SELECT id FROM discovered_urls WHERE id = ANY(?::bigint[]) AND crawl_status IN ('pending','retry_pending') FOR UPDATE SKIP LOCKED)"
        " RETURNING id, url, normalized_url, lease_gen", (owner, core.later(seconds=cfg.lease_seconds), list(picked))).fetchall()
    engine.FENCE.update({r["normalized_url"]: r["lease_gen"] for r in got})
    jobs = sorted({r["job_id"] for r in cands if r["id"] in picked})
    log.info(">>> BUSINESS-CLAIM %s: %d URL(s) for job(s) %s", owner[-12:], len(got), jobs)
    return [(r["url"], picked[r["id"]]) for r in got]

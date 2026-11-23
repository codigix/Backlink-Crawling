# Phase 9: local end-to-end test report (not a deployment)

Target: database `localhost:5432/phase9_e2e_test` (created for this phase, kept), Redis `localhost:6390` (own Memurai instance, your 6379 untouched),
API / engine / 2 Celery workers started as real processes from this repo, a local "internet" of ~25 virtual `*.localhost` sites, and a deterministic fake
search client (`ddg_lookup.DDGS` replaced, everything above it is the real code). Every process was started with an explicit environment and a preflight proved
that `.env` could not redirect it (db `localhost:5432/phase9_e2e_test`, Redis 6390). No application code was changed. The harness lives outside the repo
(`scratchpad/p9`: `world.py`, `fake_engine.py`, `stack.py`, `s*.py`) and logs are kept in `scratchpad/p9/logs_final`.

## Result in one table

| Area | Result |
|---|---|
| Startup | redis, world, API, engine, 2 workers, 2 extra cache-on APIs started cleanly; migrations applied by the first API import |
| Onboarding (search -> crawl -> extract -> store -> verify -> API) | works without manual DB steps; **one defect** (D1) can leave a job open |
| Search / discovery | 5 of 6 scenarios as designed; empty-result case re-run after fixing MY fake: `completed_no_results` |
| Crawler / indexer | robots, redirect, 404/410/500, timeout, retry, JS page, dedup, no_crawl all correct; **two defects** (D1, D2) |
| Backlink verification | 11 active backlinks == what the sites really contain; no false positive; two-strike removal, restore, history all correct |
| Competitors | registration, backlinks/referring-domains/anchors, gap, 404s work; single-slot cap **not honoured in my run** (D3) |
| API | 33/33 checks (status, validation, pagination offset+keyset, totals == SQL, errors) |
| Pool / concurrency | 64 threads, 0 errors, peak 33 connections (budget 40), no growth; DB outage bounded by pool wait (D4) |
| Cache | on/off/TTL/LRU/single-flight/key isolation/2-process independence/authorization all verified |
| Indexes | built on the local E2E DB only through the Phase 8 guard; idempotent; no INVALID |
| Failure and recovery | worker kill, Redis stop, engine kill, API restart, DB outage: no lost URL, no duplicate fetch, no leftover lease |
| Consistency | 17/17 checks |
| Regression (before any Phase 9.1 change) | 454 passed, 0 failed, 0 errors, 0 skipped, 0 warnings, 627.78 s (10:27); the Phase 8 `coroutine '_handle' was never awaited` warning did not appear |

## Defects found (nothing was fixed: root cause and smallest fix only)

**D1. A business job can stay `crawling` forever in Celery mode.** `tasks._dispatch` counts only `crawl_status='pending'` business URLs (and frontier URLs of
*admitted* domains). A business URL that is `retry_pending` and due, on a domain that is not `admitted` (here a slow site that failed admission 3 times and became
`unreachable`), is never counted, so no `crawl.batch` is queued, while `business_jobs.waiting()` says work exists. Proof: `waiting()=True`, `dispatch()=0`; one
manually queued `crawl.batch` claimed the URL and the job closed (`done`) within a minute. Impact: the job never closes, the business keeps an "active" job so
recurring scans and rescans are suppressed. Smallest fix: in `tasks._dispatch`, second query, use the same predicate as `waiting()`:
`u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?)`.

**D2. JavaScript rendering fails inside Celery workers on Windows.** The `celery` command imports the installed `flower` package, whose `flower/app.py` calls
`asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())` on Windows; Playwright then cannot start its subprocess (`NotImplementedError`) and
`crawl_round` falls back to static HTML ("Playwright pass failed; persisting static HTML"). Proven with a `set_event_loop_policy` spy. The same page rendered
correctly in the engine process (the link behind JS was found). Linux is not affected. Smallest fixes: uninstall flower on Windows dev machines, or run the task's
event loop as `asyncio.run(..., loop_factory=asyncio.ProactorEventLoop)` on `win32`.

**D3. `MAX_ACTIVE_COMPETITOR_JOBS=1` / `DDGS_SEARCH_CONCURRENCY=1` were exceeded once in the real engine.** Three competitors registered together were all claimed in
ONE poll that logged `free_slots=1` (reproduced twice, peak 3 active). `_claim_search` and `_claim_kind` return exactly 1 row when I call them directly (fresh
schema, existing rows, fresh unanalysed rows, inside rolled-back transactions). Root cause NOT found; not changed (you asked not to alter competitor scheduling).
Needs a look at the engine path (`Engine.lookups` -> `search_pending`) with statement logging.

**D4 (behaviour, not a bug). PostgreSQL down:** every API request waits for the pool (`DB_POOL_WAIT`, about 30 s here) and then fails; the cache does not rescue an
endpoint because the page of link rows is always read live. After the database returned the first request succeeded 38.5 s later and all later ones worked.

## Observations worth knowing
* A link missing once is `suspected_removed` (still counted in `total_backlinks`, not in `verified_backlinks`); two consecutive misses make it `lost`. Restore gives `reappeared`.
* A redirect source is stored under both URLs (`/old` and `/new` both appear as backlinks for one real link), and a URL with a query string (`/post?utm=1`) is a separate source page.
* Graceful stop: my Ctrl+C method needed a forced kill after 41 s for both API and engine. I cannot tell whether that is the apps or my console-signal harness; unverified.
* A cache gives no speed-up on this tiny dataset (13 MB); that is expected and says nothing about production.
* The `db_indexes.py` dry run with only the development `.env` prints the production tunnel target name `host:port/dbname` (no credentials) before refusing.

## Measurements (REAL runs of the real code on a tiny local dataset: 209 URLs, 207 links; NOT production, NOT comparable with the 100k-row synthetic Phase 4/6 benchmarks)
* API sequential medians / p95 (40 calls): summary 19 / 28 ms, referring-domains 12 / 18 ms, anchors 11 / 30 ms, history 18 / 37 ms.
* Load: 4 threads 175 req/s (median 15 ms); 32 threads 206 req/s (median 141 ms, p95 371 ms); 64 threads 222 req/s (median 273 ms, p95 388 ms); cache-on 32 threads 160 req/s. Errors 0.
* Connections: max 33 application connections across API x4, engine, 2 workers (configured pool maxima add up to 59 because four API processes ran; `DB_MAX_CONNECTIONS=40` was documentation only); 0 growth.
* Memory (peak RSS per process tree): API ~300 MB, engine 384 MB, worker ~335 MB. CPU was NOT measured (my sampler read 0).
* Onboarding timeline: search 18 s (the fake sleeps 1-2 s per page), crawl 24 pages in 30-70 s with 1 s per-domain politeness; 12-48 pages/min depending on batch shape. Not a throughput figure for production.
* Database transactions during the 4-run load test: 2130.

## Recovery results
| Test | Outcome |
|---|---|
| Worker hard-killed with 6 leased URLs | job finished 193 s later (lease 180 s); 40/40 completed, 0 fetched twice, 0 leases left |
| Redis stopped 25 s mid-crawl, restarted | API and engine stayed up; job finished 74 s after Redis returned; 24/24, 0 duplicates |
| Engine hard-killed mid-job, restarted | job closed 35 s after restart; 24/24 |
| Provider errors | job back to `pending`, provider cooldown set, nothing stored, business row intact, recovered 33 s after the provider returned |
| Empty provider answer | `completed_no_results`, no error |

## Not tested / limits
* Graceful (Ctrl+C) shutdown could not be demonstrated (see above). Advisory-lock release was checked only as "0 locks after the process died".
* Subdomain cache isolation was checked at the API (separate MISS), not with real subdomain data; memory-cap eviction relies on the unit tests (the data is too small).
* Celery prefork, several engines (scheduler lock contention) and real DuckDuckGo/Serper were not exercised.
* DB outage was simulated through a local TCP forwarder (5439 to 5432) for one API process only; PostgreSQL itself was never stopped.

Phase 9.1 (fixes for D1-D4, graceful shutdown, connection budget) is in PHASE9_1_REPORT.md.

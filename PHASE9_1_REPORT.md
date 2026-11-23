# Phase 9.1: fixes for the Phase 9 findings (local only, nothing deployed)

Target for every live test: database `localhost:5432/phase9_e2e_test`, Redis `localhost:6390` (verified by the stack preflight before each start; `.env` untouched).
Unit/regression tests ran on the local `phase1_api_test` database (isolated schema per test). Port 5433 was never used.

## Summary

| Finding | Status |
|---|---|
| D1 stuck business job | **fixed** (`tasks._dispatch`), 5 new tests (they fail on the old code), live proof below |
| D2 Playwright in Celery on Windows | **fixed** (`tasks.run_loop`), JS link persisted via Celery worker, engine and read back through the API |
| D3 competitor cap exceeded | **NOT reproduced, root cause NOT proven, no production code changed**; regression test + diagnostics added |
| D4 DB outage = 30 s wait | **implemented** (`API_DB_WAIT`, HTTP 503), outage 30 s -> 5 s, recovery 38.5 s -> 2.6 s, 3 new tests |
| Graceful shutdown (41 s) | two causes: my launcher (inherited "ignore Ctrl+C") **and a real application defect** (crawlee swallowed the first Ctrl+C during a crawl round); app defect **fixed** in `crawl_round`, 1 new test |
| Connection budget | explained below (59 theoretical / 33 observed peak / 40 declared, warning-only) |

## D1: root cause and fix
Root cause: `tasks._dispatch` counted business URLs only when `crawl_status='pending'`; a due `retry_pending` business URL (for example on a domain that is
`unreachable`) was never counted, so no `crawl.batch` was queued while `business_jobs.waiting()` (which already used the due-retry predicate) said work existed.
Fix ([tasks.py](tasks.py), second query of `_dispatch`): `WHERE u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?)`.
Claiming (`_pick`/`claim`) already handled due retries, with `FOR UPDATE SKIP LOCKED`; future retries are still not dispatched.
Tests ([tests/test_celery_tasks.py](tests/test_celery_tasks.py)): pending counted; due retry counted; future retry not counted; retry URL on an unreachable domain; several jobs (one future, one due);
`waiting()` agrees with the dispatcher and 4 concurrent claimers lease each due URL exactly once; 5 concurrent dispatchers stay within the queue bound.
Negative control: with the old predicate 4 of the new tests fail.
Live (the 9.0 situation: new business whose two pages are on the slow, already `unreachable` host `ref9`; both URLs timed out and became `retry_pending`): the dispatcher queued a `crawl.batch` the moment the retry was due
(12:56:07 due, 12:56:15 leased by the worker), the URLs failed after their retries and the job closed `done` at 12:57:18 with no manual `crawl.batch`. In 9.0 this job stayed `crawling` forever.

## D2: root cause and fix
Root cause: the `celery` command imports `flower`, whose `flower/app.py` sets `WindowsSelectorEventLoopPolicy`; `asyncio.run` then builds a selector loop that cannot spawn
Playwright's subprocess (`NotImplementedError`), and `crawl_round` fell back to static HTML.
Fix ([tasks.py](tasks.py)): `run_loop(coro)` = `asyncio.run(coro, loop_factory=asyncio.ProactorEventLoop)` on `win32` (a Proactor loop independent of the process-wide policy), plain `asyncio.run` elsewhere;
`crawl_batch` uses it. Flower is not uninstalled, no harness workaround.
Test: [tests/test_celery_tasks.py](tests/test_celery_tasks.py) sets the selector policy (as flower does), proves the old call raises `NotImplementedError` on Windows and `run_loop` can spawn a subprocess.
Live (new target pages whose link exists only after JavaScript runs): Celery worker (solo, flower importable) -> `jsb-a.localhost`: link and anchor stored in `outgoing_links` and returned by `GET /backlinks/jsb-a.localhost`;
engine in-process (`USE_CELERY=0`) -> `jsb-b.localhost`: same. Not run: a Linux host and Celery prefork (this machine is Windows; Linux takes the unchanged `asyncio.run` branch).
Python 3.12+ is required for `loop_factory` (the repo runs 3.12.10); the keyword is only passed on Windows.

## D3: evidence, status
Not fixed because not proven. What I did:
* Read every path (`Engine.lookups` -> `search_pending` -> `poll_state` -> `_claim_search` -> `_claim_kind`): `_claim_search(limit)` returns at most `limit` rows by construction (aged <= 1, customer <= limit-len, competitors <= room).
  So the 9.0 observation (limit 1, three claimed) requires one `UPDATE ... IN (SELECT ... LIMIT 1 FOR UPDATE SKIP LOCKED)` to return more than its LIMIT.
* Stress on a scratch database `phase91_d3` (new, local): 5,894 claims with three writer threads hammering the same rows, and 7,045 rounds of three competitors registered concurrently while a claimer polled:
  never more than one row, never more than one active competitor job. Plan knobs (hash/merge join off) also gave 1. The e2e database's own rows, reset in a rolled-back transaction, gave 1.
* Live re-run with the real engine and statement-level diagnostics (harness wrapper logs every `_claim_kind`: limit, result, active competitor jobs, backend pid, transaction id):
  three competitors registered in one API call were claimed one per poll (peak active 1). The 9.0 behaviour did not recur.
* Regression test added ([tests/test_competitors.py](tests/test_competitors.py)): three competitors registered concurrently while a poller claims with one free slot; no poll returns more than one job and never two active at once.
No locking or scheduling was changed (you asked for no speculative change). The 9.0 runs predate this session's code state only by files I did not edit, so I cannot attribute them; blocker: it needs the 9.0 situation to recur with the diagnostics on.
Known documented limit (not a proven cause): the slot count is read, not locked, so two engine processes polling at the same instant could both claim (`_claim_search` docstring); run exactly one engine.

## D4: design and result
Implemented: `API_DB_WAIT` (default 5 s) applies per API request ([api_timing.py](api_timing.py) sets a context variable that `crawler_core.connect()` reads; workers, engine and tasks never set it and keep `DB_POOL_WAIT=30`).
`DatabaseUnavailable` (a `TimeoutError` subclass, so existing handlers still match) and `psycopg.OperationalError` map to HTTP 503 `{"detail":"database unavailable, retry shortly"}` with `Retry-After: 5`.
`graph_cache.no_stale` ([crawler_api.py](crawler_api.py), [read_cache.py](read_cache.py)): a refresh that fails with a database outage is an error, never a 200 `STALE` aggregate (other errors keep the Phase 7 stale-on-error behaviour).
Link rows were already always read live, so no response mixes live and cached parts; a connection is taken before the cache is consulted, so an exhausted pool never reaches the cache either.
Measured (real API process through a local TCP forwarder; PostgreSQL itself never stopped), before -> after: request during the outage 30 s+ -> **5.0 s** (503); first 200 after the database returned 38.5 s -> **2.6 s**, no restart; 20/20 follow-up requests OK.
Cache on, key inside its TTL, database down: 503 (the cached aggregate is read but the live link page is not, so the response is not served); TTL expired: 503 (was 200 STALE).
Trade-off check (load against one API process with pool max 8): 64 / 128 / 256 concurrent clients -> 0 errors, no 503 (256 clients: median 3.8 s, p95 5.6 s, max 6.2 s, all time spent queueing for the request thread/connection, never a 5 s pool timeout).
A legitimate pool wait longer than 5 s would now give 503 instead of a late 200; none occurred up to 32x pool oversubscription. Tests: [tests/test_api_outage.py](tests/test_api_outage.py) (503 with the API wait, workers keep 30 s, recovery without restart, no stale 200 on outage).
Not changed: `DB_POOL_WAIT`. `.env.example` documents `API_DB_WAIT` (the real `.env` was not touched).

## Graceful shutdown
* The 41 s was two things. (1) Harness: the launcher passed down an "ignore Ctrl+C" flag, so idle processes never got the signal; with a wrapper that restores it (harness only) an idle API stops in **2.7-3.2 s**, an idle engine in **1.7-2.6 s**.
* (2) Application defect, reproduced: crawlee's `BasicCrawler.run()` turns the first Ctrl+C into "pause this round" and swallows it, so a busy engine or Celery worker finished its round and then kept running (forced kill after 42 s, `Pausing... Press CTRL+C again` in the log).
  Fix ([crawler_engine.py](crawler_engine.py), `crawl_round.run`): track the round's cancellation; after the already-fetched pages are stored, raise `KeyboardInterrupt` (or keep a real `CancelledError` when the task itself was cancelled, e.g. the task time limit).
  Test: [tests/test_shutdown.py](tests/test_shutdown.py) sends a real SIGINT during a real round (fails without the fix).
* After the fix (live, 8 URLs leased and being fetched): busy engine stops in **12.1 s** (waiting for in-flight requests), `shut down; leased URLs returned to the frontier`, 0 leased URLs, 0 advisory locks; engine in Celery mode **12.0 s**, lock released, job resumed and finished 12 s after restart;
  busy Celery worker (solo) **14.9 s**, 0 leased URLs, its unacknowledged message restored to the queue. Celery prints a harmless traceback (`cannot unpack non-iterable ExceptionInfo`) when a task ends with KeyboardInterrupt.
* Pools: the API's connections drop to 0 when it exits (7 remaining were the engine's); the stuck-forever lease case is covered by lease recovery (worker kill test below).
* Limit: `crawl_round` now relies on crawlee's private `_run_crawler` (crawlee is pinned to 1.11.1); a crawlee upgrade must re-run `tests/test_shutdown.py`.

## Connection budget (this E2E layout)
| Item | Count | Each | Total |
|---|---|---|---|
| API processes (`api`, `apic`, `apic2`, `apip`) | 4 | pool max 8 | 32 |
| Engine | 1 | pool max 6 + 1 unpooled advisory-lock connection | 7 |
| Celery solo workers (`w1`, `w2`) | 2 | pool max 10 | 20 |
| **Theoretical maximum** | 7 processes | | **59** |
| Actual peak seen (all load tests, `pg_stat_activity`) | | | **33** |
| Configured `DB_MAX_CONNECTIONS` | | declared 40 over `DB_APP_PROCESSES=5` | 40 |
`DB_MAX_CONNECTIONS` is not enforced anywhere: it only derives the default pool size and prints a warning when `pool max x DB_APP_PROCESSES > budget`. In this run the declared process count (5) was wrong (7 processes ran) and the pool maxima differed per role, so the warning compared the wrong numbers.
The real hard limit is PostgreSQL `max_connections`.
Proposal for staging (verify the counts against the real process list): API 1 x 8 + engine 1 x (6+1) + 4 solo workers x 10 = **55**; set `DB_MAX_CONNECTIONS=55`, `DB_APP_PROCESSES=6`, `DB_POOL_MAX` per role (API 8, engine 6, worker 10) in each service unit, and PostgreSQL `max_connections >= 55 + 3 reserved + ~10 admin = 68` (the default 100 is fine). Peak/theoretical was 56% here, so lowering worker pools is possible only after measuring on staging.

## Complete local E2E (re-run with all fixes in place)
* Onboarding with JavaScript pages through the Celery worker and through the in-process engine: link persisted, visible through the API (D2).
* API: 33/33 checks (status codes, validation, offset + keyset paging, totals equal SQL, error cases).
* Consistency: 17/17 checks (no duplicate links/URLs, no orphan leases, job counters equal recount, no false "verified" backlink, referring domains equal the sites that really link).
* Recovery (fresh targets): worker hard-killed with 10 URLs leased -> job done 193 s later (lease 180 s), 20/20, 0 fetched twice, 0 leases left; Redis stopped 25 s mid-crawl -> API and engine stayed up, job done 9 s after Redis returned, 20/20;
  engine stopped with Ctrl+C mid-job -> restarted, finished 12 s later, 20/20; empty provider answer -> `completed_no_results`.
* DB outage and recovery, cache on/off under outage, load: see D4.
* Not re-run in this phase (unchanged code paths, results from 9.0 stand): search failure scenarios, backlink removal/restore history, competitor gap, cache TTL/LRU/single-flight, index build.

## Regression (full suite including Playwright)
Final run after all changes: **466 passed, 0 failed, 0 errors, 0 skipped, 0 warnings, 505.39 s (8:25)** (`pytest tests -p no:cacheprovider -q -rw`, includes the Playwright tests). The `coroutine '_handle' was never awaited` warning did not appear.
An earlier run during the work had 1 failure (`test_api_request_fails_loudly_when_the_pool_is_exhausted`, a Phase 2 test that asserted the old behaviour D4 deliberately changed); the test was updated (it now asserts the 503), nothing was skipped or weakened. Baseline before 9.1: 454 passed. 466 = 454 + 12 new tests.

## Files changed in Phase 9.1
`tasks.py`, `crawler_core.py`, `api_timing.py`, `read_cache.py`, `crawler_api.py`, `crawler_engine.py`, `.env.example` (one line), tests: `tests/test_celery_tasks.py` (+6 tests), `tests/test_api_outage.py` (new, 3), `tests/test_shutdown.py` (new, 1), `tests/test_competitors.py` (+1), `tests/test_pooling.py` (one Phase 2 test updated: an exhausted pool in an API request now answers 503 within `API_DB_WAIT` instead of raising; it also asserts the unchanged pool error outside a request).
Reports: `PHASE9_E2E_REPORT.md` (regression line completed), `PHASE9_1_REPORT.md`. Harness stays in the session scratchpad. New local database `phase91_d3` (scratch for the D3 stress; kept, not dropped).
Pre-existing uncommitted changes of other work in these files are untouched (my edits are additive hunks).

## Final local acceptance (integrated run)
Single engine, one Celery worker, fresh business `fb-biz.localhost` on `phase9_e2e_test` / Redis 6390.
* Workflow: onboard -> search (3 s) -> 4 URLs scheduled -> crawl; plain page and JavaScript-rendered page (Celery worker) both stored; API: total 2, verified 2, referring domains 2, anchors correct, DB rows match.
* Worker hard-killed with 3 URLs leased, restarted: no lease left, job closed. Two pages that were already stored were fetched a second time after the kill (at-least-once, expected).
* retry_pending: two URLs on the slow, unreachable host timed out, became `retry_pending`; they were dispatched 6 s after their due time (never early, 0 violations across ~330 samples), failed after retries, job `done` after 1095 s with no manual step.
* Removal: link gone -> `suspected_removed` (08:06:30) -> `lost` (08:07:36, API total drops 2 -> 1) -> back on page -> `reappeared` (08:07:46, total 2). The first removal rounds ran inside an aborted first script run (my SQL `%` bug), the second script observed the result; the history is the evidence.
* D3: 20 scenarios x 3 competitors registered in one call: max active competitor jobs 1, max searching 1, all 60 jobs done. Engine log: one engine pid, 69 claiming polls, max 1 row per poll, 0 polls above their limit, 190 claim statements in 69 distinct transactions, claims 5.0 s apart. Violation NOT reproduced: unresolved historical observation (9.0, two occurrences).
* Outage (local forwarder): 503 in 5.0 s, cached key inside TTL -> 503, TTL expired -> 503, 20/20 after recovery in 2.4 s.
* Graceful: busy Celery worker 12.9 s, busy engine 1.4 s (celery mode holds no URL leases), API 1.4 s, idle worker 6.0 s; 0 leases, 0 advisory locks, 0 app connections afterwards.
* Final regression: 466 passed, 0 failed, 0 errors, 0 skipped, 0 warnings, 536.28 s.

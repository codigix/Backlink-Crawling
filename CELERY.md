# Redis + Celery for the backlink crawler

PostgreSQL stays the source of truth. Redis only carries "crawl a batch" messages (and a few counters). A task carries no URLs: it
claims pending URLs from PostgreSQL (`FOR UPDATE SKIP LOCKED`, leased with an expiry), crawls them with the existing
`crawl_round`, and returns unfinished ones. So duplicates and redeliveries are harmless, and a dead worker's URLs come back only
after PostgreSQL shows their lease expired (`crawl.recover`).

```
autonomous.py (USE_CELERY=1)  --dispatch/recover-->  Redis  -->  Celery workers (N)  -->  PostgreSQL
  discovery, scheduler, indexer, verifier                          claim -> crawl_round -> store -> release
```

## Install
```
pip install -r requirements.txt
copy .env.example .env      # edit DATABASE_URL, REDIS_URL ...
```

## Start (Windows development, 4 terminals + Redis)
Redis has no official Windows build: use Memurai, a Redis for Windows port, Docker (`docker run -p 6379:6379 redis:7`) or WSL (`sudo apt install redis-server && redis-server`).
```
redis-server                                                        :: 1. Redis (or the Docker/WSL equivalent)
celery -A celery_app worker -P solo -n w1@%h -l INFO               :: 2. worker (solo pool: prefork does not work on Windows); repeat with w2, w3 ... for more
celery -A celery_app flower --port=5555                             :: 3. Flower: http://localhost:5555
set USE_CELERY=1 && python autonomous.py                            :: engine: discovery/admission + keeps the Celery queue fed
uvicorn backlink_api:app --port 8000                                :: 4. existing FastAPI app (unchanged)
```
## Start (Linux production)
```
redis-server --appendonly yes
celery -A celery_app worker -P prefork -c $CELERY_WORKER_CONCURRENCY -n w1@%h -l INFO     # per host; add hosts for scale
celery -A celery_app flower --port=5555 --basic-auth=user:password
USE_CELERY=1 python autonomous.py
uvicorn backlink_api:app --host 0.0.0.0 --port 8000
```
Use systemd/supervisor with `Restart=always`. Stop workers with SIGTERM (warm shutdown: the running batch finishes, leases are released).

## Monitoring
- Every 10 s the engine logs `DIAG {...}` (logger `crawler.diag`): discovered / pending / processing / completed / failed URLs, open DB
  connections vs `DB_MAX_CONNECTIONS`, `pg_stat_activity` / `pg_locks` / `pg_stat_database` data, long queries, and under `"celery"`:
  `queue_length`, `workers`, `active_tasks`, `retries`, `failures`, `tasks_done`, `avg_task_s`.
- Workers log `crawl.batch <id>: claimed N, returned unfinished M`, retries and failures (`crawler.celery`).
- Flower shows workers, active/failed tasks and the queue. `redis-cli llen celery` is the raw queue length.

## Behaviour
| Concern | How |
|---|---|
| Temporary DB errors | `autoretry_for` (OperationalError, timeouts, deadlock/serialization/lock timeout), exponential backoff + jitter, `CELERY_MAX_RETRIES` |
| Failed HTTP fetches | unchanged: Crawlee retries, then `mark_failed` -> `retry_pending` with backoff (5, 10, 20 ... min) -> `failed`. Never marked completed |
| Task timeout | hard `CELERY_TASK_TIME_LIMIT`; the task also enforces `limit-60s` itself with `asyncio.wait_for` (the Windows solo pool cannot enforce limits) |
| Worker crash | `acks_late` + `reject_on_worker_lost`: message redelivered, but it re-claims from PostgreSQL; the dead worker's leases are released by `crawl.recover` once expired |
| Graceful shutdown | `worker_shutting_down` releases the worker's leases at once |
| Duplicate URLs / backlinks | UNIQUE constraints + upserts; `SKIP LOCKED` claims; page writes serialized by the existing advisory lock |
| Connection exhaustion | per-process cap (`DB_MAX_CONNECTIONS`), server-side `lock_timeout` 60 s, `statement_timeout` 5 min, `idle_in_transaction_session_timeout` 5 min |
| Results | `task_ignore_result=True`, expiry 1 h |

## Limits
- One worker process runs one batch at a time on Windows (solo pool); scale with more worker processes.
- Page writes are still serialized across all workers by the advisory lock (the deadlock guard), so database writes, not HTTP, cap total throughput.
- Redis is not durable by default: a lost queue is rebuilt by the dispatcher from PostgreSQL within seconds.

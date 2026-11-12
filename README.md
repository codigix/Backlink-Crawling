# Backlink Crawler

Crawls the web, stores every URL, link and backlink in PostgreSQL. Crawling runs in Celery workers (Redis is the queue);
`autonomous.py` finds new domains and keeps the queue fed. More detail on the design: [CELERY.md](CELERY.md).

## One-time setup (Windows)

1. **PostgreSQL** must be running, with a database (default name `backlinks`).
2. **Redis** (Windows has no official build; Memurai is a free Redis-compatible service). In PowerShell:
   ```
   winget install Memurai.MemuraiDeveloper
   memurai-cli ping            # must print PONG  (if not: net start Memurai)
   ```
3. **Python packages**, inside the project venv:
   ```
   cd C:\Users\User\Desktop\Backlink-Crawling
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
4. **`.env`** (see `.env.example`). It must contain exactly ONE `DATABASE_URL` line (your real one) and:
   ```
   REDIS_URL=redis://localhost:6379/0
   CELERY_BROKER_URL=redis://localhost:6379/0
   CELERY_RESULT_BACKEND=redis://localhost:6379/1
   USE_CELERY=1
   ```

## Run (every command in its own terminal; in each, first run `cd` to the project and `.venv\Scripts\activate`)

| # | Terminal | Command |
|---|---|---|
| 1 | Redis | nothing to do: Memurai runs as a Windows service |
| 2-5 | Celery workers (4) | `celery -A celery_app worker -P solo -n w1@%h -l INFO` (then `w2`, `w3`, `w4`) |
| 6 | Crawler engine | `python autonomous.py` |
| 7 | Flower dashboard | `celery -A celery_app flower --port=5555 --inspect-timeout=10000` then open http://localhost:5555 |
| 8 | API (optional) | `uvicorn backlink_api:app --port 8000` |

Start order: Redis, workers, engine, then Flower and the API. Run only ONE `autonomous.py`.
`-P solo` is required on Windows (no prefork); one worker process = one crawl batch at a time, so start several.

## Stop
Ctrl+C once in each terminal. A worker finishes its batch and returns unfinished URLs to the queue.
If a worker is killed hard, its URLs are recovered automatically after `CELERY_TASK_TIME_LIMIT` (15 min).

## Is it working?
- Engine log: `dispatched N crawl task(s)`, and a `DIAG {...}` line every 10 s (its `celery` part: `queue_length`, `workers`, `retries`, `failures`).
- Worker logs: `crawl.batch ...: claimed N`.
- `memurai-cli llen celery` = tasks waiting. Flower shows workers and tasks.
- Flower warnings `Inspect method ... failed` are harmless: a busy solo worker cannot answer the status ping in time.

## Linux production
```
redis-server --appendonly yes
celery -A celery_app worker -P prefork -c 4 -n w1@%h -l INFO     # 4 crawl tasks in parallel per host
celery -A celery_app flower --port=5555 --basic-auth=user:password
USE_CELERY=1 python autonomous.py
uvicorn backlink_api:app --host 0.0.0.0 --port 8000
```
Run workers under systemd/supervisor with `Restart=always`.

## Tests
```
python -m pytest -q                                   # full suite (needs PostgreSQL, no Redis)
python tests/run_celery_live.py                       # live: real workers + Redis, 500 URLs, worker kill + recovery
LIVE_BROKER=filesystem python tests/run_celery_live.py   # same without Redis
```

## Troubleshooting
- `password authentication failed for user "USER"`: `.env` has a second, placeholder `DATABASE_URL`. Delete it.
- `No module named 'celery'`: you are not in the project venv (prompt must show `(crawlee)`), or skipped `pip install -r requirements.txt`.
- `Connection refused ... 6379`: Redis is not running.

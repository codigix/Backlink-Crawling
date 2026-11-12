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
   Or use Docker: `docker run -p 6379:6379 redis:7`.
3. **Python packages**, inside the project venv:
   ```
   cd C:\Users\User\Desktop\Backlink-Crawling
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   playwright install chromium     # browser for JavaScript-rendered pages
   ```
4. **`.env`**: copy `.env.example` to `.env` and edit. It must contain exactly ONE `DATABASE_URL` line (your real one) and:
   ```
   DATABASE_URL=postgresql://USER:PASSWORD@localhost:5432/backlinks
   REDIS_URL=redis://localhost:6379/0
   CELERY_BROKER_URL=redis://localhost:6379/0
   CELERY_RESULT_BACKEND=redis://localhost:6379/1
   USE_CELERY=1
   ```
5. **Only if Windows Application Control blocks files** (errors like `An Application Control policy has blocked this file`):
   - `celery.exe` is blocked: always start Celery as `python -m celery ...` (the commands below already do).
   - psycopg fails with `no pq wrapper available`: its bundled DLL is blocked, so use the `libpq.dll` of your PostgreSQL install. Run once
     (change `18` to your PostgreSQL version), then close ALL terminals and restart VS Code:
     ```
     [Environment]::SetEnvironmentVariable("PSYCOPG_IMPL", "python", "User")
     $p = [Environment]::GetEnvironmentVariable("PATH", "User")
     if ($p -notlike "*PostgreSQL\18\bin*") { [Environment]::SetEnvironmentVariable("PATH", "C:\Program Files\PostgreSQL\18\bin;" + $p, "User") }
     ```
     Check: `python -c "import psycopg; print(psycopg.pq.__impl__)"` must print `python`.
     If that file is blocked too, ask your admin to allow the project folder, or run in WSL/Docker.

## Run (every command in its own terminal)

First check Redis: `memurai-cli ping` must print `PONG`. Every terminal starts with:
```
cd C:\Users\User\Desktop\Backlink-Crawling
.venv\Scripts\activate
```
Then, in this order:

| # | Terminal | Command |
|---|---|---|
| 1 | Celery worker 1 | `python -m celery -A celery_app worker -P solo -n w1@pc -l INFO` |
| 2 | Celery worker 2 (optional, more = faster) | `python -m celery -A celery_app worker -P solo -n w2@pc -l INFO` (then `w3`, `w4`) |
| 3 | Crawler engine | `$env:USE_CELERY=1; python autonomous.py` |
| 4 | API | `python -m uvicorn backlink_api:app --port 8000` (docs: http://127.0.0.1:8000/docs) |
| 5 | Flower dashboard (optional) | `python -m celery -A celery_app flower --port=5555 --inspect-timeout=10000` (http://localhost:5555) |

Start order: Redis, workers, engine, then the API and Flower. Run only ONE `autonomous.py`.
`-P solo` is required on Windows (no prefork); one worker process = one crawl batch at a time, so start several.
Give every worker its own name (`w1`, `w2`, ...). `%h` does not work in PowerShell, so the examples use `@pc`.

The engine runs until you stop it: it keeps finding domains (Tranco, feeds, Common Crawl), keeps the Celery queue full and re-verifies known backlinks.
It indexes backlinks to every site it crawls; look up a domain with `GET /backlinks/<domain>`. Coverage grows over time and is never complete.

Useful extras (same venv terminal):
```
python autonomous.py seed example.com --directories      # queue domains/URLs first (--directories adds the agency directories)
python backlink_checker.py codigix.co                    # one-off: check the default source sites for links to a domain
```

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
- `celery.exe ... blocked` or `no pq wrapper available`: see setup step 5.
- `/api/backlinks/*` and `POST /api/backlinks/discover` are not wired into the app yet; use `/backlinks/<domain>` and `/crawler/*`.

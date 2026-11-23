# Staging runbook and monitoring plan (prepared, NOT executed)

Scope: a **staging** rollout of the database optimizations (pooling, batching, bounded reads, indexes, `/history` rewrite, optional read cache).
Nothing here has been run against any staging or production system. Every threshold is a **proposal**, not a measured fact. This machine's `.env`
points `DATABASE_URL` at the production SSH tunnel (port 5433): never run any step below from a shell that inherits it.

## 0. Preconditions (stop if any is false)
- A dedicated staging PostgreSQL and Redis exist; their URLs are in a staging-only environment file or service unit, never in the development `.env`.
- You can name the staging target as `host:port/dbname` and it is not port 5433.
- A **verified** backup exists (step 1). Staging holds a restored copy of production data or synthetic data, never the live production database.

## 1. Backup and restore verification
1. `pg_dump -Fc` the staging database (or the source copy) to a file outside the application directory; record size and checksum.
2. Restore it into a throw-away database on the same server and compare row counts of `discovered_urls`, `outgoing_links`, `link_history`, `crawl_history`,
   `backlinks`, `businesses`. They must match.
3. `tests/rehearse_migration.py` automates this, **but it drops and recreates a `<db>_rehearsal` database and writes `backups/`**: run it only on the
   staging host, against a staging URL you have checked, with explicit approval.

## 2. Dependencies
`pip install -r requirements.txt` in a fresh virtualenv (pins `psycopg[binary]==3.3.6`, `psycopg-pool==3.3.0`).

## 3. psycopg implementation check
`python -c "import psycopg; print(psycopg.pq.__impl__)"` must print `binary` (or `c`) on the staging host. `python` means the pure-Python implementation, several
times slower at turning rows into Python objects (measured here: about 16-33 us per row for 100k-row reads). On the Windows development machine it prints `python` on
purpose (README step 5: Application Control blocks the bundled DLL; `PSYCOPG_IMPL=python`). Do not copy that variable to staging.

## 4. Connection budget
Total possible connections = sum of every process's pool maximum + one unpooled advisory-lock connection per engine. Count the processes that will really run:

| Process | Count | Pool maximum (proposed) |
|---|---|---|
| API (`uvicorn`; every `--workers` is its own process) | A | 6-8 (`DB_POOL_MAX`) |
| Engine (`autonomous.py`; run exactly one) | 1 (+1 unpooled) | 6 |
| Celery crawl workers (`-P solo`: one process each; prefork `-c C`: C processes per worker) | W x C | **10** (`DB_POOL_MAX=10`: the crawl round starts 8 writers that each hold a connection, plus claim/renewal/failure paths) |
| `crawler_worker.py`, `autonomous.py seed`, `backlink_checker.py`, `psql` | only while used | 6 each, or counted as admin |

Examples: API 1 + engine 1 + 4 solo workers at pool 10 = 6 + 6 + 40 + 1 = **53**. At the default derived pool of 6 it is 37. Set `DB_MAX_CONNECTIONS` (documentation and warning only)
to the computed total, and require PostgreSQL `max_connections` >= total + `superuser_reserved_connections` (3) + about 10 for admin, backup and monitoring.
Check on the server: `SHOW max_connections;` and `SELECT count(*) FROM pg_stat_activity;`. Set `DB_POOL_MAX` per process role in its own service unit (a process
environment variable beats `.env`).

## 5. Startup order (staging only)
1. PostgreSQL, Redis. 2. **API or engine first starts `migrate()`**: `backlink_api.py` runs `crawler_core.migrate(...)` at import, so the first process that starts applies pending
schema versions (the code defines up to v15; a database from another branch may already hold v10/v11 with different content: compare `schema_migrations` first). Start the
migration deliberately, with a single process and the backup in place. 3. Celery workers. 4. One engine (`USE_CELERY=1 python autonomous.py`). 5. API. 6. Flower (optional).
Keep `GRAPH_CACHE_ENABLED=false`.

## 6. Index build (operator, once, off-peak)
1. Dry run: `INDEX_DATABASE_URL=<staging url> python db_indexes.py` prints the target as `host:port/dbname` (no credentials) and what is missing; port 5433 is refused.
2. Check disk headroom (about 1.5x the sizes in the report: roughly 20-60 MB per 100k-500k rows per index) and that no long transaction is open
   (`SELECT pid, now()-xact_start FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY 2 DESC LIMIT 5;`).
3. Apply: `INDEX_DATABASE_URL=<staging url> python db_indexes.py --apply --confirm-target <host:port/dbname exactly as printed>`.
4. Verify: exit code 0; `SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE NOT indisvalid;` returns nothing. An `INVALID` index is reported, never dropped by the script:
   review it, then drop it by hand with `DROP INDEX CONCURRENTLY <name>` yourself.
5. `ANALYZE` the four tables; re-run the dry run: every line must say `exists`.

## 7. Cache activation (after the baseline is recorded)
1. Record the baseline (section 9) for at least 24 hours with the cache off.
2. Enable on **one** API process: `GRAPH_CACHE_ENABLED=true`, `GRAPH_CACHE_TTL_SECONDS=15` (never above `GRAPH_CACHE_MAX_TTL_SECONDS=30`), `GRAPH_CACHE_MAX_ENTRIES=1000`, `GRAPH_CACHE_MAX_MB=32`.
3. Watch `X-Cache` / `X-Cache-Age` and the counters (hit ratio, evictions, bytes). Totals can be up to one TTL behind the crawl; job status is live.
4. Keep it only if the hit ratio justifies it (proposal: >= 40% of cacheable requests) and no stale-data complaint appears; otherwise set it back to `false` and restart that process.

## 8. Smoke tests (read-only unless noted)
- `GET /backlinks/<known domain>?limit=5`, `/referring-domains`, `/anchors`, `/history`: HTTP 200, `Server-Timing` present, JSON shape unchanged.
- Cache on: second identical request shows `X-Cache: HIT`; a subdomain target is a separate entry.
- `GET /discovery/statistics`, `/crawler/global-status`: pool figures visible, `requests_errors` 0.
- Onboard a synthetic test domain with a staging-only seed; one crawl batch completes; `claim` leases each URL once.
- Pool: under a short load, `requests_waiting` returns to 0 and no `TimeoutError`.

## 9. Monitoring plan (what to record; thresholds are PROPOSED)
| Measure | Source | Local baseline (synthetic, not production) | Proposed alert |
|---|---|---|---|
| API median / p95 latency per route | `Server-Timing` total, access log | summary 120-200 ms, referring 150-270 ms warm | p95 > 2x the staging baseline for 10 min |
| DB time per request | `db_sql`, `db_fetch` in `Server-Timing` | see above | median > 1.5x baseline |
| Slow requests | `SLOW-API` log (default `API_SLOW_MS=1000`; lower to 300 for staging) | none | > 1% of requests |
| Pool wait | `pool_stats()` (`requests_wait_ms`, `requests_waiting`) in `/discovery/statistics` | mean 64 ms under 16 threads on 6 connections | mean wait > 1 s, or any request waiting > 5 s |
| Pool failures | `requests_errors`, `TimeoutError` in logs | 0 | any, page at 3 in 10 min |
| Connections | `pg_stat_activity` count by `application_name='backlink-crawler'` | peak 6 per process | > 80% of the computed total, or > 80% of `max_connections` |
| Cache | `graph_cache.stats()` (hits, misses, stale, errors, evictions, bytes), `X-Cache` | miss overhead < 5% | hit ratio < 20% for an hour (cost without benefit); `stale` > 0 for 5 min (database trouble); `bytes` > 80% of limit |
| Crawler throughput | engine `DIAG` log (pages/min), `crawl_history` completions per minute | n/a | pages/min < 70% of the staging baseline for 30 min |
| Discovery / verification rates | new `outgoing_links` per hour, `link_history` events, verified backlinks | n/a | drop > 50% vs the same hour yesterday |
| Storage growth | `pg_total_relation_size` of the four tables and their indexes, daily | indexes add 8-60 MB per 100k-500k rows | growth > 1.5x the 7-day trend |
| Locks / long transactions | `pg_stat_activity`, `pg_locks` | n/a | any transaction > 5 min (the server also cuts at 5 min) |

## 10. Rollback
- **Cache:** set `GRAPH_CACHE_ENABLED=false` and restart the API process: no data is affected.
- **Pool / batching / history rewrite:** redeploy the previous code revision; nothing in these changes alters the schema or rows.
- **Indexes:** additive; leaving them is safe. To remove one, `DROP INDEX CONCURRENTLY <name>` by hand, only after checking `pg_stat_user_indexes.idx_scan` and with approval.
- **Migrations applied by the first start:** restore from the step-1 backup into a new database and repoint; do not hand-edit `schema_migrations`.

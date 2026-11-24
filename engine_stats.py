"""Frontier / graph counters shared by the discovery endpoints."""

import copy
import logging
import os
import threading
import time

import api_timing
import crawler_core as core

log = logging.getLogger("crawler.stats")

# Snapshot freshness for the API: 10-30 s (STATS_TTL_SECONDS, default 15). Past it the next request refreshes; if the refresh fails the
# last snapshot is served (flagged X-Stats-Stale) for at most STATS_MAX_STALE_SECONDS, after that StatsUnavailable.
STATS_TTL = min(30.0, max(10.0, float(os.environ.get("STATS_TTL_SECONDS", 15))))
STATS_MAX_STALE = float(os.environ.get("STATS_MAX_STALE_SECONDS", 300))
STATS_RETRY = float(os.environ.get("STATS_RETRY_SECONDS", 60))  # after a failed refresh, no other attempt for this long (a down database must not cost every request a connect timeout, and a timing-out full scan must not be repeated every few seconds)

_lock = threading.Lock()
_snap: tuple[float, dict] | None = None  # (monotonic time taken, counts()-shaped dict): one entry per process, so memory is constant
_retry_at = 0.0


class StatsUnavailable(RuntimeError):
    """No snapshot yet (or none young enough) and the database could not produce one."""


def _compute(c) -> dict:
    """The counts() payload from three table passes (urls: status x method in ONE GROUP BY) instead of six queries. Totals are the sums of
    their groups, so they always agree with the breakdowns."""
    urls_by_status: dict = {}
    urls_by_source: dict = {}
    for r in c.execute("SELECT crawl_status, discovery_method, COUNT(*) FROM discovered_urls GROUP BY 1, 2").fetchall():  # r[i]: Row unpacking yields column names
        urls_by_status[r[0]] = urls_by_status.get(r[0], 0) + r[2]
        urls_by_source[r[1]] = urls_by_source.get(r[1], 0) + r[2]
    domains_by_status = {r[0]: r[1] for r in c.execute("SELECT discovery_status, COUNT(*) FROM discovered_domains GROUP BY 1")}
    return {
        "discovered_domains": sum(domains_by_status.values()),
        "discovered_urls": sum(urls_by_status.values()),
        "outgoing_links": c.execute("SELECT COUNT(*) FROM outgoing_links WHERE is_active=1").fetchone()[0],
        "domains_by_status": domains_by_status,
        "urls_by_status": urls_by_status,
        "urls_by_source": urls_by_source,
    }


def counts() -> dict:
    """Exact, uncached (tests and tools). The API uses snapshot_counts()."""
    with core.connect() as c:
        return _compute(c)


def _fresh() -> bool:
    return _snap is not None and time.monotonic() - _snap[0] < STATS_TTL


def snapshot_counts(connect=None) -> dict:
    """counts() from a process-wide snapshot: computed at most once per STATS_TTL however many requests and endpoints ask.
    Thread-safe (the API is synchronous: threadpool + psycopg). Concurrent callers on an expired snapshot: with an older one to serve they
    get it immediately while one thread refreshes; with none (first request) they wait for that single refresh. `connect()` is only called
    when a refresh is needed and must return a context manager yielding a connection. Raises StatsUnavailable if nothing usable exists."""
    global _snap, _retry_at
    if not _fresh() and _lock.acquire(blocking=_snap is None):
        try:
            if not _fresh() and time.monotonic() >= _retry_at:
                try:
                    t0 = time.monotonic()
                    with (connect or core.connect)() as c:
                        _snap = (t0, _compute(c))
                except Exception:  # noqa: BLE001 - serve the old snapshot (or StatsUnavailable below); the next attempt waits STATS_RETRY
                    _retry_at = time.monotonic() + STATS_RETRY
                    log.warning("statistics refresh failed; serving the previous snapshot if it is young enough", exc_info=True)
        finally:
            _lock.release()
    s = _snap
    age = time.monotonic() - s[0] if s else None
    if s is None or age > STATS_MAX_STALE:
        raise StatsUnavailable("statistics snapshot unavailable")
    api_timing.note("X-Stats-Age", f"{age:.0f}")
    if age >= STATS_TTL:
        api_timing.note("X-Stats-Stale", "1")
    return copy.deepcopy(s[1])


def reset_snapshot() -> None:
    """Forget the snapshot (tests)."""
    global _snap, _retry_at
    with _lock:
        _snap, _retry_at = None, 0.0


# Monitoring-only full-table aggregates (engine log lines; NO scheduling decision reads them). They used to run every 10-60 s and, on 10M+ rows,
# were cancelled by the statement timeout every time: pure load. Now: at most one refresh per SLOW_TTL, in ONE background thread, with its own
# longer timeout; a failed refresh waits a full SLOW_TTL before the next attempt; callers never wait and get the last value (or None) + its age.
SLOW_TTL = float(os.environ.get("MONITOR_SLOW_TTL_SECONDS", 600))
SLOW_TIMEOUT = os.environ.get("MONITOR_SLOW_TIMEOUT_SECONDS", "90")
_slow: dict[str, dict] = {}
_slow_lock = threading.Lock()
_slow_running: set[str] = set()


def slow_cached(key: str, sql: str, shape):
    """(value, age_seconds) of a monitoring aggregate, or (None, None) until the first refresh succeeds. `shape(cursor)` turns the result into the value."""
    now = time.monotonic()
    with _slow_lock:
        e = _slow.setdefault(key, {"value": None, "at": None, "next": 0.0})
        due = now >= e["next"] and key not in _slow_running
        if due:
            _slow_running.add(key)
            e["next"] = now + SLOW_TTL  # also the back-off after a failure: success or not, no second attempt before SLOW_TTL
        value, at = e["value"], e["at"]

    def refresh():
        try:
            with core.connect() as c:
                c.execute(f"SET LOCAL statement_timeout='{int(float(SLOW_TIMEOUT))}s'")  # transaction-scoped: pooled connection stays clean
                v = shape(c.execute(sql))
            with _slow_lock:
                _slow[key].update(value=v, at=time.monotonic())
        except Exception as ex:  # noqa: BLE001 - monitoring only: keep the previous value
            log.warning("monitoring aggregate %s failed (%s); next attempt in %.0fs", key, type(ex).__name__, SLOW_TTL)
        finally:
            with _slow_lock:
                _slow_running.discard(key)
    if due:
        threading.Thread(target=refresh, name=f"stats-{key}", daemon=True).start()
    return value, (None if at is None else round(now - at))


def metrics() -> dict:
    """Real-database snapshot for the engine's 60s log line. Rates are over the last 60 seconds. The full-table aggregates are cached (see slow_cached)."""
    urls, urls_age = slow_cached("urls_by_status", "SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1", lambda cur: {r[0]: r[1] for r in cur})
    repeats, _ = slow_cached("repeat_fetches", "SELECT COUNT(*)-COUNT(DISTINCT source_url) FROM crawl_history", lambda cur: cur.fetchone()[0])
    with core.connect() as c:
        by = lambda sql: {r[0]: r[1] for r in c.execute(sql)}  # noqa: E731
        one = lambda sql, *a: c.execute(sql, a).fetchone()[0]  # noqa: E731
        minute = core.later(seconds=-60)
        return {
            "domains": by("SELECT discovery_status, COUNT(*) FROM discovered_domains GROUP BY 1"),
            "urls": urls, "urls_age_s": urls_age,
            "recrawl_backlog": one("SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='completed' AND next_crawl_at<=?", core.now()),
            "repeat_fetches": repeats,
            "backlinks": by("SELECT link_status, COUNT(*) FROM backlinks GROUP BY 1"),
            "pages_per_min": one("SELECT COUNT(*) FROM crawl_history WHERE crawl_completed>=? AND error_message IS NULL", minute),
            "urls_discovered_per_min": one("SELECT COUNT(*) FROM discovered_urls WHERE first_seen>=?", minute),
            "workers": {r["name"]: [r["status"], r["last_heartbeat_at"], r["restart_count"]] for r in c.execute("SELECT * FROM worker_status")},
            "db_busy": core.BUSY_ERRORS,
        }


def diagnostics(stats: dict) -> dict:
    """10-second diagnostic snapshot: frontier counts, our connections, PostgreSQL's own view (pg_stat_activity / pg_locks /
    pg_stat_database), and HTTP timings. `stats` is crawler_engine.HTTP. Uses its own short-lived connection with timeouts."""
    urls, urls_age = slow_cached("urls_by_status", "SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1", lambda cur: {r[0]: r[1] for r in cur})
    urls = urls or {}
    with core.connect() as c:
        c.execute("SET LOCAL statement_timeout='5s'")  # transaction-scoped: a session-level SET would stay on the pooled connection
        q = lambda sql, *a: [list(r.values()) for r in c.execute(sql, a).fetchall()]  # noqa: E731 - plain lists
        act =q("SELECT state, wait_event_type, COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()"
                " AND application_name='backlink-crawler' GROUP BY 1,2")
        locks = q("SELECT COUNT(*) FILTER (WHERE NOT granted) a, COUNT(*) FILTER (WHERE locktype='advisory') b,"
                  " COUNT(*) FILTER (WHERE locktype='advisory' AND NOT granted) c FROM pg_locks")[0]
        long = q("SELECT pid, state, wait_event_type||':'||wait_event, round(extract(epoch FROM now()-query_start)::numeric,1), left(query,90)"
                 " FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND application_name='backlink-crawler'"
                 " AND state<>'idle' AND now()-query_start>interval '5 seconds' ORDER BY query_start LIMIT 5")
        idle_tx = q("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'"
                    " AND now()-xact_start>interval '10 seconds'")[0][0]
        db = q("SELECT numbackends, xact_commit, xact_rollback, deadlocks, temp_files FROM pg_stat_database WHERE datname=current_database()")[0]
    n = stats["done"]
    return {
        "frontier_counts_age_s": urls_age,  # None until the first cached aggregate exists; monitoring only
        "discovered": sum(urls.values()), "pending": urls.get("pending", 0) + urls.get("retry_pending", 0),
        "processing": urls.get("crawling", 0) + urls.get("recrawling", 0), "completed": urls.get("completed", 0), "failed": urls.get("failed", 0),
        "inflight_fetches": stats["started"] - n, "pool_size": f"max {core.POOL_MAX} per process; checked out now={core.OPEN_CONNS}", "pool": {k: v for k, v in core.pool_stats().items() if k in ("pool_size", "pool_available", "requests_waiting", "requests_wait_ms", "connections_num", "usage_ms")},
        "pg_conns": {f"{a}/{b}": k for a, b, k in act}, "waiting_conns": sum(k for a, b, k in act if b == "Lock"),
        "ungranted_locks": locks[0], "advisory_locks": locks[1], "advisory_waiting": locks[2], "idle_in_tx_10s+": idle_tx,
        "long_queries": [list(map(str, r)) for r in long],
        "retries": stats["retries"], "avg_http_s": round(stats["total"] / n, 2) if n else None,
        "pg_db": {"backends": db[0], "commits": db[1], "rollbacks": db[2], "deadlocks": db[3]},
    }

"""Frontier / graph counters shared by the discovery endpoints."""

import crawler_core as core


def counts() -> dict:
    with core.connect() as c:
        one = lambda sql: c.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "discovered_domains": one("SELECT COUNT(*) FROM discovered_domains"),
            "discovered_urls": one("SELECT COUNT(*) FROM discovered_urls"),
            "outgoing_links": one("SELECT COUNT(*) FROM outgoing_links WHERE is_active=1"),
            "domains_by_status": {r[0]: r[1] for r in c.execute("SELECT discovery_status, COUNT(*) FROM discovered_domains GROUP BY 1")},
            "urls_by_status": {r[0]: r[1] for r in c.execute("SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1")},
            "urls_by_source": {r[0]: r[1] for r in c.execute("SELECT discovery_method, COUNT(*) FROM discovered_urls GROUP BY 1")},
        }


def metrics() -> dict:
    """Real-database snapshot for the engine's 60s log line. Rates are over the last 60 seconds."""
    with core.connect() as c:
        by = lambda sql: {r[0]: r[1] for r in c.execute(sql)}  # noqa: E731
        one = lambda sql, *a: c.execute(sql, a).fetchone()[0]  # noqa: E731
        minute = core.later(seconds=-60)
        return {
            "domains": by("SELECT discovery_status, COUNT(*) FROM discovered_domains GROUP BY 1"),
            "urls": by("SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1"),
            "recrawl_backlog": one("SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='completed' AND next_crawl_at<=?", core.now()),
            "repeat_fetches": one("SELECT COUNT(*)-COUNT(DISTINCT source_url) FROM crawl_history"),
            "backlinks": by("SELECT link_status, COUNT(*) FROM backlinks GROUP BY 1"),
            "cc_jobs": by("SELECT status, COUNT(*) FROM discovery_jobs WHERE source='commoncrawl' GROUP BY 1"),
            "pages_per_min": one("SELECT COUNT(*) FROM crawl_history WHERE crawl_completed>=? AND error_message IS NULL", minute),
            "urls_discovered_per_min": one("SELECT COUNT(*) FROM discovered_urls WHERE first_seen>=?", minute),
            "workers": {r["name"]: [r["status"], r["last_heartbeat_at"], r["restart_count"]] for r in c.execute("SELECT * FROM worker_status")},
            "db_busy": core.BUSY_ERRORS,
        }


def diagnostics(stats: dict) -> dict:
    """10-second diagnostic snapshot: frontier counts, our connections, PostgreSQL's own view (pg_stat_activity / pg_locks /
    pg_stat_database), and HTTP timings. `stats` is crawler_engine.HTTP. Uses its own short-lived connection with timeouts."""
    with core.connect() as c:
        c.execute("SET statement_timeout='5s'")
        q = lambda sql, *a: [list(r.values()) for r in c.execute(sql, a).fetchall()]  # noqa: E731 - plain lists
        urls = {r[0]: r[1] for r in q("SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1")}
        act = q("SELECT state, wait_event_type, COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()"
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
        "discovered": sum(urls.values()), "pending": urls.get("pending", 0) + urls.get("retry_pending", 0),
        "processing": urls.get("crawling", 0) + urls.get("recrawling", 0), "completed": urls.get("completed", 0), "failed": urls.get("failed", 0),
        "inflight_fetches": stats["started"] - n, "pool_size": f"none (new connection per call); open now={core.OPEN_CONNS}",
        "pg_conns": {f"{a}/{b}": k for a, b, k in act}, "waiting_conns": sum(k for a, b, k in act if b == "Lock"),
        "ungranted_locks": locks[0], "advisory_locks": locks[1], "advisory_waiting": locks[2], "idle_in_tx_10s+": idle_tx,
        "long_queries": [list(map(str, r)) for r in long],
        "retries": stats["retries"], "avg_http_s": round(stats["total"] / n, 2) if n else None,
        "pg_db": {"backends": db[0], "commits": db[1], "rollbacks": db[2], "deadlocks": db[3]},
    }

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
            "sqlite_busy": core.BUSY_ERRORS,
        }

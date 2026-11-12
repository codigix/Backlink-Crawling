"""Performance check (not a pytest test). Isolated schema, local server.
  CRAWLER_ALLOW_PRIVATE=1 USE_CELERY=0 python tests/perf_bench.py
1) page persistence: old behaviour (one global advisory lock around every page) vs per-domain locks, same workload, 8 threads
2) end-to-end crawl of a local site set through crawl_round, with engine_stats metrics."""

import asyncio
import os
import sys
import threading
import time
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path

os.environ.setdefault("CRAWLER_ALLOW_PRIVATE", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import psycopg  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

import crawler_core as core  # noqa: E402
import crawler_engine as engine  # noqa: E402
import engine_stats  # noqa: E402
from test_crawler import HITS, SITES, Handler, page, a  # noqa: E402

DOMAINS, PAGES, LINKS, THREADS = 16, 20, 60, 8


def persistence(global_lock: bool) -> dict:
    with core.connect() as c:
        c.execute("TRUNCATE discovered_urls, discovered_domains, outgoing_links, crawl_history, backlinks, backlink_history, link_history, target_domains CASCADE")
        core.add_target(c, "shared3.org")
    cfg, errors, jobs = engine.Config(), [], []
    for d in range(DOMAINS):
        for n in range(PAGES):
            html = "".join(f'<a href="https://shared{(d + i) % 12}.org/page{i}">a{i}</a>' for i in range(LINKS)) + f'<a href="/p{n + 1}">n</a>'
            jobs.append((f"http://site{d}.org/p{n}", html))
    with core.connect() as c:
        for u, _ in jobs:
            core.save_url(c, u, None, "t", 0, True)
    waits0 = core.BUSY_ERRORS

    def work(chunk):
        for url, html in chunk:
            def once():
                with core.connect() as conn:
                    if global_lock:
                        conn.execute("SELECT pg_advisory_xact_lock(724002)")
                    engine.process_page(conn, url, url, 0, BeautifulSoup(html, "html.parser"), 200, "text/html", cfg, set(), False, core.now())
            try:
                core.retry_locked(once)
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
    ts = [threading.Thread(target=work, args=(jobs[i::THREADS],)) for i in range(THREADS)]
    t0 = time.monotonic()
    [t.start() for t in ts]
    [t.join() for t in ts]
    dt = time.monotonic() - t0
    with core.connect() as c:
        dup = c.execute("SELECT COUNT(*) FROM (SELECT 1 FROM outgoing_links GROUP BY source_url,destination_url,anchor_text HAVING COUNT(*)>1) x").fetchone()[0]
        done = c.execute("SELECT COUNT(*) FROM crawl_history").fetchone()[0]
        bad = c.execute("SELECT COUNT(*) FROM discovered_domains d WHERE pages_discovered != (SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=d.domain)").fetchone()[0]
        dead = c.execute("SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()").fetchone()[0]
    return {"mode": "global lock (old)" if global_lock else "per-domain (new)", "pages": done, "seconds": round(dt, 2), "pages_per_s": round(done / dt, 1),
            "errors": len(errors), "retried_deadlocks": core.BUSY_ERRORS - waits0, "duplicate_links": dup, "wrong_domain_counters": bad, "pg_deadlocks_total": dead}


def e2e(port: int) -> dict:
    hosts = [f"w{i}" for i in range(6)]
    for h in hosts:
        SITES[f"{h}.localhost"] = {"/": page(*[a(f"/p{n}") for n in range(40)], a("https://codigix.co/x", "t")),
                                   **{f"/p{n}": page(a(f"/p{(n + 1) % 40}"), a("https://codigix.co/y", "t")) for n in range(40)}}
    cfg = engine.Config(domain_delay=0.05, timeout=10, max_depth=2, concurrency=10, target_domain="codigix.co", max_pages=1000)
    t0 = time.monotonic()
    asyncio.run(engine.run_crawl(cfg, [f"http://{h}.localhost:{port}/" for h in hosts]))
    dt = time.monotonic() - t0
    m = engine_stats.metrics()
    with core.connect() as c:
        pages = c.execute("SELECT COUNT(*) FROM crawl_history WHERE error_message IS NULL").fetchone()[0]
        links = c.execute("SELECT COUNT(*) FROM outgoing_links").fetchone()[0]
        refs = c.execute("SELECT COUNT(DISTINCT source_domain) FROM backlinks b JOIN outgoing_links o ON o.source_url=b.source_url").fetchone()[0]
        dups = c.execute("SELECT COUNT(*) FROM (SELECT source_url FROM crawl_history GROUP BY 1 HAVING COUNT(*)>1) x").fetchone()[0]
        stuck = c.execute("SELECT COUNT(*) FROM discovered_urls WHERE crawl_status IN ('crawling','recrawling')").fetchone()[0]
        pend = c.execute("SELECT COUNT(*) FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending')").fetchone()[0]
        failed = c.execute("SELECT COUNT(*) FROM discovered_urls WHERE crawl_status='failed'").fetchone()[0]
        backs = c.execute("SELECT COUNT(*) FROM backlinks WHERE link_status='verified'").fetchone()[0]
    return {"pages": pages, "seconds": round(dt, 1), "pages_per_min": round(pages / dt * 60), "links_stored": links, "backlinks_verified": backs,
            "referring_domains": refs, "avg_http_s": round(engine.HTTP["total"] / max(engine.HTTP["done"], 1), 3), "failed_urls": failed,
            "retries": engine.HTTP["retries"], "pending_queue": pend, "stuck_urls": stuck, "repeat_fetches": dups, "db_busy_retries": m["db_busy"],
            "open_db_conns": core.OPEN_CONNS}


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    schema = "perf_" + uuid.uuid4().hex[:8]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        core.DB_SCHEMA = schema
        try:
            with core.connect() as c:
                core.migrate(c)
            print(persistence(True))
            print(persistence(False))
            with core.connect() as c:
                c.execute("TRUNCATE discovered_urls, discovered_domains, outgoing_links, crawl_history, backlinks, backlink_history, link_history, target_domains CASCADE")
            print(e2e(srv.server_address[1]))
        finally:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")

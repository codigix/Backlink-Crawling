"""Run crawls as a separate process, so the API stays responsive. State lives in the shared SQLite file.

  python crawler_worker.py crawl --target fitness-events.com --seed https://example.com/ --max-pages 500
  python crawler_worker.py crawl            # resume pending URLs
  python crawler_worker.py recrawl [--url URL ...]

Run ONE worker at a time: startup recovery resets URLs left 'crawling' by a dead process.
"""

import argparse
import asyncio
import json
import logging
import uuid

import crawler_core as core
import crawler_engine as engine


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("crawl", help="crawl seeds, or resume pending URLs when no --seed is given")
    c.add_argument("--target")
    c.add_argument("--seed", action="append", default=[])
    c.add_argument("--max-pages", type=int, default=10_000)
    c.add_argument("--max-depth", type=int, default=5)
    c.add_argument("--max-domains", type=int, default=20)
    c.add_argument("--external-discovery", action="store_true")
    r = sub.add_parser("recrawl", help="refetch due pages (or the given --url pages)")
    r.add_argument("--url", action="append")
    for p in (c, r):
        p.add_argument("--concurrency", type=int, default=5)
        p.add_argument("--playwright", action="store_true")
    a = ap.parse_args(argv)

    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%H:%M:%S"))
    logging.getLogger("crawler").addHandler(h)
    logging.getLogger("crawler").setLevel(logging.INFO)
    logging.getLogger("crawler").propagate = False

    with core.connect() as conn:
        core.migrate(conn)  # includes crash recovery; the API process deliberately does not do this
    if a.cmd == "crawl":
        target = core.clean_target(a.target) if a.target else None
        cfg = engine.Config(target_domain=target, max_pages=a.max_pages, max_depth=a.max_depth, concurrency=a.concurrency,
                            use_playwright=a.playwright, enable_external_domain_discovery=a.external_discovery,
                            max_domains=a.max_domains)
        coro = lambda: engine.run_crawl(cfg, a.seed, job_id)  # noqa: E731
    else:
        target = None
        cfg = engine.Config(use_playwright=a.playwright, concurrency=a.concurrency)
        coro = lambda: engine.recrawl_due(cfg, job_id, a.url)  # noqa: E731
    job_id = uuid.uuid4().hex
    with core.connect() as conn:
        conn.execute("INSERT INTO crawl_jobs (job_id, target_domain, status, config, created_at) VALUES (?,?,'running',?,?)",
                     (job_id, target, json.dumps(engine.config_dict(cfg)), core.now()))
    print(f"job {job_id}  (GET /crawler/status/{job_id})", flush=True)
    err = None
    try:
        asyncio.run(coro())
    except KeyboardInterrupt:
        err = "interrupted"
    except Exception as e:  # noqa: BLE001 - recorded on the job
        err = repr(e)
    status = "finished" if err is None else "interrupted" if err == "interrupted" else "failed"
    with core.connect() as conn:
        conn.execute("UPDATE crawl_jobs SET status=?, error=?, finished_at=? WHERE job_id=?", (status, err, core.now(), job_id))
    print(f"job {job_id} {status}", flush=True)
    return 0 if err is None else 1


if __name__ == "__main__":
    raise SystemExit(main())

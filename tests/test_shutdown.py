"""Phase 9.1: Ctrl+C during a crawl round must stop the process. crawlee's run() used to treat it as "pause this round" and swallow it, so a busy
engine / Celery worker ignored the first Ctrl+C. Real PostgreSQL, a real local site, a real round."""

import asyncio
import signal
import threading
import time

import pytest

import autonomous
import crawler_core as core
import crawler_engine as engine
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures


def test_sigint_during_a_round_raises_keyboard_interrupt_stores_nothing_half_done_and_leaves_no_lease(env):
    SITES["slow.localhost"] = {"/": page(*[a(f"/p{i}") for i in range(30)]), **{f"/p{i}": page("x") for i in range(30)}}
    with core.connect() as c:
        for p in ["/"] + [f"/p{i}" for i in range(30)]:
            core.save_url(c, env("slow", p), None, "test", 0, True)
        c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain='slow.localhost'")
    cfg = autonomous.AutoConfig(sources=("test",), claim_batch=31, per_batch=31, crawl=engine.Config(domain_delay=1.0, timeout=10, max_retries=1, max_depth=1, concurrency=3))
    work = autonomous.claim_work("w-int", cfg)
    assert len(work) > 5
    threading.Timer(2.5, lambda: signal.raise_signal(signal.SIGINT)).start()
    t0 = time.time()
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(engine.crawl_round(cfg.crawl, None, work, set(), False, len(work)))
    assert time.time() - t0 < 20  # not the 30+ s the full round needs
    autonomous.release("w-int")  # what the callers' finally does
    left = {r["crawl_status"]: r["n"] for r in rows("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}
    assert "crawling" not in left and left.get("pending", 0) > 0 and left.get("completed", 0) >= 1, left

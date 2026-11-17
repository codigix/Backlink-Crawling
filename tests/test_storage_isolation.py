"""Crawlee storage isolation: every crawler gets its own MemoryStorageClient, so concurrent crawlers/processes never share or purge
Crawlee's default ./storage. PostgreSQL stays the only durable state (restart recovery: test_crawler.py::test_restart_recovery)."""
import asyncio
import os

import pytest
from crawlee.storage_clients import MemoryStorageClient

import crawler_engine as engine
from test_crawler import SITES, a, cfg, env, page, port, rows  # noqa: F401 - fixtures


def storage_files() -> set:
    return {os.path.join(d, f) for d, _, fs in os.walk("storage") for f in fs}


def test_concurrent_crawlers_are_isolated_and_leave_no_files_in_default_storage(env):
    u = env
    before = storage_files()
    for h in ("a", "b", "c"):
        SITES[f"{h}.localhost"] = {"/": page(a("/p"), a(f"https://t-{h}.example/")), "/p": page("x")}

    async def go():  # three simultaneous rounds in one process, one of them failing: the others must finish
        bad = engine.crawl_round(cfg(), None, [("http://127.0.0.1:1/", 0)], set(), False, 1)
        return await asyncio.gather(*(engine.run_crawl(cfg(), [u(h)]) for h in ("a", "b", "c")), bad, return_exceptions=True)
    asyncio.run(go())
    done = {r["url"] for r in rows("SELECT url FROM discovered_urls WHERE crawl_status='completed'")}
    assert {u(h, "/p") for h in ("a", "b", "c")} <= done and {u(h) for h in ("a", "b", "c")} <= done  # each crawl's own queue and results
    assert storage_files() == before  # nothing written to, or purged from, ./storage


def test_every_crawler_gets_its_own_memory_storage_client_and_repeated_starts_do_not_conflict(env, monkeypatch):
    u = env
    seen = {"bs": [], "pw": []}
    real = engine.BeautifulSoupCrawler

    def bs(*args, **kw):
        seen["bs"].append(kw["storage_client"])
        return real(*args, **kw)

    class StopHere(Exception):
        pass

    def pw(*args, **kw):
        seen["pw"].append(kw["storage_client"])
        raise StopHere

    monkeypatch.setattr(engine, "BeautifulSoupCrawler", bs)
    monkeypatch.setattr(engine, "PlaywrightCrawler", pw)
    SITES.update({f"{h}.localhost": {"/": page("hi")} for h in "abc"})
    for h in "abc":  # repeated initialisation in one process (a completed page is not crawled again, so each round gets its own site)
        asyncio.run(engine.run_crawl(cfg(), [u(h)]))
    for _ in range(2):
        with pytest.raises(StopHere):
            asyncio.run(engine.crawl_round(cfg(use_playwright=True), None, [(u("a"), 0)], set(), False, 1))
    clients = seen["bs"] + seen["pw"]
    assert len(seen["bs"]) == 3 and len(seen["pw"]) == 2
    assert all(isinstance(c, MemoryStorageClient) for c in clients) and len({id(c) for c in clients}) == len(clients)

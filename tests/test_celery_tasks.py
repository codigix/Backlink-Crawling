"""Celery task logic against real PostgreSQL and controlled local sites. Tasks run in-process (apply()); Redis is not needed here.
The live Redis + multi-worker checks are in tests/run_celery_live.py."""

import threading

import pytest

import autonomous
import crawler_core as core
import tasks
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures

@pytest.fixture(autouse=True)
def no_redis(monkeypatch):
    monkeypatch.setattr(tasks, "queue_length", lambda: tasks.MAX_QUEUED)  # queue "full": tasks never try to reach Redis here


HOSTS = [f"s{i}" for i in range(6)]


def seed_sites(u, per_host=10):
    for h in HOSTS:
        SITES[f"{h}.localhost"] = {"/": page(*[a(f"/p{n}") for n in range(per_host - 1)]),
                                   **{f"/p{n}": page(a("https://target.example/x", "t")) for n in range(per_host - 1)}}
    with core.connect() as c:
        for h in HOSTS:
            for path in ["/"] + [f"/p{n}" for n in range(per_host - 1)]:
                core.save_url(c, u(h, path), None, "test", 0 if path == "/" else 1, True)
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (f"{h}.localhost",))


def status_counts():
    return {r["crawl_status"]: r["n"] for r in rows("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}


def test_batch_task_crawls_everything_and_leaves_no_lease(env):
    seed_sites(env)
    total = 0
    for _ in range(5):  # per_batch=10 URLs per domain per claim
        res = tasks.crawl_batch.apply()
        assert res.successful(), res.traceback
        if not res.result:
            break
        total += res.result
    assert total == 60 and status_counts() == {"completed": 60, "pending": 1}  # the pending one is the discovered external link (domain not admitted)
    assert rows("SELECT 1 FROM discovered_urls WHERE lease_owner IS NOT NULL") == []
    assert rows("SELECT source_url FROM crawl_history GROUP BY 1 HAVING COUNT(*)>1") == []  # each URL fetched exactly once
    assert tasks.crawl_batch.apply().result == 0  # idempotent: nothing pending -> a repeated/redelivered task does nothing


def test_concurrent_claims_never_overlap(env):
    seed_sites(env, per_host=40)
    cfg = autonomous.AutoConfig(per_batch=40, claim_batch=50, domain_page_budget=1000)
    got, errors = [], []

    def claim(i):
        try:
            got.append([u for u, _ in autonomous.claim_urls(f"w{i}", cfg)])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=claim, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join(60) for t in ts]
    flat = [u for g in got for u in g]
    assert not errors and flat and len(flat) == len(set(flat))  # racing workers get disjoint batches (a loser skips locked rows, never waits or duplicates)
    assert status_counts()["crawling"] == len(flat)
    while more := autonomous.claim_urls("drain", cfg):  # whatever the race skipped is still pending and gets claimed later
        flat += [u for u, _ in more]
    assert len(flat) == len(set(flat)) == 240  # every eligible URL claimed exactly once


def test_dead_worker_leases_recovered_but_live_ones_untouched(env):
    seed_sites(env, per_host=2)
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner='celery:dead', lease_expires=? WHERE id IN (SELECT id FROM discovered_urls ORDER BY id LIMIT 4)", (core.later(seconds=-5),))
        c.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner='celery:alive', lease_expires=? WHERE id IN (SELECT id FROM discovered_urls WHERE crawl_status='pending' ORDER BY id LIMIT 3)", (core.later(seconds=600),))
    assert tasks.recover.apply().result == 4
    assert status_counts() == {"pending": 9, "crawling": 3}


def test_failing_site_is_retried_then_failed_never_completed(env):
    SITES["bad.localhost"] = {"/": (503, "text/plain", "down")}
    with core.connect() as c:
        core.save_url(c, env("bad"), None, "test", 0, True)
        c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain='bad.localhost'")
    assert tasks.crawl_batch.apply().successful()
    r = rows("SELECT crawl_status, retry_count, crawl_error FROM discovered_urls")[0]
    assert r["crawl_status"] in ("retry_pending", "failed") and r["retry_count"] >= 1 and "503" in r["crawl_error"]
    assert rows("SELECT 1 FROM discovered_urls WHERE crawl_status='completed'") == []


def test_dispatch_is_bounded_and_respects_pause(env, monkeypatch):
    seed_sites(env, per_host=40)
    sent = []
    monkeypatch.setattr(tasks.crawl_batch, "delay", lambda: sent.append(1))
    monkeypatch.setattr(tasks, "queue_length", lambda: 1)
    assert tasks.dispatch() == tasks.MAX_QUEUED - 1 == len(sent)
    monkeypatch.setattr(tasks, "queue_length", lambda: tasks.MAX_QUEUED)
    assert tasks.dispatch() == 0  # queue full: no pile-up
    monkeypatch.setattr(tasks, "queue_length", lambda: 0)
    autonomous.set_paused(True)
    assert tasks.dispatch() == 0
    autonomous.set_paused(False)


def test_connection_cap_fails_loudly_and_releases(env, monkeypatch):
    monkeypatch.setattr(core, "_SLOTS", threading.BoundedSemaphore(2))
    monkeypatch.setattr(core, "DB_POOL_WAIT", 0.2)
    c1, c2 = core.connect(), core.connect()
    with pytest.raises(TimeoutError, match="connection limit"):
        core.connect()
    c1.close()
    c1.close()  # closing twice must not hand out a second slot
    c3 = core.connect()
    with pytest.raises(TimeoutError):
        core.connect()
    c2.close(); c3.close()
    with core.connect() as c:  # `with` releases it too
        c.execute("SELECT 1")
    assert core._SLOTS._value == 2

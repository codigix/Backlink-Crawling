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
    real = autonomous.AutoConfig
    monkeypatch.setattr(autonomous, "AutoConfig", lambda: real(claim_batch=20))  # the default batch (500) is larger than this fixture
    assert tasks.dispatch() == tasks.MAX_QUEUED - 1 == len(sent)
    monkeypatch.setattr(tasks, "queue_length", lambda: tasks.MAX_QUEUED)
    assert tasks.dispatch() == 0  # queue full: no pile-up
    monkeypatch.setattr(tasks, "queue_length", lambda: 0)
    autonomous.set_paused(True)
    assert tasks.dispatch() == 0
    autonomous.set_paused(False)


def test_connection_cap_fails_loudly_and_releases(env, monkeypatch):
    monkeypatch.setattr(core, "POOL_MAX", 2)  # a pool of two connections for this test (the pool is rebuilt when its settings change)
    monkeypatch.setattr(core, "POOL_MIN", 1)
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
    assert core.OPEN_CONNS == 0  # everything was returned ...
    d1, d2 = core.connect(), core.connect()  # ... so both connections are available again, and only two
    with pytest.raises(TimeoutError):
        core.connect()
    d1.close(); d2.close()
    assert core.OPEN_CONNS == 0


# ---- Phase 9.1 D1: business URLs waiting for a due retry must be dispatched (a job could stay 'crawling' forever) ----

def _business_url(env, job_id, host, n, status, next_at=None, domain_status="unreachable"):
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (id, target_domain, status, max_depth, max_pages) VALUES (?,?, 'crawling', 3, 10) ON CONFLICT DO NOTHING",
                  (job_id, f"biz{job_id}.localhost"))
        for i in range(n):
            core.save_url(c, env(host, f"/p{i}"), None, "ddg", 0, False)
        c.execute("UPDATE discovered_domains SET discovery_status=? WHERE domain=?", (domain_status, f"{host}.localhost"))
        c.execute("UPDATE discovered_urls SET crawl_status=?, next_crawl_at=? WHERE domain=?", (status, next_at, f"{host}.localhost"))
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls WHERE domain=?", (job_id, f"{host}.localhost"))


@pytest.fixture()
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(tasks, "queue_length", lambda: 0)
    monkeypatch.setattr(tasks.crawl_batch, "delay", lambda: out.append(1))
    return out


@pytest.mark.parametrize("status, when, expected", [("pending", None, 1), ("retry_pending", "past", 1), ("retry_pending", "future", 0)])
def test_dispatch_counts_business_pending_and_due_retry_but_not_future_retry(env, sent, status, when, expected):
    at = {"past": core.later(seconds=-60), "future": core.later(seconds=3600), None: None}[when]
    _business_url(env, 1, "bz", 2, status, at)  # the domain is NOT admitted: the business path must not need it
    assert tasks._dispatch() == expected
    assert len(sent) == expected


def test_dispatch_with_several_business_jobs_one_due_one_future(env, sent):
    _business_url(env, 1, "bza", 2, "retry_pending", core.later(seconds=3600))
    assert tasks._dispatch() == 0
    _business_url(env, 2, "bzb", 2, "retry_pending", core.later(seconds=-5))
    assert tasks._dispatch() == 1


def test_due_retry_is_claimed_once_and_the_job_waiting_predicate_agrees_with_the_dispatcher(env, sent):
    import business_jobs as bj
    _business_url(env, 1, "bz", 4, "retry_pending", core.later(seconds=-5))
    with core.connect() as c:
        assert bj.waiting(c) and tasks._dispatch() == 1  # the 9.0 defect: waiting() True, dispatch 0
    cfg = autonomous.AutoConfig()
    got, lock = [], threading.Lock()

    def worker(o):
        w = autonomous.claim_work(o, cfg)
        with lock:
            got.extend(w)
    ts = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(got) == 4 == len({u for u, _ in got})  # every due URL leased exactly once across 4 concurrent claimers


def test_concurrent_dispatchers_never_exceed_the_queue_bound(env, monkeypatch):
    _business_url(env, 1, "bz", 3, "retry_pending", core.later(seconds=-5))
    queued, lk = [0], threading.Lock()

    class FakeRedis:  # the real dispatcher serialises on a Redis lock; the same contract with a thread lock
        def lock(self, *a, **k):
            return self
        def __enter__(self):
            lk.acquire()
        def __exit__(self, *a):
            lk.release()
    monkeypatch.setattr(tasks, "r", lambda: FakeRedis())
    monkeypatch.setattr(tasks, "queue_length", lambda: queued[0])
    monkeypatch.setattr(tasks.crawl_batch, "delay", lambda: queued.__setitem__(0, queued[0] + 1))
    ts = [threading.Thread(target=tasks.dispatch) for _ in range(5)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert queued[0] <= tasks.MAX_QUEUED and queued[0] >= 1


# ---- Phase 9.1 D2: the task's loop must be able to spawn subprocesses (Playwright) even after flower set the selector policy ----

def test_crawl_task_loop_can_spawn_a_subprocess_under_the_flower_selector_policy():
    import asyncio
    import sys
    old = asyncio.get_event_loop_policy()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # what flower/app.py does at import
    try:
        async def spawn():
            p = await asyncio.create_subprocess_exec(sys.executable, "-c", "print(1)", stdout=asyncio.subprocess.PIPE)
            return (await p.communicate())[0].strip()
        with pytest.raises(NotImplementedError) if sys.platform == "win32" else __import__("contextlib").nullcontext():
            asyncio.run(spawn())  # the old call: proves the test reproduces the defect on Windows
        assert tasks.run_loop(spawn()) == b"1"
    finally:
        asyncio.set_event_loop_policy(old)

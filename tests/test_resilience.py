"""Regression tests for the long-running failures: recrawl starvation/state corruption, supervisor crashes, Common Crawl
job queue, rolling admission, shutdown. Network is mocked; the local test server comes from test_crawler."""

import asyncio
import httpx
import psycopg
import pytest

import autonomous
import crawler_core as core
import crawler_engine as engine
from test_autonomous import acfg
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures

PAST = "2000-01-01 00:00:00"


def admit(domain):
    with core.connect() as c:
        c.execute("INSERT OR IGNORE INTO discovered_domains (domain) VALUES (?)", (domain,))
        c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (domain,))


def add_urls(n, status="completed", domain="x.localhost", due=True, **kw):
    with core.connect() as c:
        for i in range(n):
            core.save_url(c, f"http://{domain}/{status}{i}", None, "t", 1, True, status)
        c.execute("UPDATE discovered_urls SET next_crawl_at=?, last_successful_crawl=? WHERE crawl_status=?",
                  (PAST if due else core.later(days=1), PAST, status))


# ---- recrawl: batch limit, atomic claims, state preservation ----
def test_recrawl_claims_only_a_batch_and_never_twice(env):
    add_urls(25)
    one, two = engine.claim_recrawl(10, owner="w1"), engine.claim_recrawl(10, owner="w2")
    assert len(one) == len(two) == 10 and not {u for u, *_ in one} & {u for u, *_ in two}
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='recrawling'")[0]["n"] == 20
    assert autonomous.release("w1") == 0  # recrawl leases return to 'completed', not 'pending'
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == 15


def test_failed_and_retry_pending_are_never_recrawled_or_completed(env):
    add_urls(3, "failed")
    add_urls(3, "retry_pending")
    assert engine.claim_recrawl(100) == [] and engine.due_urls() == []
    assert asyncio.run(engine.recrawl_due(engine.Config(domain_delay=0), limit=100)) == 0
    assert {r["crawl_status"] for r in rows("SELECT crawl_status FROM discovered_urls")} == {"failed", "retry_pending"}
    # retry_pending is retried by the forward crawler once its backoff has passed
    admit("x.localhost")
    got = autonomous.claim_urls("w", autonomous.AutoConfig())
    assert len(got) == 3 and all("retry_pending" in u for u, _ in got)


def test_unreached_recrawls_return_and_expired_lease_recovers(env, monkeypatch):
    add_urls(4)

    async def nothing(*a, **k):  # the round reaches nothing
        return None
    monkeypatch.setattr(engine, "crawl_round", nothing)
    assert asyncio.run(engine.recrawl_due(engine.Config(), limit=4)) == 4
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == 4
    engine.claim_recrawl(2, owner="dead")
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET lease_expires=? WHERE crawl_status='recrawling'", (PAST,))
    autonomous.release()
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='recrawling'")[0]["n"] == 0


def test_recrawl_success_and_failure_transitions(env):
    u = env
    SITES["a.localhost"] = {"/": page("ok")}
    asyncio.run(engine.run_crawl(engine.Config(domain_delay=0, timeout=10, max_retries=2), [u("a")]))
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET next_crawl_at=?", (PAST,))
    cfg = engine.Config(domain_delay=0, timeout=10, max_retries=2)
    assert asyncio.run(engine.recrawl_due(cfg, limit=5)) == 1
    r = rows("SELECT crawl_status, lease_owner FROM discovered_urls")[0]
    assert (r["crawl_status"], r["lease_owner"]) == ("completed", None)
    SITES["a.localhost"] = {"/": (500, "text/plain", "boom")}
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET next_crawl_at=?", (PAST,))
    asyncio.run(engine.recrawl_due(cfg, limit=5))
    assert rows("SELECT crawl_status, retry_count FROM discovered_urls")[0] == {"crawl_status": "retry_pending", "retry_count": 1}


# ---- forward crawl is not blocked by a recrawl round ----
def test_forward_crawl_progresses_during_recrawl(env, monkeypatch):
    add_urls(3)
    add_urls(2, "pending", domain="y.localhost")
    admit("y.localhost")
    release_recrawl, calls = asyncio.Event(), []

    async def fake_round(cfg, job_id, work, *a):
        calls.append(len(work))
        if any("completed" in u for u, _ in work):  # the recrawl round hangs until released
            await release_recrawl.wait()

    monkeypatch.setattr(engine, "crawl_round", fake_round)
    eng = autonomous.Engine(acfg())

    async def go():
        slow = asyncio.create_task(eng.verifier())
        await asyncio.sleep(0.1)
        assert eng.recrawl_lock.locked()
        assert await asyncio.wait_for(eng.crawler(), 5)  # not queued behind the recrawl
        release_recrawl.set()
        await slow
    asyncio.run(go())
    assert calls == [3, 2]


# ---- supervision ----
def test_heartbeat_db_error_does_not_kill_worker(env, monkeypatch):
    def locked(*a, **k):
        raise psycopg.errors.DeadlockDetected("deadlock")
    monkeypatch.setattr(autonomous, "_beat", locked)
    eng, runs = autonomous.Engine(acfg()), []

    async def fn():
        runs.append(1)
        if len(runs) == 3:
            eng.stop.set()
    asyncio.run(asyncio.wait_for(eng._supervise("t", fn, 0.01), 10))
    assert len(runs) == 3


def test_worker_crash_restarts_with_backoff_and_counts(env):
    eng, runs = autonomous.Engine(acfg()), []

    async def fn():
        runs.append(1)
        if len(runs) < 3:
            raise RuntimeError("boom")
        eng.stop.set()
    asyncio.run(asyncio.wait_for(eng._supervise("t", fn, 0.01), 10))
    w = rows("SELECT * FROM worker_status WHERE name='t'")[0]
    assert len(runs) == 3 and w["restart_count"] == 2 and "boom" in w["last_error"] and w["last_success_at"] and w["status"] == "stopped"


def test_cancellation_propagates(env):
    eng = autonomous.Engine(acfg())

    async def fn():
        await asyncio.sleep(60)

    async def go():
        t = asyncio.create_task(eng._supervise("t", fn, 1))
        await asyncio.sleep(0.1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
    asyncio.run(go())


def test_heartbeat_survives_failures_and_revives_dead_supervisor(env, monkeypatch):
    eng = autonomous.Engine(acfg(heartbeat_every=0.05))
    ran = []

    async def fn():
        ran.append(1)
        await asyncio.sleep(0.01)
    eng.specs = {"t": (fn, 0.01)}
    n = {"c": 0}
    real = autonomous._pulse

    def flaky(name):
        n["c"] += 1
        if n["c"] == 1:
            raise psycopg.errors.DeadlockDetected("deadlock")
        real(name)
    monkeypatch.setattr(autonomous, "_pulse", flaky)

    async def go():
        async def dead():
            raise RuntimeError("supervisor died")
        eng.tasks["t"] = asyncio.create_task(dead())
        hb = asyncio.create_task(eng._heartbeat("t"))
        await asyncio.sleep(0.5)
        eng.stop.set()
        await hb
        await asyncio.gather(*eng.tasks.values(), return_exceptions=True)
    asyncio.run(go())
    assert ran and rows("SELECT last_heartbeat_at FROM worker_status WHERE name='t'")[0]["last_heartbeat_at"]


def test_health_needs_fresh_heartbeat():
    ok = {"status": "idle", "last_heartbeat_at": core.now()}
    assert autonomous.worker_health(ok)
    assert not autonomous.worker_health({**ok, "last_heartbeat_at": PAST}) and not autonomous.worker_health({**ok, "last_heartbeat_at": None})
    assert not autonomous.worker_health({**ok, "status": "error"})


def test_shutdown_keeps_unfinished_work_recoverable(env, monkeypatch):
    add_urls(2, "pending", domain="y.localhost")
    add_urls(2, due=True)
    admit("y.localhost")
    admit("x.localhost")  # keep the engine from admitting (and seeding) it itself

    async def hang(*a, **k):
        await asyncio.sleep(60)
    monkeypatch.setattr(engine, "crawl_round", hang)
    eng = autonomous.Engine(acfg(heartbeat_every=0.05))

    async def go():
        t = asyncio.create_task(eng.run())
        await asyncio.sleep(1.5)
        assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('crawling','recrawling')")[0]["n"] == 4
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
    asyncio.run(go())
    st = {r["crawl_status"]: r["n"] for r in rows("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}
    assert st == {"pending": 2, "completed": 2}  # nothing lost, nothing falsely finished, no leases left


# ---- Common Crawl job queue ----
def cc_run(handler, cfg=None):
    cfg = cfg or autonomous.AutoConfig(sources=("commoncrawl",), cc_delay=0)

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as cl:
            return await autonomous.run_source("commoncrawl", cfg, cl, force=True)
    return asyncio.run(go())


def cc_handler(code, text="", headers=None):
    def h(req):
        if req.url.path.endswith("collinfo.json"):
            return httpx.Response(200, json=[{"cdx-api": "https://cc.test/idx"}])
        return httpx.Response(code, text=text, headers=headers or {})
    return h


def add_admitted(domain, id_):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (id, domain, discovery_status) VALUES (?,?,'admitted')", (id_, domain))


def job():
    return rows("SELECT * FROM discovery_jobs")[0]


@pytest.mark.parametrize("code", [500, 502, 503, 504])
def test_cc_server_errors_retry(env, code):
    add_admitted("site.org", 1)
    cc_run(cc_handler(code))
    j = job()
    assert (j["status"], j["attempts"]) == ("retry_pending", 1) and j["next_retry_at"] > core.now() and str(code) in j["last_error"]


def test_cc_zero_results_completes_and_404_is_not_retried(env):
    add_admitted("site.org", 1)
    assert cc_run(cc_handler(200, "")) == 0 and job()["status"] == "completed"
    with core.connect() as c:
        c.execute("DELETE FROM discovery_jobs")
    cc_run(cc_handler(404, "No Captures found"))
    assert job()["status"] == "completed"
    cc_run(cc_handler(200, ""))  # a completed job is never queried again
    assert job()["attempts"] == 1


def test_cc_429_honors_retry_after_and_400_fails_permanently(env):
    add_admitted("site.org", 1)
    add_admitted("two.org", 2)
    cc_run(cc_handler(429, "", {"Retry-After": "7200"}))
    first, second = rows("SELECT * FROM discovery_jobs ORDER BY id")
    assert first["status"] == "retry_pending" and first["next_retry_at"] > core.later(seconds=5000)
    assert (second["status"], second["attempts"]) == ("pending", 0)  # slowed down: the rest were handed back untouched
    with core.connect() as c:
        c.execute("DELETE FROM discovery_jobs")
    cc_run(cc_handler(400, "bad"))
    assert {r["status"] for r in rows("SELECT * FROM discovery_jobs")} == {"failed"}


def test_cc_retries_stop_after_the_last_delay(env):
    add_admitted("site.org", 1)
    with core.connect() as c:
        c.execute("INSERT INTO discovery_jobs (domain_id, source, attempts) VALUES (1,'commoncrawl',?)", (len(autonomous.RETRY_DELAYS),))
    cc_run(cc_handler(504))
    assert job()["status"] == "failed" and "504" in job()["last_error"]  # kept with its error, not silently dropped


def test_cc_lower_id_domain_admitted_later_still_gets_queried(env):
    add_admitted("late-high.org", 300)
    cc_run(cc_handler(200, '{"url": "https://late-high.org/a"}\n'))
    add_admitted("late-low.org", 50)  # an id far below everything processed so far (the old cursor skipped it forever)
    assert cc_run(cc_handler(200, '{"url": "https://late-low.org/a"}\n')) == 1
    assert {r["status"] for r in rows("SELECT * FROM discovery_jobs")} == {"completed"} and len(rows("SELECT * FROM discovery_jobs")) == 2


def test_cc_expired_processing_job_recovers_and_failure_does_not_stop_other_sources(env):
    add_admitted("site.org", 1)
    with core.connect() as c:
        c.execute("INSERT INTO discovery_jobs (domain_id, source, status, updated_at) VALUES (1,'commoncrawl','processing',?)", (PAST,))
    cc_run(cc_handler(200, ""))
    assert job()["status"] == "completed"

    async def feed(client, cfg, cursor):
        return [("https://blog.example.org/p", None, 0)], cursor
    boom = cc_handler(500)
    cfg = autonomous.AutoConfig(sources=("commoncrawl", "feeds"), cc_delay=0)
    add_admitted("two.org", 2)

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as cl:
            old = autonomous.SOURCES
            autonomous.SOURCES = {**old, "feeds": feed}
            try:
                return [await autonomous.run_source(n, cfg, cl, force=True) for n in ("commoncrawl", "feeds")]
            finally:
                autonomous.SOURCES = old
    assert asyncio.run(go()) == [0, 1]


# ---- rolling admission ----
def test_domain_slot_released_only_when_really_exhausted(env):
    cfg = autonomous.AutoConfig(domain_page_budget=3, crawl=engine.Config(max_depth=2))
    add_urls(2, "completed", "done.localhost")
    add_urls(1, "completed", "busy.localhost")
    add_urls(1, "retry_pending", "busy.localhost")  # waiting for retry: not exhausted
    add_urls(1, "crawling", "leased.localhost")  # active lease: not exhausted
    add_urls(3, "completed", "budget.localhost")
    add_urls(2, "pending", "budget.localhost")  # budget spent: released despite pending work
    for d in ("done", "busy", "leased", "budget"):
        admit(f"{d}.localhost")
    assert autonomous.retire_exhausted(cfg) == 2
    st = {r["domain"]: r["discovery_status"] for r in rows("SELECT * FROM discovered_domains")}
    assert st == {"done.localhost": "crawled", "busy.localhost": "admitted", "leased.localhost": "admitted", "budget.localhost": "crawled"}
    assert rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"] == 10  # nothing deleted


def test_freed_slot_admits_a_new_domain_and_exhausted_one_is_not_readmitted(env):
    u = env
    SITES["b.localhost"] = {"/": page("hi")}
    cfg = acfg(max_admitted=1)
    with core.connect() as c:
        core.save_url(c, u("a"), None, "t", 0, True, "completed")
        core.save_url(c, u("b"), None, "t", 0, True)
    admit("a.localhost")
    assert asyncio.run(autonomous.admit_batch(cfg)) == 0  # full
    assert autonomous.retire_exhausted(cfg) == 1
    assert asyncio.run(autonomous.admit_batch(cfg)) == 1
    st = {r["domain"]: r["discovery_status"] for r in rows("SELECT * FROM discovered_domains")}
    assert st == {"a.localhost": "crawled", "b.localhost": "admitted"}
    assert asyncio.run(autonomous.admit_batch(cfg)) == 0  # 'crawled' is not 'discovered': a finished domain is not admitted again


def test_retry_locked_retries_only_transient_errors(env):
    n = []

    def flaky():
        n.append(1)
        if len(n) < 3:
            raise psycopg.errors.DeadlockDetected("deadlock")
        return "ok"
    assert core.retry_locked(flaky) == "ok" and len(n) == 3
    with pytest.raises(psycopg.errors.UndefinedTable):
        core.retry_locked(lambda: (_ for _ in ()).throw(psycopg.errors.UndefinedTable("no such table: x")))


# robots.txt lookups must not queue behind each other: five 1-second sites are checked in ~1s, not ~5s
def test_robots_lookups_run_in_parallel(monkeypatch):
    import time

    async def slow_find(url, client):
        await asyncio.sleep(1)
        return "robots"
    monkeypatch.setattr(engine.RobotsTxtFile, "find", slow_find)

    class Fake:
        _http_client = None
    f = Fake()
    engine.concurrent_robots(f, timeout=5)

    async def go():
        t = time.monotonic()
        got = await asyncio.gather(*(f._get_robots_txt_file_for_url(f"https://s{i}.example/x") for i in range(5)),
                                   f._get_robots_txt_file_for_url("https://s0.example/y"))  # same site: one fetch, shared
        return time.monotonic() - t, got
    took, got = asyncio.run(go())
    assert took < 2.5 and got == ["robots"] * 6

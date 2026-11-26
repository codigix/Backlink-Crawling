"""Priority-based multi-business onboarding: jobs, fair/capacity-aware claiming, shared graph, depth/budget, failure isolation, fencing.
Real PostgreSQL (isolated schema per test), real local HTTP sites, real Crawlee rounds. Only DuckDuckGo is replaced."""

import asyncio
import threading

import pytest
from bs4 import BeautifulSoup

import autonomous
import business_jobs as bj
import crawler_api
import crawler_core as core
import crawler_engine as engine
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures


def cfg(**kw) -> autonomous.AutoConfig:
    return autonomous.AutoConfig(sources=("test",), **{"claim_batch": 3, "per_batch": 25, **kw},
                                 crawl=engine.Config(domain_delay=0, timeout=10, max_retries=1, max_depth=3, concurrency=3))


def search_with(monkeypatch, results: dict):
    def fake(domain, n, **_):
        r = results[domain]
        if isinstance(r, Exception):
            raise r
        return r
    monkeypatch.setattr(bj.ddg_lookup, "search_urls", fake)


def onboard(*domains, **kw) -> list[dict]:
    with core.connect() as c:
        return [bj.create_job(c, d, **kw)[0] for d in domains]


def job_status(domain) -> dict:
    return rows("SELECT * FROM business_jobs WHERE target_domain=? ORDER BY id DESC LIMIT 1", domain)[0]


def one_round(c: autonomous.AutoConfig, owner: str) -> list:
    work = autonomous.claim_work(owner, c)
    if work:
        asyncio.run(engine.crawl_round(c.crawl, None, work, set(), False, len(work)))
    autonomous.release(owner)
    return work


def drive(c, domains, rounds=60, claims=None) -> None:
    """Search, then crawl round after round (one worker at a time) until every job is closed."""
    for i in range(rounds):
        asyncio.run(bj.search_pending())  # the engine's lookups worker loops like this; DDG_CONCURRENCY bounds each pass
        bj.tick()
        if all(job_status(d)["status"] in ("done", "failed") for d in domains):
            return
        work = one_round(c, f"w{i % 3}")
        if claims is not None:
            claims.append(work)
    raise AssertionError("jobs did not finish: " + str([job_status(d)["status"] for d in domains]))


def add_job_urls(job_id: int, host: str, n: int, depth=0) -> list[str]:
    """n pending URLs on `host` associated with a crawling job (no network involved)."""
    urls = [f"http://{host}.localhost:1/p{i}" for i in range(n)]
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='crawling' WHERE id=?", (job_id,))
        for u in urls:
            core.save_url(c, u, None, "ddg", 0, False)
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, ? FROM discovered_urls WHERE domain=?", (job_id, depth, f"{host}.localhost"))
    return urls


# 1-3, 7-9, 11, 5, 6: three businesses at once, end to end on the shared graph
def test_three_businesses_share_one_graph_and_each_gets_its_own_backlinks(env, monkeypatch):
    u = env
    for t in ("target-a", "target-b", "target-c", "ext4"):
        SITES[f"{t}.localhost"] = {"/": page()}
    SITES["cand-a1.localhost"] = {"/": page(a(u("target-a"), "A"), a("/p2"), a(u("ext1"))), "/p2": page(a(u("target-a", "/deep"), "deep"), a("/p3")),
                                "/p3": page(a("/p4")), "/p4": page(a(u("target-a", "/x"), "x"), a("/p5")), "/p5": page()}  # /p5 would be depth 4
    SITES["ext1.localhost"] = {"/": page(a(u("target-a", "/y"), "y"))}  # linked from a candidate, but external: stored, never crawled
    SITES["shared.localhost"] = {"/": page(a(u("target-a"), "A"), a(u("target-b"), "B"))}
    SITES["cand-b1.localhost"] = {"/": page(a(u("target-b"), "B"))}
    SITES["cand-c1.localhost"] = {"/": page(a(u("target-c"), "C"))}
    search_with(monkeypatch, {"target-a.localhost": [u("cand-a1"), u("shared")], "target-b.localhost": [u("cand-b1"), u("shared")],
                              "target-c.localhost": [u("cand-c1")]})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    with core.connect() as c:  # autonomous work that must wait its turn
        c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES ('auto.localhost', 'admitted', 5)")
        core.save_url(c, u("auto"), None, "t", 0, True)
    SITES["auto.localhost"] = {"/": page()}
    domains = ["target-a.localhost", "target-b.localhost", "target-c.localhost"]

    # concurrent onboarding incl. duplicates -> exactly three independent jobs
    out: list = []
    ts = [threading.Thread(target=lambda d=d: out.append(onboard(d)[0]["id"])) for d in domains + domains[:2]]
    [t.start() for t in ts]
    [t.join() for t in ts]
    jobs = rows("SELECT * FROM business_jobs ORDER BY id")
    assert len(jobs) == 3 and {j["target_domain"] for j in jobs} == set(domains) and len(set(out)) == 3
    assert {j["priority"] for j in jobs} == {bj.CrawlPriority.BUSINESS_ONBOARDING}

    asyncio.run(bj.search_pending())  # DDG results stay with their own business
    by = lambda d: {r["url"] for r in rows("SELECT u.url FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id JOIN business_jobs j ON j.id=ju.job_id"
                                          " WHERE j.target_domain=?", d)}  # noqa: E731
    assert by("target-a.localhost") == {u("cand-a1"), u("shared")} and by("target-b.localhost") == {u("cand-b1"), u("shared")}
    assert by("target-c.localhost") == {u("cand-c1")}
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE url=?", u("shared"))[0]["n"] == 1  # candidate deduplicated globally

    claims: list = []
    drive(cfg(), domains, claims=claims)

    first = {url for url, _ in claims[0]}  # fair: the very first claim serves every business, and no autonomous URL beat them
    assert all(by(d) & first for d in domains) and u("auto") not in first
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", u("auto"))[0]["crawl_status"] == "pending"
    assert [j["status"] for j in rows("SELECT status FROM business_jobs")] == ["done"] * 3

    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", u("shared"))[0]["n"] == 1  # fetched once for two businesses
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE source_url=? AND link_type='html'", u("shared"))[0]["n"] == 2  # links stored once
    got = {d: crawler_api.backlinks(d, include_removed=False, limit=100, offset=0) for d in domains}
    assert [got[d]["total_backlinks"] for d in domains] == [4, 2, 1] and all(g["status"] == "ok" for g in got.values())
    assert {b["source_domain"] for b in got["target-b.localhost"]["backlinks"]} == {"shared.localhost", "cand-b1.localhost"}
    assert {b["source_url"] for b in got["target-c.localhost"]["backlinks"]} == {u("cand-c1")}
    assert [job_status(d)["backlinks_found"] for d in domains] == [4, 2, 1]

    # 11: depth is per job and never beyond 3: /p4 (depth 3) was crawled for A, /p5 (depth 4) never entered the job
    assert rows("SELECT MAX(ju.depth) m FROM job_urls ju")[0]["m"] == 3
    assert not any("/p5" in x for x in by("target-a.localhost"))
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url LIKE '%/p5%'")[0]["n"] == 0
    # external destinations are stored as links but not followed by default
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url LIKE '%ext1%'")[0]["n"] == 0
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE source_url=? AND destination_domain='ext1.localhost'", u("cand-a1"))[0]["n"] == 1

    # 6: all business jobs finished -> autonomous crawling takes the capacity
    assert [x for x, _ in autonomous.claim_work("auto-w", cfg())] == [u("auto")]


# 4, 5, 13: fairness between businesses, business before autonomous, reserved capacity, borrowing, per-business and global limits
def test_capacity_priority_and_fairness(env):
    jobs = onboard("biz-a.localhost", "biz-b.localhost", "biz-c.localhost")
    for j, h in zip(jobs, ("a", "b", "c")):
        add_job_urls(j["id"], f"biz-{h}", 4)
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES ('auto.localhost', 'admitted', 5)")
        for i in range(30):
            core.save_url(c, f"http://auto.localhost:1/p{i}", None, "t", 0, True)
    job_of = lambda url: rows("SELECT j.target_domain d FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id JOIN business_jobs j ON j.id=ju.job_id WHERE u.url=?", url)[0]["d"]  # noqa: E731

    c3 = cfg(claim_batch=3, max_crawl_workers=5, business_reserved_workers=3, max_jobs_per_business=2)
    w1 = autonomous.claim_work("w1", c3)
    assert sorted(job_of(x) for x, _ in w1) == ["biz-a.localhost", "biz-b.localhost", "biz-c.localhost"]  # one each: nobody monopolizes
    w2 = autonomous.claim_work("w2", c3)
    assert sorted(job_of(x) for x, _ in w2) == ["biz-a.localhost", "biz-b.localhost", "biz-c.localhost"]
    # every business now has 2 workers (its limit) although URLs are still pending: w3 gets no business URL, only the unreserved autonomous share
    w3 = autonomous.claim_work("w3", c3)
    assert w3 and all("auto.localhost" in x for x, _ in w3)
    assert autonomous.claim_work("w4", c3) and autonomous.claim_work("w5", c3) == []  # auto may hold 5-3=2 workers while business is waiting
    for o in ("w1", "w2", "w3", "w4"):
        autonomous.release(o)

    # global worker cap
    c1 = cfg(claim_batch=2, max_crawl_workers=2, business_reserved_workers=1, max_jobs_per_business=9)
    assert autonomous.claim_work("x1", c1) and autonomous.claim_work("x2", c1)
    assert autonomous.claim_work("x3", c1) == []  # two workers busy = MAX_CRAWL_WORKERS
    for o in ("x1", "x2"):
        autonomous.release(o)


def test_reserved_capacity_is_kept_for_business_and_borrowed_when_idle(env):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES ('auto.localhost', 'admitted', 5)")
        for i in range(30):
            core.save_url(c, f"http://auto.localhost:1/p{i}", None, "t", 0, True)
    c = cfg(claim_batch=2, per_batch=2, max_crawl_workers=3, business_reserved_workers=2, domain_page_budget=100)

    # no business work: autonomous may borrow every worker
    assert len(autonomous.claim_work("a1", c)) == 2 and len(autonomous.claim_work("a2", c)) == 2 and len(autonomous.claim_work("a3", c)) == 2
    for o in ("a1", "a2", "a3"):
        autonomous.release(o)

    # a business arrives (still searching): 2 of 3 workers are reserved at once
    onboard("late.localhost")
    assert len(autonomous.claim_work("a1", c)) == 2  # the one unreserved worker
    assert autonomous.claim_work("a2", c) == [] and autonomous.claim_work("a3", c) == []
    # its pages are claimed ahead of the autonomous backlog as soon as they exist
    add_job_urls(rows("SELECT id FROM business_jobs")[0]["id"], "late", 4)
    got = autonomous.claim_work("b1", c)
    assert len(got) == 2 and all("late.localhost" in x for x, _ in got)
    autonomous.release("a1")

    # misconfiguration is rejected
    with pytest.raises(ValueError):
        autonomous.AutoConfig(max_crawl_workers=2, business_reserved_workers=3)


def test_one_business_may_use_spare_capacity_up_to_its_own_limit(env):
    (job,) = onboard("solo.localhost")
    add_job_urls(job["id"], "solo", 10)
    c = cfg(claim_batch=2, max_crawl_workers=5, business_reserved_workers=3, max_jobs_per_business=2)
    assert len(autonomous.claim_work("s1", c)) == 2 and len(autonomous.claim_work("s2", c)) == 2  # beyond its 'one reserved share'
    assert autonomous.claim_work("s3", c) == []  # per-business concurrency limit (2 workers)


# 10, 12: one failing job does not stop others; DDGS cooldown and retries; page budget
def test_failure_isolation_cooldown_and_retries(env, monkeypatch):
    u = env
    SITES["ok-src.localhost"] = {"/": page(a("https://x.org/"))}
    search_with(monkeypatch, {"good.localhost": [u("ok-src")], "bad.localhost": RuntimeError("ratelimit"), "good2.localhost": [u("ok-src")]})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    monkeypatch.setattr(bj, "SEARCH_RETRIES", 2)
    onboard("good.localhost", "bad.localhost", "good2.localhost")
    asyncio.run(bj.search_pending())
    st = {r["target_domain"]: r for r in rows("SELECT * FROM business_jobs")}
    assert st["good.localhost"]["status"] == "crawling" and st["good2.localhost"]["status"] == "crawling"  # the failure touched nobody else
    assert st["bad.localhost"]["status"] == "pending" and st["bad.localhost"]["retry_count"] == 1 and "ratelimit" in st["bad.localhost"]["last_error"]
    assert rows("SELECT value FROM engine_state WHERE key='ddg_cooldown_until'")  # provider-level cooldown recorded
    assert asyncio.run(bj.search_pending()) is False  # ...and honoured
    with core.connect() as c:
        c.execute("DELETE FROM engine_state WHERE key='ddg_cooldown_until'")
        c.execute("UPDATE business_jobs SET next_retry_at=NULL")
    asyncio.run(bj.search_pending())  # second failure reaches SEARCH_RETRIES
    assert job_status("bad.localhost")["status"] == "failed" and job_status("good.localhost")["status"] == "crawling"

    # an exception while advancing one job is recorded on it; the other job still progresses
    real = bj.expand
    monkeypatch.setattr(bj, "expand", lambda conn, job: (_ for _ in ()).throw(RuntimeError("boom")) if job["target_domain"] == "good.localhost" else real(conn, job))
    for i in range(6):  # precondition, not the behaviour under test: the shared page is fetched for real, and a fetch may need a retry round
        one_round(cfg(claim_batch=5), f"w{i}")
        bj.tick()  # good's expansion raises EVERY time; good2 must advance regardless
        if job_status("good2.localhost")["pages_crawled"] == 1:
            break
    assert "boom" in job_status("good.localhost")["last_error"] and job_status("good2.localhost")["last_error"] is None
    why = rows("SELECT u.url, u.crawl_status, u.crawl_error, u.retry_count, u.lease_owner FROM discovered_urls u JOIN job_urls ju ON ju.url_id=u.id JOIN business_jobs j ON j.id=ju.job_id"
               " WHERE j.target_domain='good2.localhost'")
    assert job_status("good2.localhost")["pages_crawled"] == 1, f"the shared candidate was not completed by the one round: {why}"


def test_page_budget_and_empty_search(env, monkeypatch):
    u = env
    SITES["many.localhost"] = {"/": page(*[a(f"/p{i}") for i in range(8)]), **{f"/p{i}": page() for i in range(8)}}
    search_with(monkeypatch, {"cap.localhost": [u("many")], "none.localhost": []})
    onboard("cap.localhost", max_pages=3)
    onboard("none.localhost")
    drive(cfg(claim_batch=2), ["cap.localhost", "none.localhost"])
    j = job_status("cap.localhost")
    assert rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", j["id"])[0]["n"] <= 3 and j["pages_crawled"] <= 3 and j["pages_discovered"] <= 3
    assert job_status("none.localhost")["status"] == "done" and job_status("none.localhost")["pages_discovered"] == 0
    assert crawler_api.backlinks("none.localhost", include_removed=False, limit=10, offset=0)["status"] == "completed_no_results"


# 14: leases expire and are recovered; a stale worker cannot overwrite the new owner's result (fencing); engine restart
def test_lease_recovery_and_fencing(env):
    (job,) = onboard("fence.localhost")
    (url,) = add_job_urls(job["id"], "fence", 1)
    norm = core.normalize_url(url)
    c = cfg(claim_batch=5, lease_seconds=1)
    assert [x for x, _ in autonomous.claim_work("old", c)] == [url]
    old_fence = dict(engine.FENCE)
    with core.connect() as conn:
        conn.execute("UPDATE discovered_urls SET lease_expires=? WHERE url=?", (core.later(seconds=-5), url))  # the worker died / stalled
    assert autonomous.release() == 1  # recovery returns it to pending
    assert [x for x, _ in autonomous.claim_work("new", c)] == [url]
    new_fence = dict(engine.FENCE)
    assert old_fence[norm] != new_fence[norm]

    soup = BeautifulSoup("<html><body><a href='https://t.org/'>t</a></body></html>", "html.parser")
    def write(fence, html_status=200):
        engine.FENCE.clear(); engine.FENCE.update(fence)
        with core.connect() as conn:
            return engine.process_page(conn, url, url, 0, soup, html_status, "text/html", c.crawl, set(), False, core.now())
    write(old_fence)  # stale worker finishes late
    assert rows("SELECT crawl_status, lease_owner FROM discovered_urls WHERE url=?", url)[0] == {"crawl_status": "crawling", "lease_owner": "new"}
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", url)[0]["n"] == 0
    with core.connect() as conn:  # a stale failure is ignored too
        engine.FENCE.clear(); engine.FENCE.update(old_fence)
        engine.mark_failed(conn, url, "late error", None, c.crawl, core.now())
    assert rows("SELECT crawl_status, retry_count FROM discovered_urls WHERE url=?", url)[0] == {"crawl_status": "crawling", "retry_count": 0}
    write(new_fence)  # the current owner's result is stored
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", url)[0]["crawl_status"] == "completed"

    # engine restart: a search in flight is re-run, a crawling job keeps its pages and its leases come back
    with core.connect() as conn:
        conn.execute("INSERT INTO business_jobs (target_domain, status, max_depth, max_pages) VALUES ('interrupted.localhost','searching',3,10)")
        conn.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner='dead' WHERE url=?", (url,))
        core.migrate(conn)
    bj.reset_interrupted()
    assert job_status("interrupted.localhost")["status"] == "pending" and job_status("fence.localhost")["status"] == "crawling"
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", url)[0]["crawl_status"] == "pending"
    assert rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", job["id"])[0]["n"] == 1


def test_onboarding_is_idempotent_and_api_creates_jobs(env):
    with core.connect() as c:
        j1, made1 = bj.create_job(c, "https://www.Idem.localhost/x")
        j2, made2 = bj.create_job(c, "idem.localhost")
    assert made1 and not made2 and j1["id"] == j2["id"]
    r = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="idem.localhost"))
    assert r["created"] is False and r["job_id"] == j1["id"]
    assert crawler_api.backlinks("idem.localhost", include_removed=False, limit=10, offset=0)["status"] == "discovering"  # GET only reports, creates nothing
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 1
    assert crawler_api.business_status("idem.localhost")["status"] == "pending"


def test_recurring_schedule_is_independent_staggered_and_recoverable(env, monkeypatch):
    monkeypatch.setattr(bj, "SEARCH_INTERVAL", 3600)
    domains = [f"rec{i}.localhost" for i in range(3)]
    for d in domains:
        SITES[d] = {"/": page()}
    search_with(monkeypatch, {"rec0.localhost": [], "rec1.localhost": RuntimeError("429"), "rec2.localhost": []})
    monkeypatch.setattr(bj, "SEARCH_RETRIES", 1)
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    onboard(*domains)
    assert bj.schedule_due() == 0  # active jobs: no duplicate scheduled job
    drive(cfg(), domains)  # rec1's search fails for good; the others are unaffected
    assert [job_status(d)["status"] for d in domains] == ["done", "failed", "done"]
    nxt = {r["domain"]: r["next_run_at"] for r in rows("SELECT * FROM businesses")}
    assert all(v > core.later(seconds=3500) for v in nxt.values())  # every run (even the failed one) pushed the next one an interval away
    assert bj.schedule_due() == 0 and rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 3
    with core.connect() as c:  # engine was down past two due dates: each fires once, as P2, limited by search concurrency
        c.execute("UPDATE businesses SET next_run_at=? WHERE domain<>'rec2.localhost'", (core.later(seconds=-5),))
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    assert bj.schedule_due() == 1
    assert bj.schedule_due() == 0  # that business now has an active job; the second one is released by the next tick after it
    new = rows("SELECT * FROM business_jobs ORDER BY id DESC LIMIT 1")[0]
    assert new["priority"] == bj.CrawlPriority.RECURRING and new["status"] == "pending"
    assert bj.api_state(new) == "discovering" and bj.api_state(None) == "not_started"


def test_onboard_endpoint_and_get_has_no_side_effect(env):
    assert crawler_api.backlinks("nobody.localhost", include_removed=False, limit=10, offset=0)["status"] == "not_started"
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 0
    r = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="https://www.Shop.localhost/x"))
    assert r["domain"] == "shop.localhost" and r["status"] == "queued" and r["priority"] == "P0"
    assert crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="shop.localhost"))["job_id"] == r["job_id"]
    s = crawler_api.discovery_business_status("shop.localhost")
    assert s["status"] == "discovering" and s["schedule"]["domain"] == "shop.localhost"
    assert len(crawler_api.discovery_business_runs("shop.localhost", 10)["runs"]) == 1


def test_search_has_a_wall_clock_budget_and_never_turns_a_timeout_into_no_backlinks(monkeypatch):
    import ddg_lookup, time as _t
    real_sleep, calls = _t.sleep, []  # ddg_lookup.time IS the time module: keep the real sleep for the fakes

    def slow(q, page, attempts, end=float("inf")):
        calls.append(q)
        real_sleep(0.3)
        return [{"href": f"https://site{len(calls)}.example.net/p"}] if len(calls) == 1 else []

    monkeypatch.setattr(ddg_lookup, "_page", slow)
    monkeypatch.setattr(ddg_lookup.time, "sleep", lambda s: None)
    got = ddg_lookup.search_urls("biz.example.org", 50, name="Biz", budget=0.2)  # one slow page eats the budget: partial result kept
    assert got == ["https://site1.example.net/p"] and len(calls) == 1
    calls.clear()
    monkeypatch.setattr(ddg_lookup, "_page", lambda *a, **k: (real_sleep(0.3), [])[1])
    assert ddg_lookup.search_urls("biz.example.org", 50, budget=10) == []  # empty answer, no error: genuinely no results
    monkeypatch.setattr(ddg_lookup, "_page", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("429")))
    with pytest.raises(RuntimeError):  # failure with nothing found is raised, never []
        ddg_lookup.search_urls("biz.example.org", 50, budget=10)


def test_poll_state_explains_why_a_pending_job_waits(env, monkeypatch):
    onboard("poll-a.localhost", "poll-b.localhost")
    st = bj.poll_state()
    assert (st["pending"], st["eligible"], st["searching"], st["free"], st["blocked_by"]) == (2, 2, 0, 1, None)
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='searching' WHERE id=(SELECT MIN(id) FROM business_jobs)")
    st = bj.poll_state()
    assert st["pending"] == 1 and st["free"] == 0 and st["blocked_by"] == "search_slots_full(1/1)"  # the case seen live: one slow search holds the slot
    monkeypatch.setattr(bj, "DDGS_ENABLED", False)
    assert bj.poll_state()["blocked_by"] == "ddgs_disabled"


def test_slow_indexer_does_not_freeze_the_event_loop(env, monkeypatch):
    """Regression: the indexer ran one DB query per link ON the loop; a big target froze the business/lookups workers for minutes."""
    import time as _t
    monkeypatch.setattr(autonomous.Engine, "_index", staticmethod(lambda: _t.sleep(1.0)))

    async def go():
        ticks, eng = [], autonomous.Engine(cfg())

        async def ticker():
            while True:
                ticks.append(1)
                await asyncio.sleep(0.05)
        t = asyncio.create_task(ticker())
        await eng.indexer()
        t.cancel()
        return len(ticks)
    assert asyncio.run(go()) >= 10  # ~20 ticks while the 1s index ran; blocked on the loop it would be 1


def test_onboard_body_takes_only_a_domain_or_url(env):
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(crawler_api.router)
    c = TestClient(app)
    ok = c.post("/discovery/onboard", json={"domain": "https://www.Body.localhost/path"})
    assert ok.status_code == 202 and ok.json()["domain"] == "body.localhost" and ok.json()["priority"] == "P0"
    assert c.post("/discovery/onboard", json={"domain": "body.localhost"}).json()["job_id"] == ok.json()["job_id"]
    assert c.post("/discovery/onboard", json={"domain": "not a domain"}).status_code == 422
    assert c.post("/discovery/onboard", json={"domain": "x.localhost", "business_name": "X"}).status_code == 422  # only the domain
    assert c.post("/discovery/onboard", json={}).status_code == 422
    props = c.get("/openapi.json").json()["components"]["schemas"]["OnboardRequest"]["properties"]
    assert list(props) == ["domain"]  # Swagger shows one field, not additionalProp1


def test_superseded_search_result_is_discarded_and_candidates_respect_the_page_budget(env, monkeypatch):
    """Found by the staging run: an engine restart re-queued a search another engine was still running; both results landed on one job
    (63 rows against a budget of 60, search_results disagreeing with the stored candidates)."""
    onboard("fence.localhost", max_pages=5)
    (old,) = bj._claim_search(1)  # engine A starts the search (token 1)
    bj.reset_interrupted()  # engine B restarts: the search is re-queued...
    (new,) = bj._claim_search(1)  # ...and claimed again (token 2)
    assert (old["id"], new["id"]) == (old["id"], old["id"]) and old["search_gen"] != new["search_gen"]
    urls = [f"https://cand{i}.example.net/p" for i in range(9)]
    bj._search_done(old, urls[:3])  # engine A's late result: discarded
    assert rows("SELECT COUNT(*) n FROM job_urls")[0]["n"] == 0 and job_status("fence.localhost")["status"] == "searching"
    bj._search_failed(old, RuntimeError("late failure"))  # a late failure is ignored too (no retry counted, no cooldown)
    assert job_status("fence.localhost")["retry_count"] == 0 and not rows("SELECT * FROM engine_state WHERE key='ddg_cooldown_until'")
    bj._search_done(new, urls)  # the owner's result: stored, capped by max_pages=5
    j = job_status("fence.localhost")
    assert j["status"] == "crawling" and rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", j["id"])[0]["n"] == 5
    assert j["search_results"] == 5  # the counter is what is stored, not what the last run returned
    bj._search_done(new, urls)  # a duplicate delivery of the same result changes nothing (the job is no longer 'searching')
    assert rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", j["id"])[0]["n"] == 5


def test_a_search_stuck_in_searching_is_returned_to_pending_without_a_restart(env, monkeypatch):
    search_with(monkeypatch, {"stuck.localhost": [], "live.localhost": []})
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (target_domain, status, max_depth, max_pages, updated_at) VALUES ('stuck.localhost','searching',3,10,?)",
                  (core.later(seconds=-bj.STALE_SEARCH - 5),))  # its worker died long ago (DB error after the search, crashed task)
        c.execute("INSERT INTO business_jobs (target_domain, status, max_depth, max_pages, updated_at) VALUES ('live.localhost','searching',3,10,?)",
                  (core.now(),))  # a search running right now is left alone
    assert bj.reset_interrupted(bj.STALE_SEARCH) == 1
    assert job_status("stuck.localhost")["status"] == "pending" and job_status("live.localhost")["status"] == "searching"


def _finish(domain):
    """The engine closing a job: done, and the business rescheduled one interval away (what _progress does)."""
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE target_domain=? AND status IN ('pending','searching','crawling')",
                  (core.now(), domain))
        bj._reschedule(c, domain, True)


def _sched(domain):
    return rows("SELECT next_run_at, last_run_at, search_interval_seconds FROM businesses WHERE domain=?", domain)[0]


def test_repeated_onboarding_and_dashboard_polling_never_start_another_scan(env):
    """A client that POSTs /discovery/onboard on every page view (seen live: ~30 jobs/hour) gets one scan; the scheduler owns the rest."""
    first = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="dash.localhost"))
    assert first["created"] and first["priority"] == "P0"  # a brand-new business: its first scan, immediately
    again = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="https://www.dash.localhost/"))
    assert not again["created"] and again["job_id"] == first["job_id"]  # running: the same active job
    _finish("dash.localhost")
    before = _sched("dash.localhost")
    assert before["search_interval_seconds"] == bj.SEARCH_INTERVAL and before["next_run_at"] > core.later(seconds=bj.SEARCH_INTERVAL - 60)
    for _ in range(5):  # dashboard opened / refreshed: onboard + status, over and over, inside the 6h interval
        r = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="dash.localhost"))
        assert not r["created"] and r["job_id"] == first["job_id"] and r["status"] == "completed_no_results"
        assert r["next_run_at"] == before["next_run_at"]
        crawler_api.discovery_business_status("dash.localhost")
        crawler_api.business_status("dash.localhost")
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 1
    assert _sched("dash.localhost") == before  # next_run_at / last_run_at untouched by any of it
    with core.connect() as c:  # overdue: onboarding still does not run it; the scheduler does, once, as a recurring (P2) job
        c.execute("UPDATE businesses SET next_run_at=? WHERE domain='dash.localhost'", (core.later(seconds=-60),))
    assert not crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="dash.localhost"))["created"]
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 1
    assert bj.schedule_due() == 1 and bj.schedule_due() == 0
    assert rows("SELECT priority FROM business_jobs ORDER BY id DESC LIMIT 1")[0]["priority"] == bj.CrawlPriority.RECURRING


def test_explicit_rescan_is_the_only_way_to_scan_again_now(env):
    onboard("again.localhost")
    r = crawler_api.discovery_rescan("again.localhost")
    assert not r["created"]  # a scan is already active: rescanning cannot stack a second one
    _finish("again.localhost")
    r = crawler_api.discovery_rescan("https://again.localhost/x")
    assert r["created"] and r["priority"] == "P0" and r["status"] == "queued"
    assert crawler_api.discovery_rescan("again.localhost")["job_id"] == r["job_id"]  # double click: the same job
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 2
    with pytest.raises(crawler_api.HTTPException) as e:
        crawler_api.discovery_rescan("never.localhost")
    assert e.value.status_code == 404 and rows("SELECT COUNT(*) n FROM businesses WHERE domain='never.localhost'")[0]["n"] == 0


def test_concurrent_onboarding_requests_create_exactly_one_job(env):
    barrier, out = threading.Barrier(8), []

    def hit():
        with core.connect() as c:
            barrier.wait()
            out.append(bj.create_job(c, "race.localhost"))
    for wave in (1, 2):  # wave 1: a new business, 8 simultaneous requests; wave 2: the same after its scan finished
        out.clear()
        ts = [threading.Thread(target=hit) for _ in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert sum(made for _, made in out) == (1 if wave == 1 else 0) and len({j["id"] for j, _ in out}) == 1
        assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 1
        _finish("race.localhost")


def test_competitor_turned_customer_gets_its_first_customer_scan_and_interval(env):
    with core.connect() as c:
        bj.create_job(c, "was-rival.localhost", target_type="competitor")
    _finish("was-rival.localhost")
    assert _sched("was-rival.localhost")["search_interval_seconds"] == bj.COMPETITOR_INTERVAL
    r = crawler_api.discovery_onboard(crawler_api.OnboardRequest(domain="was-rival.localhost"))
    assert r["created"] and r["priority"] == "P0"
    assert _sched("was-rival.localhost")["search_interval_seconds"] == bj.SEARCH_INTERVAL

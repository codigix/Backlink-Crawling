"""Competitor targets on the shared graph: registration rules, scheduling/priorities/caps, one crawled page serving several targets,
backlink gap. Real PostgreSQL (isolated schema), real local HTTP fixtures, real Crawlee rounds; only DuckDuckGo is replaced."""

import asyncio
import copy
import threading
import time

import pytest
from fastapi import HTTPException

import business_jobs as bj
import competitors
import crawler_api
import crawler_core as core
from test_business_jobs import cfg, drive, job_status, onboard, search_with
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures


def db(fn, *a):
    with core.connect() as c:
        return fn(c, *a)


def reg_comp(owner, *items):
    """Per-business registration (library level; the API registers competitors globally)."""
    out = []
    with core.connect() as c:
        if not competitors.own_business(c, owner):
            bj.create_job(c, owner)
        for d, n in items:
            try:
                out.append(competitors.register(c, owner, d, n))
            except ValueError as e:
                out.append({"domain": d, "status": "rejected", "error": str(e)})
    return out


def link(src: str, dst: str, anchor="x", rel=None, nofollow=0):
    """A stored link as the crawler would leave it (hosts are registrable *.localhost names in these tests)."""
    with core.connect() as c:
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, source_domain, anchor_text, rel_attributes, is_nofollow)"
                  " VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                  (src, dst, core.registrable_domain(core.hostname(dst)), core.registrable_domain(core.hostname(src)), anchor, rel, nofollow))


def targets():
    return {r["domain"]: r for r in rows("SELECT * FROM businesses")}


# ---- registration ----
def test_registration_rules_ownership_and_removal_keeps_data(env):
    onboard("own.localhost", "other.localhost")
    with pytest.raises(LookupError):
        with core.connect() as c:
            competitors.register(c, "nobody.localhost", "a.localhost")  # the module requires an onboarded owner...
    r = reg_comp("fresh.localhost", ("fa.localhost", None))[0]  # ...the API onboards it on the fly, so no separate step is needed
    assert r["status"] == "registered" and targets()["fresh.localhost"]["target_type"] == "own_business"
    assert rows("SELECT priority FROM business_jobs WHERE target_domain='fresh.localhost'")[0]["priority"] == bj.CrawlPriority.BUSINESS_ONBOARDING
    with pytest.raises(ValueError):
        reg_comp("omxtechnologies", ("fa.localhost", None))  # no TLD: not a domain
    res = reg_comp("own.localhost", ("https://www.Comp-A.localhost/x", "Comp A"), ("comp-b.localhost", None), ("own.localhost", None),
                   ("not a domain", None), ("comp-a.localhost", None))
    assert [r["status"] for r in res] == ["registered", "registered", "rejected", "rejected", "duplicate"]
    assert "own competitor" in res[2]["error"] and res[0]["domain"] == "comp-a.localhost" and res[0]["priority"] == "P3"
    t = targets()
    assert t["comp-a.localhost"]["target_type"] == "competitor" and t["own.localhost"]["target_type"] == "own_business"  # classified, never confused
    assert t["comp-a.localhost"]["search_interval_seconds"] == bj.COMPETITOR_INTERVAL != t["own.localhost"]["search_interval_seconds"]
    assert len(rows("SELECT * FROM business_competitors WHERE business_domain='own.localhost'")) == 2 and len(rows("SELECT * FROM business_jobs WHERE target_domain='comp-a.localhost'")) == 1

    # a competitor that is already a customer business keeps its type, its schedule and gets no extra job
    r = reg_comp("own.localhost", ("other.localhost", None))[0]
    assert r["status"] == "registered" and r["job_id"] is None and r["target_type"] == "own_business"

    # two owners, one shared competitor target
    onboard("own2.localhost")
    reg_comp("own2.localhost", ("comp-a.localhost", None))
    assert len(rows("SELECT * FROM business_jobs WHERE target_domain='comp-a.localhost'")) == 1  # not searched twice
    assert [c["competitor_domain"] for c in db(competitors.listing, "own2.localhost")] == ["comp-a.localhost"]  # no leak between owners

    # disable / enable
    link("http://x.localhost/p", "http://comp-a.localhost/", "A")
    db(competitors.set_active, "own.localhost", "comp-a.localhost", False)
    assert targets()["comp-a.localhost"]["active"] == 1  # own2 still monitors it
    db(competitors.set_active, "own2.localhost", "comp-a.localhost", False)
    assert targets()["comp-a.localhost"]["active"] == 0 and not db(competitors.detail, "own.localhost", "comp-a.localhost")["active"]
    db(competitors.set_active, "own.localhost", "comp-a.localhost", True)
    assert targets()["comp-a.localhost"]["active"] == 1

    # removal drops the relation only; the graph, jobs and history stay
    assert db(competitors.remove, "own.localhost", "comp-a.localhost")
    assert db(competitors.detail, "own.localhost", "comp-a.localhost") is None
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE destination_domain='comp-a.localhost'")[0]["n"] == 1
    assert len(rows("SELECT * FROM business_jobs WHERE target_domain='comp-a.localhost'")) == 1


# ---- existing graph is used at once ----
def test_existing_graph_links_show_up_before_any_crawl(env):
    link("http://old1.localhost/p", "http://newcomp.localhost/", "N")
    link("http://old2.localhost/p", "http://newcomp.localhost/pricing", "N2", rel="nofollow", nofollow=1)
    link("http://newcomp.localhost/about", "http://newcomp.localhost/", "internal")  # its own site: not a backlink
    onboard("own.localhost")
    r = reg_comp("own.localhost", ("newcomp.localhost", None))[0]
    assert r["known_backlinks"] == 2  # no search, no crawl yet
    out = crawler_api.competitor_backlinks("newcomp.localhost", False, 100, 0)
    assert out["total_backlinks"] == 2 and out["target_type"] == "competitor" and out["discovery_status"] if "discovery_status" in out else True
    assert {b["rel"] for b in out["backlinks"]} == {None, "nofollow"} or {b["rel"] for b in out["backlinks"]} == {"", "nofollow"}  # attributes kept, not filtered
    assert crawler_api.competitor_referring_domains("newcomp.localhost")["total_referring_domains"] == 2
    with pytest.raises(HTTPException):
        crawler_api.competitor_backlinks("unregistered.localhost", False, 100, 0)


# ---- scheduling: priorities, cap, schedule, search size ----
def test_competitor_priorities_cap_and_recurring_schedule(env, monkeypatch):
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 5)
    monkeypatch.setattr(bj, "COMPETITOR_RESULTS", 7)
    onboard("own.localhost")
    reg_comp("own.localhost", ("c1.localhost", "C1"), ("c2.localhost", None), ("c3.localhost", None))
    asked: dict = {}

    def fake(domain, n, **kw):
        asked[domain] = (n, kw.get("name"))
        return []
    monkeypatch.setattr(bj.ddg_lookup, "search_urls", fake)
    first = bj._claim_search(5)
    assert [j["target_domain"] for j in first] == ["own.localhost", "c1.localhost"]  # customer first; only MAX_ACTIVE_COMPETITOR_JOBS=1 competitor starts
    assert [j["target_type"] for j in first] == ["own_business", "competitor"]
    assert bj._claim_search(5) == []  # c1 holds the competitor slot while it searches/crawls
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done' WHERE target_domain='c1.localhost'")
    assert [j["target_domain"] for j in bj._claim_search(5)] == ["c2.localhost"]  # fair: next competitor in order, one at a time
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done' WHERE target_domain IN ('c2.localhost','own.localhost')")
    for j in [{"target_domain": "c3.localhost", "name": None, "target_type": "competitor", "id": 0, "retry_count": 0, "search_gen": 1},
              {"target_domain": "own.localhost", "name": None, "target_type": "own_business", "id": 0, "retry_count": 0, "search_gen": 1}]:
        asyncio.run(bj._search_one(j))
    assert asked["c3.localhost"][0] == 7 and asked["own.localhost"][0] == bj.DDG_RESULTS  # per-type result size

    # recurring: own P2, competitor P4, and COMPETITOR_DISCOVERY_ENABLED=false stops competitor reruns only
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("UPDATE businesses SET next_run_at=?", (core.later(seconds=-5),))
    monkeypatch.setattr(bj, "COMPETITOR_ENABLED", False)
    assert bj.schedule_due() == 1 and rows("SELECT priority FROM business_jobs ORDER BY id DESC LIMIT 1")[0]["priority"] == bj.CrawlPriority.RECURRING
    monkeypatch.setattr(bj, "COMPETITOR_ENABLED", True)
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("UPDATE businesses SET next_run_at=? WHERE target_type='own_business'", (core.later(hours=1),))  # only competitors are due now
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    assert bj.schedule_due() == 1
    assert rows("SELECT j.priority FROM business_jobs j JOIN businesses b ON b.domain=j.target_domain WHERE b.target_type='competitor' ORDER BY j.id DESC LIMIT 1")[0]["priority"] \
        == bj.CrawlPriority.COMPETITOR_RECURRING


# ---- one crawled page serves several targets; gap on real crawled data ----
def test_one_page_serves_all_targets_and_gap_from_crawled_graph(env, monkeypatch):
    u = env
    for t in ("own", "ca", "cb"):
        SITES[f"{t}.localhost"] = {"/": page()}
    SITES["shared.localhost"] = {"/": page(a(u("own"), "O"), a(u("ca"), "A", rel="nofollow"), a(u("cb"), "B"), a("https://unrelated.example/", "U"))}
    SITES["caonly.localhost"] = {"/": page(a(u("ca"), "A2", rel="sponsored"))}
    search_with(monkeypatch, {"own.localhost": [u("shared")], "ca.localhost": [u("shared"), u("caonly")], "cb.localhost": [u("shared")]})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    monkeypatch.setattr(bj, "MAX_ACTIVE_COMPETITOR_JOBS", 2)
    onboard("own.localhost")
    assert [r["status"] for r in reg_comp("own.localhost", ("ca.localhost", None), ("cb.localhost", None))] == ["registered"] * 2
    domains = ["own.localhost", "ca.localhost", "cb.localhost"]
    drive(cfg(), domains)

    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", u("shared"))[0]["n"] == 1  # fetched once for three targets
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE source_url=? AND link_type='html'", u("shared"))[0]["n"] == 4  # each link stored once
    assert rows("SELECT COUNT(*) n FROM job_urls ju JOIN discovered_urls d ON d.id=ju.url_id WHERE d.url=?", u("shared"))[0]["n"] == 3  # one URL, three jobs
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url LIKE '%unrelated.example%'")[0]["n"] == 0  # unrelated external: stored, not followed
    pr = {r["target_domain"]: r["priority"] for r in rows("SELECT * FROM business_jobs")}
    assert pr["ca.localhost"] == pr["cb.localhost"] == bj.CrawlPriority.COMPETITOR_INITIAL and pr["own.localhost"] < bj.CrawlPriority.COMPETITOR_INITIAL

    own = crawler_api.backlinks("own.localhost", False, 100, 0)
    ca = crawler_api.competitor_backlinks("ca.localhost", False, 100, 0)
    cb = crawler_api.competitor_backlinks("cb.localhost", False, 100, 0)
    assert (own["total_backlinks"], ca["total_backlinks"], cb["total_backlinks"]) == (1, 2, 1)
    assert {b["source_url"] for b in own["backlinks"]} == {u("shared")} and {b["source_url"] for b in cb["backlinks"]} == {u("shared")}  # no contamination
    assert {b["source_url"]: b["rel"] for b in ca["backlinks"]} == {u("shared"): "nofollow", u("caonly"): "sponsored"}  # attributes preserved
    assert {j["target_domain"]: j["status"] for j in rows("SELECT * FROM business_jobs")} == dict.fromkeys(domains, "done")

    g = crawler_api.backlink_gap("own.localhost", None, 100)
    assert g["summary"]["gap_domains"] == 1 and g["summary"]["shared_domains"] == 1 and g["summary"]["own_referring_domains"] == 1
    assert [o["referring_domain"] for o in g["opportunities"]] == ["caonly.localhost"] and g["opportunities"][0]["competitor_targets"] == ["ca.localhost"]
    assert g["shared_domains"] == ["shared.localhost"] and "discovered index" in g["coverage"]["basis"]
    assert crawler_api.backlink_gap("own.localhost", "cb.localhost", 100)["summary"]["gap_domains"] == 0  # cb's only linker also links to own


# ---- gap classification on stored links ----
def test_backlink_gap_classification_pages_attributes_and_coverage(env):
    onboard("own.localhost")
    reg_comp("own.localhost", ("ca.localhost", None), ("cb.localhost", None))
    for src in ("news.localhost/a", "industry.localhost/a", "partner.localhost/a"):
        link(f"http://{src}", "http://own.localhost/", "own")
    link("http://own.localhost/about", "http://own.localhost/", "internal")  # own internal link: never counts
    for src in ("news.localhost/b", "industry.localhost/a", "directory.localhost/a", "blog.localhost/1", "blog.localhost/2"):
        link(f"http://{src}", "http://ca.localhost/", "a", rel="nofollow" if "blog" in src else None, nofollow=int("blog" in src))
    link("http://directory.localhost/a", "http://cb.localhost/", "b")
    link("http://extra.localhost/x", "http://cb.localhost/", "b", rel="ugc")
    g = crawler_api.backlink_gap("own.localhost", None, 100)
    assert g["summary"] == {"own_referring_domains": 3, "competitor_referring_domains": 5, "gap_domains": 3, "shared_domains": 2, "own_only_domains": 1, "gap_pages": 5}
    assert {o["referring_domain"]: o["competitor_targets"] for o in g["opportunities"]} == \
        {"directory.localhost": ["ca.localhost", "cb.localhost"], "blog.localhost": ["ca.localhost"], "extra.localhost": ["cb.localhost"]}
    blog = next(o for o in g["opportunities"] if o["referring_domain"] == "blog.localhost")
    assert len(blog["source_pages"]) == 2  # two pages, still ONE referring domain
    assert g["own_only_domains"] == ["partner.localhost"] and g["shared_domains"] == ["industry.localhost", "news.localhost"]
    nb = next(p for p in g["source_page_gap"] if p["source_url"] == "http://news.localhost/b")  # shared domain, but this page does not link to own
    assert nb["domain_links_to_own_business"] is True
    bl = next(p for p in g["source_page_gap"] if p["source_url"] == "http://blog.localhost/1")
    assert bl["links"][0]["nofollow"] is True and bl["links"][0]["rel"] == "nofollow"  # attributes preserved, not filtered
    assert any("own.localhost" in w for w in g["coverage"]["warnings"])  # discovery not finished: the answer says so
    assert crawler_api.backlink_gap("own.localhost", "cb.localhost", 100)["summary"]["gap_domains"] == 2  # directory, extra
    for bad in ("unknown.localhost", "own.localhost"):  # not registered for this business (or itself): 404, never a silent answer
        with pytest.raises(HTTPException):
            crawler_api.backlink_gap("own.localhost", bad, 100)
    db(competitors.set_active, "own.localhost", "cb.localhost", False)
    assert crawler_api.backlink_gap("own.localhost", None, 100)["competitors"] == ["ca.localhost"]  # disabled competitors leave the comparison


def test_gap_returns_only_the_competitors_registered_for_that_business(env):
    R = crawler_api.CompetitorsRequest
    onboard("own2.localhost")
    crawler_api.add_business_competitors("own1.localhost", R(domains=["c1.localhost"]))
    crawler_api.add_business_competitors("own2.localhost", R(domains=["c2.localhost"]))
    link("http://d.localhost/a", "http://c1.localhost/")
    link("http://d.localhost/b", "http://c2.localhost/")
    assert crawler_api.backlink_gap("own1.localhost", None, 100)["competitors"] == ["c1.localhost"]
    assert crawler_api.backlink_gap("own2.localhost", None, 100)["competitors"] == ["c2.localhost"]


def test_all_backlinks_route_returns_businesses_and_competitors(env):
    onboard("own1.localhost")
    reg_comp("own1.localhost", ("ca.localhost", None), ("gc.localhost", None))
    link("http://n.localhost/a", "http://own1.localhost/")
    link("http://n.localhost/b", "http://ca.localhost/")
    link("http://d.localhost/a", "http://gc.localhost/")
    link("http://own1.localhost/x", "http://own1.localhost/")  # internal: never counts
    r = crawler_api.all_backlinks(None, False, 100, 0)
    assert r["total_backlinks"] == 3
    assert {(b["target_domain"], b["target_type"]) for b in r["backlinks"]} == {("own1.localhost", "own_business"), ("ca.localhost", "competitor"), ("gc.localhost", "competitor")}
    assert {t["domain"]: t["backlinks"] for t in r["targets"]} == {"own1.localhost": 1, "ca.localhost": 1, "gc.localhost": 1}
    assert crawler_api.all_backlinks("competitor", False, 100, 0)["total_backlinks"] == 2


def test_business_competitor_routes_keep_each_business_own_list(env):
    R = crawler_api.CompetitorsRequest
    crawler_api.add_business_competitors("b1.localhost", R(domains=["c1.localhost", "bad domain"]))
    crawler_api.add_business_competitors("b2.localhost", R(domains=["c2.localhost"]))
    assert [x["competitor_domain"] for x in crawler_api.list_business_competitors("b1.localhost")["competitors"]] == ["c1.localhost"]
    assert [x["competitor_domain"] for x in crawler_api.list_business_competitors("b2.localhost")["competitors"]] == ["c2.localhost"]
    assert crawler_api.remove_business_competitor("b1.localhost", "c1.localhost")["removed"]
    with pytest.raises(HTTPException):
        crawler_api.remove_business_competitor("b1.localhost", "c2.localhost")


# ---- end to end: customer + two competitors through the real engine crawler loop, with a batch timeout and a dead worker ----
def test_competitors_crawl_to_completion_one_slot_at_a_time_across_timeout_and_crash(env, monkeypatch):
    import autonomous
    import crawler_engine as engine
    u = env
    SITES["dir.localhost"] = {"/": page(a("https://codigix.co/"))}
    for n in (1, 2, 3):  # webfx: 3 candidate sites x (root + 2 internal pages), every page links to webfx.com
        SITES[f"blog{n}.localhost"] = {"/": page(a("https://webfx.com/"), a("/p0"), a("/p1")), "/p0": page(a("https://webfx.com/")), "/p1": page(a("https://webfx.com/"))}
    SITES["blog2.localhost"]["/"] = (200, "sleep:3", SITES["blog2.localhost"]["/"][2])  # one slow page: its batch hits the time limit
    SITES["news.localhost"] = {"/": page(a("https://ignitevisibility.com/"), a("/a")), "/a": page(a("https://ignitevisibility.com/"))}
    search_with(monkeypatch, {"codigix.co": [u("dir")], "webfx.com": [u(f"blog{n}") for n in (1, 2, 3)], "ignitevisibility.com": [u("news")]})
    monkeypatch.setattr(bj, "COMPETITOR_STARVATION_SECONDS", 60)

    reg_comp("codigix.co", ("webfx.com", None), ("ignitevisibility.com", None))
    assert {r["target_domain"]: r["priority"] for r in rows("SELECT * FROM business_jobs")} == {"codigix.co": 0, "webfx.com": 3, "ignitevisibility.com": 3}
    assert targets()["webfx.com"]["parent_domain"] == targets()["ignitevisibility.com"]["parent_domain"] == "codigix.co"

    c = cfg(claim_batch=3)
    eng = autonomous.Engine(c)
    webfx_closed_at = ignite_started_at = None
    timed_out = crashed = aged = False
    for i in range(80):
        asyncio.run(bj.search_pending())
        bj.tick()
        st = {r["target_domain"]: r for r in rows("SELECT * FROM business_jobs")}
        assert sum(st[d]["status"] in ("searching", "crawling") for d in ("webfx.com", "ignitevisibility.com")) <= 1  # one competitor slot
        if webfx_closed_at is None and st["webfx.com"]["status"] == "done":
            webfx_closed_at = i
        if ignite_started_at is None and st["ignitevisibility.com"]["status"] != "pending":
            ignite_started_at = i
        if all(r["status"] == "done" for r in st.values()):
            break
        if st["webfx.com"]["status"] == "crawling" and not aged:  # ignitevisibility now waited "long": the aged claim must still respect the cap
            with core.connect() as conn:
                conn.execute("UPDATE business_jobs SET created_at=? WHERE target_domain='ignitevisibility.com'", (core.later(hours=-1),))
            aged = True
        if st["webfx.com"]["status"] == "crawling" and not timed_out:  # a worker whose round runs past its execution limit
            work = autonomous.claim_work("slow-worker", c)
            assert u("blog2") in [x for x, _ in work]
            one = copy.copy(c.crawl)
            one.concurrency = 1  # so the other two pages have not started when the limit hits
            t0 = time.monotonic()
            try:  # Crawlee absorbs the cancel: it lets the request in flight finish, drops the rest and returns
                asyncio.run(asyncio.wait_for(engine.crawl_round(one, None, work, set(), False, len(work)), 1.5))
            except TimeoutError:
                pass
            assert time.monotonic() - t0 < 6  # cut short: the two fast pages were never fetched
            autonomous.release("slow-worker")  # what Engine.crawler's finally / the Celery task's error path does
            status = {r["url"]: r["crawl_status"] for r in rows("SELECT url, crawl_status FROM discovered_urls WHERE url = ANY(?::text[])", [x for x, _ in work])}
            assert sorted(status.values()).count("pending") >= 1 and "completed" in status.values() and len(set(status.values())) == 2  # progress kept, the rest back in the queue, nothing stuck
            timed_out = True
            continue
        if timed_out and not crashed and st["webfx.com"]["status"] == "crawling":  # a worker claims a batch and dies without a word
            assert autonomous.claim_work("dead-worker", c)
            with core.connect() as conn:
                conn.execute("UPDATE discovered_urls SET lease_expires=? WHERE lease_owner='dead-worker'", (core.later(seconds=-1),))
            crashed = True
            continue
        asyncio.run(eng.crawler())  # the running engine: recovers expired leases, claims, crawls with lease renewal, releases
    else:
        raise AssertionError(f"not finished: {[(r['target_domain'], r['status']) for r in rows('SELECT * FROM business_jobs')]}")

    assert timed_out and crashed and aged
    assert webfx_closed_at is not None and ignite_started_at is not None and ignite_started_at >= webfx_closed_at  # slot handed over, not shared
    jobs = {r["target_domain"]: r for r in rows("SELECT * FROM business_jobs")}
    assert jobs["webfx.com"]["pages_crawled"] == jobs["webfx.com"]["pages_discovered"] == 9 and jobs["webfx.com"]["backlinks_found"] == 9
    assert jobs["ignitevisibility.com"]["pages_crawled"] == 2 and jobs["ignitevisibility.com"]["backlinks_found"] == 2
    assert jobs["codigix.co"]["backlinks_found"] == 1
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling'")[0]["n"] == 0  # no stuck or duplicate lease
    assert rows("SELECT COUNT(*) n FROM (SELECT source_url FROM crawl_history GROUP BY source_url HAVING COUNT(*)>1) x")[0]["n"] == 0  # no page stored twice
    assert all(t["next_run_at"] > core.now() and t["last_success_at"] for t in targets().values())
    assert bj.schedule_due() == 0  # a finished target is not requeued at once


def _done(d):
    db(lambda c: c.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE target_domain=? AND status<>'done'", (core.now(), d)))
    db(bj._reschedule, d, True)


def _jobs():
    return rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"]


def test_resubmitting_competitors_never_rescans_them(env):
    """The same list POSTed again (or by a second business sharing a competitor) adds no scan; a new one gets exactly one; the scheduler owns the rest."""
    req = crawler_api.CompetitorsRequest
    first = crawler_api.add_business_competitors("own.localhost", req(domains=["r1.localhost", "r2.localhost"]))
    assert [x["discovery"] for x in first["results"]] == ["queued", "queued"] and _jobs() == 3  # the business + 2 competitors
    for d in ("r1.localhost", "r2.localhost", "own.localhost"):
        _done(d)
    keep = "SELECT domain, next_run_at FROM businesses WHERE domain IN ('r1.localhost','r2.localhost','own.localhost') ORDER BY domain"
    nxt = rows(keep)
    for _ in range(3):
        again = crawler_api.add_business_competitors("own.localhost", req(domains=["r1.localhost", "r2.localhost"]))
        assert [x["status"] for x in again["results"]] == ["duplicate", "duplicate"]
    shared = crawler_api.add_business_competitors("other.localhost", req(domains=["r1.localhost"]))
    assert shared["results"][0]["discovery"].startswith("already monitored")  # within its 24h: no extra scan for the second owner
    added = crawler_api.add_business_competitors("own.localhost", req(domains=["r1.localhost", "r3.localhost"]))
    assert [x["status"] for x in added["results"]] == ["duplicate", "registered"] and added["results"][1]["discovery"] == "queued"
    assert _jobs() == 5  # + other.localhost's own first scan + r3's first scan; none for r1/r2
    assert rows(keep) == nxt  # schedules untouched
    _done("other.localhost")
    _done("r3.localhost")
    db(lambda c: c.execute("UPDATE businesses SET next_run_at=? WHERE domain='r1.localhost'", (core.later(seconds=-60),)))
    assert bj.schedule_due() == 1  # overdue r1: the scheduler runs it, as a recurring competitor job
    assert rows("SELECT target_domain, priority FROM business_jobs ORDER BY id DESC LIMIT 1")[0] == \
        {"target_domain": "r1.localhost", "priority": bj.CrawlPriority.COMPETITOR_RECURRING}


def test_concurrent_competitor_requests_create_no_duplicate_jobs(env):
    import threading
    crawler_api.add_business_competitors("own2.localhost", crawler_api.CompetitorsRequest(domains=["x1.localhost"]))
    barrier = threading.Barrier(6)

    def hit():
        barrier.wait()
        try:
            crawler_api.add_business_competitors("own2.localhost", crawler_api.CompetitorsRequest(domains=["x1.localhost", "x2.localhost"]))
        except Exception:  # noqa: BLE001 - a request that loses a race may fail; it must never leave an extra job
            pass
    ts = [threading.Thread(target=hit) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert {r["target_domain"]: r["n"] for r in rows("SELECT target_domain, COUNT(*) n FROM business_jobs GROUP BY 1")} == \
        {"own2.localhost": 1, "x1.localhost": 1, "x2.localhost": 1}


def test_three_competitors_registered_at_once_never_exceed_the_single_slot_under_polling(env, monkeypatch):
    """Phase 9.1 D3 regression: one engine polls (search_pending's claim) while three competitors are registered concurrently. A poll with one free
    slot returns at most one job and at no moment are two competitor jobs past 'pending'. (The 9.0 live run once claimed all three in one poll;
    that was not reproduced, see PHASE9_1 report: this test pins the contract.)"""
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    monkeypatch.setattr(bj, "MAX_ACTIVE_COMPETITOR_JOBS", 1)
    onboard("own.localhost")
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
    stop, errors, seen = threading.Event(), [], []

    def register(d):
        with core.connect() as c:
            competitors.register(c, "own.localhost", d)

    def poll():
        while not stop.is_set():
            got = bj._claim_search(1)
            active = rows("SELECT COUNT(*) n FROM business_jobs j JOIN businesses b ON b.domain=j.target_domain AND b.target_type='competitor'"
                          " WHERE j.status IN ('searching','crawling')")[0]["n"]
            if len(got) > 1 or active > 1:
                errors.append((len(got), active))
            if got:
                seen.extend(j["target_domain"] for j in got)
                with core.connect() as c:  # the claimed search "finishes": frees the slot for the next competitor
                    c.execute("UPDATE business_jobs SET status='done' WHERE id=ANY(?::bigint[])", ([j["id"] for j in got],))
            time.sleep(0.01)
    t = threading.Thread(target=poll)
    t.start()
    regs = [threading.Thread(target=register, args=(f"c{i}.localhost",)) for i in range(3)]
    [r.start() for r in regs]
    [r.join() for r in regs]
    deadline = time.time() + 30
    while len(seen) < 3 and time.time() < deadline:
        time.sleep(0.05)
    stop.set()
    t.join()
    assert not errors, errors
    assert sorted(seen) == ["c0.localhost", "c1.localhost", "c2.localhost"]  # all served, one at a time

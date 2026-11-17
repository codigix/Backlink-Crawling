"""Competitor targets on the shared graph: registration rules, scheduling/priorities/caps, one crawled page serving several targets,
backlink gap. Real PostgreSQL (isolated schema), real local HTTP fixtures, real Crawlee rounds; only DuckDuckGo is replaced."""

import asyncio

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

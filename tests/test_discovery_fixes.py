"""Target registration, reuse of already-crawled seeds, forward crawling after a theme batch, verified-alias reporting, blocked sites.
Real PostgreSQL (schema per test), real local HTTP sites, real Crawlee rounds. Only the search providers are replaced."""

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import autonomous
import business_intelligence as bi
import business_jobs as bj
import crawler_api
import crawler_core as core
import crawler_engine as engine
import ddg_lookup
from test_business_intelligence import on, themed_target  # noqa: F401 - fixtures
from test_business_jobs import SITES, a, cfg as acfg, env, job_status, one_round, page, port, rows  # noqa: F401
from test_crawler import HITS, cfg as ecfg


@pytest.fixture()
def api(env):
    app = FastAPI()
    app.include_router(crawler_api.router)
    return TestClient(app)


def keenable(monkeypatch, pool):
    monkeypatch.setattr(ddg_lookup, "_keenable", lambda q, page, own=None: [{"href": h} for h in pool] if page == 1 else [])


def targets():
    return {r["domain"]: r for r in rows("SELECT * FROM target_domains")}


def crawl_until_done(domains, rounds=40):
    c = acfg()
    for i in range(rounds):
        bj.tick()
        if all(job_status(d)["status"] == "done" for d in domains):
            return
        one_round(c, f"w{i % 3}")
    raise AssertionError([job_status(d)["status"] for d in domains])


# ---------------------------------------------------------------- Phase 1: every onboarded business and competitor is a backlink target

def test_onboarding_registers_business_and_competitor_as_active_targets_and_the_crawler_uses_them(api, env):
    u = env
    assert targets() == {}
    r = api.post("/discovery/onboard", json={"domain": "newbiz.localhost"})
    assert r.status_code == 202 and r.json()["created"] is True
    assert api.post("/businesses/newbiz.localhost/competitors", json={"domains": ["rival.localhost"]}).json()["results"][0]["status"] == "registered"
    t = targets()
    assert set(t) == {"newbiz.localhost", "rival.localhost"} and all(x["is_active"] == 1 for x in t.values())
    ids = {d: x["id"] for d, x in t.items()}
    for _ in range(2):  # repeated onboarding: no second row, same ids, one job
        api.post("/discovery/onboard", json={"domain": "newbiz.localhost"})
        api.post("/businesses/newbiz.localhost/competitors", json={"domains": ["rival.localhost"]})
    assert {d: x["id"] for d, x in targets().items()} == ids and len(rows("SELECT * FROM business_jobs WHERE target_domain='newbiz.localhost'")) == 1
    SITES["blog.localhost"] = {"/": page(a(u("newbiz"), "New"), a(u("rival", "/x"), "Rival"))}
    SITES["newbiz.localhost"] = {"/": page()}
    SITES["rival.localhost"] = {"/": page(), "/x": page()}
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='crawling', max_depth=0")
        core.save_url(c, u("blog"), None, "ddg", 0, False)
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT j.id, d.id, 0 FROM business_jobs j, discovered_urls d WHERE d.domain='blog.localhost'")
    crawl_until_done(["newbiz.localhost", "rival.localhost"])
    got = {(r["target_domain_id"], r["discovery_method"], r["link_status"]) for r in rows("SELECT * FROM backlinks")}
    assert got == {(ids["newbiz.localhost"], "forward_crawl", "verified"), (ids["rival.localhost"], "forward_crawl", "verified")}  # each link belongs to the right target
    assert "target_domain_id" in rows("SELECT * FROM backlinks LIMIT 1")[0]


def test_registration_never_reactivates_a_disabled_target_and_heals_a_missing_one(on, monkeypatch):
    themed_target("acme")
    with core.connect() as c:
        c.execute("DELETE FROM target_domains")  # a business onboarded before targets were registered
    keenable(monkeypatch, ["https://k1.example/p"])
    asyncio.run(bj.search_pending())
    assert set(targets()) == {"acme.localhost"} and targets()["acme.localhost"]["is_active"] == 1
    tid = targets()["acme.localhost"]["id"]
    with core.connect() as c:
        c.execute("UPDATE target_domains SET is_active=0")
        assert core.register_target(c, "acme.localhost") == tid  # same id, still off
        bj.create_job(c, "acme.localhost")
    assert targets()["acme.localhost"]["is_active"] == 0 and len(targets()) == 1


# ---------------------------------------------------------------- Phase 2/3: seeds that already exist

def test_seed_states_never_crawled_reused_stale_failed_and_queued(on, monkeypatch):
    u = on
    SITES["fresh.localhost"] = {"/": page(a(u("acme"), "Acme"), a(u("rival"), "Rival"))}  # crawled before either business existed
    SITES["stale.localhost"] = {"/": page(a(u("acme"), "Acme"))}
    SITES["new.localhost"] = {"/": page(a(u("acme"), "Acme"))}
    SITES["dead.localhost"] = {"/": (404, "text/html", "gone")}
    SITES["acme.localhost"] = {"/": page()}
    asyncio.run(engine.run_crawl(ecfg(max_depth=0), [u("fresh"), u("stale"), u("dead")]))
    assert {r["crawl_status"] for r in rows("SELECT * FROM discovered_urls WHERE domain IN ('fresh.localhost','stale.localhost')")} == {"completed"}
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET last_crawled='2000-01-01 00:00:00' WHERE domain='stale.localhost'")
        core.save_url(c, u("queued"), None, "x", 0, False)  # already queued by someone else
    themed_target("acme")
    themed_target("rival")
    before = dict(HITS)
    keenable(monkeypatch, [u("fresh"), u("stale"), u("new"), u("dead"), u("queued"), "https://acme.localhost/own"])
    asyncio.run(bj.search_pending())
    asyncio.run(bj.search_pending())  # the second target takes its turn
    st = {r["domain"]: r["crawl_status"] for r in rows("SELECT * FROM discovered_urls WHERE domain IN ('fresh.localhost','stale.localhost','new.localhost','dead.localhost','queued.localhost')")}
    assert st["fresh.localhost"] == "completed" and HITS.get(("fresh.localhost", "/")) == before.get(("fresh.localhost", "/"))  # Case B: not fetched again
    assert st["stale.localhost"] == "pending" and st["new.localhost"] == "pending" and st["queued.localhost"] == "pending"  # stale: bounded recrawl; A and D: normal queue
    assert st["dead.localhost"] == "failed"  # Case C stays under the existing failure policy
    reused = rows("SELECT b.*, t.domain target FROM backlinks b JOIN target_domains t ON t.id=b.target_domain_id")
    assert {(r["target"], r["discovery_method"], r["link_status"]) for r in reused} == {("acme.localhost", "graph_reuse", "verified"), ("rival.localhost", "graph_reuse", "verified")}
    fresh_crawl = rows("SELECT last_crawled FROM discovered_urls WHERE domain='fresh.localhost'")[0]["last_crawled"]
    assert {r["last_verified"] for r in reused} == {fresh_crawl}  # verification time = the source's crawl time, not "now"
    j = job_status("acme.localhost")
    assert (j["seed_candidates"], j["search_results"], j["seeds_reused"]) == (5, 5, 1)  # the own page is not a candidate
    with core.connect() as c:
        s = bj.discovery_status(c, "acme.localhost")["seeds"]
        assert s == {"admitted": 5, "crawled": 1, "failed": 1, "skipped": 0, "open": 3, "candidates": 5, "reused": 1}
        tid = core.register_target(c, "acme.localhost")
        assert bj._reuse_seeds(c, tid, "acme.localhost", [r["url_id"] for r in c.execute("SELECT url_id FROM job_urls WHERE job_id=?", (j["id"],))]) == 1
    assert len(rows("SELECT * FROM backlinks")) == 2  # idempotent: no duplicate record
    crawl_until_done(["acme.localhost"])
    methods = {r["source_url"].split("//")[1].split(".")[0]: r["discovery_method"] for r in rows("SELECT * FROM backlinks b JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain='acme.localhost'")}
    assert methods == {"fresh": "graph_reuse", "stale": "forward_crawl", "new": "forward_crawl"}  # fetched ones are verified by the live crawl


def test_a_target_switched_off_gets_no_reused_backlinks(on, monkeypatch):
    u = on
    SITES["fresh.localhost"] = {"/": page(a(u("acme"), "Acme"))}
    asyncio.run(engine.run_crawl(ecfg(max_depth=0), [u("fresh")]))
    themed_target("acme")
    with core.connect() as c:
        core.register_target(c, "acme.localhost")
        c.execute("UPDATE target_domains SET is_active=0")
    keenable(monkeypatch, [u("fresh")])
    asyncio.run(bj.search_pending())
    assert rows("SELECT COUNT(*) n FROM backlinks")[0]["n"] == 0


# ---------------------------------------------------------------- Phase 4: forward crawling keeps going after a depth-0 theme batch

def test_a_seed_feeds_the_shared_frontier_and_the_autonomous_crawler_processes_it(on, monkeypatch):
    u = on
    themed_target("acme", themes=2)
    SITES["seed.localhost"] = {"/": page(a(u("acme"), "Acme"), a(u("found", "/next"), "elsewhere"))}
    SITES["found.localhost"] = {"/next": page(a(u("acme"), "Acme again"))}
    SITES["acme.localhost"] = {"/": page()}
    keenable(monkeypatch, [u("seed")])
    asyncio.run(bj.search_pending())
    assert job_status("acme.localhost")["max_depth"] == 0  # the batch is its seeds ...
    crawl_until_done(["acme.localhost"])
    f = lambda: rows("SELECT crawl_status FROM discovered_urls WHERE domain='found.localhost'")[0]["crawl_status"]  # noqa: E731
    assert f() == "pending"  # ... but what they found is queued in the shared frontier, not dropped
    assert rows("SELECT discovery_status FROM discovered_domains WHERE domain='found.localhost'")[0]["discovery_status"] == "discovered"
    ac = acfg()
    assert asyncio.run(autonomous.admit_batch(ac)) >= 1  # domain admission
    assert rows("SELECT discovery_status FROM discovered_domains WHERE domain='found.localhost'")[0]["discovery_status"] == "admitted"
    bj.tick()  # theme 2 is claimable meanwhile: it must not disturb the frontier
    for i in range(10):
        if f() == "completed":
            break
        one_round(ac, f"auto{i}")
    assert f() == "completed"  # processed by the autonomous crawler, not by a business job
    assert {r["source_url"] for r in rows("SELECT * FROM backlinks")} == {u("seed"), u("found", "/next")}
    assert rows("SELECT status FROM search_themes WHERE domain='acme.localhost' AND seq=1")[0]["status"] == "done"


# ---------------------------------------------------------------- Phase 6: verified aliases are the business, in reports too

def test_alias_links_are_not_reported_as_backlinks_but_stay_in_the_graph(api, env):
    u = env
    SITES["shop.localhost"] = {"/": page(a(u("owner"), "Our shop"))}
    SITES["third.localhost"] = {"/": page(a(u("owner"), "Review"))}
    SITES["owner.localhost"] = {"/": page()}
    api.post("/discovery/onboard", json={"domain": "owner.localhost"})
    asyncio.run(engine.run_crawl(ecfg(max_depth=0), [u("shop"), u("third")]))
    domains = lambda: {r["source_domain"] for r in api.get("/backlinks/owner.localhost?limit=50").json()["backlinks"]}  # noqa: E731
    assert domains() == {"shop.localhost", "third.localhost"}
    assert bi.add_alias("owner.localhost", "shop.localhost")
    assert domains() == {"third.localhost"}
    body = api.get("/backlinks/owner.localhost/referring-domains").json()
    assert [r["domain"] for r in body["referring_domains"]] == ["third.localhost"]
    assert api.get("/backlinks/owner.localhost").json()["total_backlinks"] == 1  # one backlink per source page, as before
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE source_domain='shop.localhost'")[0]["n"] == 1  # the evidence is kept
    with core.connect() as c:
        c.execute("DELETE FROM business_aliases")
    assert domains() == {"shop.localhost", "third.localhost"}  # and comes back when the ownership claim is withdrawn


# ---------------------------------------------------------------- Phase 7: blocked sites

def test_blocked_and_challenge_pages_are_recorded_not_crawled_and_do_not_stop_others(env):
    u = env
    SITES["ok.localhost"] = {"/": page(a("https://target.example/", "T"))}
    SITES["forbidden.localhost"] = {"/": (403, "text/html", "<html><body>Access denied</body></html>")}
    SITES["challenge.localhost"] = {"/": (200, "text/html", "<html><head><title>Just a moment...</title></head><body>"
                                                            + a("https://target.example/", "T") + "</body></html>")}
    asyncio.run(engine.run_crawl(ecfg(max_depth=0), [u("forbidden"), u("challenge"), u("ok")]))
    st = {r["domain"]: r for r in rows("SELECT domain, crawl_status, crawl_error FROM discovered_urls WHERE domain LIKE '%.localhost'")}
    assert st["ok.localhost"]["crawl_status"] == "completed"
    for d in ("forbidden.localhost", "challenge.localhost"):
        assert st[d]["crawl_status"] == "failed" and st[d]["crawl_error"].startswith("access_restricted"), d
    assert {r["source_domain"] for r in rows("SELECT source_domain FROM outgoing_links WHERE destination_domain='target.example'")} == {"ok.localhost"}  # no links claimed from a page we never saw


# ---------------------------------------------------------------- Phase 5: verified yield orders the next theme

def test_theme_kinds_that_produced_verified_backlinks_are_searched_first(on):
    themed_target("acme", themes=0)
    with core.connect() as c:
        bi.save_themes(c, "acme.localhost", [{"theme": "reviews", "reason": "m", "kind": "mention"}, {"theme": "seo guide", "reason": "t", "kind": "topic"},
                                             {"theme": "ranking", "reason": "t", "kind": "topic"}], 1, "Acme")
    jid = job_status("acme.localhost")["id"]
    first = bi.claim_theme("acme.localhost", jid)
    assert first["kind"] == "mention"  # no yield anywhere yet: creation order
    with core.connect() as c:  # that batch's seed turned out to carry a verified backlink ... but it was a topic batch in this scenario
        c.execute("UPDATE search_themes SET status='done' WHERE id=?", (first["id"],))
        c.execute("UPDATE search_themes SET kind='topic' WHERE id=?", (first["id"],))
        core.save_url(c, "https://seed.example/p", None, "ddg", 0, False)
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls WHERE url='https://seed.example/p'", (jid,))
        tid = core.register_target(c, "acme.localhost")
        core.record_backlink(c, tid, "https://seed.example/p", "https://acme.localhost/", "Acme", None)
        c.execute("UPDATE search_themes SET kind='mention' WHERE domain='acme.localhost' AND theme='seo guide'")
        c.execute("UPDATE search_themes SET kind='topic' WHERE domain='acme.localhost' AND theme='ranking'")
    assert bi.claim_theme("acme.localhost", jid)["theme"] == "ranking"  # topic produced a backlink; the earlier-created mention theme waits

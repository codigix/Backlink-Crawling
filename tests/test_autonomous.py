"""Autonomous engine tests: no seeds from the user, controlled local sites (reuses the server from test_crawler)."""

import asyncio

import httpx

import autonomous
import crawler_api
import crawler_core as core
import crawler_engine as engine
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures


def acfg(**kw) -> autonomous.AutoConfig:
    return autonomous.AutoConfig(sources=("test",), max_admitted=kw.pop("max_admitted", 2), **kw,
                                 crawl=engine.Config(domain_delay=0, timeout=10, max_retries=1, max_depth=2, concurrency=2))


def use_source(monkeypatch, urls_fn):
    async def src(client, cfg, cursor):
        return urls_fn(), cursor
    monkeypatch.setattr(autonomous, "SOURCES", {"test": src})


def bl(d, **kw):  # endpoint functions called directly need explicit values for their Query() defaults
    return crawler_api.backlinks(d, **{"include_removed": False, "limit": 100, "offset": 0, **kw})


def cycle(eng, n=4):
    async def go():
        for _ in range(n):
            await eng.scheduler()
            await eng.crawler()
    asyncio.run(go())


# bootstrap w/o seeds, domain discovery, recursion, dedup, global index, arbitrary-domain search, no crawl on search
def test_autonomous_pipeline_and_database_first_search(env, monkeypatch):
    u = env
    SITES["a.localhost"] = {"/": page(a(u("b")), a("/p2"), a("https://codigix.co/x", "Codigix", "nofollow"), a("/p2")),
                            "/p2": page(a("https://codigix.co/y", "Codigix again"))}
    SITES["b.localhost"] = {"/": page(a("https://example.com/", "Ex"), a("https://codigix.co/", "Codigix"))}
    use_source(monkeypatch, lambda: [(u("a"), 1, 0)])
    cfg = acfg()
    asyncio.run(autonomous.bootstrap(cfg))
    assert rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"] == 1  # the only input is the source, no user seed
    cycle(autonomous.Engine(cfg), 2)  # rolling admission frees finished domains' slots; stop before codigix.co could take one
    assert {r["domain"] for r in rows("SELECT domain FROM discovered_domains WHERE discovery_status='admitted'")} == {"a.localhost", "b.localhost"}
    assert rows("SELECT COUNT(*) n FROM discovered_domains WHERE domain='codigix.co'")[0]["n"] == 1  # discovered, never admitted
    assert rows("SELECT COUNT(*) n FROM discovered_urls GROUP BY normalized_url HAVING COUNT(*)>1") == []  # dedup
    assert len(rows("SELECT * FROM crawl_history WHERE source_url LIKE '%/p2'")) == 1  # /p2 linked twice, fetched once
    assert rows("SELECT 1 FROM outgoing_links WHERE source_domain IS NULL") == []

    before = rows("SELECT COUNT(*) n FROM crawl_history")[0]["n"]
    r = bl("codigix.co")  # never registered as a target
    assert r["status"] == "ok" and r["total_backlinks"] == 3 and r["referring_domains"] == 2
    assert {b["source_domain"] for b in r["backlinks"]} == {"a.localhost", "b.localhost"}
    assert crawler_api.referring_domains("codigix.co")["total_referring_domains"] == 2
    assert {x["anchor_text"]: x["count"] for x in crawler_api.backlink_anchors("codigix.co")["anchors"]}["Codigix"] == 2
    miss = bl("never-seen.org")
    assert miss["status"] == "not_found_in_current_index" and miss["total_backlinks"] == 0
    assert rows("SELECT COUNT(*) n FROM crawl_history")[0]["n"] == before  # a search never crawls
    assert rows("SELECT COUNT(*) n FROM target_domains")[0]["n"] == 0  # ...and never registers anything


def test_lost_backlink_shows_in_history(env, monkeypatch):
    u = env
    SITES["a.localhost"] = {"/": page(a("https://codigix.co/", "C"))}
    use_source(monkeypatch, lambda: [(u("a"), 1, 0)])
    cfg = acfg(max_admitted=1)
    asyncio.run(autonomous.bootstrap(cfg))
    cycle(autonomous.Engine(cfg), 1)
    SITES["a.localhost"] = {"/": page()}
    asyncio.run(engine.recrawl_due(cfg.crawl, None, [u("a")]))
    first = bl("codigix.co")  # one miss = suspected, still listed, not yet a lost event
    assert first["total_backlinks"] == 1 and first["backlinks"][0]["link_status"] == "suspected_removed"
    assert {e["event"] for e in crawler_api.backlink_history("codigix.co", limit=200)["events"]} == {"new", "suspected_removed"}
    asyncio.run(engine.recrawl_due(cfg.crawl, None, [u("a")]))  # second successful miss confirms
    h = crawler_api.backlink_history("codigix.co", limit=200)["events"]
    assert {e["event"] for e in h} == {"new", "suspected_removed", "lost"}
    assert bl("codigix.co")["status"] == "not_found_in_current_index"
    assert bl("codigix.co", include_removed=True)["total_backlinks"] == 1


# leases are atomic, per-domain budget and fairness hold, expired leases recover (worker restart)
def test_claims_are_atomic_and_recoverable(env):
    cfg = autonomous.AutoConfig(claim_batch=7, per_batch=3, domain_page_budget=4)
    with core.connect() as c:
        for d in ("x.localhost", "y.localhost"):
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_status) VALUES (?, 'admitted')", (d,))
            for i in range(10):
                core.save_url(c, f"http://{d}/{i}", None, "t", 1, True)
    one, two = autonomous.claim_urls("w1", cfg), autonomous.claim_urls("w2", cfg)
    assert len(one) == 6 and not set(one) & set(two)  # 3 per domain, nothing handed out twice
    assert len(two) == 2  # budget 4 per domain counts leased URLs: only 1 left each
    assert autonomous.claim_urls("w3", cfg) == []
    assert autonomous.release("w1") == 6  # graceful shutdown returns leases
    assert len(autonomous.claim_urls("w3", cfg)) == 6
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET lease_expires='2000-01-01 00:00:00' WHERE lease_owner='w3'")
    assert autonomous.release() == 6  # dead worker: expired leases recovered
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner='dead'")
        core.migrate(c)  # engine startup recovery
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling'")[0]["n"] == 0


# Common Crawl disabled; failing sources never break the engine
def test_disabled_and_failing_sources(env, monkeypatch):
    async def boom(client, cfg, cursor):
        raise httpx.ConnectError("down")
    monkeypatch.setattr(autonomous, "SOURCES", {"commoncrawl": boom, "tranco": boom})
    out = asyncio.run(autonomous.bootstrap(autonomous.AutoConfig(sources=("tranco",))))
    assert out == {"commoncrawl": 0, "tranco": 0}
    st = {r["name"]: r for r in rows("SELECT * FROM discovery_sources")}
    assert st["commoncrawl"]["enabled"] == 0 and st["commoncrawl"]["runs"] == 0  # disabled: never even attempted
    assert st["tranco"]["status"] == "error" and "down" in st["tranco"]["last_error"]


def test_real_adapters_against_mock_servers(env):
    def handler(req: httpx.Request) -> httpx.Response:
        u = str(req.url)
        if "api/lists" in u:
            return httpx.Response(200, json={"list_id": "ABC"})
        if "tranco-list.eu/download" in u:
            return httpx.Response(200, text="1,google.com\n2,wikipedia.org\n3,example.net\n")
        if u.endswith("collinfo.json"):
            return httpx.Response(200, json=[{"cdx-api": "https://cc.test/idx"}])
        if "cc.test" in u:
            return httpx.Response(200, text='{"url": "https://site.org/a"}\n{"url": "https://other.com/z"}\n')
        return httpx.Response(200, text="<rss><channel><item><link>https://blog.example.org/post</link></item></channel></rss>")
    cfg = autonomous.AutoConfig(sources=("tranco", "feeds", "commoncrawl"), tranco_limit=3, tranco_batch=2, feeds=("https://feed.test/rss",))

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as cl:
            r = [await autonomous.run_source(n, cfg, cl, force=True) for n in ("tranco", "feeds")]
            with core.connect() as c:
                c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES ('site.org','admitted')")
            r.append(await autonomous.run_source("commoncrawl", cfg, cl, force=True))
            r.append(await autonomous.run_source("tranco", cfg, cl, force=True))  # second batch resumes from cursor 2
        return r
    assert asyncio.run(go()) == [2, 1, 1, 1]
    got = {r["normalized_url"]: r for r in rows("SELECT * FROM discovered_urls")}
    assert "https://site.org/a" in got and "https://other.com/z" not in got  # CC rows are filtered to the admitted domain
    assert got["https://site.org/a"]["discovery_method"] == "commoncrawl"
    assert rows("SELECT priority FROM discovered_domains WHERE domain='google.com'")[0]["priority"] == 1
    assert not rows("SELECT * FROM outgoing_links")  # imported URLs are never links, let alone backlinks


# robots.txt is honored by the autonomous crawler
def test_robots_txt_respected(env, monkeypatch):
    u = env
    SITES["a.localhost"] = {"/": page(a("/private/x"), a("/open")), "/open": page(), "/private/x": page(a("https://leak.com/")),
                            "/robots.txt": (200, "text/plain", "User-agent: *\nDisallow: /private/\n")}
    use_source(monkeypatch, lambda: [(u("a"), 1, 0)])
    cfg = acfg(max_admitted=1)
    asyncio.run(autonomous.bootstrap(cfg))
    cycle(autonomous.Engine(cfg), 4)
    assert not rows("SELECT * FROM crawl_history WHERE source_url LIKE '%/private/%'")
    assert rows("SELECT * FROM crawl_history WHERE source_url LIKE '%/open'")
    blocked = rows("SELECT crawl_status, skip_reason, lease_owner FROM discovered_urls WHERE url LIKE '%/private/x'")[0]
    assert (blocked["crawl_status"], blocked["skip_reason"], blocked["lease_owner"]) == ("skipped", "robots_txt", None)
    autonomous.release("anyone"); autonomous.release()  # lease cleanup must not resurrect it
    assert autonomous.claim_urls("w", cfg) == [] or all("/private/" not in u for u, _ in autonomous.claim_urls("w", cfg))
    with core.connect() as c:
        core.migrate(c)  # restart recovery
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url LIKE '%/private/x'")[0]["crawl_status"] == "skipped"


# SSRF guard also covers autonomous admission
def test_private_domains_never_admitted(env, monkeypatch):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    with core.connect() as c:
        core.save_url(c, "http://127.0.0.1/", None, "t", 0, True)
    asyncio.run(autonomous.admit_batch(autonomous.AutoConfig()))
    assert rows("SELECT discovery_status FROM discovered_domains WHERE domain='127.0.0.1'")[0]["discovery_status"] == "blocked"


def test_unreachable_domain_retired_and_pause(env):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES ('dead.localhost','admitted')")
        for i in range(5):
            core.save_url(c, f"http://dead.localhost/{i}", None, "t", 1, True, "failed")
    assert autonomous.retire_unreachable() == 1

    runs = []
    eng = autonomous.Engine()

    async def fn():
        runs.append(1)
        eng.stop.set()

    async def resume_later():
        await asyncio.sleep(0.2)
        autonomous.set_paused(False)

    async def go():
        await asyncio.wait_for(asyncio.gather(eng._supervise("t", fn, 0.05), resume_later()), 10)
    autonomous.set_paused(True)
    asyncio.run(go())
    assert runs == [1] and not autonomous.is_paused()


def test_seed_admits_at_top_priority(env):
    SITES["a.localhost"] = {"/": page(a("https://codigix.co/", "C"))}
    assert asyncio.run(autonomous.seed([env("a")])) == {env("a"): True}
    d = rows("SELECT discovery_status, priority FROM discovered_domains WHERE domain='a.localhost'")[0]
    assert (d["discovery_status"], d["priority"]) == ("admitted", 0)
    cycle(autonomous.Engine(acfg()), 1)
    assert bl("codigix.co")["total_backlinks"] == 1


# seeded domains (priority 0) get their per_batch slots first, capped by seed_batch; everyone else still gets the rest
def test_seeded_domains_claimed_first_without_starving_others(env):
    cfg = autonomous.AutoConfig(claim_batch=20, per_batch=10, seed_batch=10, domain_page_budget=100)
    with core.connect() as c:
        for d, prio in [("s1.localhost", 0), ("s2.localhost", 0)] + [(f"n{i}.localhost", 1) for i in range(15)]:
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_status, priority) VALUES (?, 'admitted', ?)", (d, prio))
            for i in range(10):
                core.save_url(c, f"http://{d}/{i}", None, "t", 1, True)
    got = [u for u, _ in autonomous.claim_urls("w", cfg)]
    seeded = [u for u in got if "//s" in u]
    assert len(got) == 20 and len(set(got)) == 20
    assert len(seeded) == 10  # seed_batch caps the reservation, shared round-robin between seeds
    assert len({u.split("/")[2] for u in got if "//n" in u}) == 10  # the rest is round-robin over normal domains


# POST /crawler/seed only leaves flags; the engine's scheduler must turn them into priority-0 admitted domains
def test_api_seed_flags_are_admitted_at_top_priority(env):
    import autonomous_api
    SITES["a.localhost"] = {"/": page("hi")}
    autonomous_api.seed(autonomous_api.SeedRequest(urls=[env("a")]))
    eng = autonomous.Engine(acfg())
    asyncio.run(eng.scheduler())
    d = rows("SELECT * FROM discovered_domains WHERE domain='a.localhost'")[0]
    assert d["priority"] == 0 and d["discovery_status"] == "admitted"
    assert rows("SELECT COUNT(*) n FROM engine_state WHERE key LIKE 'seed:%'")[0]["n"] == 0  # consumed

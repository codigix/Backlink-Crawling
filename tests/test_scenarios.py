"""Scenario tests for the hardening work: large sites, HTTP failures, SSRF on every hop, JavaScript rendering, Common Crawl
paging, backlink history, concurrent persistence, API consistency. Real PostgreSQL (isolated schema per test), local test server."""

import asyncio
import json
import socket
import threading

import httpx
import pytest
from bs4 import BeautifulSoup

import autonomous
import crawler_api
import crawler_core as core
import crawler_engine as engine
from test_crawler import HITS, SITES, a, cfg, env, page, port, rows  # noqa: F401 - fixtures


def crawl(c, *urls, **kw):
    asyncio.run(engine.run_crawl(c, list(urls), **kw))


def status(url):
    return rows("SELECT crawl_status, skip_reason, retry_count, crawl_error, http_status FROM discovered_urls WHERE url=?", url)[0]


# ---- 1/2: multi-page and 100+ URL sites ----
def test_site_with_150_internal_urls_is_fully_crawled_once(env):
    u = env
    SITES["big.localhost"] = {"/": page(*[a(f"/p{i}") for i in range(150)]), **{f"/p{i}": page(a(f"/p{(i + 1) % 150}")) for i in range(150)}}
    crawl(cfg(max_depth=3, concurrency=10), u("big"))
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == 151
    assert rows("SELECT source_url FROM crawl_history GROUP BY 1 HAVING COUNT(*)>1") == []  # no page fetched twice
    assert rows("SELECT pages_discovered p FROM discovered_domains WHERE domain='big.localhost'")[0]["p"] == 151


# ---- 8-12: HTTP failures ----
def test_http_errors_and_timeout_are_recorded_without_false_success(env):
    u = env
    SITES["e.localhost"] = {"/": page(a("/e403"), a("/e404"), a("/e429"), a("/e500"), a("/slow")),
                            "/e403": (403, "text/plain", "no"), "/e404": (404, "text/plain", "no"),
                            "/e429": (429, "text/plain", "slow down"), "/e500": (500, "text/plain", "boom"),
                            "/slow": (200, "sleep:4", "late")}
    crawl(cfg(max_depth=2, max_retries=2, timeout=1, rate_limit_base=0.3, rate_limit_stall=4), u("e"))
    for path, permanent in (("/e403", True), ("/e404", True)):
        st = status(u("e", path))
        assert st["crawl_status"] == "failed" and st["http_status"] == int(path[2:]) and HITS[("e.localhost", path)] == 1  # no retry, no bypass
    for path in ("/e429", "/e500", "/slow"):
        st = status(u("e", path))
        assert st["crawl_status"] in ("retry_pending", "failed") and st["crawl_error"], path  # recorded, never 'completed'
        assert HITS[("e.localhost", path)] >= 2, path  # retried by Crawlee
    assert status(u("e", "/e429"))["http_status"] == 429 and status(u("e", "/e500"))["http_status"] == 500
    assert status(u("e", "/"))["crawl_status"] == "completed"


# ---- 13: SSRF: validated before every hop ----
class FakeHeaders(dict):
    def get_list(self, k):
        return []


class FakeResp:
    def __init__(self, code, headers, url, body=b"x"):
        self.status_code, self.headers, self.url, self._body, self.is_closed = code, FakeHeaders(headers), url, body, False

    async def aiter_bytes(self):
        for i in range(0, len(self._body), 1000):
            yield self._body[i:i + 1000]

    def close(self):
        self.is_closed = True


class FakeInner:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    async def request(self, *, url, **kw):
        self.calls.append(url)
        return self.routes[url]


def guarded_client(monkeypatch, routes, dns, max_bytes=1000):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    monkeypatch.setattr(socket, "getaddrinfo", lambda host, *a, **k: [(2, 1, 6, "", (dns[host].pop(0) if len(dns[host]) > 1 else dns[host][0], 0))])
    inner = FakeInner(routes)
    client = engine.SafeImpitClient(max_response_bytes=max_bytes, browser=None)
    client._guards[id(inner)] = engine._Guarded(inner, max_bytes)
    monkeypatch.setattr(engine.ImpitHttpClient, "_get_client", lambda self, proxy_url: inner)
    return client, inner


@pytest.mark.parametrize("target", ["http://evil.test/admin", "http://169.254.169.254/latest/meta-data", "http://[::ffff:10.0.0.1]/", "http://100.64.0.1/"])
def test_redirect_to_internal_address_is_blocked_before_it_is_fetched(monkeypatch, target):
    routes = {"http://good.test/": FakeResp(302, {"location": target}, "http://good.test/")}
    client, inner = guarded_client(monkeypatch, routes, {"good.test": ["93.184.216.34"], "evil.test": ["10.0.0.5"],
                                                         "169.254.169.254": ["169.254.169.254"], "::ffff:10.0.0.1": ["::ffff:10.0.0.1"], "100.64.0.1": ["100.64.0.1"]})
    with pytest.raises(core.UnsafeURL):
        asyncio.run(client.send_request("http://good.test/"))
    assert inner.calls == ["http://good.test/"]  # the internal destination was never requested


def test_dns_rebinding_second_lookup_is_checked_again(monkeypatch):
    routes = {"http://rebind.test/a": FakeResp(302, {"location": "http://rebind.test/b"}, "http://rebind.test/a"),
              "http://rebind.test/b": FakeResp(200, {}, "http://rebind.test/b")}
    client, inner = guarded_client(monkeypatch, routes, {"rebind.test": ["93.184.216.34", "127.0.0.1"]})  # public first, loopback on the next lookup
    with pytest.raises(core.UnsafeURL):
        asyncio.run(client.send_request("http://rebind.test/a"))
    assert inner.calls == ["http://rebind.test/a"]


def test_safe_redirect_is_followed_and_body_size_is_capped(monkeypatch):
    routes = {"http://good.test/": FakeResp(302, {"location": "http://other.test/p"}, "http://good.test/"),
              "http://other.test/p": FakeResp(200, {}, "http://other.test/p", body=b"y" * 500),
              "http://big.test/": FakeResp(200, {}, "http://big.test/", body=b"z" * 5000)}
    client, inner = guarded_client(monkeypatch, routes, {"good.test": ["93.184.216.34"], "other.test": ["93.184.216.35"], "big.test": ["93.184.216.36"]})
    assert asyncio.run(client.send_request("http://good.test/")).headers is not None and len(inner.calls) == 2
    with pytest.raises(engine.ResponseTooLarge):  # cut off while downloading, not after
        asyncio.run(client.send_request("http://big.test/"))


def test_crawl_round_refuses_unsafe_seed_and_records_reason(env, monkeypatch, port):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    url = f"http://127.0.0.1:{port}/"
    with core.connect() as c:
        core.save_url(c, url, None, "seed", 0, True)
    asyncio.run(engine.crawl_round(cfg(), None, [(url, 0)], set(), False, 1))
    st = status(url)
    assert st["crawl_status"] == "failed" and "blocked before fetch" in st["crawl_error"]
    assert not HITS  # nothing reached the server


def test_unsafe_domain_is_never_admitted(env, monkeypatch):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    assert asyncio.run(engine.admit_domain(cfg(), None, "localhost", "http://localhost:80/")) is False
    assert rows("SELECT discovery_status s FROM discovered_domains WHERE domain='localhost'")[0]["s"] == "blocked"


# ---- JavaScript rendering through the same pipeline ----
def test_js_rendered_backlink_is_found_via_playwright_and_persisted(env):
    u = env
    shell = ('<html><body><div id="root"></div><script>1</script><script>2</script><script>3</script><script>'
             'document.getElementById("root").innerHTML = \'<a href="https://codigix.co/js">JS link</a>\';</script></body></html>')
    SITES["js.localhost"] = {"/": (200, "text/html", shell)}
    crawl(cfg(max_depth=1, target_domain="codigix.co"), u("js"))
    assert [(r["link_status"], r["anchor_text"]) for r in rows("SELECT * FROM backlinks")] == [("verified", "JS link")]
    assert status(u("js"))["crawl_status"] == "completed" and rows("SELECT COUNT(*) n FROM crawl_history")[0]["n"] == 1  # persisted once


def test_static_pages_never_launch_a_browser(env, monkeypatch):
    u = env
    SITES["s.localhost"] = {"/": page(a("/1"), a("/2"), a("/3"), a("https://codigix.co/")), "/1": page(), "/2": page(), "/3": page()}
    called = []
    monkeypatch.setattr(engine, "PlaywrightCrawler", lambda *a, **k: called.append(1))
    crawl(cfg(max_depth=2), u("s"))
    assert not called and rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == 4


def test_needs_js_heuristic():
    soup = lambda h: BeautifulSoup(h, "html.parser")  # noqa: E731
    assert engine.needs_js(soup('<body><div id="app"></div></body>'))
    assert engine.needs_js(soup("<body><noscript>Please enable JavaScript</noscript></body>"))
    assert not engine.needs_js(soup("<body><a href='/a'>1</a><a href='/b'>2</a><a href='/c'>3</a></body>"))
    assert not engine.needs_js(soup("<body><p>" + "text " * 200 + "</p></body>"))


# ---- 21: Common Crawl paging, index id, rediscovery ----
def cc_transport(pages=3, index="CC-MAIN-2026-01", log=None):
    def handler(req: httpx.Request) -> httpx.Response:
        q = dict(req.url.params)
        if str(req.url).endswith("collinfo.json"):
            return httpx.Response(200, json=[{"id": index, "cdx-api": "https://cc.test/idx"}])
        log is not None and log.append(q)
        if "showNumPages" in q:
            return httpx.Response(200, text=json.dumps({"pages": pages, "pageSize": 5, "blocks": 9}))
        return httpx.Response(200, text="".join(json.dumps({"url": f"https://site.org/p{q['page']}-{i}"}) + "\n" for i in range(2)))
    return httpx.MockTransport(handler)


def run_cc(transport, cfg_):
    async def go():
        async with httpx.AsyncClient(transport=transport) as cl:
            return await autonomous.run_source("commoncrawl", cfg_, cl, force=True)
    return asyncio.run(go())


def test_commoncrawl_pages_resume_complete_and_rediscover_on_new_index(env):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES ('site.org','crawled')")  # retired domains are included
    cfg_ = autonomous.AutoConfig(sources=("commoncrawl",), cc_delay=0, cc_pages_per_run=2)
    log = []
    assert run_cc(cc_transport(log=log), cfg_) == 4  # probe + pages 0,1
    job = rows("SELECT * FROM discovery_jobs")[0]
    assert (job["status"], job["page_cursor"], job["total_pages"], job["index_id"]) == ("pending", 2, 3, "CC-MAIN-2026-01")
    assert run_cc(cc_transport(log=log), cfg_) == 2  # page 2 -> done
    job = rows("SELECT * FROM discovery_jobs")[0]
    assert (job["status"], job["page_cursor"]) == ("completed", 3) and job["last_error"] is None
    assert [q["page"] for q in log if "page" in q] == ["0", "1", "2"]  # each page fetched exactly once
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE discovery_method='commoncrawl' AND crawl_status='pending'")[0]["n"] == 6
    assert rows("SELECT COUNT(*) n FROM backlinks")[0]["n"] == 0 and rows("SELECT COUNT(*) n FROM outgoing_links")[0]["n"] == 0  # URLs only, never backlinks
    assert run_cc(cc_transport(), cfg_) == 0  # same index: nothing to do (and no duplicate job)
    assert len(rows("SELECT * FROM discovery_jobs")) == 1
    cfg_._cc_api = (0, None, None)  # cache expired; a newer index is published
    run_cc(cc_transport(index="CC-MAIN-2026-05"), cfg_)  # job reset to pending ...
    assert run_cc(cc_transport(index="CC-MAIN-2026-05"), cfg_) >= 0  # ... and worked again
    assert rows("SELECT index_id FROM discovery_jobs")[0]["index_id"] == "CC-MAIN-2026-05"


def test_commoncrawl_failed_job_gets_a_fresh_start_later_and_keeps_its_error(env):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES ('site.org','admitted')")
        c.execute("INSERT INTO discovery_jobs (domain_id, source, status, attempts, last_error, updated_at) VALUES (1,'commoncrawl','failed',9,'HTTP 500','2000-01-01 00:00:00')")
    run_cc(cc_transport(pages=1), autonomous.AutoConfig(sources=("commoncrawl",), cc_delay=0))
    job = rows("SELECT * FROM discovery_jobs")[0]
    assert job["status"] == "completed" and job["attempts"] >= 1


def test_admission_creates_the_commoncrawl_job(env):
    assert asyncio.run(engine.admit_domain(cfg(enable_sitemap_discovery=False), None, "a.localhost", "http://a.localhost/"))
    assert rows("SELECT source, status FROM discovery_jobs") == [{"source": "commoncrawl", "status": "pending"}]
    asyncio.run(engine.admit_domain(cfg(enable_sitemap_discovery=False), None, "a.localhost", "http://a.localhost/"))
    assert len(rows("SELECT * FROM discovery_jobs")) == 1  # no duplicate job on re-admission


# ---- 15-17: repeated removal / reappearance keeps every event ----
def test_repeated_loss_and_return_are_all_kept_in_history(env):
    c = engine.Config()
    html = lambda h: BeautifulSoup(h, "html.parser")  # noqa: E731

    def crawl_once(h):
        with core.connect() as conn:
            engine.process_page(conn, "http://x.org/a", "http://x.org/a", 0, html(h), 200, "text/html", c, set(), False, core.now())
    link = '<a href="https://codigix.co/">c</a>'
    for _ in range(2):  # two full cycles: seen, missed, missed (removed), seen again
        crawl_once(link)
        crawl_once("<p/>")
        crawl_once("<p/>")
    crawl_once(link)
    ev = [e["event"] for e in crawler_api.backlink_history("codigix.co", limit=200)["events"]]
    assert sorted(ev) == sorted(["new", "suspected_removed", "lost", "reappeared", "suspected_removed", "lost", "reappeared"])  # nothing erased
    bl = crawler_api.backlinks("codigix.co", limit=100, offset=0)
    assert bl["total_backlinks"] == 1 and bl["backlinks"][0]["link_status"] == "verified" and bl["verified_backlinks"] == 1


# ---- 26: API for root domains and subdomains ----
def test_api_root_domain_subdomain_pagination_and_statuses(env):
    c = engine.Config()
    html = lambda h: BeautifulSoup(h, "html.parser")  # noqa: E731
    with core.connect() as conn:
        for i in range(5):
            core.save_url(conn, f"http://src{i}.org/p", None, "t", 0, True)
            engine.process_page(conn, f"http://src{i}.org/p", f"http://src{i}.org/p", 0, html(
                '<a href="https://codigix.co/">root</a><a href="https://blog.codigix.co/post">sub</a><a href="https://evilcodigix.co/">no</a>'),
                200, "text/html", c, set(), False, core.now())
        engine.process_page(conn, "http://codigix.co/self", "http://codigix.co/self", 0, html('<a href="https://codigix.co/x">self</a>'),
                            200, "text/html", c, set(), False, core.now())
    root = crawler_api.backlinks("codigix.co", limit=3, offset=0)
    assert root["total_backlinks"] == 10 and len(root["backlinks"]) == 3 and root["referring_domains"] == 5  # self-links excluded, evilcodigix.co excluded
    assert len(crawler_api.backlinks("codigix.co", limit=3, offset=9)["backlinks"]) == 1
    sub = crawler_api.backlinks("blog.codigix.co", limit=100, offset=0)
    assert sub["total_backlinks"] == 5 and {b["destination_url"] for b in sub["backlinks"]} == {"https://blog.codigix.co/post"}
    assert crawler_api.backlinks("evilcodigix.co", limit=100, offset=0)["total_backlinks"] == 5
    assert sum(x["count"] for x in crawler_api.backlink_anchors("codigix.co")["anchors"]) == 10
    assert crawler_api.referring_domains("codigix.co")["total_referring_domains"] == 5
    with core.connect() as conn:  # a failed source fetch must not be reported as verified; a suspected link neither
        conn.execute("UPDATE discovered_urls SET crawl_status='failed' WHERE url='http://src0.org/p'")
        conn.execute("UPDATE outgoing_links SET lost_at='2026-01-01 00:00:00' WHERE source_url='http://src1.org/p'")
    r = crawler_api.backlinks("codigix.co", limit=100, offset=0)
    states = [b["link_status"] for b in r["backlinks"]]
    assert states.count("fetch_failed") == 2 and states.count("suspected_removed") == 2 and states.count("verified") == 6
    assert r["verified_backlinks"] == 6 and r["total_backlinks"] == 10


# ---- concurrent persistence: same and different domains, overlapping links ----
def test_concurrent_page_persistence_is_consistent_and_deadlock_free(env):
    c = engine.Config()
    shared = "".join(f'<a href="https://shared{i}.org/page{j}">l{i}-{j}</a>' for i in range(6) for j in range(8))
    jobs = [(f"http://site{d}.org/p{n}", f"<html>{shared}<a href='/p{n + 1}'>n</a><a href='https://site{(d + 1) % 4}.org/p0'>x</a></html>")
            for d in range(4) for n in range(8)]
    with core.connect() as conn:
        core.add_target(conn, "shared3.org")
        for url, _ in jobs:
            core.save_url(conn, url, None, "t", 0, True)
    errors = []

    def work(chunk):
        for url, h in chunk:
            try:
                def once():
                    with core.connect() as conn:
                        engine.process_page(conn, url, url, 0, BeautifulSoup(h, "html.parser"), 200, "text/html", c, {"site0.org"}, True, core.now())
                core.retry_locked(once)
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
    threads = [threading.Thread(target=work, args=(jobs[i::8],)) for i in range(8)]
    [t.start() for t in threads]
    [t.join(120) for t in threads]
    assert not errors, errors[:3]
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == len(jobs)
    assert rows("SELECT COUNT(*) n FROM crawl_history")[0]["n"] == len(jobs)  # every page persisted exactly once
    # no lost updates: the counter equals the real number of rows per domain
    assert rows("SELECT d.domain FROM discovered_domains d WHERE d.pages_discovered != (SELECT COUNT(*) FROM discovered_urls u WHERE u.domain=d.domain)") == []
    assert rows("SELECT source_url, destination_url, anchor_text FROM outgoing_links GROUP BY 1,2,3 HAVING COUNT(*)>1") == []
    assert rows("SELECT COUNT(*) n FROM backlinks")[0]["n"] == len(jobs) * 8  # every page links to shared3.org/page0..7
    assert rows("SELECT COUNT(*) n FROM backlinks WHERE link_status!='verified'")[0]["n"] == 0

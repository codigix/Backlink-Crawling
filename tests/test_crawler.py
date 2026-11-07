"""End-to-end tests against controlled local websites (a.localhost, b.localhost, ... served by one local server).

Run: .venv/Scripts/python -m pytest tests -q
"""

import asyncio
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import pytest

import crawler_core as core
import crawler_engine as engine

SITES: dict[str, dict[str, tuple[int, str, str]]] = {}  # host -> path -> (status, content_type, body)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        status, ctype, body = SITES.get(host, {}).get(self.path, (404, "text/plain", "nope"))
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *a):
        pass


def page(*links: str) -> tuple[int, str, str]:
    return 200, "text/html", "<html><body>" + "".join(links) + "</body></html>"


@pytest.fixture(scope="module")
def port():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


@pytest.fixture()
def env(port, monkeypatch):
    """Each test gets its own PostgreSQL schema in the database at DATABASE_URL (dropped afterwards)."""
    schema = "t_" + uuid.uuid4().hex[:12]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        monkeypatch.setattr(core, "DB_SCHEMA", schema)
        monkeypatch.setattr(core, "ALLOW_PRIVATE", True)
        SITES.clear()
        with core.connect() as c:
            core.migrate(c)
        yield lambda host, path="/": f"http://{host}.localhost:{port}{path}"
        admin.execute(f"DROP SCHEMA {schema} CASCADE")


def cfg(**kw) -> engine.Config:
    return engine.Config(**{"domain_delay": 0, "timeout": 10, "max_retries": 1, **kw})


def rows(sql, *args):
    with core.connect() as c:
        return [dict(r) for r in c.execute(sql, args)]


def a(href, text="x", rel=""):
    return f'<a href="{href}"{f" rel={rel!r}" if rel else ""}>{text}</a>'


# 1. basic extraction: every URL stored, including unrelated external ones
def test_extraction_stores_everything(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("/about/"), a(u("b")), a(u("c", "/x?id=1#frag")), a("https://d.example.org/z"),
                                      a("mailto:x@y.z"), '<link rel="canonical" href="/canon">')}
    asyncio.run(engine.run_crawl(cfg(max_depth=0), [u("a")]))
    urls = {r["url"] for r in rows("SELECT url FROM discovered_urls")}
    assert {u("a", "/about/"), u("b"), u("c", "/x?id=1#frag"), "https://d.example.org/z", u("a", "/canon")} <= urls
    assert not any(x.startswith("mailto") for x in urls)
    out = rows("SELECT * FROM outgoing_links WHERE source_url=?", u("a"))
    assert len(out) == 5 and {r["link_type"] for r in out} == {"html", "canonical"}
    assert u("c", "/x?id=1#frag") in {r["destination_url"] for r in out}  # query and fragment kept
    assert {"b.localhost", "c.localhost", "example.org"} <= {r["domain"] for r in rows("SELECT domain FROM discovered_domains")}
    # depth 0: external and internal links are stored but not crawled
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", u("a", "/about/"))[0]["crawl_status"] == "pending"
    assert rows("SELECT COUNT(*) n FROM crawl_history")[0]["n"] == 1


# 2. recursion
def test_recursive_internal_crawl(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("/page2")), "/page2": page(a("/page3")), "/page3": page("end"),
                            "/f.pdf": (200, "application/pdf", "%PDF")}
    SITES["a.localhost"]["/page3"] = page(a("/f.pdf"), a("/logout"))
    asyncio.run(engine.run_crawl(cfg(), [u("a")]))
    st = {r["url"]: r["crawl_status"] for r in rows("SELECT url, crawl_status FROM discovered_urls")}
    assert [st[u("a", p)] for p in ("/", "/page2", "/page3")] == ["completed"] * 3
    assert st[u("a", "/f.pdf")] == "skipped" and st[u("a", "/logout")] == "skipped"


# 3. external domain discovery with controlled admission
def test_external_domain_admission(env):
    u = env
    SITES["a.localhost"] = {"/": page(a(u("b")))}
    SITES["b.localhost"] = {"/": page(a(u("c")))}
    SITES["c.localhost"] = {"/": page("leaf")}
    asyncio.run(engine.run_crawl(cfg(enable_external_domain_discovery=False), [u("a")]))
    assert {r["domain"] for r in rows("SELECT domain FROM discovered_domains")} == {"a.localhost", "b.localhost"}
    assert rows("SELECT crawl_status FROM discovered_urls WHERE domain='b.localhost'")[0]["crawl_status"] == "pending"
    asyncio.run(engine.run_crawl(cfg(enable_external_domain_discovery=True), []))  # resumes from SQLite
    d = {r["domain"]: r["discovery_status"] for r in rows("SELECT domain, discovery_status FROM discovered_domains")}
    assert d == {"a.localhost": "crawled", "b.localhost": "crawled", "c.localhost": "crawled"}
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed'")[0]["n"] == 3


def test_domain_cap(env):
    u = env
    SITES["a.localhost"] = {"/": page(a(u("b")), a(u("c")))}
    SITES["b.localhost"] = SITES["c.localhost"] = {"/": page("x")}
    asyncio.run(engine.run_crawl(cfg(enable_external_domain_discovery=True, max_domains=2), [u("a")]))
    assert len([r for r in rows("SELECT * FROM discovered_domains WHERE discovery_status IN ('admitted','crawled')")]) == 2


# 4. target-independent matching
def test_backlink_matching(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("https://www.codigix.co/services/", "Codigix", "nofollow"), a("https://codigix.co.fake.com/"),
                                      a("https://fakecodigix.co/"), a("https://other.com/?url=codigix.co"), a("https://sub.codigix.co/"))}
    SITES["b.localhost"] = {"/": page(a("https://example.com/p", "Ex"))}
    asyncio.run(engine.run_crawl(cfg(target_domain="codigix.co", max_depth=0), [u("a"), u("b")]))
    got = {r["destination_url"]: r for r in rows("SELECT * FROM backlinks")}
    assert set(got) == {"https://www.codigix.co/services/", "https://sub.codigix.co/"}
    assert got["https://www.codigix.co/services/"]["rel_attributes"] == "nofollow"
    assert got["https://www.codigix.co/services/"]["link_status"] == "verified"
    # a second target is answered from the stored graph, no new crawl
    with core.connect() as c:
        core.match_target_from_graph(c, "example.com")
    ex = rows("SELECT b.destination_url FROM backlinks b JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain='example.com'")
    assert [r["destination_url"] for r in ex] == ["https://example.com/p"]


def test_matching_helpers():
    assert core.host_matches("sub.codigix.co", "codigix.co") and core.host_matches("codigix.co", "codigix.co")
    assert not core.host_matches("codigix.co.fake.com", "codigix.co") and not core.host_matches("fakecodigix.co", "codigix.co")
    assert core.registrable_domain("a.b.example.co.uk") == "example.co.uk"
    assert core.clean_target("https://www.Codigix.co/x") == "codigix.co"
    with pytest.raises(ValueError):
        core.clean_target("not a domain")


def test_normalization():
    n = core.normalize_url
    assert n("HTTP://Example.COM:80/a?b=1&a=2#frag") == "http://example.com/a?b=1&a=2"
    assert n("https://example.com") == "https://example.com/"
    assert n("https://example.com:8443/") == "https://example.com:8443/"
    assert n("ftp://x.com") is None and n("javascript:void(0)") is None
    assert core.resolve("https://example.com/blog/article/", "/about/") == "https://example.com/about/"


def test_ssrf_guard(monkeypatch):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    for bad in ("http://127.0.0.1/", "http://localhost/", "http://10.0.0.5/", "http://169.254.169.254/latest",
                "http://a.localhost/", "file:///etc/passwd", "http://user:pw@example.com/"):
        assert not core.is_safe_url(bad), bad


def test_duplicate_prevention(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("/p"), a("/p#top"), a(u("a", "/p").replace("http://a", "HTTP://A")), a("/p"))}
    asyncio.run(engine.run_crawl(cfg(max_depth=0), [u("a")]))
    assert len(rows("SELECT * FROM discovered_urls WHERE normalized_url LIKE '%/p'")) == 1
    asyncio.run(engine.run_crawl(cfg(max_depth=0), [u("a")]))  # same crawl again: nothing duplicates
    assert len(rows("SELECT * FROM discovered_urls")) == 2


# 5. continuous recrawl: add, remove, failure must not remove
def test_recrawl_add_remove(env):
    u = env
    src = u("a", "/blog")
    SITES["a.localhost"] = {"/": page(a("/blog")), "/blog": page("nothing")}
    asyncio.run(engine.run_crawl(cfg(target_domain="codigix.co"), [u("a")]))
    assert rows("SELECT * FROM backlinks") == []

    SITES["a.localhost"]["/blog"] = page(a("https://codigix.co/", "Codigix"))
    asyncio.run(engine.recrawl_due(cfg(), urls=[src]))
    assert [r["link_status"] for r in rows("SELECT * FROM backlinks")] == ["verified"]

    SITES["a.localhost"]["/blog"] = (500, "text/plain", "boom")  # failed fetch is not proof of removal
    asyncio.run(engine.recrawl_due(cfg(), urls=[src]))
    b = rows("SELECT * FROM backlinks")[0]
    assert b["is_active"] == 1 and b["link_status"] == "fetch_failed"  # unverifiable, but NOT removed
    u_row = rows("SELECT * FROM discovered_urls WHERE url=?", src)[0]
    assert u_row["crawl_status"] in ("failed", "retry_pending") and u_row["crawl_error"] and u_row["next_crawl_at"]

    SITES["a.localhost"]["/blog"] = page("link removed")
    asyncio.run(engine.recrawl_due(cfg(), urls=[src]))
    b = rows("SELECT * FROM backlinks")[0]
    assert b["is_active"] == 1 and b["link_status"] == "suspected_removed"  # one miss is only a suspicion
    asyncio.run(engine.recrawl_due(cfg(), urls=[src]))
    b = rows("SELECT * FROM backlinks")[0]
    assert b["is_active"] == 0 and b["link_status"] == "removed"
    assert rows("SELECT is_active FROM outgoing_links WHERE source_url=? AND destination_url='https://codigix.co/'", src)[0]["is_active"] == 0

    SITES["a.localhost"]["/blog"] = page(a("https://codigix.co/", "Codigix"))
    asyncio.run(engine.recrawl_due(cfg(), urls=[src]))
    events = [r["event_type"] for r in rows("SELECT event_type FROM backlink_history ORDER BY id")]
    assert events == ["first_discovered", "verified", "suspected_removed", "confirmed_removed", "reappeared"]
    assert rows("SELECT content_hash, last_successful_crawl FROM discovered_urls WHERE url=?", src)[0]["content_hash"]
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", src)[0]["n"] == 6


def test_scheduler_picks_due_pages(env):
    u = env
    SITES["a.localhost"] = {"/": page("hi")}
    asyncio.run(engine.run_crawl(cfg(), [u("a")]))
    assert engine.due_urls() == []
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET next_crawl_at='2000-01-01 00:00:00'")
    assert [x[0] for x in engine.due_urls()] == [u("a")]
    assert asyncio.run(engine.recrawl_due(cfg())) == 1
    assert engine.due_urls() == []


# 6. restart recovery
def test_restart_recovery(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("/p2"), a("/p3")), "/p2": page("2"), "/p3": page("3")}
    asyncio.run(engine.run_crawl(cfg(max_pages=1), [u("a")]))  # "process dies" after one page
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET crawl_status='crawling' WHERE url=?", (u("a", "/p2"),))  # mid-flight at crash
        c.execute("INSERT INTO crawl_jobs (job_id, status) VALUES ('j','running')")
    before = rows("SELECT url, crawl_status FROM discovered_urls")
    assert {r["crawl_status"] for r in before} == {"completed", "crawling", "pending"}
    with core.connect() as c:
        core.migrate(c)  # application restart
    assert rows("SELECT status FROM crawl_jobs")[0]["status"] == "interrupted"
    assert rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"] == 3  # nothing lost
    asyncio.run(engine.run_crawl(cfg(), []))
    assert {r["crawl_status"] for r in rows("SELECT crawl_status FROM discovered_urls")} == {"completed"}


def test_failed_request_handling(env):
    u = env
    SITES["a.localhost"] = {"/": page(a("/bad")), "/bad": (500, "text/plain", "x")}
    asyncio.run(engine.run_crawl(cfg(max_retries=2), [u("a")]))
    r = rows("SELECT * FROM discovered_urls WHERE url=?", u("a", "/bad"))[0]
    assert r["crawl_status"] in ("retry_pending", "failed") and r["retry_count"] >= 1 and r["crawl_error"]
    assert rows("SELECT * FROM crawl_history WHERE source_url=? AND error_message IS NOT NULL", u("a", "/bad"))


def test_sitemap_discovery(env):
    u = env
    SITES["a.localhost"] = {
        "/": page("home"),
        "/robots.txt": (200, "text/plain", f"User-agent: *\nSitemap: {u('a', '/maps/index.xml')}\n"),
        "/maps/index.xml": (200, "application/xml", f"<sitemapindex xmlns='x'><sitemap><loc>{u('a', '/maps/posts.xml')}</loc></sitemap></sitemapindex>"),
        "/maps/posts.xml": (200, "application/xml", f"<urlset xmlns='x'><url><loc>{u('a', '/post-1')}</loc><lastmod>2024-05-01</lastmod></url></urlset>"),
        "/post-1": page("post"),
    }
    asyncio.run(engine.run_crawl(cfg(max_depth=1), [u("a")]))
    r = rows("SELECT * FROM discovered_urls WHERE url=?", u("a", "/post-1"))[0]
    assert r["discovery_method"] == "sitemap" and r["sitemap_lastmod"] == "2024-05-01" and r["crawl_status"] == "completed"


def test_migration_is_idempotent(env):
    with core.connect() as c:
        core.migrate(c)
        core.migrate(c)
        assert c.execute("SELECT COUNT(*) FROM discovered_urls").fetchone()[0] == 0


def test_idna_and_user_agent_constant():
    assert core.normalize_url("https://Bücher.de/x") == "https://xn--bcher-kva.de/x"
    assert core.clean_target("bücher.de") == "xn--bcher-kva.de"
    assert core.host_matches(core.hostname("https://shop.bücher.de/"), core.clean_target("bücher.de"))
    assert "BacklinkGraphBot" in core.USER_AGENT


def test_user_agent_is_sent(env):
    u = env
    seen = []
    orig = Handler.do_GET

    def spy(self):
        seen.append(self.headers.get("User-Agent"))
        orig(self)

    Handler.do_GET = spy
    try:
        SITES["a.localhost"] = {"/": page("hi")}
        asyncio.run(engine.run_crawl(cfg(max_depth=0, enable_sitemap_discovery=False), [u("a")]))
    finally:
        Handler.do_GET = orig
    assert seen and all("BacklinkGraphBot" in (x or "") for x in seen), seen


def test_client_errors_are_permanent_not_retried(env):
    u = env
    hits = []
    orig = Handler.do_GET

    def spy(self):
        hits.append(self.path)
        orig(self)

    Handler.do_GET = spy
    try:
        SITES["a.localhost"] = {"/": page(a("/gone"), a("/cdn-cgi/l/email-protection")), "/gone": (404, "text/html", "<html>nope</html>")}
        asyncio.run(engine.run_crawl(cfg(max_retries=3), [u("a")]))
    finally:
        Handler.do_GET = orig
    assert hits.count("/gone") == 1 and "/cdn-cgi/l/email-protection" not in hits  # no retries, no Cloudflare link
    r = rows("SELECT * FROM discovered_urls WHERE url=?", u("a", "/gone"))[0]
    assert r["crawl_status"] == "failed" and r["http_status"] == 404
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE source_url=?", u("a", "/gone"))[0]["n"] == 0


def test_rich_extraction_and_placements(env):
    u = env
    SITES["a.localhost"] = {"/": (200, "text/html", "<html><head><title>Hello</title></head><body>"
        "<nav>" + a("https://codigix.co/n", "navlink") + "</nav><p>Read " + a("https://codigix.co/", "first", "nofollow sponsored")
        + " and " + a("https://codigix.co/", "second") + "</p>"
        + '<a href="https://codigix.co/img"><img src="i.png" alt="logo"></a>'
        + a("https://codigix.co/ugc", "u", "UGC") + "</body></html>")}
    asyncio.run(engine.run_crawl(cfg(target_domain="codigix.co", max_depth=0), [u("a")]))
    by = {(r["destination_url"], r["anchor_text"]): r for r in rows("SELECT * FROM outgoing_links WHERE link_type='html'")}
    first = by[("https://codigix.co/", "first")]
    assert first["is_nofollow"] and first["is_sponsored"] and not first["is_ugc"] and first["position"] == "content"
    assert "Read" in first["context"]
    assert by[("https://codigix.co/n", "navlink")]["position"] == "nav"
    assert by[("https://codigix.co/img", "")]["img_alt"] == "logo"
    assert by[("https://codigix.co/ugc", "u")]["is_ugc"] == 1
    assert len(by) == 5  # both placements of the same destination are kept
    assert rows("SELECT title FROM discovered_urls WHERE url=?", u("a"))[0]["title"] == "Hello"
    # two placements, one backlink; re-crawling must not flip anchor history
    assert len(rows("SELECT * FROM backlinks WHERE destination_url='https://codigix.co/'")) == 1
    asyncio.run(engine.recrawl_due(cfg(), urls=[u("a")]))
    assert "link_changed" not in [r["event_type"] for r in rows("SELECT event_type FROM backlink_history")]


def test_worker_process_entrypoint(env):
    import crawler_worker
    SITES["a.localhost"] = {"/": page(a("https://codigix.co/", "Codigix"))}
    assert crawler_worker.main(["crawl", "--target", "codigix.co", "--seed", env("a"), "--max-depth", "0"]) == 0
    assert rows("SELECT status FROM crawl_jobs")[0]["status"] == "finished"
    assert [r["link_status"] for r in rows("SELECT * FROM backlinks")] == ["verified"]

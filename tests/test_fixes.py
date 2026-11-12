"""Regression tests for the review fixes: domain reactivation, depth/budget skips, lease owners, link status, auth."""

import pytest
from fastapi import HTTPException

import autonomous
import crawler_api
import crawler_core as core
import crawler_engine as engine
from test_crawler import env, port, rows  # noqa: F401 - fixtures


def cfg(**kw):
    return autonomous.AutoConfig(**{"domain_page_budget": 100, **kw})


def domain(status, name="x.org"):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES (?,?)", (name, status))


def status_of(url):
    return rows("SELECT crawl_status, skip_reason FROM discovered_urls WHERE url=?", url)[0]


def test_crawled_domain_reactivated_by_new_url(env):
    domain("crawled")
    with core.connect() as c:
        core.save_url(c, "http://x.org/new", None, "t", 1, True)
    assert autonomous.reactivate_domains(cfg()) == 1
    assert rows("SELECT discovery_status s FROM discovered_domains")[0]["s"] == "discovered"


def test_blocked_domain_and_spent_budget_not_reactivated(env):
    domain("blocked", "b.org")
    domain("crawled", "c.org")
    with core.connect() as c:
        core.save_url(c, "http://b.org/p", None, "t", 1, True)
        core.save_url(c, "http://c.org/p", None, "t", 1, True)
        core.save_url(c, "http://c.org/done", None, "t", 1, True, "completed")
    assert autonomous.reactivate_domains(cfg(domain_page_budget=1)) == 0
    assert autonomous.retire_exhausted(cfg(domain_page_budget=1)) == 0  # nothing admitted; but the pending URL is skipped, not left pending
    assert status_of("http://c.org/p") == {"crawl_status": "skipped", "skip_reason": "domain_page_budget_exhausted"}


def test_too_deep_urls_skipped_with_reason(env):
    domain("admitted")
    with core.connect() as c:
        core.save_url(c, "http://x.org/deep", None, "t", 4, True)
        core.save_url(c, "http://x.org/ok", None, "t", 3, True)
    autonomous.retire_exhausted(cfg())  # default max_depth is 3
    assert status_of("http://x.org/deep") == {"crawl_status": "skipped", "skip_reason": "max_depth"}
    assert status_of("http://x.org/ok")["crawl_status"] == "pending"


def test_process_page_skips_depth_overflow(env):
    from bs4 import BeautifulSoup
    c = engine.Config(max_depth=3)
    with core.connect() as conn:
        engine.process_page(conn, "http://x.org/a", "http://x.org/a", 3, BeautifulSoup('<a href="/b">b</a><a href="https://y.org/">y</a>', "html.parser"),
                            200, "text/html", c, {"x.org"}, True, core.now())
    assert status_of("http://x.org/b") == {"crawl_status": "skipped", "skip_reason": "max_depth"}
    assert status_of("https://y.org/")["crawl_status"] == "pending"  # external links start at depth 0


def test_release_only_touches_own_owner(env):
    domain("admitted")
    with core.connect() as c:
        for i in range(2):
            core.save_url(c, f"http://x.org/{i}", None, "t", 1, True)
        c.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner='fwd', lease_expires=? WHERE url='http://x.org/0'", (core.later(seconds=600),))
        c.execute("UPDATE discovered_urls SET crawl_status='recrawling', lease_owner='ver', lease_expires=? WHERE url='http://x.org/1'", (core.later(seconds=600),))
    autonomous.release("fwd")
    assert status_of("http://x.org/0")["crawl_status"] == "pending"
    assert status_of("http://x.org/1")["crawl_status"] == "recrawling"
    e = autonomous.Engine()
    assert e.forward_owner != e.verify_owner


def test_renew_extends_only_own_leases(env):
    domain("admitted")
    with core.connect() as c:
        for i, o in enumerate(("a", "b")):
            core.save_url(c, f"http://x.org/{i}", None, "t", 1, True)
            c.execute("UPDATE discovered_urls SET crawl_status='crawling', lease_owner=?, lease_expires='2000-01-01 00:00:00' WHERE url=?", (o, f"http://x.org/{i}"))
    assert autonomous.renew("a", 600) == 1
    assert autonomous.release() == 1  # only b's lease is still expired


def test_link_status_states():
    r = {"is_active": 1, "lost_at": None, "src_status": "completed"}
    assert crawler_api.link_status(r) == "verified"
    assert crawler_api.link_status({**r, "src_status": "failed"}) == "fetch_failed"
    assert crawler_api.link_status({**r, "lost_at": "2026-01-01 00:00:00"}) == "suspected_removed"
    assert crawler_api.link_status({**r, "is_active": 0}) == "removed"


def test_migration_upgrades_an_old_schema_in_place(env):
    """An older database: tables exist without later columns, no version table, real rows. Upgrade keeps the rows."""
    with core.connect() as c:
        core.save_url(c, "http://x.org/keep", None, "t", 1, True)
        c.execute("DROP TABLE schema_migrations")
        c.execute("DROP TABLE link_history")
        c.execute("DROP INDEX idx_outgoing_dest_active")
        c.execute("ALTER TABLE discovered_urls DROP COLUMN skip_reason")
        c.execute("ALTER TABLE discovery_jobs DROP COLUMN index_id")
        assert core.schema_problems(c)
        core.migrate(c)
        assert core.schema_problems(c) == []
        assert [r["url"] for r in c.execute("SELECT url FROM discovered_urls")] == ["http://x.org/keep"]
        assert [r["version"] for r in c.execute("SELECT version FROM schema_migrations ORDER BY 1")] == [v for v, _, _ in core.MIGRATIONS]
        core.migrate(c)  # idempotent


def test_failed_migration_version_rolls_back_and_earlier_versions_stay(env, monkeypatch):
    bad = core.MIGRATIONS + [(99, "broken", ["CREATE TABLE t99 (x INT)", "THIS IS NOT SQL"])]
    monkeypatch.setattr(core, "MIGRATIONS", bad)
    with core.connect() as c:
        with pytest.raises(Exception):
            core.migrate(c)
    with core.connect() as c:
        assert c.execute("SELECT to_regclass('t99')").fetchone()[0] is None  # half-applied version left nothing behind
        assert c.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == core.SCHEMA_VERSION



def test_two_misses_needed_to_remove_link_and_backlink(env):
    from bs4 import BeautifulSoup
    c = engine.Config()
    page = lambda html: BeautifulSoup(html, "html.parser")  # noqa: E731

    def crawl(html):
        with core.connect() as conn:
            engine.process_page(conn, "http://x.org/a", "http://x.org/a", 0, page(html), 200, "text/html", c, set(), False, core.now())

    with core.connect() as conn:
        core.add_target(conn, "codigix.co")
    crawl('<a href="https://codigix.co/">c</a>')
    state = lambda: (rows("SELECT is_active, lost_at FROM outgoing_links WHERE destination_domain='codigix.co'")[0],  # noqa: E731
                     rows("SELECT is_active, link_status FROM backlinks")[0])
    assert state()[1]["link_status"] == "verified"
    crawl("<p>gone</p>")  # first successful miss: suspected in both tables, still active
    link, bl = state()
    assert link["is_active"] == 1 and link["lost_at"] and bl["link_status"] == "suspected_removed" and bl["is_active"] == 1
    crawl('<a href="https://codigix.co/">c</a>')  # reappears: back to verified, lost_at cleared
    link, bl = state()
    assert link["lost_at"] is None and bl["link_status"] == "verified"
    crawl("<p>gone</p>")
    crawl("<p>gone</p>")  # second consecutive miss confirms
    link, bl = state()
    assert link["is_active"] == 0 and bl["is_active"] == 0 and bl["link_status"] == "removed"


def test_idn_unicode_and_punycode_match():
    assert core.host_matches("shop.xn--bcher-kva.de", "bücher.de") and core.host_matches("shop.bücher.de", "xn--bcher-kva.de")
    assert core.clean_target("https://www.BÜCHER.de/x") == "xn--bcher-kva.de"
    assert not core.host_matches("evilbücher.de", "bücher.de")


def test_depth_skip_reactivated_only_for_max_depth(env):
    with core.connect() as c:
        core.save_url(c, "https://a.example.org/deep", None, "x", 3, True, "skipped", reason="max_depth")
        core.save_url(c, "https://a.example.org/robots", None, "x", 3, True, "skipped", reason="robots_txt")
        core.save_url(c, "https://a.example.org/deep", None, "x", 4, True, "skipped", reason="max_depth")  # still too deep: stays skipped
        core.save_url(c, "https://a.example.org/deep", None, "x", 1, True, "pending")
        core.save_url(c, "https://a.example.org/robots", None, "x", 1, True, "pending")
    r = {x["url"].rsplit("/", 1)[1]: x for x in rows("SELECT url, crawl_status, skip_reason, crawl_depth FROM discovered_urls")}
    assert (r["deep"]["crawl_status"], r["deep"]["skip_reason"], r["deep"]["crawl_depth"]) == ("pending", None, 1)
    assert (r["robots"]["crawl_status"], r["robots"]["skip_reason"]) == ("skipped", "robots_txt")

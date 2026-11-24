"""bs4's lxml builder raises `ValueError: not enough values to unpack` for markup it cannot handle (attribute name starting with '{'). Only that
exact error falls back to html.parser, the page and its links are kept, and a whole 500-URL batch is never lost to one page. Real PostgreSQL
(isolated schema) and the local test web server. Nothing is deleted anywhere."""

import asyncio

import pytest
from bs4.builder import _lxml

import crawler_core as core
import crawler_engine as engine
from test_crawler import SITES, cfg, env, page, port, rows  # noqa: F401 - fixtures

UNPACK = "not enough values to unpack (expected 2, got 1)"
MARK = b"BREAKME"


@pytest.fixture
def lxml_rejects_marked_pages(monkeypatch):
    """Simulates the production failure deterministically: the lxml builder rejects a document that contains MARK, exactly as bs4 does
    for a '{'-prefixed attribute name."""
    real = _lxml.LXMLTreeBuilder.feed

    def feed(self, markup):
        if MARK in (markup if isinstance(markup, bytes) else markup.encode()):
            raise ValueError(UNPACK)
        return real(self, markup)
    monkeypatch.setattr(_lxml.LXMLTreeBuilder, "feed", feed)


def links(soup):
    return [a["href"] for a in soup.select("a[href]")]


def test_unpack_error_falls_back_to_html_parser_and_keeps_the_links(lxml_rejects_marked_pages):
    p = engine.FallbackSoupParser()
    body = b'<html><body BREAKME><a href="https://t.example.com/x">x</a><a href="/rel">r</a></body></html>'
    assert links(p._parse_body(body, "text/html")) == ["https://t.example.com/x", "/rel"]  # same bytes, links intact
    assert links(p._parse_body(b'<a href="/ok">ok</a>', "text/html; charset=utf-8")) == ["/ok"]  # ordinary markup: the lxml path, unchanged
    assert links(p._parse_body(b"", None)) == []  # empty body


def test_only_that_exact_error_is_caught(monkeypatch):
    p = engine.FallbackSoupParser()
    monkeypatch.setattr(_lxml.LXMLTreeBuilder, "feed", lambda self, m: (_ for _ in ()).throw(ValueError("something else entirely")))
    with pytest.raises(ValueError, match="something else"):  # not swallowed, not retried with another parser
        p._parse_body(b"<a href='/x'>x</a>", "text/html")


def test_batch_of_500_with_unparseable_pages_loses_no_valid_url_and_stores_every_link(env, lxml_rejects_marked_pages):
    host = "bulk.localhost"
    n = 500
    bad = {7, 123, 499}
    SITES[host] = {"/": page()}
    for i in range(n):
        extra = " BREAKME" if i in bad else ""
        SITES[host][f"/p{i}"] = (200, "text/html", f'<html><body{extra}><a href="https://dest{i}.example.org/l">l{i}</a></body></html>')
    base = env(host.split(".")[0])
    urls = [base.rstrip("/") + f"/p{i}" for i in range(n)]
    with core.connect() as c:
        for u in urls:
            core.save_url(c, u, None, "t", 1, True, "crawling")
    before = rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"]
    asyncio.run(engine.crawl_round(cfg(max_retries=1, concurrency=10), None, [(u, 1) for u in urls], set(), False, n))
    st = {r["crawl_status"]: r["n"] for r in rows("SELECT crawl_status, COUNT(*) n FROM discovered_urls WHERE url LIKE ? GROUP BY 1", base.rstrip("/") + "/p%")}
    assert st == {"completed": n}, st  # no page failed, including the three the lxml builder rejects
    dests = {r["destination_url"] for r in rows("SELECT destination_url FROM outgoing_links")}
    assert {f"https://dest{i}.example.org/l" for i in range(n)} <= dests  # every link of every page, the fallback pages included
    assert rows("SELECT COUNT(*) n FROM (SELECT source_url, destination_url, anchor_text FROM outgoing_links GROUP BY 1,2,3 HAVING COUNT(*)>1) d")[0]["n"] == 0  # no duplicates
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE lease_owner IS NOT NULL AND url LIKE ?", base.rstrip("/") + "/p%")[0]["n"] == 0  # leases cleared
    assert rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"] >= before  # nothing deleted

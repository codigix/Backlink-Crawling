"""Real PostgreSQL comparison with a deterministic Link Gap HTTP boundary."""
import httpx
import pytest

import business_jobs
import competitors
import crawler_core as core
import openpagescore as ops
from test_api_perf import client, db  # noqa: F401 - isolated PostgreSQL schema and HTTP client

OWNER, RIVAL = "own.com", "rival.com"
PATH = f"/businesses/{OWNER}/backlink-gap"


@pytest.fixture()
def graph(db):
    with core.connect() as c:
        business_jobs.create_job(c, OWNER)
        competitors.register(c, OWNER, RIVAL)
        for domain, target in [("alpha.net", RIVAL), ("zulu.net", RIVAL), ("shared.net", OWNER),
                               ("shared.net", RIVAL), ("ours.net", OWNER)]:
            c.execute("INSERT INTO outgoing_links(source_domain,source_url,destination_domain,destination_url,anchor_text)"
                      " VALUES(?,?,?,?,?)", (domain, f"https://{domain}/article", target, f"https://{target}/", "credit"))
        return competitors.gap(c, OWNER)


def provider(monkeypatch, reply):
    ops.cache.clear()
    calls = []
    def get(url, *, params, timeout):
        assert url == ops.URL and params["min"] == 1 and params["limit"] == 1000 and timeout <= 5
        assert core.OPEN_CONNS == 0
        calls.append(params)
        response = reply(params)
        response.request = httpx.Request("GET", url)
        return response
    monkeypatch.setattr(ops, "http_get", get)
    return calls


def test_append_and_walk(graph, client, monkeypatch):
    domains = ["HTTPS://WWW.ZULU.NET/", "medium.com", "blog.medium.com", "builtin.com",
               "www.ours.net", "SHARED.NET", OWNER, RIVAL]
    calls = provider(monkeypatch, lambda p: httpx.Response(200, json={
        "domains": [{"domain": d, "openpage_score": 78.5} for d in domains], "total": 1134}))
    def page(**params):
        response = client.get(PATH, params=params)
        assert response.status_code == 200
        return response.json()

    g = page(competitor="WWW.RIVAL.COM", limit=10)
    assert calls == [{"domain": OWNER, "competitor": RIVAL, "min": 1, "limit": 1000}]
    assert g["opportunities"][:2] == graph["opportunities"]
    assert [r["referring_domain"] for r in g["opportunities"]] == ["alpha.net", "zulu.net", "medium.com", "builtin.com"]
    assert g["opportunities"][2] == {"referring_domain": "medium.com", "classification": "competitor_only",
        "competitor_targets": [RIVAL], "links_to_own_business": False, "source_pages": [], "authority_score": 78.5,
        "source": "openpagescore", "verification_status": "historical_unverified"}
    assert g["summary"] == {**graph["summary"], "postgres_gap_domains": 2, "gap_domains": 4,
                            "openpagescore_unique_domains": 2, "total_unique_opportunities": 4}
    assert g["coverage"]["discovery_status"] == graph["coverage"]["discovery_status"]
    assert "OpenPageScore: truncated results" in g["coverage"]["warnings"]
    for key in ("shared_domains", "own_only_domains", "source_page_gap"):
        assert g[key] == graph[key]
    for limit in (1, 2, 3):
        params = {}
        for offset in range(0, 4, limit):
            by_cursor, by_offset = page(limit=limit, **params), page(limit=limit, offset=offset)
            assert by_cursor["opportunities"] == by_offset["opportunities"] == g["opportunities"][offset:offset+limit]
            with core.connect() as c:
                baseline = competitors.gap(c, OWNER, limit=limit, offset=offset)
            for key in ("shared_domains", "own_only_domains", "source_page_gap"):
                assert by_offset[key] == baseline[key]
                assert by_offset["pagination"]["lists"][key] == baseline["pagination"]["lists"][key]
            info = by_cursor["pagination"]["lists"]["opportunities"]
            assert info["total"] == 4 and info["has_more"] == (offset + limit < 4)
            params = {"after_opportunity": info["next_after"]}
        assert page(limit=limit, offset=25)["opportunities"] == []
    assert len(calls) == 1
    assert client.get(PATH, params={"after_opportunity": "openpagescore:-1"}).status_code == 422
    assert client.get(PATH, params={"competitor": "unknown.com"}).status_code == 404
    assert client.get("/businesses/unknown.com/backlink-gap").status_code == 404
    assert len(calls) == 1
    with core.connect() as c:
        assert competitors.gap(c, OWNER) == graph
        assert c.execute("SELECT COUNT(*) FROM backlinks").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM outgoing_links").fetchone()[0] == 5


@pytest.mark.parametrize("failure", [400, 404, 429, 500, "timeout", "connection", "json", "shape", "empty", "skipped"])
def test_fallback(graph, client, monkeypatch, failure):
    def reply(_):
        if failure == "timeout":
            raise httpx.ReadTimeout("timeout")
        if failure == "connection":
            raise httpx.ConnectError("connection")
        if failure == "json":
            return httpx.Response(200, content=b"broken")
        if failure == "skipped":
            return httpx.Response(200, json={"domains": [{"domain": "external.com"}], "skipped": [RIVAL]})
        if failure == "empty":
            return httpx.Response(200, json={"domains": []})
        return httpx.Response(200 if failure == "shape" else failure, json={})
    provider(monkeypatch, reply)
    response = client.get(PATH)
    assert response.status_code == 200
    g = response.json()
    for key in ("opportunities", "shared_domains", "own_only_domains", "source_page_gap", "pagination"):
        assert g[key] == graph[key]
    assert all(g["summary"][key] == value for key, value in graph["summary"].items())
    assert g["summary"]["openpagescore_unique_domains"] == 0
    assert g["coverage"]["warnings"][:len(graph["coverage"]["warnings"])] == graph["coverage"]["warnings"]
    assert any(w.startswith("OpenPageScore:") for w in g["coverage"]["warnings"])


def test_selected_competitors_batches_and_relationships(graph, client, monkeypatch):
    rivals = [f"r{i}.com" for i in range(10)]
    with core.connect() as c:
        for target in rivals:
            competitors.register(c, OWNER, target)
    def reply(params):
        batch = params["competitor"].split(",")
        if len(batch) == 1:
            return httpx.Response(200, json={"domains": [{"domain": "also.net"}]})
        return httpx.Response(200, json={"skipped": [batch[-1]], "domains": [
            {"domain": "www.also.net", "links_to": [batch[2], "foreign.com", batch[-1]]},
            {"domain": "unknown.net", "count": 3}]})
    calls = provider(monkeypatch, reply)
    result = client.get(PATH, params={"refresh": "true"}).json()
    assert [p["competitor"].split(",") for p in calls] == [rivals, [RIVAL]]
    assert result["opportunities"][-1]["competitor_targets"] == [rivals[2], RIVAL]
    assert result["summary"]["openpagescore_unique_domains"] == 1
    assert "OpenPageScore: missing competitor relationship" in result["coverage"]["warnings"]
    specific = client.get(PATH, params={"competitor": rivals[0]}).json()
    assert calls[-1]["competitor"] == rivals[0]
    assert specific["opportunities"][-1]["competitor_targets"] == [rivals[0]]


def test_legacy_sources_and_filtered_competitor_sites(graph, client, monkeypatch, caplog):
    caplog.set_level("DEBUG", logger=ops.log.name)
    ops.log.addHandler(caplog.handler)
    try:
        with core.connect() as c:
            for domain, target, source, link_type in [
                ("HTTPS://WWW.BLOG.LEGACY.CO.UK/", RIVAL, "https://blog.legacy.co.uk/article", "html"),
                (None, OWNER, "https://sub.nullsource.net/article", "html"),
                ("portfolio.net", RIVAL, "https://portfolio.net/", "html"),
                ("image.net", OWNER, "https://image.net/article", "img")]:
                c.execute("INSERT INTO outgoing_links(source_domain,source_url,destination_domain,destination_url,link_type)"
                          " VALUES(?,?,?,?,?)", (domain, source, target, f"https://{target}/", link_type))
            core.record_backlink(c, core.add_target(c, "blog." + OWNER), "https://www.registered.net/p",
                                 f"https://blog.{OWNER}/", "test", None)
            expected = competitors.gap(c, OWNER)
        domains = ["legacy.co.uk", "nullsource.net", "portfolio.net", "image.net", "registered.net", "unique.net"]
        provider(monkeypatch, lambda _: httpx.Response(200, json={"domains": [{"domain": d} for d in domains]}))
        result = client.get(PATH, params={"refresh": "true"}).json()
        assert result["opportunities"][:-2] == expected["opportunities"]
        assert [r["referring_domain"] for r in result["opportunities"][-2:]] == ["portfolio.net", "unique.net"]
        assert result["source_page_gap"] == expected["source_page_gap"]
        assert result["summary"]["openpagescore_unique_domains"] == 2
        assert result["pagination"]["lists"]["opportunities"]["total"] == len(expected["opportunities"]) + 2
        for domain in ("nullsource.net", "image.net", "registered.net"):
            assert f"domain={domain} reason=links_to_business" in caplog.text
        assert "domain=legacy.co.uk reason=database_opportunity" in caplog.text
        assert "database_opportunities=1 links_to_business=3 remaining=2" in caplog.text
    finally:
        ops.log.removeHandler(caplog.handler)


def test_filtered_for_one_competitor_but_opportunity_for_another(graph, client, monkeypatch):
    other = "second.com"
    with core.connect() as c:
        competitors.register(c, OWNER, other)
        for src, target in [("https://mixed.net/", RIVAL), ("https://mixed.net/article", other)]:
            c.execute("INSERT INTO outgoing_links(source_domain,source_url,destination_domain,destination_url) VALUES(?,?,?,?)",
                      ("mixed.net", src, target, f"https://{target}/"))
        original = competitors.gap(c, OWNER)
    provider(monkeypatch, lambda _: httpx.Response(200, json={"domains": [{"domain": "mixed.net", "links_to": [RIVAL]}]}))
    # This HTTP page ends before mixed.net in the database ordering.
    first = client.get(PATH, params={"limit": 1}).json()
    assert first["summary"]["openpagescore_unique_domains"] == 0
    assert first["summary"]["total_unique_opportunities"] == len(original["opportunities"])
    assert client.get(PATH).json()["opportunities"] == original["opportunities"]
    single = client.get(PATH, params={"competitor": RIVAL}).json()
    assert single["opportunities"][:-1] == graph["opportunities"]
    assert single["opportunities"][-1]["referring_domain"] == "mixed.net"
    assert single["opportunities"][-1]["source"] == "openpagescore"


def test_independent_cursors_on_external_page(graph, client, monkeypatch):
    provider(monkeypatch, lambda _: httpx.Response(200, json={"domains": [{"domain": "one.net"}, {"domain": "two.net"}]}))
    params = {"limit": 1, "after_opportunity": "openpagescore:1", "after_shared": "shared.net",
              "after_own_only": "ours.net", "after_page": "https://alpha.net/article"}
    result = client.get(PATH, params=params).json()
    assert [r["referring_domain"] for r in result["opportunities"]] == ["two.net"]
    assert result["shared_domains"] == result["own_only_domains"] == []
    assert result["source_page_gap"] == graph["source_page_gap"][1:]
    assert not result["pagination"]["has_more"]
    assert client.get(PATH, params={**params, "offset": 1}).status_code == 422


def test_request_budget_and_partial_batch_failure(monkeypatch):
    def reply(params):
        if params["competitor"] == RIVAL:
            return httpx.Response(429)
        return httpx.Response(200, json={"domains": [{"domain": "additional.net", "links_to": ["r0.com"]}]})
    calls = provider(monkeypatch, reply)
    selected = [RIVAL] + [f"r{i}.com" for i in range(10)]
    rows, warnings = ops.candidates(OWNER, selected)
    assert list(rows) == ["additional.net"] and any("429" in w for w in warnings)
    assert len(calls) == 2
    ops._requests.extend([ops.time.monotonic()] * 28)
    rows, warnings = ops.candidates(OWNER, ["other.com"])
    assert not rows and warnings and len(calls) == 2

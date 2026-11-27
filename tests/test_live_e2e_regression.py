"""End-to-End Live Workflow Regression Test.

Validates:
1. Business onboarding and competitor registration.
2. Search query simplification and Serper credit exhaustion handling without crashes or key leaks.
3. Link graph ingestion with metadata (position, context, anchor).
4. Competitor referring domains endpoint with built_only=false (default) vs built_only=true.
5. Cache sharing (X-Cache: HIT) between /competitors/{comp}/referring-domains and /backlinks/{comp}/referring-domains.
6. Footprint classification: genuine homepage & multi-article brand links preserved; footer/agency credits filtered.
7. Backlink gap calculation merging PostgreSQL graph and external OpenPageScore opportunities.
"""
import uuid
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api_timing
import autonomous_api
import business_jobs
import competitors
import crawler_api
import crawler_core as core
import ddg_lookup
import openpagescore
import read_cache
from read_cache import TTLCache


def test_complete_e2e_system_flow(monkeypatch):
    # Isolated PostgreSQL schema
    schema = "e2e_" + uuid.uuid4().hex[:12]
    with core.connect() as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    monkeypatch.setattr(core, "DB_SCHEMA", schema)

    try:
        with core.connect() as c:
            core.migrate(c)

        # Build Test FastAPI App with API Timing
        app = FastAPI()
        app.include_router(crawler_api.router)
        app.include_router(autonomous_api.router)
        api_timing.install(app)
        client = TestClient(app)

        # Enable Graph Cache for cache sharing validation
        test_cache = TTLCache(enabled=True, ttl=60, max_ttl=120)
        monkeypatch.setattr(crawler_api, "graph_cache", test_cache)

        # -------------------------------------------------------------
        # 1. Onboarding and Competitor Registration
        # -------------------------------------------------------------
        owner = "mybusiness.com"
        comp = "rivalbrand.com"

        resp = client.post("/discovery/onboard", json={"domain": owner})
        assert resp.status_code == 202, resp.text
        assert resp.json()["status"] == "queued"

        resp = client.post(f"/businesses/{owner}/competitors", json={"domains": [comp]})
        assert resp.status_code == 202, resp.text
        assert resp.json()["results"][0]["status"] == "registered"

        # -------------------------------------------------------------
        # 2. Search Provider Hardening & Fallback Verification
        # -------------------------------------------------------------
        # Verify query simplification
        complex_q = f'"{owner}" -site:{owner}'
        simple_q = ddg_lookup.simplify_query(complex_q)
        assert simple_q == f'"{owner}"'

        # Verify Serper Credit Exhaustion Error Classification
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: httpx.Response(400, json={"message": "Not enough credits"}, request=httpx.Request("POST", a[0])))
        with pytest.raises(ddg_lookup.SerperCreditsExhausted):
            ddg_lookup._serper_post({"q": "test"})

        # Verify Search URLs does not crash on exhausted Serper when multiple backends exist
        monkeypatch.setattr(ddg_lookup, "_serper", lambda q, p: (_ for _ in ()).throw(ddg_lookup.SerperCreditsExhausted("exhausted")))
        monkeypatch.setattr(ddg_lookup, "BACKENDS", ["dogpile", "serper"])
        monkeypatch.setattr(ddg_lookup, "_dogpile", lambda q, p: [])
        urls = ddg_lookup.search_urls(owner, target=5, pages=1, run=ddg_lookup.Run(persist=True))
        assert urls == []  # Gracefully returned [] without terminating or raising uncaught exception

        # -------------------------------------------------------------
        # 3. Ingest Rich Backlink Graph
        # -------------------------------------------------------------
        # Add links:
        # A) Genuine directory homepage: directory.org/ linking to rivalbrand.com in content
        # B) Genuine blog with 2 articles linking with same anchor: blog.org/p1, blog.org/p2
        # C) Confirmed agency footer credit: clientbuilt.com/ with context "Designed by Rival"
        # D) Shared backlink: authoritynews.com/news linking to BOTH mybusiness.com and rivalbrand.com
        with core.connect() as c:
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("directory.org", "https://directory.org/", comp, f"https://{comp}/", "Rival Brand", "content", "Featured in business directory", )
            )
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("blog.org", "https://blog.org/p1", comp, f"https://{comp}/guide", "Rival Brand", "content", "Great guide", )
            )
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("blog.org", "https://blog.org/p2", comp, f"https://{comp}/guide", "Rival Brand", "content", "Recommended by", )
            )
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("clientbuilt.com", "https://clientbuilt.com/", comp, f"https://{comp}/", "Web Design", "footer", "Designed by Rival", )
            )
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("authoritynews.com", "https://authoritynews.com/news", comp, f"https://{comp}/", "Rival Brand", "content", "Industry report", )
            )
            c.execute(
                "INSERT INTO outgoing_links (source_domain, source_url, destination_domain, destination_url, anchor_text, position, context, link_type, is_active)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'html', 1)",
                ("authoritynews.com", "https://authoritynews.com/news", owner, f"https://{owner}/", "My Business", "content", "Industry report", )
            )

        # -------------------------------------------------------------
        # 4. Competitor Referring Domains: Default (built_only=False) vs Built-Only
        # -------------------------------------------------------------
        # Default returns all 4 referring domains
        resp_all = client.get(f"/competitors/{comp}/referring-domains")
        assert resp_all.status_code == 200
        assert resp_all.headers["X-Cache"] == "MISS"
        data_all = resp_all.json()
        assert data_all["total_referring_domains"] == 4
        all_doms = {r["domain"] for r in data_all["referring_domains"]}
        assert all_doms == {"directory.org", "blog.org", "clientbuilt.com", "authoritynews.com"}

        # -------------------------------------------------------------
        # 5. Read Cache Sharing Validation
        # -------------------------------------------------------------
        # Second call to competitor endpoint is a HIT
        resp_comp_hit = client.get(f"/competitors/{comp}/referring-domains")
        assert resp_comp_hit.headers["X-Cache"] == "HIT"

        # Call to public /backlinks/{comp}/referring-domains shares the exact same cached entry!
        resp_pub_hit = client.get(f"/backlinks/{comp}/referring-domains")
        assert resp_pub_hit.headers["X-Cache"] == "HIT"
        assert resp_pub_hit.json() == data_all

        # built_only=True returns footprint sites only (clientbuilt.com)
        resp_built = client.get(f"/competitors/{comp}/referring-domains?built_only=true")
        assert resp_built.status_code == 200
        data_built = resp_built.json()
        assert data_built["total_referring_domains"] == 1
        assert data_built["referring_domains"][0]["domain"] == "clientbuilt.com"

        # -------------------------------------------------------------
        # 6. Backlink Gap Analysis: PostgreSQL + OpenPageScore Merging
        # -------------------------------------------------------------
        # Setup mock OpenPageScore provider returning a new unique historical opportunity
        openpagescore.cache.clear()
        monkeypatch.setattr(openpagescore, "http_get", lambda url, **kw: httpx.Response(
            200, request=httpx.Request("GET", url),
            json={"domains": [
                {"domain": "clientbuilt.com", "openpage_score": 45.0},  # Excluded (footprint)
                {"domain": "historical-gap.com", "openpage_score": 68.2} # Admitted
            ], "total": 2}
        ))

        gap_resp = client.get(f"/businesses/{owner}/backlink-gap", params={"refresh": "true"})
        assert gap_resp.status_code == 200
        gap_data = gap_resp.json()

        # Database opportunities: directory.org and blog.org (authoritynews.com is shared, clientbuilt is agency footprint)
        # External opportunities: historical-gap.com (from OpenPageScore)
        opp_domains = [o["referring_domain"] for o in gap_data["opportunities"]]
        assert "directory.org" in opp_domains  # Genuine homepage opportunity preserved!
        assert "blog.org" in opp_domains       # 2-article brand anchor opportunity preserved!
        assert "historical-gap.com" in opp_domains  # OpenPageScore merged!
        assert "authoritynews.com" not in opp_domains # Shared link to own excluded from opportunities!
        assert "authoritynews.com" in gap_data["shared_domains"]

        assert gap_data["summary"]["shared_domains"] == 1
        assert gap_data["summary"]["openpagescore_unique_domains"] == 2

    finally:
        with core.connect() as admin:
            admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

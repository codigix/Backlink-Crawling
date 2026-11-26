"""Phase 1 API read-path optimisation: every optimised endpoint is compared with a verbatim copy of the ALGORITHM IT REPLACED (the
`legacy_*` functions below: full graph loaded into Python, aggregated in Python) on the same data, plus the statistics snapshot
(freshness, single flight, staleness, outage), keyset paging, connection counts and the Server-Timing instrumentation.
Real PostgreSQL in an isolated schema (like every other test file)."""

import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api_timing
import autonomous
import autonomous_api
import business_jobs
import crawler_api
import crawler_core as core
import engine_stats
from api_data import EVIL, SUB, TARGET, load


@pytest.fixture()
def db(monkeypatch):
    """Own schema per test; the statistics snapshot is process-wide, so it is reset around every test."""
    schema = "t_" + uuid.uuid4().hex[:12]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        monkeypatch.setattr(core, "DB_SCHEMA", schema)
        with core.connect() as c:
            core.migrate(c)
        engine_stats.reset_snapshot()
        yield
        engine_stats.reset_snapshot()
        admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture()
def client(db):
    app = FastAPI()  # same wiring as backlink_api, without its import-time migration
    app.include_router(crawler_api.router)
    app.include_router(autonomous_api.router)
    api_timing.install(app)
    return TestClient(app)


def edge_rows(c, info):
    """Cases the generator cannot reach at small scale: a NULL source_domain on a subdomain target (counts as one referring domain), a
    link_history 'lost' event that suppresses the lost_at fallback of its row, and hosts the plain-host fast path must NOT handle."""
    d = info["domains"]
    g = info["links"] // 170 * 170  # the NEWEST removed+lost link of the generator (g%10==0 removed, g%17==0 lost): its link_history 'lost' event must suppress
    dest = f"https://{SUB}/x{g}" if g % 8 == 0 else f"https://{TARGET}/page{g % 50}"  # the lost_at fallback, so with a small limit the skipped candidate sits at the top
    anchor = ["Codigix", "click here", "", "home", "Codigix"][g % 5]
    c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, source_domain, first_seen, last_seen) VALUES"
              f" ('https://nullsrc.example/a','https://{SUB}/n1','{TARGET}','Codigix',NULL,'2026-02-01 00:00:00','2026-02-01 00:00:01'),"
              f" ('https://nullsrc2.example/a','https://{SUB}/n2','{TARGET}','',NULL,'2026-02-01 00:00:00','2026-02-01 00:00:02')")
    c.execute("UPDATE outgoing_links SET lost_at = to_char(timestamp '2026-06-01 00:00:00' + (id % 100000) * interval '1 minute', 'YYYY-MM-DD HH24:MI:SS')"
              " WHERE COALESCE(is_active,0)=0 AND lost_at IS NOT NULL")  # recent and increasing with id, so the fallback events reach the top of the answer
    c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at) VALUES"
              f" ('https://s{g % d}.example/p{g}','{dest}','{TARGET}','{anchor}','lost','2026-03-01 00:00:00'),"
              f" ('HTTPS://S1.EXAMPLE/UP','https://Codigix.co/Up','{TARGET}','x','reappeared','2026-03-01 00:00:00'),"
              f" ('https://s1.example:8080/p','https://{TARGET}/port','{TARGET}','x','lost','2026-03-01 00:00:00'),"
              f" ('https://user@s2.example/p','https://{TARGET}/ui','{TARGET}','x','lost','2026-03-01 00:00:00'),"
              f" ('https://bücher.example/x','https://{TARGET}/uni','{TARGET}','x','reappeared','2026-03-01 00:00:00'),"
              f" ('https://www.{TARGET}/self','https://{TARGET}/y','{TARGET}','x','lost','2026-03-01 00:00:00'),"  # from the target's own host: dropped
              f" ('https://{TARGET}./self2','https://{TARGET}/y','{TARGET}','x','lost','2026-03-01 00:00:00')")


# ------------------------------------------------------------------ the replaced algorithms, verbatim ------------------------------------------------------------------

def legacy_graph(c, target, active_only=True, limit=None, offset=0):
    reg = core.registrable_domain(target)
    cond = " WHERE destination_domain=? AND link_type='html' AND source_domain!=?" + (" AND is_active=1" if active_only else "")
    args = [reg, reg]
    if target != reg:
        cond = cond.replace("source_domain!=?", "TRUE") + " AND destination_url ~* ? AND source_url !~* ?"
        args = [reg, crawler_api._host_re(target), crawler_api._host_re(target)]
    total = c.execute("SELECT COUNT(*) FROM outgoing_links" + cond, args).fetchone()[0]
    rows = c.execute("SELECT *, (SELECT crawl_status FROM discovered_urls u WHERE u.url=outgoing_links.source_url) AS src_status FROM outgoing_links"
                     + cond + " ORDER BY id" + (" LIMIT ? OFFSET ?" if limit else ""), (*args, limit, offset) if limit else args).fetchall()
    return total, rows


def legacy_backlinks(target_domain, include_removed, limit, offset):
    target = crawler_api._clean(target_domain)
    reg = core.registrable_domain(target)
    with core.connect() as c:
        total, rows = legacy_graph(c, target, not include_removed, limit, offset)
        every = legacy_graph(c, target, not include_removed)[1]
        active = sum(crawler_api.link_status(r) == "verified" for r in every)
        refs = len({r["source_domain"] for r in every})
        job = business_jobs.latest(c, reg)
    state = business_jobs.api_state(job)
    if total == 0:
        return {"domain": target, "target_domain": target, "total_backlinks": 0, "referring_domains": 0, "total_referring_domains": 0,
                "status": state, "job_id": job and job["id"], "backlinks": [],
                "message": "Not onboarded: POST /discovery/onboard to start discovery." if not job else
                f"No backlink found yet ({job['pages_crawled']} page(s) crawled" + (f"; last error: {job['last_error']}" if job["last_error"] else "") + ")."}
    return {
        "domain": target, "target_domain": target, "status": "ok", "discovery_status": state, "total_backlinks": total, "verified_backlinks": active,
        "referring_domains": refs, "total_referring_domains": refs, "limit": limit, "offset": offset,
        "backlinks": [{
            "source_url": r["source_url"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"],
            "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": crawler_api.link_status(r),
            "verification_status": crawler_api.link_status(r), "discovery_method": "forward_crawl",
            "first_seen": r["first_seen"], "last_verified": r["last_seen"], "is_active": bool(r["is_active"]),
        } for r in rows],
    }


def legacy_referring_domains(target_domain):
    target = crawler_api._clean(target_domain)
    agg = {}
    with core.connect() as c:
        for r in legacy_graph(c, target)[1]:
            d = agg.setdefault(r["source_domain"], {"domain": r["source_domain"], "backlinks": 0, "first_seen": r["first_seen"], "last_verified": r["last_seen"]})
            d["backlinks"] += 1
            d["first_seen"], d["last_verified"] = min(d["first_seen"], r["first_seen"]), max(d["last_verified"], r["last_seen"])
    items = sorted(agg.values(), key=lambda d: -d["backlinks"])
    return {"domain": target, "status": "ok" if items else "not_found_in_current_index", "total_referring_domains": len(items), "referring_domains": items}


def legacy_anchors(target_domain):
    target = crawler_api._clean(target_domain)
    counts = {}
    with core.connect() as c:
        for r in legacy_graph(c, target)[1]:
            counts[r["anchor_text"] or ""] = counts.get(r["anchor_text"] or "", 0) + 1
    return {"target_domain": target, "domain": target, "anchors": [{"anchor_text": a, "count": n} for a, n in sorted(counts.items(), key=lambda x: -x[1])]}


def legacy_history(target_domain, limit):
    target = crawler_api._clean(target_domain)
    reg = core.registrable_domain(target)
    ev, lost_seen = [], set()
    with core.connect() as c:
        for r in legacy_graph(c, target, False)[1]:
            ev.append({"event": "new", "at": r["first_seen"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"})
        for r in c.execute("SELECT * FROM link_history WHERE destination_domain=? ORDER BY id DESC LIMIT 100000", (reg,)):
            if core.host_matches(core.hostname(r["destination_url"]), target) and not core.host_matches(core.hostname(r["source_url"]), target):
                ev.append({"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                           "anchor_text": r["anchor_text"], "origin": "graph"})
                if r["event_type"] == "lost":
                    lost_seen.add((r["source_url"], r["destination_url"], r["anchor_text"]))
        for r in legacy_graph(c, target, False)[1]:
            if r["lost_at"] and not r["is_active"] and (r["source_url"], r["destination_url"], r["anchor_text"]) not in lost_seen:
                ev.append({"event": "lost", "at": r["lost_at"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"})
        for r in c.execute("SELECT h.event_type, h.detected_at, h.details, b.source_url, b.destination_url FROM backlink_history h"
                           " JOIN backlinks b ON b.id=h.backlink_id JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=?"
                           " ORDER BY h.id DESC LIMIT 100000", (target,)):
            ev.append({"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                       "details": r["details"], "origin": "backlink"})
    ev.sort(key=lambda e: e["at"], reverse=True)
    return {"domain": target, "events": ev[:limit]}


# ------------------------------------------------------------------ equivalence with the replaced algorithms ------------------------------------------------------------------

TARGETS = [TARGET, SUB, EVIL, "nothing-here.org"]


NEW_KEYS = {"next_after_id", "next_offset", "total_anchors", "has_more", "next_cursor"}  # additive response fields; every other key must be exactly what the replaced code returned


def old_shape(r: dict) -> dict:
    """The response without additive paging fields or backlink position (list-all limit/offset follow the old response shape)."""
    out = {k: v for k, v in r.items() if k not in NEW_KEYS}
    if "backlinks" in out:
        out["backlinks"] = [{k: v for k, v in row.items() if k != "position"} for row in out["backlinks"]]
    if "anchors" in out or "referring_domains" in out and "backlinks" not in out:
        out.pop("limit", None), out.pop("offset", None)
    return out


def populate(scale):
    with core.connect() as c:
        info = load(c, scale=scale)
        edge_rows(c, info)
    return info


@pytest.mark.parametrize("scale", [1, 10])  # 10x: ~70k rows, thousands of links per target, a 6k-row history window
def test_backlink_endpoints_match_the_replaced_algorithms(db, scale):
    populate(scale)
    for t in TARGETS:
        for removed in (False, True):
            for limit, offset in ((100, 0), (7, 0), (7, 21), (1000, 0), (50, 10**6)):
                assert old_shape(crawler_api.backlinks(t, removed, limit, offset)) == legacy_backlinks(t, removed, limit, offset), (t, removed, limit, offset)
        assert old_shape(crawler_api.referring_domains(t, crawler_api.MAX_LIMIT)) == legacy_referring_domains(t), t
        assert old_shape(crawler_api.backlink_anchors(t, crawler_api.MAX_LIMIT)) == legacy_anchors(t), t
        for limit in (1, 2, 7, 50, 200, 1000):
            assert crawler_api.backlink_history(t, limit) == legacy_history(t, limit), (t, limit)


def test_pages_cover_every_link_once_and_counts_are_exact(db):
    populate(1)
    first = crawler_api.backlinks(TARGET, True, 1000, 0)
    every = first["backlinks"]
    assert first["total_backlinks"] == len(every) > 100
    paged = []
    for offset in range(0, len(every) + 13, 13):
        paged += crawler_api.backlinks(TARGET, True, 13, offset)["backlinks"]
    assert paged == every and len({(b["source_url"], b["destination_url"], b["anchor_text"]) for b in paged}) == len(paged)
    active = crawler_api.backlinks(TARGET, False, 1000, 0)
    assert active["verified_backlinks"] == sum(b["link_status"] == "verified" for b in active["backlinks"])
    assert active["referring_domains"] == len({b["source_domain"] for b in active["backlinks"]})
    assert crawler_api.referring_domains(TARGET)["total_referring_domains"] == active["referring_domains"]
    assert sum(a["count"] for a in crawler_api.backlink_anchors(TARGET)["anchors"]) == active["total_backlinks"]


def test_null_source_domain_counts_as_one_referring_domain_like_the_python_set(db):
    populate(1)
    r = crawler_api.backlinks(SUB, False, 100, 0)  # the subdomain filter keeps NULL source_domain rows (the registrable one drops them in SQL)
    assert r["referring_domains"] == len({b["source_domain"] for b in r["backlinks"]}) and None in {b["source_domain"] for b in r["backlinks"]}


def test_invalid_target_domain_is_rejected_by_every_endpoint(db):
    for fn, args in ((crawler_api.backlinks, ("not a domain", False, 10, 0)), (crawler_api.referring_domains, ("not a domain",)),
                     (crawler_api.backlink_anchors, ("not a domain",)), (crawler_api.backlink_history, ("not a domain", 10)),
                     (crawler_api.backlinks, ("", False, 10, 0))):
        with pytest.raises(HTTPException) as e:
            fn(*args)
        assert e.value.status_code == 422


def test_empty_database(db, client):
    assert old_shape(crawler_api.backlinks(TARGET, False, 10, 0)) == legacy_backlinks(TARGET, False, 10, 0)
    assert old_shape(crawler_api.referring_domains(TARGET)) == {"domain": TARGET, "status": "not_found_in_current_index", "total_referring_domains": 0, "referring_domains": []}
    assert crawler_api.backlink_anchors(TARGET)["anchors"] == [] and crawler_api.backlink_history(TARGET, 10)["events"] == []
    zero = {"discovered_domains": 0, "discovered_urls": 0, "outgoing_links": 0, "domains_by_status": {}, "urls_by_status": {}, "urls_by_source": {}}
    assert engine_stats.counts() == zero == client.get("/discovery/statistics").json()
    assert client.get("/crawler/outgoing-links").json() == {"total": 0, "items": []}
    assert client.get("/crawler/discovered-urls").json() == {"total": 0, "items": []}


def test_history_fast_host_path_equals_hostname_for_plain_hosts():
    urls = ["http://a.com/", "https://a.com", "https://a.com?x=1", "https://a.com#f", "https://a.b-c.example/p", "https://a.com./x", "https://a..com/x", "https://-a.com/",
            "https://a.com:8080/", "https://u@a.com/", "https://u:p@a.com/", "https://A.com/", "HTTPS://a.com/", "https://a.com\\x", "https://a.com;p/x", "https://[::1]/x",
            "https://bücher.de/x", "https://xn--bcher-kva.de/x", "https://" + "a" * 70 + ".com/", "https://1.2.3.4/x", "https://a_b.com/", "https:///x", "https://.a.com/"]
    for u in urls:
        m = crawler_api._PLAIN_HOST.match(u)
        if m:
            assert m.group(1).rstrip(".") == core.hostname(u), u
    assert crawler_api._PLAIN_HOST.match("https://a.com/") and not crawler_api._PLAIN_HOST.match("https://A.com/") and not crawler_api._PLAIN_HOST.match("https://a.com:8080/")


# ------------------------------------------------------------------ statistics snapshot ------------------------------------------------------------------

def test_snapshot_equals_exact_counts_and_keeps_types(db):
    populate(1)
    exact = engine_stats.counts()
    snap = engine_stats.snapshot_counts()
    assert snap == exact and set(snap) == {"discovered_domains", "discovered_urls", "outgoing_links", "domains_by_status", "urls_by_status", "urls_by_source"}
    assert all(isinstance(snap[k], int) for k in ("discovered_domains", "discovered_urls", "outgoing_links")) and None in snap["urls_by_source"]
    assert snap["discovered_urls"] == sum(snap["urls_by_status"].values()) == sum(snap["urls_by_source"].values())
    snap["urls_by_status"]["tampered"] = 1  # callers get copies
    assert "tampered" not in engine_stats.snapshot_counts()["urls_by_status"]


def test_concurrent_requests_share_one_refresh(db, monkeypatch):
    populate(1)
    calls, real = [], engine_stats._compute
    monkeypatch.setattr(engine_stats, "_compute", lambda c: (calls.append(1), time.sleep(0.4), real(c))[2])
    with ThreadPoolExecutor(16) as ex:
        out = list(ex.map(lambda _: engine_stats.snapshot_counts(), range(16)))
    assert len(calls) == 1 and all(o == out[0] for o in out)


def test_all_statistics_endpoints_reuse_one_snapshot(db, client, monkeypatch):
    populate(1)
    calls, real = [], engine_stats._compute
    monkeypatch.setattr(engine_stats, "_compute", lambda c: (calls.append(1), real(c))[1])
    for url in ("/discovery/statistics", "/crawler/global-status", "/crawler/discovered-urls", "/crawler/outgoing-links", "/discovery/statistics"):
        assert client.get(url).status_code == 200
    assert len(calls) == 1


def test_expiry_refreshes_and_staleness_is_bounded_by_the_ttl(db, monkeypatch):
    populate(1)
    monkeypatch.setattr(engine_stats, "STATS_TTL", 0.3)
    n0 = engine_stats.snapshot_counts()["discovered_urls"]
    with core.connect() as c:
        c.execute("INSERT INTO discovered_urls (url, normalized_url, domain, hostname) VALUES ('https://x.example/1','https://x.example/1','x.example','x.example')")
    assert engine_stats.snapshot_counts()["discovered_urls"] == n0  # within the TTL: the snapshot, by design
    time.sleep(0.4)
    assert engine_stats.snapshot_counts()["discovered_urls"] == n0 + 1  # expired: refreshed
    assert engine_stats.counts()["discovered_urls"] == n0 + 1


def test_refresh_in_progress_does_not_block_readers_that_have_an_older_snapshot(db, monkeypatch):
    populate(1)
    monkeypatch.setattr(engine_stats, "STATS_TTL", 0.2)
    old = engine_stats.snapshot_counts()
    time.sleep(0.3)
    real = engine_stats._compute
    monkeypatch.setattr(engine_stats, "_compute", lambda c: (time.sleep(1.5), real(c))[1])
    t = threading.Thread(target=engine_stats.snapshot_counts)
    t.start()
    time.sleep(0.2)
    t0 = time.monotonic()
    assert engine_stats.snapshot_counts() == old and time.monotonic() - t0 < 0.5  # served the old one at once
    t.join()


def test_snapshot_unavailable_gives_503_and_does_not_retry_every_request(db, client, monkeypatch):
    attempts = []
    def down():
        attempts.append(1)
        raise psycopg.OperationalError("database is down")
    monkeypatch.setattr(core, "connect", down)
    for _ in range(4):
        r = client.get("/discovery/statistics")
        assert r.status_code == 503 and "unavailable" in r.json()["detail"]
    assert len(attempts) == 1  # STATS_RETRY back-off: later requests fail fast


def test_database_down_serves_the_last_snapshot_flagged_stale_then_gives_up(db, client, monkeypatch):
    populate(1)
    good = client.get("/discovery/statistics")
    assert good.status_code == 200 and good.headers["X-Stats-Age"] == "0" and "X-Stats-Stale" not in good.headers
    monkeypatch.setattr(engine_stats, "STATS_TTL", 0.05)
    monkeypatch.setattr(core, "connect", lambda: (_ for _ in ()).throw(psycopg.OperationalError("down")))
    time.sleep(0.1)
    stale = client.get("/discovery/statistics")
    assert stale.status_code == 200 and stale.json() == good.json() and stale.headers["X-Stats-Stale"] == "1"
    monkeypatch.setattr(engine_stats, "STATS_MAX_STALE", 0.01)
    assert client.get("/discovery/statistics").status_code == 503
    engine_stats.reset_snapshot()


def test_ttl_configuration_is_clamped_to_10_30_seconds():
    import importlib, os
    old = os.environ.get("STATS_TTL_SECONDS")
    try:
        for given, want in (("1", 10.0), ("20", 20.0), ("999", 30.0)):
            os.environ["STATS_TTL_SECONDS"] = given
            assert importlib.reload(engine_stats).STATS_TTL == want
    finally:
        os.environ.pop("STATS_TTL_SECONDS", None) if old is None else os.environ.__setitem__("STATS_TTL_SECONDS", old)
        importlib.reload(engine_stats)


# ------------------------------------------------------------------ global-status, paging, instrumentation ------------------------------------------------------------------

def test_global_status_uses_one_connection_and_keeps_its_response(db, monkeypatch):
    populate(1)
    with core.connect() as c:
        c.execute("INSERT INTO worker_status (name, status, heartbeat, last_heartbeat_at) VALUES ('scheduler','idle','x',?), ('crawler','running','x','2020-01-01 00:00:00')", (core.now(),))
    for paused in (False, True):
        autonomous.set_paused(paused)
        engine_stats.reset_snapshot()
        opened, real = [], core.connect
        monkeypatch.setattr(core, "connect", lambda: (opened.append(1), real())[1])
        got = autonomous_api.global_status()
        assert len(opened) == 1  # was 3 (workers, is_paused, counts), refresh included
        got2 = autonomous_api.global_status()
        assert len(opened) == 2 and got2 == got  # warm: one connection, no statistics query
        monkeypatch.setattr(core, "connect", real)
        with core.connect() as c:  # the old implementation, for comparison
            workers = [{**r, "healthy": autonomous.worker_health(r)} for r in map(dict, c.execute("SELECT * FROM worker_status ORDER BY name"))]
        assert got == {"paused": autonomous.is_paused(), "workers": workers, **engine_stats.counts()} and got["paused"] is paused
        assert [w["healthy"] for w in got["workers"]] == [False, True]


def test_statistics_endpoint_opens_no_connection_when_the_snapshot_is_fresh(db, monkeypatch):
    populate(1)
    autonomous_api.discovery_statistics()
    monkeypatch.setattr(core, "connect", lambda: pytest.fail("a fresh snapshot must not touch the database"))
    assert autonomous_api.discovery_statistics()["discovered_urls"] > 0


def test_list_endpoints_totals_and_keyset_paging(db, client):
    populate(1)
    for path, table in (("/crawler/outgoing-links", "outgoing_links"), ("/crawler/discovered-urls", "discovered_urls")):
        with core.connect() as c:
            ids = [r[0] for r in c.execute(f"SELECT id FROM {table}" + (" WHERE is_active=1" if table == "outgoing_links" else "") + " ORDER BY id")]
        first = client.get(path + "?limit=1000").json()
        assert first["total"] == len(ids) and [i["id"] for i in first["items"]] == ids[:1000]
        got, after = [], 0
        while True:  # keyset: pages of 250 follow each other without gaps or overlap
            page = client.get(f"{path}?limit=250&after_id={after}").json()["items"]
            if not page:
                break
            got += [i["id"] for i in page]
            after = page[-1]["id"]
        assert got == ids
    off = client.get("/crawler/outgoing-links?limit=5&offset=10").json()
    assert [i["id"] for i in off["items"]] == ids_of("outgoing_links", True)[10:15]
    assert client.get("/crawler/outgoing-links?offset=5&after_id=3").status_code == 422
    # filtered totals are exact counts, not the snapshot
    with core.connect() as c:
        n = c.execute("SELECT COUNT(*) FROM outgoing_links WHERE destination_domain=?", (TARGET,)).fetchone()[0]
        m = c.execute("SELECT COUNT(*) FROM outgoing_links").fetchone()[0]
        d = c.execute("SELECT COUNT(*) FROM discovered_urls WHERE domain='s1.example'").fetchone()[0]
    assert client.get(f"/crawler/outgoing-links?destination_domain={TARGET}&active_only=false&limit=1").json()["total"] == n
    assert client.get("/crawler/outgoing-links?active_only=false&limit=1").json()["total"] == m
    assert client.get("/crawler/discovered-urls?domain=s1.example&limit=1").json()["total"] == d


def ids_of(table, active):
    with core.connect() as c:
        return [r[0] for r in c.execute(f"SELECT id FROM {table}" + (" WHERE is_active=1" if active else "") + " ORDER BY id")]


def test_server_timing_header_and_logging_are_safe(db, client, caplog, monkeypatch):
    populate(1)
    r = client.get(f"/backlinks/{TARGET}?limit=5")
    st = r.headers["Server-Timing"]
    assert re.fullmatch(r'db_connect;dur=[\d.]+;desc="1 connection\(s\)", db_sql;dur=[\d.]+, db_fetch;dur=[\d.]+, app;dur=[\d.]+;desc="python, serialization", total;dur=[\d.]+', st), st
    parts = {k: float(v) for k, v in re.findall(r"(db_connect|db_sql|db_fetch|total);dur=([\d.]+)", st)}
    assert parts["db_sql"] > 0 and parts["total"] >= parts["db_connect"] + parts["db_sql"] + parts["db_fetch"] - 0.5
    assert "SELECT" not in st
    monkeypatch.setattr(api_timing, "SLOW_MS", 0)  # log every request
    seen = []
    class Grab(logging.Handler):  # attached to the logger itself: backlink_api sets propagate=False on the parent, which would hide it from caplog
        def emit(self, record):
            seen.append(record.getMessage())
    h = Grab()
    api_timing.log.addHandler(h)
    try:
        client.get(f"/backlinks/{TARGET}?limit=5&api_key=SECRET123&password=hunter2")
    finally:
        api_timing.log.removeHandler(h)
    line = " ".join(m for m in seen if "SLOW-API" in m)
    assert line
    assert "/backlinks/{target_domain}" in line and "SECRET123" not in line and "hunter2" not in line and "limit=5" not in line and TARGET not in line
    with api_timing.connect() as c:  # outside a request: plain core.connect(), no timing wrapper
        assert type(c.cursor()) is core.Cursor


def test_simultaneous_requests_return_the_same_answers(db, client):
    populate(1)
    urls = [f"/backlinks/{TARGET}?limit=50", f"/backlinks/{SUB}?limit=50&include_removed=true", f"/backlinks/{TARGET}/referring-domains", f"/backlinks/{TARGET}/anchors",
            f"/backlinks/{TARGET}/history?limit=100", "/discovery/statistics", "/crawler/global-status", "/crawler/outgoing-links?limit=20", "/crawler/discovered-urls?limit=20"]
    app = client.app
    want = {u: TestClient(app).get(u) for u in urls}
    assert all(r.status_code == 200 for r in want.values())
    def hit(u):
        return u, TestClient(app).get(u)
    with ThreadPoolExecutor(12) as ex:
        for u, r in ex.map(hit, urls * 4):
            assert r.status_code == 200 and r.json() == want[u].json(), u


# ------------------------------------------------------------------ bounded lists, keyset paging, explicit columns ------------------------------------------------------------------

def test_unbounded_endpoints_have_a_default_limit_and_a_maximum(db, client):
    import inspect
    for fn in (crawler_api.backlinks, crawler_api.all_backlinks, crawler_api.referring_domains, crawler_api.backlink_anchors,
               crawler_api.competitor_backlinks, crawler_api.competitor_referring_domains, crawler_api.competitor_anchors):
        assert inspect.signature(fn).parameters["limit"].default == crawler_api.DEFAULT_LIMIT, fn.__name__
    for path in (f"/backlinks/{TARGET}", "/backlinks", f"/backlinks/{TARGET}/referring-domains", f"/backlinks/{TARGET}/anchors"):
        assert client.get(path + "?limit=0").status_code == 422, path
        assert client.get(path + f"?limit={crawler_api.MAX_LIMIT + 1}").status_code == 422, path
        assert client.get(path + "?offset=-1").status_code == 422, path
    assert client.get(f"/backlinks/{TARGET}?offset=5&after_id=3").status_code == 422


def test_omitting_limit_returns_one_bounded_page_and_keyset_walks_every_link_once(db, client):
    populate(10)  # thousands of links into TARGET: more than one default page
    full = client.get(f"/backlinks/{TARGET}?include_removed=true&limit={crawler_api.MAX_LIMIT}").json()
    first = client.get(f"/backlinks/{TARGET}?include_removed=true").json()  # no limit given
    assert full["total_backlinks"] > crawler_api.DEFAULT_LIMIT, "fixture too small to show truncation"
    assert first["limit"] == crawler_api.DEFAULT_LIMIT and len(first["backlinks"]) == crawler_api.DEFAULT_LIMIT
    assert first["total_backlinks"] == full["total_backlinks"] and first["backlinks"] == full["backlinks"][:crawler_api.DEFAULT_LIMIT]
    assert first["next_after_id"] is not None
    got, after = [], None
    while True:  # follow next_after_id until the last page (null)
        r = client.get(f"/backlinks/{TARGET}?include_removed=true&limit=700" + (f"&after_id={after}" if after else "")).json()
        got += r["backlinks"]
        after = r["next_after_id"]
        if after is None:
            break
    assert got == full["backlinks"]  # same rows, same order, no gap, no overlap
    assert client.get(f"/backlinks/{TARGET}?limit=5&offset=10").json()["backlinks"] == client.get(f"/backlinks/{TARGET}?limit=15").json()["backlinks"][10:]  # offset clients still work
    assert full["next_after_id"] is None  # everything fitted in one page


def test_all_backlinks_default_page_is_bounded_and_reports_next_offset(db, client):
    populate(10)
    with core.connect() as c:  # /backlinks lists the links of monitored targets only
        c.execute("INSERT INTO businesses (domain, search_interval_seconds, next_run_at) VALUES (?, 3600, '2099-01-01 00:00:00')", (TARGET,))
    r = client.get("/backlinks?include_removed=true").json()
    assert r["limit"] == crawler_api.DEFAULT_LIMIT and len(r["backlinks"]) == crawler_api.DEFAULT_LIMIT and r["total_backlinks"] > crawler_api.DEFAULT_LIMIT
    assert r["next_offset"] == crawler_api.DEFAULT_LIMIT
    rest = client.get(f"/backlinks?include_removed=true&limit={crawler_api.MAX_LIMIT}").json()
    assert rest["next_offset"] is None and rest["backlinks"][:crawler_api.DEFAULT_LIMIT] == r["backlinks"]


@pytest.mark.parametrize("path,key,ident", [("referring-domains", "referring_domains", "domain"), ("anchors", "anchors", "anchor_text")])
def test_referring_domains_and_anchors_are_paged_with_exact_totals(db, client, path, key, ident):
    populate(1)
    everything = client.get(f"/backlinks/{TARGET}/{path}?limit={crawler_api.MAX_LIMIT}").json()
    total = everything["total_referring_domains" if path == "referring-domains" else "total_anchors"]
    assert total == len(everything[key]) > 3
    paged = []
    for offset in range(0, total + 4, 4):
        page = client.get(f"/backlinks/{TARGET}/{path}?limit=4&offset={offset}").json()
        assert page["total_referring_domains" if path == "referring-domains" else "total_anchors"] == total  # the total is the group count, not the page size
        assert len(page[key]) <= 4
        paged += page[key]
    assert paged == everything[key]
    past = client.get(f"/backlinks/{TARGET}/{path}?limit=4&offset=10000").json()  # past the end: empty page, true total
    assert past[key] == [] and past["total_referring_domains" if path == "referring-domains" else "total_anchors"] == total
    assert past.get("status", "ok") == "ok"
    nothing = client.get(f"/backlinks/nothing-here.org/{path}").json()
    assert nothing[key] == [] and nothing["total_referring_domains" if path == "referring-domains" else "total_anchors"] == 0


def test_competitor_endpoints_forward_the_same_bounds(db, client):
    populate(1)
    with core.connect() as c:
        c.execute("INSERT INTO business_competitors (business_domain, competitor_domain) VALUES ('owner.example', ?)", (TARGET,))
    a = client.get(f"/competitors/{TARGET}/backlinks?limit=5").json()
    b = client.get(f"/backlinks/{TARGET}?limit=5").json()
    assert a == {**b, "target_type": "competitor"} and len(a["backlinks"]) == 5
    assert client.get(f"/competitors/{TARGET}/backlinks?after_id={a['next_after_id']}&limit=5").json()["backlinks"] == client.get(f"/backlinks/{TARGET}?limit=10").json()["backlinks"][5:]
    assert client.get(f"/competitors/{TARGET}/referring-domains?limit=2").json() == client.get(f"/backlinks/{TARGET}/referring-domains?limit=2").json()
    assert len(client.get(f"/competitors/{TARGET}/anchors?limit=1").json()["anchors"]) == 1


def _columns(table):
    with core.connect() as c:
        return {r[0] for r in c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=?", (table,))}


def test_list_items_keep_every_column_the_select_star_returned_except_the_heavy_text(db, client):
    populate(1)
    with core.connect() as c:
        c.execute("UPDATE outgoing_links SET context='some surrounding text', img_alt='alt text' WHERE id=(SELECT MIN(id) FROM outgoing_links)")
        c.execute("UPDATE discovered_urls SET title='t', crawl_error='e' WHERE id=(SELECT MIN(id) FROM discovered_urls)")
    heavy = {"context", "img_alt"}
    light = client.get("/crawler/outgoing-links?limit=3").json()["items"]
    assert light and set(light[0]) == _columns("outgoing_links") - heavy
    full = client.get("/crawler/outgoing-links?limit=3&include_details=true").json()["items"]
    assert set(full[0]) == _columns("outgoing_links") and full[0]["context"] == "some surrounding text" and full[0]["img_alt"] == "alt text"
    assert [{k: v for k, v in r.items() if k not in heavy} for r in full] == light  # the same rows in the same order
    for path, table in (("/crawler/discovered-urls", "discovered_urls"), ("/crawler/discovered-domains", "discovered_domains")):
        items = client.get(path + "?limit=3").json()["items"]
        assert items and set(items[0]) == _columns(table), path  # nothing dropped, nothing new


def test_discovered_domains_keyset_paging_and_offset_compat(db, client):
    populate(1)
    with core.connect() as c:
        ids = [r[0] for r in c.execute("SELECT id FROM discovered_domains ORDER BY id")]
    got, after = [], 0
    while True:
        page = client.get(f"/crawler/discovered-domains?limit=3&after_id={after}").json()["items"]
        if not page:
            break
        got += [i["id"] for i in page]
        after = page[-1]["id"]
    assert got == ids
    assert [i["id"] for i in client.get("/crawler/discovered-domains?limit=3&offset=2").json()["items"]] == ids[2:5]
    assert client.get("/crawler/discovered-domains?offset=1&after_id=1").status_code == 422
    with core.connect() as c:
        one = c.execute("SELECT discovery_status FROM discovered_domains WHERE id=?", (ids[0],)).fetchone()[0]
    filtered = client.get(f"/crawler/discovered-domains?discovery_status={one}&limit=2&after_id={ids[0]}").json()
    assert all(i["discovery_status"] == one and i["id"] > ids[0] for i in filtered["items"])
    with core.connect() as c:
        assert filtered["total"] == c.execute("SELECT COUNT(*) FROM discovered_domains WHERE discovery_status=?", (one,)).fetchone()[0]


def test_record_backlink_explicit_columns_keep_the_event_sequence(db):
    with core.connect() as c:
        tid = core.add_target(c, "codigix.co")
        core.record_backlink(c, tid, "https://a.example/p", "https://codigix.co/", "x", None)
        core.record_backlink(c, tid, "https://a.example/p", "https://codigix.co/", "y", "nofollow")  # anchor/rel changed -> link_changed
        c.execute("UPDATE backlinks SET is_active=0, link_status='removed'")
        core.record_backlink(c, tid, "https://a.example/p", "https://codigix.co/", "y", "nofollow")  # -> reappeared
        events = [r[0] for r in c.execute("SELECT event_type FROM backlink_history ORDER BY id")]
    assert events == ["first_discovered", "verified", "link_changed", "reappeared"]

"""Phase 12: SQL-level comparison and paging of the backlink gap, keyset paging of the list endpoints, the rewritten aggregates.
Every rewritten query is compared with the algorithm / SQL it replaced (verbatim oracles below) on the same data. Real PostgreSQL, isolated schema."""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api_timing
import business_jobs as bj
import competitors
import crawler_api
import crawler_core as core
from gap_reference import gap_reference
from read_cache import TTLCache
from test_api_perf import client, db  # noqa: F401 - fixtures

OWN, CA, CB, CC, OTHER = "own-biz.com", "comp-a.com", "comp-b.com", "comp-c.com", "unrelated.org"
SRC = ["s1.net", "s2.net", "s3.net", "s10.net", "a.net", "a-b.net", "a_c.net", "Zeta.net", "alpha.net", "mix-1.org", "mix-2.org"] + [f"d{i}.com" for i in range(25)]


POOL = {OWN: SRC[:12] + ["own-only1.net", "own-only2.net"], CA: SRC[6:22], CB: SRC[10:30], CC: SRC[:8] + SRC[26:], OTHER: SRC}


def build(c, n=1100, seed=7):
    """Owner + three competitors and a random graph over them: inactive and non-html links, NULL source domains, the target's own pages, one page linking
    several competitors, pages linking the owner through an inactive link only, several anchors for one source/destination."""
    bj.create_job(c, OWN)
    for comp in (CA, CB, CC):
        competitors.register(c, OWN, comp)
    rnd = random.Random(seed)
    for i in range(n):
        dest = rnd.choices([OWN, CA, CB, CC, OTHER], [2, 4, 3, 2, 1])[0]
        sd = rnd.choice(POOL[dest] + [dest] * 2)  # each target has its own set of linking domains (overlapping), sometimes its own domain: never a backlink
        if rnd.random() < 0.03:
            sd = None
        page = f"https://{sd or 'nullhost.net'}/p{rnd.randrange(12)}"
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, rel_attributes, link_type, is_active, is_nofollow, source_domain,"
                  " first_seen, last_seen, lost_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                  (page, f"https://{dest}/x{rnd.randrange(8)}", dest, rnd.choice(["", "a", "b", "Same", "same"]), rnd.choice([None, "nofollow", "ugc"]),
                   "html" if rnd.random() > .05 else "image", 1 if rnd.random() > .15 else 0, rnd.randrange(2), sd,
                   f"2026-01-{1 + i % 28:02d} 00:00:00", f"2026-02-{1 + i % 28:02d} 00:00:00", None if rnd.random() > .1 else "2026-03-01 00:00:00"))


class Spy:
    """Wraps a connection and records the biggest number of rows any statement handed to Python."""

    def __init__(self, conn):
        self.conn, self.max_rows = conn, 0

    def execute(self, *a, **k):
        spy, cur = self, self.conn.execute(*a, **k)

        class Cur:
            def fetchall(self):
                rows = cur.fetchall()
                spy.max_rows = max(spy.max_rows, len(rows))
                return rows

            def fetchone(self):
                return cur.fetchone()

            def __iter__(self):
                return iter(self.fetchall())
        return Cur()


def strip(g: dict) -> dict:
    return {k: v for k, v in g.items() if k != "pagination"}


# ------------------------------------------------------------------ backlink gap ------------------------------------------------------------------

@pytest.mark.parametrize("only", [None, CA, CC])
@pytest.mark.parametrize("limit", [1, 5, 200, 2000])
def test_gap_equals_the_replaced_python_implementation(db, only, limit):
    with core.connect() as c:
        build(c)
        want = gap_reference(c, OWN, only, limit)
        got = competitors.gap(c, OWN, only, limit)
    assert want["summary"]["gap_domains"] > 0 and want["summary"]["shared_domains"] > 0 and want["summary"]["own_only_domains"] > 0 and want["summary"]["gap_pages"] > 0
    assert strip(got) == want  # same lists, same order, same counts, same nested links


def test_gap_with_no_competitor_data_and_for_a_business_without_competitors(db):
    with core.connect() as c:
        bj.create_job(c, OWN)
        assert strip(competitors.gap(c, OWN, None, 10)) == gap_reference(c, OWN, None, 10)  # nothing at all
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, source_domain) VALUES ('https://x.net/1', ?, ?, 'x.net')", (f"https://{OWN}/", OWN))
        g = competitors.gap(c, OWN, None, 10)
        assert strip(g) == gap_reference(c, OWN, None, 10) and g["own_only_domains"] == ["x.net"] and g["competitors"] == []
        with pytest.raises(LookupError):
            competitors.gap(c, "never-onboarded.com")


def test_gap_pages_through_offset_and_cursors_with_no_missing_and_no_duplicate_entries(db):
    with core.connect() as c:
        build(c)
        full = gap_reference(c, OWN, None, 100000)
    keys = {"opportunities": lambda e: e["referring_domain"], "shared_domains": lambda e: e, "own_only_domains": lambda e: e, "source_page_gap": lambda e: e["source_url"]}
    for lim in (1, 7, 50):
        for mode in ("offset", "cursor"):
            seen = {k: [] for k in keys}
            offset, after, done = 0, {}, set()
            for _ in range(2000):
                with core.connect() as c:
                    g = competitors.gap(c, OWN, None, lim, offset if mode == "offset" else 0, after if mode == "cursor" else None)
                lists = g["pagination"]["lists"]
                for k in keys:
                    assert len(g[k]) <= lim
                    if k not in done:  # a list whose has_more was false is finished: a client stops asking for it (its cursor would return its last entries again)
                        seen[k] += g[k]
                    if not lists[k]["has_more"]:
                        done.add(k)
                assert g["pagination"]["has_more"] == any(v["has_more"] for v in lists.values())
                assert all(v["total"] == len(full[k]) if k != "source_page_gap" else v["total"] == full["summary"]["gap_pages"] for k, v in lists.items())
                if not g["pagination"]["has_more"]:
                    assert g["pagination"]["next_offset"] is None
                    break
                offset = g["pagination"]["next_offset"]
                after = {**after, **{k: v["next_after"] for k, v in lists.items() if v["has_more"]}}
            for k in keys:
                assert seen[k] == full[k], (lim, mode, k)  # all pages together == the unpaginated answer: nothing missing, nothing twice, same order
                assert len({keys[k](e) for e in seen[k]}) == len(seen[k])


def test_gap_empty_page_past_the_end_and_invalid_parameters(db, client):
    with core.connect() as c:
        build(c)
        g = competitors.gap(c, OWN, None, 10, offset=100000)
    assert g["opportunities"] == g["shared_domains"] == g["own_only_domains"] == g["source_page_gap"] == [] and g["pagination"]["has_more"] is False
    assert g["summary"]["gap_domains"] > 0  # the summary still describes the whole comparison
    for q in ("limit=0", "limit=2001", "offset=-1", f"offset=1&after_page=x"):
        assert client.get(f"/businesses/{OWN}/backlink-gap?{q}").status_code == 422
    assert client.get(f"/businesses/{OWN}/backlink-gap?limit=3").status_code == 200
    assert client.get(f"/businesses/{OWN}/backlink-gap?limit=3&competitor=unknown.com").status_code == 404


def test_gap_route_cursor_walk_over_http_equals_the_unpaginated_route(db, client):
    with core.connect() as c:
        build(c)
    full = client.get(f"/businesses/{OWN}/backlink-gap?limit=2000").json()
    got, after = [], None
    for _ in range(500):
        q = f"limit=4" + (f"&after_opportunity={after}" if after else "")
        g = client.get(f"/businesses/{OWN}/backlink-gap?{q}").json()
        got += g["opportunities"]
        after = g["pagination"]["lists"]["opportunities"]["next_after"]
        if not after:
            break
    assert got == full["opportunities"] and len(got) == full["summary"]["gap_domains"]


def test_gap_never_loads_the_link_sets_into_python(db):
    with core.connect() as c:
        build(c, n=3000)
        spy = Spy(c)
        g = competitors.gap(spy, OWN, None, 5)
        total_links = c.execute("SELECT COUNT(*) FROM outgoing_links WHERE link_type='html' AND is_active=1").fetchone()[0]
    assert g["summary"]["gap_pages"] > 100 and total_links > 1500
    assert spy.max_rows <= 6 + 20, spy.max_rows  # limit+1 rows of the biggest page query, plus the few links of the returned pages: nowhere near the graph


# ------------------------------------------------------------------ global backlinks ------------------------------------------------------------------

def legacy_all(c, target_type, include_removed):
    """GET /backlinks before Phase 12 (verbatim SQL): total, per-target aggregates, every row in the old order."""
    cond = (" FROM outgoing_links l JOIN businesses b ON b.domain=l.destination_domain WHERE l.link_type='html' AND l.source_domain<>b.domain"
            + ("" if include_removed else " AND l.is_active=1") + (" AND b.target_type=?" if target_type else ""))
    args = [target_type] if target_type else []
    total = c.execute("SELECT COUNT(*)" + cond, args).fetchone()[0]
    rows = c.execute("SELECT l.id, l.destination_domain, l.source_url, b.target_type" + cond + " ORDER BY b.target_type, l.destination_domain, l.id", args).fetchall()
    per = c.execute("SELECT b.domain, b.target_type, COUNT(l.id) AS backlinks, COUNT(DISTINCT l.source_domain) AS referring_domains FROM businesses b"
                    " LEFT JOIN outgoing_links l ON l.destination_domain=b.domain AND l.link_type='html' AND l.source_domain<>b.domain"
                    + ("" if include_removed else " AND l.is_active=1") + (" WHERE b.target_type=?" if target_type else "")
                    + " GROUP BY b.domain, b.target_type ORDER BY b.target_type, b.domain", args).fetchall()
    return total, [dict(r) for r in per], [(r["target_type"], r["destination_domain"], r["source_url"]) for r in rows]


@pytest.mark.parametrize("target_type", [None, "own_business", "competitor"])
@pytest.mark.parametrize("include_removed", [False, True])
def test_all_backlinks_totals_aggregates_and_pages_equal_the_replaced_query(db, client, target_type, include_removed):
    with core.connect() as c:
        build(c)
        total, per, rows = legacy_all(c, target_type, include_removed)
    base = f"/backlinks?include_removed={str(include_removed).lower()}" + (f"&target_type={target_type}" if target_type else "")
    first = client.get(base + "&limit=1").json()
    assert first["total_backlinks"] == total and first["targets"] == per and total > 50
    for mode in ("offset", "cursor"):
        got, off, cur = [], 0, None
        for _ in range(1000):
            r = client.get(base + "&limit=37" + (f"&offset={off}" if mode == "offset" else (f"&cursor={cur}" if cur else ""))).json()
            got += [(b["target_type"], b["target_domain"], b["source_url"]) for b in r["backlinks"]]
            assert r["total_backlinks"] == total and r["targets"] == per
            if not r["has_more"]:
                assert r["next_offset"] is None and r["next_cursor"] is None
                break
            off, cur = r["next_offset"], r["next_cursor"]
        assert got == rows, mode  # in the old order, none missing, none twice
    assert sum(t["backlinks"] for t in per) == total


def test_all_backlinks_cursor_validation(db, client):
    with core.connect() as c:
        build(c)
    for bad in ("garbage", "competitor:x", "nonsense:comp-a.com:5", "competitor:comp-a.com:abc"):
        assert client.get(f"/backlinks?cursor={bad}").status_code == 422
    assert client.get("/backlinks?cursor=competitor:comp-a.com:1&offset=5").status_code == 422
    assert client.get("/backlinks?cursor=own_business:zzz.com:99999999&limit=5").json()["backlinks"] == []  # past the end: empty, not an error


# ------------------------------------------------------------------ target pages, referring domains, anchors ------------------------------------------------------------------

def walk(client, path, key, step, total_key=None):
    """All items of a list endpoint through both paging styles; returns {mode: items}."""
    out = {}
    for mode in ("offset", "cursor"):
        got, nxt = [], None
        for _ in range(3000):
            sep = "&" if "?" in path else "?"
            r = client.get(f"{path}{sep}limit={step}" + (f"&offset={nxt}" if mode == "offset" and nxt else "") + (f"&cursor={nxt}" if mode == "cursor" and nxt else "")).json()
            got += r[key]
            if total_key:
                assert r[total_key] >= len(got)
            if not r["has_more"]:
                assert r["next_offset"] is None and r["next_cursor"] is None
                break
            nxt = r["next_offset"] if mode == "offset" else r["next_cursor"]
        out[mode] = got
    return out


@pytest.mark.parametrize("route,key,total_key", [("referring-domains", "referring_domains", "total_referring_domains"), ("anchors", "anchors", "total_anchors")])
def test_grouped_routes_page_deterministically_with_ties_through_offset_and_cursor(db, client, route, key, total_key):
    with core.connect() as c:
        build(c)
    path = f"/backlinks/{CB}/{route}"
    full = client.get(path + "?limit=10000").json()
    assert len(full[key]) > 4 and len({tuple(sorted(i.items())) for i in full[key]}) == len(full[key])
    counts = [i.get("backlinks", i.get("count")) for i in full[key]]
    assert counts == sorted(counts, reverse=True) and (route == "anchors" or len(set(counts)) < len(counts))  # sorted by size, with ties among the domains
    for step in (1, 3, 8):
        got = walk(client, path, key, step, total_key)
        assert got["offset"] == full[key] and got["cursor"] == full[key], step
    r = client.get(path + "?limit=3").json()
    assert r[total_key] == full[total_key] and r["has_more"] is True and r["next_offset"] == 3 and r["next_cursor"]
    after = client.get(path + f"?limit=3&cursor={r['next_cursor']}").json()
    assert after[total_key] == full[total_key] and after[key] == full[key][3:6]  # the total counts every group, not only the groups after the cursor
    assert client.get(path + "?limit=3&cursor=0:0").json()[key] == [] and client.get(path + "?limit=3&cursor=0:0").json()[total_key] == full[total_key]
    assert client.get(path + "?cursor=nope").status_code == 422 and client.get(path + "?cursor=1:1&offset=1").status_code == 422
    assert client.get(path + "?limit=0").status_code == 422 and client.get(path + "?offset=-1").status_code == 422


def test_target_backlinks_keyset_and_offset_cover_every_row_once(db, client):
    with core.connect() as c:
        build(c)
        # the API's contract: ONE backlink per source page (its first link into the target); raw link rows are counted separately below
        base = "FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1 AND source_domain<>?"
        want = [r[0] for r in c.execute("SELECT source_url||'|'||destination_url||'|'||anchor_text " + base + " AND id IN (SELECT MIN(id) " + base + " GROUP BY source_url) ORDER BY id", (CA, CA, CA, CA))]
        raw = c.execute("SELECT COUNT(*) " + base, (CA, CA)).fetchone()[0]
    assert raw > len(want)  # pages that link the target several times are still one backlink each
    assert len(want) > 40
    for limit in (1, 7, 100):
        for style in ("after_id", "offset"):
            got, nxt = [], None
            for _ in range(3000):
                r = client.get(f"/backlinks/{CA}?limit={limit}" + (f"&{style}={nxt}" if nxt else "")).json()
                got += [f"{b['source_url']}|{b['destination_url']}|{b['anchor_text']}" for b in r["backlinks"]]
                assert r["total_backlinks"] == len(want)
                if not r["has_more"]:
                    assert r["next_after_id"] is None and r["next_offset"] is None
                    break
                nxt = r["next_after_id"] if style == "after_id" else r["next_offset"]
            assert got == want, (limit, style)  # the exact-full last page ends the walk without an extra empty page
    assert client.get(f"/backlinks/{CA}?after_id=1&offset=1").status_code == 422
    r = client.get(f"/competitors/{CA}/backlinks?limit=2").json()
    assert client.get(f"/competitors/{CA}/backlinks?limit=2&after_id={r['next_after_id']}").json()["backlinks"] == client.get(f"/backlinks/{CA}?limit=4").json()["backlinks"][2:]


def test_pages_are_cut_in_sql_not_in_python(db):
    with core.connect() as c:
        build(c, n=2500)
        spy = Spy(c)
        crawler_api._graph_page(spy, CB, True, 6, 0, None)
        assert spy.max_rows <= 6
        spy = Spy(c)
        crawler_api._grouped_page(spy, "source_domain AS domain, COUNT(*) AS backlinks", *crawler_api._graph_cond(CB)[:1], crawler_api._graph_cond(CB)[1], "source_domain", 4, 0)
        assert spy.max_rows <= 5


# ------------------------------------------------------------------ aggregates ------------------------------------------------------------------

def refs_independent(c, target, active=True):
    return c.execute("SELECT COUNT(DISTINCT source_domain) FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND source_domain<>?"
                     + (" AND is_active=1" if active else ""), (target, target)).fetchone()[0]


def test_summary_counts_match_independent_sql_after_links_are_removed_and_reappear(db):
    with core.connect() as c:
        build(c)
        for active in (True, False):
            t, v, refs = crawler_api._graph_summary(c, CA, active)
            assert refs == refs_independent(c, CA, active) and t == c.execute(
                "SELECT COUNT(DISTINCT source_url) FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND source_domain<>?" + (" AND is_active=1" if active else ""), (CA, CA)).fetchone()[0]
        _, _, before = crawler_api._graph_summary(c, CA, True)
        doms = [r[0] for r in c.execute("SELECT DISTINCT source_domain FROM outgoing_links WHERE destination_domain=? AND is_active=1 AND source_domain<>? ORDER BY 1 LIMIT 3", (CA, CA))]
        c.execute("UPDATE outgoing_links SET is_active=0, lost_at='2026-04-01 00:00:00' WHERE destination_domain=? AND source_domain=ANY(?)", (CA, doms))  # removed
        _, _, gone = crawler_api._graph_summary(c, CA, True)
        assert gone == refs_independent(c, CA) < before
        c.execute("UPDATE outgoing_links SET is_active=1, lost_at=NULL WHERE destination_domain=? AND source_domain=ANY(?)", (CA, doms))  # they come back
        assert crawler_api._graph_summary(c, CA, True)[2] == before == refs_independent(c, CA)
        # a subdomain target with a NULL source domain still counts that as one referring domain (the old COUNT(DISTINCT) + 1 rule)
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, source_domain) VALUES ('https://nul.example/a', ?, ?, NULL)", (f"https://blog.{CA}/n", CA))
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, source_domain) VALUES ('https://nul2.example/a', ?, ?, NULL)", (f"https://blog.{CA}/m", CA))
        sub = crawler_api._graph_summary(c, f"blog.{CA}", True)
        assert sub[0] == 2 and sub[2] == 1


# ------------------------------------------------------------------ cache ------------------------------------------------------------------

@pytest.fixture()
def cache(monkeypatch):
    now = [1000.0]
    c = TTLCache(enabled=True, ttl=15, max_ttl=30, clock=lambda: now[0])
    c.now = now
    monkeypatch.setattr(crawler_api, "graph_cache", c)
    return c


def test_cache_keys_separate_targets_filters_pages_and_cursors(db, client, cache):
    with core.connect() as c:
        build(c)
    a = client.get(f"/backlinks/{CA}/referring-domains?limit=3")
    assert a.headers["X-Cache"] == "MISS" and client.get(f"/backlinks/{CA}/referring-domains?limit=3").headers["X-Cache"] == "HIT"
    assert client.get(f"/backlinks/{CB}/referring-domains?limit=3").headers["X-Cache"] == "MISS"  # another target
    assert client.get(f"/backlinks/{CA}/referring-domains?limit=4").headers["X-Cache"] == "MISS"  # another page size
    assert client.get(f"/backlinks/{CA}/referring-domains?limit=3&offset=3").headers["X-Cache"] == "MISS"  # another page
    cur = a.json()["next_cursor"]
    c1 = client.get(f"/backlinks/{CA}/referring-domains?limit=3&cursor={cur}")
    assert c1.headers["X-Cache"] == "MISS" and c1.json()["referring_domains"] == client.get(f"/backlinks/{CA}/referring-domains?limit=6").json()["referring_domains"][3:]
    assert client.get(f"/backlinks/{CA}/referring-domains?limit=3&cursor={cur}").headers["X-Cache"] == "HIT"
    assert client.get(f"/backlinks/{CA}/anchors?limit=3").headers["X-Cache"] == "MISS"  # another kind
    s1, s2 = client.get(f"/backlinks/{CA}?limit=1"), client.get(f"/backlinks/{CA}?limit=1&include_removed=true")  # another filter: different totals
    assert s1.json()["total_backlinks"] < s2.json()["total_backlinks"] and s2.headers["X-Cache"] == "MISS"
    assert client.get(f"/backlinks/blog.{CA}?limit=1").headers["X-Cache"] == "MISS"  # a subdomain target never shares the registrable domain's entry
    assert client.get(f"/competitors/{CA}/backlinks?limit=1").headers["X-Cache"] == "HIT"  # a competitor shares the target's entry on purpose: same data
    cache.now[0] += 16
    assert client.get(f"/backlinks/{CA}/referring-domains?limit=3").headers["X-Cache"] == "MISS"  # expired entries are never served as fresh


def test_cached_and_uncached_responses_are_identical_and_not_shared_by_reference(db, client, cache, monkeypatch):
    with core.connect() as c:
        build(c)
    paths = [f"/backlinks/{CA}?limit=5", f"/backlinks/{CA}/referring-domains?limit=5", f"/backlinks/{CA}/anchors?limit=5", f"/backlinks/{CA}/referring-domains?limit=5&offset=2"]
    hot = [(client.get(p).json(), client.get(p).json()) for p in paths]
    monkeypatch.setattr(crawler_api, "graph_cache", TTLCache())  # off
    for p, (miss, hit) in zip(paths, hot):
        off = client.get(p)
        assert "X-Cache" not in off.headers and off.json() == miss == hit
    # a caller that mutates its answer cannot change the cache
    v, _, _ = cache.get(("k",), lambda: {"a": [1]})
    v["a"].append(2)
    assert cache.get(("k",), lambda: 1 / 0)[0] == {"a": [1]}


# ------------------------------------------------------------------ concurrency ------------------------------------------------------------------

def test_concurrent_hot_requests_return_identical_data_and_leak_no_connection(db, monkeypatch):
    monkeypatch.setattr(core, "POOL_MAX", 4)
    monkeypatch.setattr(core, "POOL_MIN", 1)
    monkeypatch.setattr(core, "DB_POOL_WAIT", 20)
    monkeypatch.setattr(api_timing, "API_DB_WAIT", 20)
    with core.connect() as c:
        build(c, n=1500)
    app = FastAPI()
    app.include_router(crawler_api.router)
    api_timing.install(app)
    urls = [f"/backlinks/{CA}?limit=20", f"/backlinks/{CB}?limit=20", f"/backlinks/{CA}/referring-domains?limit=5", f"/backlinks/{CB}/anchors?limit=5",
            f"/competitors/{CA}/backlinks?limit=20&offset=10", "/backlinks?limit=10", f"/businesses/{OWN}/backlink-gap?limit=5"]
    want = {u: TestClient(app).get(u).json() for u in urls}
    peak, stop = [0], threading.Event()

    def watch():
        while not stop.is_set():
            peak[0] = max(peak[0], core.pool_stats()["pool_size"])
            time.sleep(.005)
    w = threading.Thread(target=watch)
    w.start()
    try:
        with ThreadPoolExecutor(20) as ex:
            res = list(ex.map(lambda u: (u, TestClient(app).get(u)), urls * 8))
    finally:
        stop.set()
        w.join()
    assert all(r.status_code == 200 and r.json() == want[u] for u, r in res)  # no deadlock, no missing or duplicated data
    assert peak[0] <= 4 and core.OPEN_CONNS == 0 and core.pool_stats()["pool_available"] == core.pool_stats()["pool_size"]  # everything came back
    core.close_pool()


def test_a_slow_gap_request_under_a_busy_pool_answers_503_not_a_hang(db, monkeypatch):
    monkeypatch.setattr(core, "POOL_MAX", 1)
    monkeypatch.setattr(core, "DB_POOL_WAIT", 0.2)
    monkeypatch.setattr(api_timing, "API_DB_WAIT", 0.2)
    app = FastAPI()
    app.include_router(crawler_api.router)
    api_timing.install(app)
    hold = core.connect()
    try:
        t0 = time.monotonic()
        r = TestClient(app).get(f"/businesses/{OWN}/backlink-gap")
        assert r.status_code == 503 and r.headers["Retry-After"] and time.monotonic() - t0 < 5
    finally:
        hold.close()
        core.close_pool()


# ------------------------------------------------------------------ other lists ------------------------------------------------------------------

def test_competitor_listing_pages_in_sql_in_registration_order(db, client):
    with core.connect() as c:
        bj.create_job(c, OWN)
        for comp in (CA, CB, CC, "comp-d.com"):
            competitors.register(c, OWN, comp)
    full = client.get(f"/businesses/{OWN}/competitors").json()["competitors"]
    assert [x["competitor_domain"] for x in full] == [CA, CB, CC, "comp-d.com"]
    assert [x["competitor_domain"] for x in client.get(f"/businesses/{OWN}/competitors?limit=2").json()["competitors"]] == [CA, CB]
    assert [x["competitor_domain"] for x in client.get(f"/businesses/{OWN}/competitors?limit=2&offset=2").json()["competitors"]] == [CC, "comp-d.com"]
    assert client.get(f"/businesses/{OWN}/competitors?limit=2&offset=9").json()["competitors"] == []
    assert client.get(f"/businesses/{OWN}/competitors?limit=0").status_code == 422


def test_existing_keyset_list_endpoints_cover_every_row_once(db, client):
    with core.connect() as c:
        build(c, n=300)
    for path, key in (("/crawler/outgoing-links?active_only=false", "id"),):
        every = [i[key] for i in client.get(path + "&limit=1000").json()["items"]]
        for style in ("after_id", "offset"):
            got, nxt = [], 0
            for _ in range(1000):
                items = client.get(f"{path}&limit=50&{style}={nxt}").json()["items"]
                got += [i[key] for i in items]
                if len(items) < 50:
                    break
                nxt = items[-1]["id"] if style == "after_id" else len(got)
            assert got == every == sorted(every)

"""Phase 6: the in-memory read cache (read_cache.py) and its use by the backlink-graph endpoints (crawler_api.py). The unit tests need no database; the
endpoint tests run on the local scratch database, one schema per test (the `db` fixture). The cache is OFF by default; every test that wants it builds
its own instance with a fake clock, so nothing here depends on wall-clock time."""

import threading
import time

import pytest
from fastapi import HTTPException

import business_jobs
import competitors
import crawler_api
import crawler_core as core
import read_cache
from api_data import SUB, TARGET
from read_cache import HIT, MISS, OFF, STALE, TTLCache
from test_api_perf import client, db, populate  # noqa: F401 - fixtures


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def cache_of(**kw):
    clock = Clock()
    return TTLCache(**{"enabled": True, "ttl": 10, "max_ttl": 30, "max_entries": 100, "max_stale": 30, "retry": 5, "clock": clock, **kw}), clock


class Counter:
    """compute() that returns a new value each call and counts its calls."""
    def __init__(self, value=None):
        self.n, self.value = 0, value

    def __call__(self):
        self.n += 1
        return self.value if self.value is not None else {"n": self.n}


# ------------------------------------------------------------------ unit: the cache itself ------------------------------------------------------------------

def test_miss_then_fresh_hit_without_recomputing():
    c, clock = cache_of()
    f = Counter()
    assert c.get("k", f) == ({"n": 1}, MISS, 0.0)
    clock.advance(4)
    v, status, age = c.get("k", f)
    assert (v, status, age, f.n) == ({"n": 1}, HIT, 4, 1)
    s = c.stats()
    assert (s["hits"], s["misses"], s["refreshes"], s["entries"]) == (1, 1, 1, 1) and s["hit_ratio"] == 0.5


def test_entry_expires_after_the_ttl():
    c, clock = cache_of(ttl=10)
    f = Counter()
    c.get("k", f)
    clock.advance(9.99)
    assert c.get("k", f)[1] == HIT
    clock.advance(0.02)  # 10.01 s old
    v, status, _ = c.get("k", f)
    assert (status, v, f.n) == (MISS, {"n": 2}, 2)


def test_ttl_is_clamped_to_the_configured_maximum_and_zero_disables(monkeypatch):
    assert TTLCache(enabled=True, ttl=100, max_ttl=30).ttl == 30
    assert TTLCache(enabled=True, ttl=5, max_ttl=30).ttl == 5
    assert TTLCache(enabled=True, ttl=0).enabled is False
    for k in ("GRAPH_CACHE_ENABLED", "GRAPH_CACHE_TTL_SECONDS", "GRAPH_CACHE_MAX_TTL_SECONDS", "GRAPH_CACHE_MAX_ENTRIES", "GRAPH_CACHE_MAX_MB"):
        monkeypatch.delenv(k, raising=False)
    d = TTLCache.from_env()  # the documented defaults: off, 15 s, ceiling 30 s, 1000 entries
    assert (d.enabled, d.ttl, d.max_ttl, d.max_entries) == (False, 15, 30, 1000)
    monkeypatch.setenv("GRAPH_CACHE_ENABLED", "true")
    monkeypatch.setenv("GRAPH_CACHE_TTL_SECONDS", "100")
    monkeypatch.setenv("GRAPH_CACHE_MAX_ENTRIES", "7")
    e = TTLCache.from_env()
    assert (e.enabled, e.ttl, e.max_entries) == (True, 30, 7)  # 100 s asked for, 30 s allowed


def test_lru_eviction_removes_the_least_recently_used_entry():
    c, _ = cache_of(max_entries=3)
    for k in ("a", "b", "c"):
        c.get(k, Counter({"k": k}))
    c.get("a", Counter())  # a is now the most recently used; b the least
    c.get("d", Counter({"k": "d"}))
    assert set(c._entries) == {"a", "c", "d"} and c.stats()["evictions"] == 1
    assert c.get("b", Counter({"k": "b2"}))[1] == MISS  # b was evicted


def test_entry_and_memory_bounds():
    big = lambda: {"rows": ["x" * 100 for _ in range(100)]}  # ~ 10-20 kB estimated
    size = read_cache.estimate_size(big())
    c, _ = cache_of(max_bytes=int(size * 2.5), max_entries=100)
    for k in range(5):
        c.get(k, big)
    s = c.stats()
    assert s["entries"] == 2 and s["bytes"] <= c.max_bytes and s["evictions"] == 3  # the memory budget, not the entry cap, was binding
    v, status, _ = c.get("huge", lambda: {"rows": ["y" * 100 for _ in range(1000)]})  # larger than the whole budget: served, never stored
    assert status == MISS and len(v["rows"]) == 1000 and c.stats()["oversize"] == 1 and "huge" not in c._entries
    assert c.stats()["entries"] == 2


def test_single_flight_one_computation_for_concurrent_requests_of_one_key():
    c, _ = cache_of()
    n, calls, out = 8, [], []

    def compute():
        calls.append(1)
        deadline = time.monotonic() + 10  # wait until every other thread is queued behind this refresh
        while c.stats()["coalesced"] < n - 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        return {"v": 1}
    threads = [threading.Thread(target=lambda: out.append(c.get("k", compute))) for _ in range(n)]
    [t.start() for t in threads]
    [t.join(15) for t in threads]
    assert len(calls) == 1 and len(out) == n and all(o[0] == {"v": 1} for o in out)
    assert sorted(o[1] for o in out) == [HIT] * (n - 1) + [MISS]
    assert c.stats()["coalesced"] == n - 1 and c.stats()["refreshes"] == 1


def test_different_keys_refresh_independently():
    c, _ = cache_of()
    gate, a_started, b_done = threading.Event(), threading.Event(), threading.Event()

    def slow():
        a_started.set()
        assert gate.wait(10)
        return "a"
    ta = threading.Thread(target=lambda: c.get("A", slow))
    ta.start()
    assert a_started.wait(10)
    tb = threading.Thread(target=lambda: (c.get("B", lambda: "b"), b_done.set()))
    tb.start()
    assert b_done.wait(10), "key B had to wait for key A's refresh"
    gate.set()
    ta.join(10)
    tb.join(10)
    assert c.get("A", Counter())[0] == "a" and c.get("B", Counter())[0] == "b"


def failing(exc=ValueError("db down")):
    def f():
        f.n += 1
        raise exc
    f.n = 0
    return f


def test_failed_refresh_serves_the_old_value_for_a_bounded_time_and_backs_off():
    c, clock = cache_of(ttl=10, max_stale=30, retry=5)
    c.get("k", Counter({"v": 1}))
    clock.advance(11)  # expired, a refresh is due
    f = failing()
    v, status, age = c.get("k", f)
    assert (v, status, age, f.n) == ({"v": 1}, STALE, 11, 1)
    clock.advance(2)  # inside the retry window: not even tried
    assert c.get("k", f)[1] == STALE and f.n == 1
    clock.advance(4)  # window over: tried again, fails again, still served
    assert c.get("k", f)[1] == STALE and f.n == 2
    s = c.stats()
    assert s["errors"] == 2 and s["stale"] == 3


def test_stale_age_is_capped_and_then_the_exception_reaches_the_caller():
    c, clock = cache_of(ttl=10, max_stale=30, retry=5)
    c.get("k", Counter({"v": 1}))
    clock.advance(39)  # 39 s old: younger than ttl + max_stale (40 s)
    assert c.get("k", failing())[1] == STALE
    clock.advance(2)  # 41 s old: too old to serve
    f = failing(ValueError("still down"))
    with pytest.raises(ValueError, match="still down"):
        c.get("k", f)
    assert "k" not in c._entries and c.stats()["expired"] == 1
    with pytest.raises(ValueError):
        c.get("k", f)  # nothing was cached from the failure: it tries again
    assert f.n == 2


def test_successful_refresh_resets_the_stale_age():
    c, clock = cache_of(ttl=10, max_stale=30, retry=5)
    c.get("k", Counter({"v": 1}))
    clock.advance(11)
    c.get("k", failing())  # stale, retry_at set
    clock.advance(6)
    v, status, _ = c.get("k", Counter({"v": 2}))  # retry window over, database is back
    assert (v, status) == ({"v": 2}, MISS)
    clock.advance(11)  # the new entry is 11 s old: a failure now serves THIS one (age 11), not a 28 s old one, and is not capped by the old birth
    v, status, age = c.get("k", failing())
    assert (v, status, age) == ({"v": 2}, STALE, 11)
    clock.advance(25)  # 36 s old: still inside ttl + max_stale measured from the SUCCESSFUL refresh (it would be 53 s from the first one)
    assert c.get("k", failing())[1] == STALE
    assert c._entries["k"].retry_at > 0


def test_exception_without_an_old_value_propagates_to_every_waiting_caller_and_is_not_cached():
    c, _ = cache_of()
    n, errors, calls = 6, [], []

    def compute():
        calls.append(1)
        deadline = time.monotonic() + 10
        while c.stats()["coalesced"] < n - 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        raise RuntimeError("boom")

    def call():
        try:
            c.get("k", compute)
        except RuntimeError as e:
            errors.append(str(e))
    threads = [threading.Thread(target=call) for _ in range(n)]
    [t.start() for t in threads]
    [t.join(15) for t in threads]
    assert errors == ["boom"] * n and len(calls) == 1
    assert c.get("k", Counter({"ok": 1}))[1] == MISS  # the next call computes normally


def test_values_are_copied_in_and_out():
    c, _ = cache_of()
    original = {"items": [{"a": 1}], "n": 1}
    v, status, _ = c.get("k", lambda: original)
    v["items"][0]["a"] = 999  # the caller changes the first answer ...
    original["items"].append("changed after the fact")  # ... and the producer keeps changing its own object
    hit, _, _ = c.get("k", Counter())
    assert hit == {"items": [{"a": 1}], "n": 1}
    hit["items"].clear()
    assert c.get("k", Counter())[0] == {"items": [{"a": 1}], "n": 1}


def test_disabled_cache_is_a_plain_call():
    c = TTLCache()  # default: disabled
    f = Counter()
    assert [c.get("k", f)[1] for _ in range(3)] == [OFF, OFF, OFF] and f.n == 3
    s = c.stats()
    assert (s["hits"], s["misses"], s["entries"], s["bytes"]) == (0, 0, 0, 0)


def test_two_caches_are_independent_like_two_api_processes():
    a, _ = cache_of()
    b, _ = cache_of()
    fa, fb = Counter({"who": "a"}), Counter({"who": "b"})
    a.get("k", fa)
    assert b.get("k", fb)[1] == MISS and fb.n == 1  # B never saw A's entry
    assert a.get("k", fa)[1] == HIT and fa.n == 1
    a.reset_after_fork()  # what a forked child does: empty, with fresh locks, and the other cache is unaffected
    assert a.stats()["entries"] == 0 and b.stats()["entries"] == 1
    assert a.get("k", fa)[1] == MISS


# ------------------------------------------------------------------ endpoints ------------------------------------------------------------------

@pytest.fixture()
def cache(monkeypatch, db):
    c, clock = cache_of(ttl=15, max_ttl=30)
    c.clock_ctl = clock
    monkeypatch.setattr(crawler_api, "graph_cache", c)
    return c


@pytest.fixture()
def queries(monkeypatch):
    """Counts the aggregate queries the endpoints really run."""
    n = {"summary": 0, "grouped": 0}
    real_s, real_g = crawler_api._graph_summary, crawler_api._grouped_page
    monkeypatch.setattr(crawler_api, "_graph_summary", lambda *a, **k: (n.__setitem__("summary", n["summary"] + 1), real_s(*a, **k))[1])
    monkeypatch.setattr(crawler_api, "_grouped_page", lambda *a, **k: (n.__setitem__("grouped", n["grouped"] + 1), real_g(*a, **k))[1])
    return n


def uncached(monkeypatch, fn, *a, **k):
    """The same call with the cache switched off (the reference answer)."""
    with monkeypatch.context() as m:
        m.setattr(crawler_api, "graph_cache", TTLCache())
        return fn(*a, **k)


def test_default_cache_is_disabled_and_adds_no_headers(client, queries):
    populate(1)
    assert read_cache.graph_cache.enabled is False  # the shipped default
    r1, r2 = client.get(f"/backlinks/{TARGET}"), client.get(f"/backlinks/{TARGET}")
    assert r1.status_code == r2.status_code == 200 and "X-Cache" not in r1.headers and "X-Cache-Age" not in r1.headers
    assert queries["summary"] == 2  # every request ran its aggregate


def test_hit_miss_headers_and_unchanged_json(client, cache, queries, monkeypatch):
    populate(1)
    for path in (f"/backlinks/{TARGET}?limit=5", f"/backlinks/{TARGET}/referring-domains?limit=5", f"/backlinks/{TARGET}/anchors?limit=5"):
        first = client.get(path)
        cache.clock_ctl.advance(3)
        second = client.get(path)
        assert (first.headers["X-Cache"], first.headers["X-Cache-Age"]) == ("MISS", "0")
        assert (second.headers["X-Cache"], second.headers["X-Cache-Age"]) == ("HIT", "3")
        assert first.json() == second.json()
        with monkeypatch.context() as m:  # byte-for-byte what the endpoint answered before there was a cache
            m.setattr(crawler_api, "graph_cache", TTLCache())
            reference = client.get(path)
        assert "X-Cache" not in reference.headers and reference.json() == first.json()
    assert queries == {"summary": 2, "grouped": 4}  # 3 cached endpoints x (miss + hit) = 1 + 2 aggregates, plus the same 1 + 2 for the 3 uncached reference calls
    assert set(client.get(f"/backlinks/{TARGET}").headers) >= {"x-cache", "x-cache-age", "server-timing"}


def test_only_summary_referring_domains_and_anchors_are_cached(client, cache):
    populate(1)
    for path in (f"/backlinks/{TARGET}", f"/backlinks/{TARGET}?include_removed=true&limit=3", f"/backlinks/{TARGET}/history", f"/backlinks/{TARGET}/referring-domains",
                 f"/backlinks/{TARGET}/anchors", "/backlinks", "/crawler/outgoing-links?limit=5", "/discovery/statistics", "/businesses"):
        client.get(path)
    assert {k[0] for k in cache._entries} == {"summary", "referring_domains", "anchors"}


def test_subdomain_and_registrable_domain_never_share_an_entry(client, cache, monkeypatch):
    populate(1)
    for fn, path in ((crawler_api.referring_domains, "referring-domains"), (crawler_api.backlink_anchors, "anchors")):
        reg, sub = client.get(f"/backlinks/{TARGET}/{path}").json(), client.get(f"/backlinks/{SUB}/{path}").json()
        assert reg != sub  # the two targets really select different links in this data set
        assert client.get(f"/backlinks/{TARGET}/{path}").json() == reg and client.get(f"/backlinks/{SUB}/{path}").json() == sub  # and each is served its own from the cache
        assert reg == uncached(monkeypatch, fn, TARGET) and sub == uncached(monkeypatch, fn, SUB)
    assert client.get(f"/backlinks/{TARGET}").json()["total_backlinks"] != client.get(f"/backlinks/{SUB}").json()["total_backlinks"]
    assert {(k[0], k[1]) for k in cache._entries} >= {("summary", TARGET), ("summary", SUB), ("referring_domains", TARGET), ("referring_domains", SUB)}


def test_case_and_url_spellings_of_one_target_share_one_entry(client, cache, queries):
    populate(1)
    spellings = [TARGET, TARGET.upper(), f"www.{TARGET}", f"https://www.{TARGET.upper()}/some/path?q=1"]  # a URL cannot be an HTTP path segment, so it goes in as a call
    bodies = [crawler_api.backlinks(sp, False, 5, 0) for sp in spellings]
    assert all(b["total_backlinks"] == bodies[0]["total_backlinks"] and b["domain"] == TARGET for b in bodies)
    assert client.get(f"/backlinks/{TARGET.upper()}").headers["X-Cache"] == "HIT"  # and the HTTP route normalizes the same way
    assert queries["summary"] == 1  # four spellings, one aggregate
    assert [k for k in cache._entries if k[0] == "summary"] == [("summary", TARGET, True)]


def test_limit_and_offset_are_part_of_the_key(client, cache, monkeypatch):
    populate(1)
    seen = {}
    for kind, fn, path in (("referring_domains", crawler_api.referring_domains, "referring-domains"), ("anchors", crawler_api.backlink_anchors, "anchors")):
        for limit, offset in ((2, 0), (3, 0), (2, 2), (50, 0), (50, 10 ** 6)):
            got = client.get(f"/backlinks/{TARGET}/{path}?limit={limit}&offset={offset}").json()
            again = client.get(f"/backlinks/{TARGET}/{path}?limit={limit}&offset={offset}").json()
            assert got == again == uncached(monkeypatch, fn, TARGET, limit, offset), (kind, limit, offset)
            seen[(kind, limit, offset)] = got
    assert len([k for k in cache._entries if k[0] in ("referring_domains", "anchors")]) == 10  # five pages of each kind, none shared
    assert seen[("referring_domains", 2, 0)] != seen[("referring_domains", 2, 2)] and seen[("anchors", 50, 10 ** 6)].get("anchors") == []


def test_include_removed_and_active_only_are_part_of_the_key(client, cache, monkeypatch):
    populate(1)
    for removed in (False, True, False, True):
        got = client.get(f"/backlinks/{TARGET}?include_removed={str(removed).lower()}&limit=3").json()
        want = uncached(monkeypatch, crawler_api.backlinks, TARGET, removed, 3, 0)
        assert got["total_backlinks"] == want["total_backlinks"] and got["verified_backlinks"] == want["verified_backlinks"], removed
    s = [k for k in cache._entries if k[0] == "summary"]
    assert sorted(k[2] for k in s) == [False, True] and cache.stats()["hits"] == 2
    totals = {r: client.get(f"/backlinks/{TARGET}?include_removed={r}&limit=1").json()["total_backlinks"] for r in ("true", "false")}
    assert totals["true"] > totals["false"]  # the two variants are different numbers, so sharing a key would have been visible


def test_authorization_runs_on_every_request_including_cache_hits(client, cache, monkeypatch):
    populate(1)
    allowed = {"yes": True}
    monkeypatch.setattr(competitors, "is_competitor", lambda c, d: allowed["yes"])
    checks = []
    real = crawler_api._comp
    monkeypatch.setattr(crawler_api, "_comp", lambda d: (checks.append(d), real(d))[1])
    for path in ("referring-domains", "anchors"):
        ok = client.get(f"/competitors/{TARGET}/{path}")
        assert ok.status_code == 200 and ok.headers["X-Cache"] == "MISS"
        assert client.get(f"/competitors/{TARGET}/{path}").headers["X-Cache"] == "HIT"
        public = client.get(f"/backlinks/{TARGET}/{path}")  # the same graph query, same parameters: the same entry
        assert public.headers["X-Cache"] == "HIT" and public.json() == ok.json()
        allowed["yes"] = False  # ... and with the entry fresh in the cache, the check still decides
        denied = client.get(f"/competitors/{TARGET}/{path}")
        assert denied.status_code == 404 and "X-Cache" not in denied.headers
        allowed["yes"] = True
    assert len(checks) == 6  # every competitor request ran _comp, hit or not
    allowed["yes"] = False
    with pytest.raises(HTTPException) as e:
        crawler_api.competitor_backlinks(TARGET)
    assert e.value.status_code == 404 and cache.stats()["hits"] == 4


def test_live_job_state_is_rebuilt_on_every_request_even_on_a_hit(client, cache):
    populate(1)
    with core.connect() as c:
        job, _ = business_jobs.create_job(c, TARGET)
    first = client.get(f"/backlinks/{TARGET}?limit=3")
    assert first.headers["X-Cache"] == "MISS"
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='completed' WHERE id=?", (job["id"],))
    second = client.get(f"/backlinks/{TARGET}?limit=3")
    assert second.headers["X-Cache"] == "HIT"  # the totals came from the cache ...
    assert second.json()["discovery_status"] != first.json()["discovery_status"]  # ... the job state did not
    assert {k: v for k, v in second.json().items() if k != "discovery_status"} == {k: v for k, v in first.json().items() if k != "discovery_status"}


def test_totals_are_at_most_one_ttl_behind_and_a_refresh_picks_up_new_links(client, cache):
    populate(1)
    before = client.get(f"/backlinks/{TARGET}?limit=1").json()["total_backlinks"]
    with core.connect() as c:  # a crawl writes one more backlink
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, source_domain, first_seen)"
                  f" VALUES ('https://fresh.example/p','https://{TARGET}/x','{TARGET}','fresh','fresh.example','2026-05-05 00:00:00')")
    cache.clock_ctl.advance(14.9)
    assert client.get(f"/backlinks/{TARGET}?limit=1").json()["total_backlinks"] == before  # inside the TTL: the documented lag
    cache.clock_ctl.advance(0.2)
    r = client.get(f"/backlinks/{TARGET}?limit=1")
    assert r.headers["X-Cache"] == "MISS" and r.json()["total_backlinks"] == before + 1


def test_concurrent_requests_for_one_key_run_one_aggregate(client, cache, queries):
    populate(1)
    barrier, out = threading.Barrier(8), []

    def hit():
        barrier.wait()
        out.append(client.get(f"/backlinks/{TARGET}/referring-domains?limit=5").status_code)
    threads = [threading.Thread(target=hit) for _ in range(8)]
    [t.start() for t in threads]
    [t.join(60) for t in threads]
    assert out == [200] * 8 and queries["grouped"] == 1


def test_a_database_failure_serves_a_bounded_stale_total_then_fails_loudly(client, cache, monkeypatch):
    populate(1)
    good = client.get(f"/backlinks/{TARGET}/referring-domains?limit=5").json()
    cache.clock_ctl.advance(16)

    def down(*a, **k):
        raise RuntimeError("database unavailable")
    monkeypatch.setattr(crawler_api, "_grouped_page", down)
    r = client.get(f"/backlinks/{TARGET}/referring-domains?limit=5")
    assert r.status_code == 200 and r.headers["X-Cache"] == "STALE" and r.headers["X-Cache-Age"] == "16" and r.json() == good
    cache.clock_ctl.advance(200)  # far beyond ttl + max_stale
    with pytest.raises(RuntimeError, match="database unavailable"):
        crawler_api.referring_domains(TARGET, 5, 0)

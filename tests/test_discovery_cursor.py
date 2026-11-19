"""Phase 5: continuous DDGS discovery. Real PostgreSQL (isolated schema per test); only the ddgs page fetch is replaced by a deterministic fake
(pages of 10 from a per-query pool). Covers cursors, restart, dedup/provenance, fairness, concurrency, backlog cap, states and an end-to-end run."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

import business_jobs as bj
import competitors
import crawler_api
import crawler_core as core
import crawler_engine as engine
import ddg_lookup


@pytest.fixture(autouse=True)
def _fresh_backend_health():
    ddg_lookup._health.clear()
    yield
    ddg_lookup._health.clear()


from test_business_jobs import SITES, a, add_job_urls, cfg, drive, env, job_status, onboard, page, port, rows  # noqa: F401 - fixtures

PAST = "2000-01-01 00:00:00"


def urls(n: int, tag: str = "x") -> list[str]:
    return [f"https://{tag}{i}.example.net/p" for i in range(n)]


def web(monkeypatch, pools: dict[str, list[str]], fail=None, per_page: int = 10, same: bool = False, backends=("b1",)) -> list:
    """Replace the ddgs page fetch. pools: query -> URLs; page p is pools[query][(p-1)*10 : p*10] (or always page 1 when `same`)."""
    calls: list = []

    def fake(q, page, attempts, end=float("inf"), backend="auto"):
        calls.append((q, page, backend))
        if fail and fail(q, page, backend):
            raise RuntimeError(f"429 {backend}")
        p = 1 if same else page
        return [{"href": h} for h in pools.get(q, [])[(p - 1) * per_page: p * per_page]]

    monkeypatch.setattr(ddg_lookup, "_page", fake)
    monkeypatch.setattr(ddg_lookup.time, "sleep", lambda s: None)
    monkeypatch.setattr(ddg_lookup, "BACKENDS", list(backends))
    monkeypatch.setattr(bj, "DDG_RESULTS", 50)
    return calls


def got(domain: str) -> list[str]:
    """Candidate URLs (depth 0) of the target's latest job, in the order they were found."""
    return [r["url"] for r in rows("SELECT u.url FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE ju.depth=0 AND ju.job_id="
                                   "(SELECT MAX(id) FROM business_jobs WHERE target_domain=?) ORDER BY u.id", domain)]


def next_batch(domain: str) -> list[str]:
    """The previous run is over and the schedule fires again: one more discovery run."""
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE target_domain=? AND status IN ('pending','searching','crawling')", (core.now(), domain))
        c.execute("UPDATE businesses SET next_run_at=? WHERE domain=?", (PAST, domain))
    assert bj.schedule_due() == 1
    asyncio.run(bj.search_pending())
    return got(domain)


def state(domain: str) -> dict:
    with core.connect() as c:
        return bj.discovery_status(c, domain)


def cursors(domain: str) -> dict:
    return {(r["query"], r["backend"]): r for r in rows("SELECT * FROM discovery_cursors WHERE domain=?", domain)}


# 1-4: batches continue from the saved position, never from page 1, and the position is in PostgreSQL
def test_batches_continue_from_saved_position_and_survive_restart(env, monkeypatch):
    d = "shop.localhost"
    q0 = ddg_lookup.queries(d)[0]
    pool = urls(200)
    calls = web(monkeypatch, {q0: pool})
    onboard(d)
    asyncio.run(bj.search_pending())
    assert got(d) == pool[:50]  # 1: first batch is results 1-50
    assert cursors(d)[(q0, "b1")]["next_page"] == 6
    bj.reset_interrupted()  # 4: a restart keeps no memory; the cursor is read from the database
    assert bj._load_run(d).cursors[(ddg_lookup.qhash(q0), "b1")]["next_page"] == 6
    assert next_batch(d) == pool[50:100]  # 2: second batch continues
    assert next_batch(d) == pool[100:150]  # 3: and again, beyond 100
    assert [p for q, p, _ in calls if q == q0] == list(range(1, 16))  # never restarted at page 1
    assert job_status(d)["search_results"] == 50


# 6: every query variation has its own cursor; an exhausted one is skipped, the next one carries on
def test_query_variations_keep_independent_cursors(env, monkeypatch):
    d = "multi.localhost"
    q0, q1, q2 = ddg_lookup.queries(d)
    web(monkeypatch, {q0: urls(30, "a"), q1: urls(60, "b")})
    onboard(d)
    asyncio.run(bj.search_pending())
    c = cursors(d)
    assert len(got(d)) == 50 and c[(q0, "b1")]["status"] == "exhausted" and (q1, "b1") in c and c[(q1, "b1")]["next_page"] == 3
    assert (q2, "b1") not in c  # not reached yet
    assert next_batch(d) == urls(60, "b")[20:60]  # q0 is skipped, q1 resumes at page 3
    c = cursors(d)
    assert c[(q1, "b1")]["status"] == "exhausted" and (q2, "b1") not in c  # q2 answered an empty page 1: nothing to store, never "exhausted"


# 13: results that repeat forever stop the pagination; "exhausted" and "waiting" are different states; rediscovery reads from the start again
def test_repeated_results_exhaust_then_wait_then_rediscover(env, monkeypatch):
    d = "loop.localhost"
    qs = ddg_lookup.queries(d)
    pool = urls(10)
    calls = web(monkeypatch, {q: pool for q in qs}, same=True)  # every page answers with the same 10 URLs
    onboard(d)
    asyncio.run(bj.search_pending())
    assert len(got(d)) == 10
    assert len(calls) <= 3 * (ddg_lookup.DUP_PAGES_LIMIT + 1)  # the three queries stopped after the duplicate-only streak
    assert {k["status"] for k in cursors(d).values()} == {"exhausted"}
    assert state(d)["exhausted_cursors"] == 3
    with core.connect() as c:
        assert c.execute("SELECT discovery_state FROM businesses WHERE domain=?", (d,)).fetchone()[0] == "waiting_rediscovery"
    n = len(calls)
    assert next_batch(d) == [] and len(calls) == n  # waiting: no request is made at all, and it is not reported as a failure
    assert job_status(d)["status"] == "crawling" and not job_status(d)["last_error"]
    with core.connect() as c:  # tick closes the empty job and pushes the next run to the rediscovery time, not one interval away
        bj.tick()
        nxt = c.execute("SELECT next_run_at FROM businesses WHERE domain=?", (d,)).fetchone()[0]
    assert nxt > core.later(days=6)
    with core.connect() as c:  # a week passes: the queries become readable again from page 1
        c.execute("UPDATE discovery_cursors SET rediscover_after=? WHERE domain=?", (PAST, d))
    web(monkeypatch, {q: pool + urls(5, "fresh") for q in qs}, per_page=10)
    assert next_batch(d) == urls(5, "fresh")  # controlled rediscovery finds what is new, the known 10 are not stored again


# 5, 14: one URL found by several targets / several queries is stored and crawled once, with every association kept
def test_url_found_by_many_targets_and_queries_keeps_all_associations(env, monkeypatch):
    onboard("own.localhost")
    with core.connect() as c:
        competitors.register(c, "own.localhost", "rival.localhost")
    shared = "https://shared.example.net/page"
    q = {t: ddg_lookup.queries(t) for t in ("own.localhost", "rival.localhost")}
    web(monkeypatch, {q["own.localhost"][0]: [shared, "https://o1.example.net/"], q["own.localhost"][1]: [shared],
                      q["rival.localhost"][0]: ["https://r1.example.net/", shared]})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 2)
    asyncio.run(bj.search_pending())
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done' WHERE status='crawling'")
    asyncio.run(bj.search_pending())
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE url=?", shared)[0]["n"] == 1  # global dedup: one row, one crawl
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", shared)[0]["crawl_status"] == "pending"
    d = rows("SELECT d.target_domain, d.query_hash FROM url_discoveries d JOIN discovered_urls u ON u.id=d.url_id WHERE u.url=? ORDER BY 1, 2", shared)
    own_q = {ddg_lookup.qhash(x) for x in q["own.localhost"][:2]}
    assert {(r["target_domain"], r["query_hash"]) for r in d} == {("own.localhost", h) for h in own_q} | {("rival.localhost", ddg_lookup.qhash(q["rival.localhost"][0]))}
    assert rows("SELECT COUNT(DISTINCT ju.job_id) n FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE u.url=?", shared)[0]["n"] == 2  # both jobs


# 3/9: a URL a target already has is never stored for it twice, but the query that found it again is recorded
def test_known_url_is_not_readded_but_its_new_query_is_recorded(env, monkeypatch):
    d = "prov.localhost"
    q0, q1, _ = ddg_lookup.queries(d)
    web(monkeypatch, {q0: ["https://a.example.net/"]})
    onboard(d)
    asyncio.run(bj.search_pending())
    web(monkeypatch, {q0: ["https://a.example.net/"], q1: ["https://a.example.net/", "https://b.example.net/"]})
    with core.connect() as c:
        c.execute("UPDATE discovery_cursors SET status='active', next_page=1, dup_streak=0, rediscover_after=NULL WHERE domain=?", (d,))
    assert next_batch(d) == ["https://b.example.net/"]  # the known URL is not a candidate of the new job
    n = rows("SELECT COUNT(*) n FROM url_discoveries d JOIN discovered_urls u ON u.id=d.url_id WHERE d.target_domain=? AND u.url='https://a.example.net/'", d)[0]["n"]
    assert n == 2  # found by q0 and, later, by q1


# backends: pinned per cursor. A fallback backend continues from ITS OWN saved page (never another engine's offset), so a dead first backend does not
# send every run back to page 1 of the next one (found live: DuckDuckGo timing out made every run re-read Bing pages 1-2)
def test_fallback_backend_continues_from_its_own_cursor_and_a_dead_backend_is_tried_once(env, monkeypatch):
    d = "pin.localhost"
    q0, q1, q2 = ddg_lookup.queries(d)
    pool = urls(200)
    calls = web(monkeypatch, {q0: pool, q1: urls(200, "k")}, backends=("b1", "b2"), fail=lambda q, p, b: b == "b1")
    onboard(d)
    with core.connect() as c:  # b2 had been read up to page 7 earlier
        c.execute("INSERT INTO discovery_cursors (domain, query_hash, backend, query, next_page, head_checked_at) VALUES (?,?,?,?,7,?)", (d, ddg_lookup.qhash(q0), "b2", q0, core.now()))
    asyncio.run(bj.search_pending())
    assert [p for q, p, b in calls if b == "b2" and q == q0][0] == 7  # its own position, not 1 and not anything of b1's
    assert got(d) == pool[60:110]
    assert [b for q, p, b in calls if b == "b1"] == ["b1"] and len([1 for q, p, b in calls if b == "b1"]) == 1  # b1 failed once; the other queries did not retry it
    assert (q0, "b1") not in cursors(d) and cursors(d)[(q0, "b2")]["next_page"] == 12
    next_batch(d)
    assert cursors(d)[(q0, "b2")]["next_page"] > 12  # and the NEXT run keeps going, it does not restart at page 1


def test_pinned_backend_is_used_again_when_it_recovers_and_keeps_its_own_position(env, monkeypatch):
    d = "pin2.localhost"
    q0 = ddg_lookup.queries(d)[0]
    pool = urls(200)
    web(monkeypatch, {q0: pool}, backends=("b1", "b2"), fail=lambda q, p, b: b == "b1")
    onboard(d)
    asyncio.run(bj.search_pending())
    assert cursors(d)[(q0, "b2")]["next_page"] == 6 and (q0, "b1") not in cursors(d)
    web(monkeypatch, {q0: pool}, backends=("b1", "b2"))  # b1 is back: first usable again, from its own page 1 (all known to this target, so it only dedups)
    next_batch(d)
    c = cursors(d)
    assert c[(q0, "b1")]["status"] == "exhausted" or c[(q0, "b1")]["next_page"] > 1
    assert c[(q0, "b2")]["next_page"] >= 6  # b2's position was never reset


def test_failed_page_is_never_skipped(env, monkeypatch):
    d = "fail3.localhost"
    q0 = ddg_lookup.queries(d)[0]
    pool = urls(100)
    web(monkeypatch, {q0: pool}, fail=lambda q, p, b: p == 3)
    onboard(d)
    asyncio.run(bj.search_pending())
    assert got(d) == pool[:20] and cursors(d)[(q0, "b1")]["next_page"] == 3  # pages 1-2 stored; the cursor sits ON the failed page
    web(monkeypatch, {q0: pool})
    assert next_batch(d) == pool[20:70]  # nothing skipped: continues at page 3


# a search that fails with nothing found is a failure, never "no backlinks", and moves no cursor
def test_failed_search_moves_no_cursor_and_does_not_block_others(env, monkeypatch):
    monkeypatch.setattr(bj, "DDG_COOLDOWN", 0)
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 2)
    bad, good = "bad.localhost", "good.localhost"
    qb, qg = ddg_lookup.queries(bad)[0], ddg_lookup.queries(good)[0]
    web(monkeypatch, {qg: urls(20)}, fail=lambda q, p, b: q == qb)
    onboard(bad, good)
    asyncio.run(bj.search_pending())
    assert len(got(good)) == 20 and job_status(good)["status"] == "crawling"
    jb = job_status(bad)
    assert jb["status"] == "pending" and jb["retry_count"] == 1 and "429" in jb["last_error"]
    assert not cursors(bad)
    last = rows("SELECT domain, last_search_at FROM businesses")  # the failed attempt still counted as its turn
    assert all(r["last_search_at"] for r in last)
    for _ in range(bj.SEARCH_RETRIES):
        asyncio.run(bj.search_pending())
    assert job_status(bad)["status"] == "failed"
    assert rows("SELECT discovery_state FROM businesses WHERE domain=?", bad)[0]["discovery_state"] == "failed"  # distinct from exhausted/waiting/deferred
    assert rows("SELECT discovery_state FROM businesses WHERE domain=?", good)[0]["discovery_state"] == "active"


# 7, 9, 10, 8: three businesses and their competitors get discovery turns in rotation; one with many competitors starves nobody
def test_fair_rotation_across_businesses_and_competitors(env, monkeypatch):
    monkeypatch.setattr(bj, "FAIR_POLICY", "target")  # the per-target policy (DDGS_FAIR_POLICY=target): every target exactly equal
    web(monkeypatch, {})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    biz = ["big.localhost", "mid.localhost", "solo.localhost"]
    onboard(*biz)
    comps = [f"c{i}.localhost" for i in range(4)]
    with core.connect() as c:
        for x in comps:
            competitors.register(c, "big.localhost", x)
        competitors.register(c, "mid.localhost", "m1.localhost")
    targets = biz + comps + ["m1.localhost"]
    order: list[str] = []
    for _ in range(len(targets) * 3):
        bj.schedule_due()
        j = bj._claim_search(1)
        assert j, "a due target must always be claimable"
        order.append(j[0]["target_domain"])
        asyncio.run(bj._search_one(j[0]))
        with core.connect() as c:
            c.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE status='crawling'", (core.now(),))
            c.execute("UPDATE businesses SET next_run_at=? WHERE active=1", (PAST,))
    assert sorted(order) == sorted(targets * 3)  # everyone exactly 3 turns: nobody starved, nobody favoured
    n = len(targets)
    for i in range(0, len(order), n):
        assert sorted(order[i:i + n]) == sorted(targets)  # each full cycle serves every target once, incl. the lone business despite 5 competitors around
    gaps = [b - a for t in ("solo.localhost", "mid.localhost") for a, b in zip(*(lambda ix: (ix, ix[1:]))([i for i, x in enumerate(order) if x == t]))]
    assert max(gaps) <= n


# 16: a newly onboarded business goes first, and does not push the existing ones out for good
def test_new_business_gets_priority_without_starving_existing(env, monkeypatch):
    web(monkeypatch, {})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    onboard("old.localhost")
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("UPDATE businesses SET last_search_at='2020-01-01 00:00:00', next_run_at=?", (PAST,))
    bj.schedule_due()  # the old business's recurring job (P2) is pending
    onboard("fresh.localhost")  # a P0 job
    first = bj._claim_search(1)[0]
    assert first["target_domain"] == "fresh.localhost"
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done' WHERE target_domain='fresh.localhost'")
    assert bj._claim_search(1)[0]["target_domain"] == "old.localhost"  # served right after, not postponed behind newer work


# 11: two workers cannot claim the same target
def test_concurrent_claims_never_return_the_same_target(env, monkeypatch):
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 8)
    onboard(*[f"t{i}.localhost" for i in range(5)])
    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda _: [j["target_domain"] for j in bj._claim_search(2)], range(8)))
    claimed = [d for r in res for d in r]
    assert len(claimed) == len(set(claimed)) == 5
    assert rows("SELECT COUNT(*) n FROM business_jobs WHERE status='searching'")[0]["n"] == 5


# 12: a search that died with its process is run again; a result from a superseded search cannot move a cursor
def test_interrupted_search_is_recovered_and_stale_result_is_fenced(env, monkeypatch):
    d = "rec.localhost"
    q0 = ddg_lookup.queries(d)[0]
    web(monkeypatch, {q0: urls(60)})
    onboard(d)
    job = bj._claim_search(1)[0]
    run = bj._load_run(d)
    stale = ddg_lookup.search_urls(d, 50, run=run)
    bj.reset_interrupted()  # engine restarted while the search ran
    assert job_status(d)["status"] == "pending"
    job2 = bj._claim_search(1)[0]
    assert job2["search_gen"] == job["search_gen"] + 1
    bj._search_done(job, stale, run)  # the old search reports late
    assert not cursors(d) and not got(d)
    bj._search_done(job2, stale, run)  # the current owner may write
    assert len(got(d)) == 50 and cursors(d)[(q0, "b1")]["next_page"] == 6


# backlog cap: discovery pauses while the target has too many open URLs and resumes by itself when they are crawled
def test_target_over_the_outstanding_cap_is_deferred_then_eligible_again(env, monkeypatch):
    monkeypatch.setattr(bj, "MAX_OUTSTANDING_URLS", 5)
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 5)
    web(monkeypatch, {})
    j1, j2 = [j["id"] for j in onboard("busy.localhost", "calm.localhost")]
    add_job_urls(j1, "busyp", 6)  # 6 open URLs >= cap 5
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("UPDATE businesses SET next_run_at=?", (PAST,))
    assert bj.schedule_due() == 1  # only calm.localhost; busy is deferred, and does not block calm
    assert rows("SELECT target_domain FROM business_jobs WHERE status='pending'")[0]["target_domain"] == "calm.localhost"
    assert state("busy.localhost")["state"] == "deferred_backlog"
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET crawl_status='completed' WHERE domain='busyp.localhost'")
    assert bj.schedule_due() == 1  # its backlog went below the cap: eligible again with no other trigger
    assert {r["target_domain"] for r in rows("SELECT target_domain FROM business_jobs WHERE status='pending'")} == {"calm.localhost", "busy.localhost"}
    asyncio.run(bj.search_pending())
    assert state("busy.localhost")["state"] == "active"


# paused is its own state (a switched-off target is not "failed" or "exhausted")
def test_paused_target_is_reported_as_paused_and_not_scheduled(env, monkeypatch):
    web(monkeypatch, {})
    onboard("p.localhost")
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("UPDATE businesses SET active=0, next_run_at=?", (PAST,))
    assert bj.schedule_due() == 0
    assert state("p.localhost")["state"] == "paused"


# migration: v12 is additive, idempotent, and leaves existing rows alone
def test_migration_12_is_additive_and_backfills_provenance(env):
    onboard("old.localhost")
    with core.connect() as c:
        jid = c.execute("SELECT id FROM business_jobs").fetchone()[0]
    add_job_urls(jid, "legacy", 3)
    with core.connect() as c:
        c.execute("DELETE FROM url_discoveries")
        c.execute("DELETE FROM schema_migrations WHERE version=12")
        core.migrate(c, recover=False)
        core.migrate(c, recover=False)  # idempotent
        assert c.execute("SELECT COUNT(*) FROM url_discoveries WHERE target_domain='old.localhost' AND query_hash='legacy'").fetchone()[0] == 3
        assert not core.schema_problems(c)
    assert rows("SELECT COUNT(*) n FROM business_jobs")[0]["n"] == 1  # nothing existing was dropped or rewritten


# 15 + end to end: 3 businesses + 2 competitors, two discovery batches each, real crawling, real backlink verification
def test_end_to_end_three_businesses_and_competitors_two_batches(env, monkeypatch):
    u = env
    biz, comp = ["ea.localhost", "eb.localhost", "ec.localhost"], {"eca.localhost": "ea.localhost", "ecb.localhost": "eb.localhost"}
    targets = biz + list(comp)
    for t in targets:
        SITES[t] = {"/": page()}
    pools = {}
    for t in targets:
        h = t.split(".")[0]
        names = [f"s{h}{i}" for i in range(7)]
        for n in names:
            SITES[f"{n}.localhost"] = {"/": page(a(u(h), "link"))}
        pool = [u(n) for n in names]
        if t in ("ea.localhost", "eca.localhost"):  # one page links to a business and to a competitor of it
            pool.insert(1, u("shared"))
            SITES["shared.localhost"] = {"/": page(a(u("ea"), "A"), a(u("eca"), "CA"))}
        pools[ddg_lookup.queries(t)[0]] = pool
    web(monkeypatch, pools, per_page=2)  # 2 results a page, 4 per batch: two batches cover a pool of 8
    monkeypatch.setattr(bj, "DDG_RESULTS", 3)
    monkeypatch.setattr(bj, "COMPETITOR_RESULTS", 3)
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    monkeypatch.setattr(bj, "MAX_ACTIVE_COMPETITOR_JOBS", 2)
    onboard(*biz)
    with core.connect() as c:
        for cm, owner in comp.items():
            competitors.register(c, owner, cm)
    drive(cfg(), targets, rounds=120)  # batch 1: everyone discovers, crawls, verifies
    first = {t: set(got(t)) for t in targets}
    assert all(len(v) == 4 for v in first.values())
    with core.connect() as c:
        c.execute("UPDATE businesses SET next_run_at=?", (PAST,))
    drive(cfg(), targets, rounds=120)  # batch 2 continues from each target's cursor
    for t in targets:
        assert not (set(got(t)) & first[t]), f"{t}: second batch repeated first-batch URLs"
        assert len(got(t)) == (4 if t in ("ea.localhost", "eca.localhost") else 3)  # pools of 8 and 7
        assert cursors(t)[(ddg_lookup.queries(t)[0], "b1")]["next_page"] == 5
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", u("shared"))[0]["n"] == 1  # fetched once for two targets
    assert {r["target_domain"] for r in rows("SELECT d.target_domain FROM url_discoveries d JOIN discovered_urls x ON x.id=d.url_id WHERE x.url=?", u("shared"))} == {"ea.localhost", "eca.localhost"}
    want = {"ea.localhost": 8, "eb.localhost": 7, "ec.localhost": 7, "eca.localhost": 8, "ecb.localhost": 7}
    for t, n in want.items():  # each target verified independently against its own domain
        assert rows("SELECT COUNT(DISTINCT source_url) n FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1", t)[0]["n"] == n
    assert crawler_api.backlinks("ea.localhost", include_removed=False, limit=100, offset=0)["total_backlinks"] == 8
    assert {s["status"] for s in rows("SELECT status FROM business_jobs")} == {"done"}
    assert all(r["last_search_at"] for r in rows("SELECT last_search_at FROM businesses"))
    assert all(crawler_api.discovery_business_status(t)["discovery"]["state"] == "active" for t in biz)


# the whole product path: API registration -> engine search worker -> real _page() with only the ddgs CLIENT faked -> crawl -> backlinks via API
def test_engine_end_to_end_via_api_with_only_the_ddgs_client_faked(env, monkeypatch):
    import autonomous
    from test_autonomous import acfg, bl

    u = env
    pools, log = {}, []

    class FakeDDGS:  # same call shape as ddgs.DDGS: context manager, text(query, ..., max_results, page, backend)
        def __init__(self, timeout=None):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *e):
            return False

        def text(self, query, region=None, safesearch=None, max_results=10, page=1, backend="auto"):
            log.append((query, page, backend))
            return [{"href": h, "title": "t", "body": "b"} for h in pools.get(query, [])[(page - 1) * max_results: page * max_results]]

    monkeypatch.setattr(ddg_lookup, "DDGS", FakeDDGS)
    monkeypatch.setattr(ddg_lookup.time, "sleep", lambda s: None)
    monkeypatch.setattr(bj, "DDG_RESULTS", 10)
    monkeypatch.setattr(bj, "COMPETITOR_RESULTS", 10)
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    monkeypatch.setattr(bj, "MAX_ACTIVE_COMPETITOR_JOBS", 2)
    targets = ["ea.localhost", "eb.localhost", "eca.localhost"]
    for t in targets:
        SITES[t] = {"/": page()}
        h = t.split(".")[0]
        pool = [u(f"s{h}{i}") for i in range(11)]
        for i in range(11):
            SITES[f"s{h}{i}.localhost"] = {"/": page(a(u(h), "link"))}
        if t != "eb.localhost":
            pool.insert(3, u("shared"))
        pools[ddg_lookup.queries(t)[0]] = pool  # 12 results each: batch 1 reads page 1 (10), batch 2 page 2 (2)
    SITES["shared.localhost"] = {"/": page(a(u("ea"), "A"), a(u("eca"), "CA"))}

    crawler_api.onboard_business(crawler_api.OnboardRequest(domain="ea.localhost"))
    crawler_api.onboard_business(crawler_api.OnboardRequest(domain="eb.localhost"))
    assert crawler_api.add_business_competitors("ea.localhost", crawler_api.CompetitorsRequest(domains=["eca.localhost"]))["results"][0]["status"] == "registered"
    cfg = acfg(claim_batch=10)

    def run_until_done():
        async def go():
            eng = autonomous.Engine(cfg)
            for i in range(60):
                await eng.lookups()  # the engine's search worker: fair claim, cursor read, ddgs pages
                await eng.business()  # its tick: schedules due targets, expands, closes jobs
                if all(r["status"] == "done" for r in rows("SELECT status FROM business_jobs")) and len(rows("SELECT 1 FROM business_jobs")) >= 3:
                    return
                work = autonomous.claim_work(f"w{i % 2}", cfg)
                if work:
                    await engine.crawl_round(cfg.crawl, None, work, set(), False, len(work))
                autonomous.release(f"w{i % 2}")
            raise AssertionError([r["status"] for r in rows("SELECT target_domain, status FROM business_jobs")])
        asyncio.run(go())

    run_until_done()  # batch 1
    assert {t: len(got(t)) for t in targets} == {t: 10 for t in targets}
    assert not {r["status"] for r in rows("SELECT status FROM business_jobs")} - {"done"}
    with core.connect() as c:
        c.execute("UPDATE businesses SET next_run_at=?", (PAST,))
    n_jobs = len(rows("SELECT 1 FROM business_jobs"))
    asyncio.run(autonomous.Engine(cfg).business())  # scheduler tick creates the second round of jobs
    assert len(rows("SELECT 1 FROM business_jobs")) > n_jobs
    for _ in range(60):  # batch 2 through the same worker loop
        async def go2():
            eng = autonomous.Engine(cfg)
            await eng.lookups()
            await eng.business()
            work = autonomous.claim_work("w9", cfg)
            if work:
                await engine.crawl_round(cfg.crawl, None, work, set(), False, len(work))
            autonomous.release("w9")
        asyncio.run(go2())
        if all(r["status"] == "done" for r in rows("SELECT status FROM business_jobs")) and len(rows("SELECT 1 FROM business_jobs")) >= 6:
            break
    assert all(r["status"] == "done" for r in rows("SELECT status FROM business_jobs"))

    for t in targets:
        q0 = ddg_lookup.queries(t)[0]
        assert [p for q, p, b in log if q == q0 and b == "duckduckgo"] == [1, 2, 3]  # batch 1: page 1; batch 2: page 2, then the empty page 3. Never back to page 1
        assert cursors(t)[(q0, "duckduckgo")]["status"] == "exhausted"  # read past the last page; the query then rotated to the next backends (only known URLs there)
        assert rows("SELECT COUNT(DISTINCT url_id) n FROM url_discoveries WHERE target_domain=?", t)[0]["n"] == (11 if t == "eb.localhost" else 12)  # every result of the pool, none twice
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", u("shared"))[0]["n"] == 1
    for t in targets:
        r = crawler_api.backlinks(t, include_removed=False, limit=100, offset=0)
        assert r["status"] == "ok" and r["total_backlinks"] == (11 if t == "eb.localhost" else 12), (t, r["total_backlinks"])
    assert crawler_api.competitor_backlinks("eca.localhost", False, 100, 0)["total_backlinks"] == 12
    assert crawler_api.discovery_business_status("ea.localhost")["discovery"]["state"] in ("active", "waiting_rediscovery")


# ---- Phase 5.1: hierarchical fairness (level 1 = customer business, level 2 = its own domain and its competitors) ----
def contention(monkeypatch, rounds: int, owners: dict[str, int]) -> tuple[dict, dict, list]:
    """Sustained overload: every target is due again after each single search slot. owners: business -> number of competitors."""
    web(monkeypatch, {})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 1)
    onboard(*owners)
    parent = {b: b for b in owners}
    with core.connect() as c:
        for b, n in owners.items():
            for i in range(n):
                competitors.register(c, b, f"{b.split('.')[0]}-c{i}.localhost")
                parent[f"{b.split('.')[0]}-c{i}.localhost"] = b
    order: list[str] = []
    for _ in range(rounds):
        bj.schedule_due()
        j = bj._claim_search(1)
        assert j, "a due target must always be claimable"
        order.append(j[0]["target_domain"])
        asyncio.run(bj._search_one(j[0]))
        with core.connect() as c:
            c.execute("UPDATE business_jobs SET status='done', finished_at=? WHERE status='crawling'", (core.now(),))
            c.execute("UPDATE businesses SET next_run_at=? WHERE active=1", (PAST,))
    order = order[sum(owners.values()) + len(owners):]  # drop the warm-up: the onboarding jobs of every target are claimed first, group by group
    by_target = {t: order.count(t) for t in parent}
    by_parent = {b: sum(n for t, n in by_target.items() if parent[t] == b) for b in owners}
    return by_target, by_parent, order


OWNERS = {"big.localhost": 10, "mid.localhost": 2, "solo.localhost": 0}


def test_hierarchical_policy_gives_every_business_an_equal_share_whatever_its_competitor_count(env, monkeypatch):
    by_target, by_parent, order = contention(monkeypatch, 84 + 15, OWNERS)  # 14 targets in 3 groups, 15 warm-up searches
    assert max(by_parent.values()) - min(by_parent.values()) <= 1 and sum(by_parent.values()) == 84, by_parent  # 28/28/28
    for b, n in OWNERS.items():  # level 2: inside a group its own domain and its competitors rotate evenly
        mine = [v for t, v in by_target.items() if t == b or t.startswith(b.split(".")[0] + "-c")]
        assert max(mine) - min(mine) <= 1, (b, mine)
    for i in range(0, 84 - 3, 3):  # nobody waits more than one cycle of groups: the lone business is served every 3 turns
        assert "solo.localhost" in order[i:i + 3]
    print("hierarchical by parent:", by_parent, "| by target:", by_target)


def test_target_policy_lets_a_business_with_many_competitors_take_most_turns(env, monkeypatch):
    monkeypatch.setattr(bj, "FAIR_POLICY", "target")
    _, by_parent, _ = contention(monkeypatch, 84 + 15, OWNERS)
    assert by_parent["big.localhost"] > 0.7 * 84 and by_parent["solo.localhost"] <= 8, by_parent  # the limitation the hierarchy removes
    print("target-policy by parent:", by_parent)


def test_shared_competitor_is_one_stream_and_follows_its_remaining_owner(env, monkeypatch):
    web(monkeypatch, {})
    onboard("one.localhost", "two.localhost")
    with core.connect() as c:
        competitors.register(c, "two.localhost", "shared.localhost")
        competitors.register(c, "one.localhost", "shared.localhost")  # a second owner registers the same competitor
        assert c.execute("SELECT COUNT(*) FROM businesses WHERE domain='shared.localhost'").fetchone()[0] == 1  # one row = one discovery stream
        assert c.execute("SELECT COUNT(*) FROM business_jobs WHERE target_domain='shared.localhost'").fetchone()[0] == 1
        assert c.execute("SELECT parent_domain FROM businesses WHERE domain='shared.localhost'").fetchone()[0] == "one.localhost"  # lowest active owner
        competitors.set_active(c, "one.localhost", "shared.localhost", False)
        assert c.execute("SELECT parent_domain FROM businesses WHERE domain='shared.localhost'").fetchone()[0] == "two.localhost"
        assert c.execute("SELECT COUNT(*) FROM business_competitors WHERE competitor_domain='shared.localhost'").fetchone()[0] == 2  # relations kept
        assert c.execute("SELECT parent_domain FROM businesses WHERE domain='one.localhost'").fetchone()[0] is None  # a customer is its own group


def test_unknown_fair_policy_is_rejected(env, monkeypatch):
    monkeypatch.setattr(bj, "FAIR_POLICY", "weighted")
    with pytest.raises(ValueError):
        bj.schedule_due()


# ---- Phase 5.1: ddgs signals "empty" by raising DDGSException("No results found."); a failure is any other error and must never exhaust a cursor ----
def ddgs_client(monkeypatch, pools, fail=None, empty_always=False):
    """Fake the ddgs CLIENT with its real signalling: pages past the pool raise DDGSException('No results found.'), failures raise their own error."""
    from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException
    calls = []

    class C:
        def __init__(self, timeout=None):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *e):
            return False

        def text(self, query, region=None, safesearch=None, max_results=10, page=1, backend="auto"):
            calls.append((query, page, backend))
            if fail and fail(query, page, backend) == "rate":
                raise RatelimitException("429 Too Many Requests")
            if fail and fail(query, page, backend) == "timeout":
                raise TimeoutException("timed out")
            res = [] if empty_always else pools.get(query, [])[(page - 1) * 10: page * 10]
            if not res:
                raise DDGSException("No results found.")
            return [{"href": h} for h in res]

    monkeypatch.setattr(ddg_lookup, "DDGS", C)
    monkeypatch.setattr(ddg_lookup.time, "sleep", lambda s: None)
    monkeypatch.setattr(ddg_lookup, "BACKENDS", ["b1"])
    monkeypatch.setattr(bj, "DDG_RESULTS", 50)
    return calls


def test_page_tells_empty_from_failure():
    import pytest as _p
    from ddgs.exceptions import DDGSException
    for exc, empty in ((DDGSException("No results found."), True), (DDGSException("RatelimitException: 429"), False), (TimeoutError("x"), False)):
        assert ddg_lookup._is_empty(exc) is empty


def test_running_off_the_end_exhausts_but_only_after_the_engine_answered(env, monkeypatch):
    d = "end.localhost"
    q0 = ddg_lookup.queries(d)[0]
    ddgs_client(monkeypatch, {q0: urls(25)})  # pages: 10, 10, 5, then "No results found."
    onboard(d)
    asyncio.run(bj.search_pending())
    c = cursors(d)[(q0, "b1")]
    assert len(got(d)) == 25 and c["status"] == "exhausted" and state(d)["exhausted_cursors"] == 1  # page 4 was empty AFTER pages 1-3 answered: the real end
    assert job_status(d)["status"] == "crawling" and not job_status(d)["last_error"]  # an empty page is not an error


def test_a_run_that_starts_on_an_empty_page_needs_confirmation_before_exhausting(env, monkeypatch):
    d = "start.localhost"
    q0 = ddg_lookup.queries(d)[0]
    calls = ddgs_client(monkeypatch, {q0: urls(20)})
    onboard(d)
    with core.connect() as c:  # the cursor already sits past the last page, e.g. after a rate-limited weekend
        c.execute("INSERT INTO discovery_cursors (domain, query_hash, backend, query, next_page) VALUES (?,?,?,?,3)", (d, ddg_lookup.qhash(q0), "b1", q0))
    asyncio.run(bj.search_pending())
    k = cursors(d)[(q0, "b1")]
    assert k["status"] == "active" and k["empty_streak"] == 1 and k["next_page"] == 3  # could be a blocked client: counted, not believed
    assert next_batch(d) == []
    k = cursors(d)[(q0, "b1")]
    assert k["status"] == "exhausted" and k["empty_streak"] == 2  # the second such run confirms it
    assert state(d)["state"] in ("active", "waiting_rediscovery") and not job_status(d)["last_error"]


def test_blocked_client_or_rate_limit_never_exhausts_anything(env, monkeypatch):
    d = "block.localhost"
    q0 = ddg_lookup.queries(d)[0]
    onboard(d)
    ddgs_client(monkeypatch, {}, empty_always=True)  # a bot challenge: every page "No results found."
    asyncio.run(bj.search_pending())
    assert not cursors(d) and job_status(d)["status"] == "crawling" and job_status(d)["search_results"] == 0  # nothing was learned, nothing was marked
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done'")
        c.execute("INSERT INTO discovery_cursors (domain, query_hash, backend, query, next_page) VALUES (?,?,?,?,4)", (d, ddg_lookup.qhash(q0), "b1", q0))
    for kind in ("rate", "timeout"):  # transport failures: the search FAILS (retried with backoff), the cursor stays on its page, never exhausted
        ddgs_client(monkeypatch, {q0: urls(60)}, fail=lambda q, p, b, k=kind: k)
        monkeypatch.setattr(bj, "DDG_COOLDOWN", 0)
        with core.connect() as c:
            c.execute("UPDATE business_jobs SET status='done' WHERE status<>'done'")
            c.execute("UPDATE businesses SET next_run_at=?", (PAST,))
        assert bj.schedule_due() == 1
        asyncio.run(bj.search_pending())
        j = job_status(d)
        assert j["status"] == "pending" and j["retry_count"] == 1 and j["last_error"], (kind, j)
        k = cursors(d)[(q0, "b1")]
        assert k["status"] == "active" and k["next_page"] == 4 and k["empty_streak"] == 0
        with core.connect() as c:
            c.execute("UPDATE business_jobs SET status='done'")


def test_deep_cursor_rereads_the_head_once_per_interval_and_keeps_its_page(env, monkeypatch):
    d = "head.example"
    q0 = ddg_lookup.queries(d)[0]
    calls = web(monkeypatch, {q0: urls(200)}, backends=("b1",))
    onboard(d)
    with core.connect() as c:  # deep cursor whose head was last read two days ago
        c.execute("INSERT INTO discovery_cursors (domain, query_hash, backend, query, next_page, head_checked_at) VALUES (?,?,?,?,8,?)",
                  (d, ddg_lookup.qhash(q0), "b1", q0, core.later(days=-2)))
    asyncio.run(bj.search_pending())
    pages = [p for q, p, b in calls if q == q0]
    assert pages[:2] == [1, 2] and pages[2] == 8  # head first, then the deep read where it was
    k = cursors(d)[(q0, "b1")]
    assert k["next_page"] > 8 and k["head_checked_at"] > core.later(hours=-1)


def test_dead_backend_is_skipped_after_two_strikes_and_comes_back_after_an_answer(env, monkeypatch):
    doms = ["dead1.example", "dead2.example", "dead3.example"]
    calls = web(monkeypatch, {q: urls(60, d) for d in doms for q in ddg_lookup.queries(d)}, backends=("b1", "b2"), fail=lambda q, p, b: b == "b1")
    for d in doms:
        onboard(d)
        asyncio.run(bj.search_pending())
    assert len([1 for q, p, b in calls if b == "b1"]) == 2  # two searches paid b1's failure; the third went straight to b2
    assert ddg_lookup._cooling(["b1", "b2"]) == {"b1"}
    ddg_lookup._note("b1", True)
    assert ddg_lookup._cooling(["b1", "b2"]) == set()
    ddg_lookup._note("b1", False), ddg_lookup._note("b1", False), ddg_lookup._note("b2", False), ddg_lookup._note("b2", False)
    assert ddg_lookup._cooling(["b1", "b2"]) == set()  # everything cooling: try anyway

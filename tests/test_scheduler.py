"""Scheduler regressions: set-based exhausted-domain retirement equals the old per-row COUNT rule, scales, skips locked rows; the cycle isolates
failing operations and clears a seed flag only after that seed succeeded. Real PostgreSQL (isolated schema)."""

import asyncio
import threading
import time

import pytest

import autonomous
import crawler_core as core
from test_autonomous import acfg
from test_crawler import env, port, rows  # noqa: F401 - fixtures

# the rule as it was before the optimisation: a COUNT(*) per pending row (kept here only as the reference to compare against)
OLD_RULE = ("SELECT id FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending') AND domain IN (SELECT domain FROM discovered_domains"
            " WHERE discovery_status IN ('admitted','crawled')) AND (SELECT COUNT(*) FROM discovered_urls k WHERE k.domain=discovered_urls.domain"
            " AND k.crawl_status='completed')>=?")


def domain(name, status="admitted", **counts):
    """counts: status -> number of URLs, e.g. completed=3, pending=5, failed=2."""
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES (?,?)", (name, status))
        for st, n in counts.items():
            for i in range(n):
                core.save_url(c, f"http://{name}/{st}{i}", None, "t", 1, True, st)


def skipped_budget() -> set[int]:
    return {r["id"] for r in rows("SELECT id FROM discovered_urls WHERE skip_reason='domain_page_budget_exhausted'")}


@pytest.mark.parametrize("budget", [1, 3, 5])
def test_new_retirement_selects_exactly_what_the_old_rule_selected(env, budget):
    domain("below.localhost", completed=budget - 1, pending=4)
    domain("exact.localhost", completed=budget, pending=4, retry_pending=2)
    domain("above.localhost", completed=budget + 3, pending=4, failed=5)
    domain("notcounted.localhost", completed=max(budget - 1, 0), failed=9, recrawling=9, crawling=2, pending=3)  # only 'completed' counts toward the budget
    domain("crawledstatus.localhost", "crawled", completed=budget, pending=2)  # retired earlier, still gets its leftovers skipped
    domain("discovered.localhost", "discovered", completed=budget + 5, pending=6)  # not admitted: untouched
    domain("blocked.localhost", "blocked", completed=budget + 5, pending=6)
    expected = {r["id"] for r in rows(OLD_RULE, budget)}
    assert expected  # the scenario really selects something
    cfg = acfg(domain_page_budget=budget)
    autonomous.retire_exhausted(cfg)
    assert skipped_budget() == expected  # same rows, nothing more, nothing less
    left = {r["domain"] for r in rows("SELECT domain FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending')")}
    assert {"below.localhost", "discovered.localhost", "blocked.localhost"} <= left and "exact.localhost" not in left and "above.localhost" not in left
    autonomous.retire_exhausted(cfg)  # idempotent
    assert skipped_budget() == expected


def test_retirement_scales_and_domains_are_retired_in_one_pass(env):
    """60k pending + 3k completed URLs over 300 admitted domains: the old per-row COUNT is O(pending x domain size), this must stay a few seconds."""
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) SELECT 'bulk' || g || '.localhost', 'admitted' FROM generate_series(1,300) g")
        for st, per in (("pending", 200), ("completed", 10)):
            c.execute("INSERT INTO discovered_urls (url, normalized_url, domain, hostname, crawl_status, crawl_depth)"
                      " SELECT 'http://bulk' || g || '.localhost/" + st + "' || i, 'http://bulk' || g || '.localhost/" + st + "' || i, 'bulk' || g || '.localhost',"
                      " 'bulk' || g || '.localhost', '" + st + "', 1 FROM generate_series(1,300) g, generate_series(1," + str(per) + ") i")
    t0 = time.monotonic()
    retired = autonomous.retire_exhausted(acfg(domain_page_budget=10))
    took = time.monotonic() - t0
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE skip_reason='domain_page_budget_exhausted'")[0]["n"] == 300 * 200
    assert retired == 300 and took < 20, f"retire_exhausted took {took:.1f}s"
    below = autonomous.retire_exhausted(acfg(domain_page_budget=10))
    assert below == 0  # nothing left to do


def test_locked_rows_are_skipped_not_waited_for_and_retired_next_cycle(env):
    domain("busy.localhost", completed=3, pending=4)
    cfg = acfg(domain_page_budget=3)
    locked, release = threading.Event(), threading.Event()

    def hold():  # a Celery claim in flight: row lock on one pending URL
        with core.connect() as c:
            c.execute("SELECT id FROM discovered_urls WHERE url='http://busy.localhost/pending0' FOR UPDATE")
            locked.set()
            release.wait(30)
    th = threading.Thread(target=hold)
    th.start()
    assert locked.wait(10)
    t0 = time.monotonic()
    autonomous.retire_exhausted(cfg)
    assert time.monotonic() - t0 < 5  # did not wait for the lock (would have been the 60s lock timeout)
    assert len(skipped_budget()) == 3 and rows("SELECT crawl_status FROM discovered_urls WHERE url='http://busy.localhost/pending0'")[0]["crawl_status"] == "pending"
    release.set()
    th.join()
    autonomous.retire_exhausted(cfg)
    assert len(skipped_budget()) == 4  # the next cycle gets it


def test_reactivation_matches_old_rule_and_budget_domains_stay_retired(env):
    cfg = acfg(domain_page_budget=3)
    domain("fresh.localhost", "crawled", completed=1, pending=2)  # finished, new work, budget left -> back to 'discovered'
    domain("spent.localhost", "crawled", completed=3, pending=2)  # budget spent -> stays
    domain("nowork.localhost", "crawled", completed=1)  # nothing pending -> stays
    domain("zero.localhost", "crawled", pending=2)  # no completed page at all counts 0 < budget -> reactivated
    assert autonomous.reactivate_domains(cfg) == 2
    st = {r["domain"]: r["discovery_status"] for r in rows("SELECT * FROM discovered_domains")}
    assert st == {"fresh.localhost": "discovered", "spent.localhost": "crawled", "nowork.localhost": "crawled", "zero.localhost": "discovered"}


def test_cycle_isolates_a_failing_operation_and_seed_flags_follow_success(env, monkeypatch):
    calls = []
    monkeypatch.setattr(autonomous, "release", lambda owner=None: calls.append("release") or 0)
    monkeypatch.setattr(autonomous, "retire_unreachable", lambda: calls.append("unreachable") or 0)

    def boom(cfg):
        calls.append("exhausted")
        raise RuntimeError("canceling statement due to lock timeout")
    monkeypatch.setattr(autonomous, "retire_exhausted", boom)
    monkeypatch.setattr(autonomous, "reactivate_domains", lambda cfg: calls.append("reactivate") or 0)

    async def admit(cfg):
        calls.append("admit")
        return 1
    monkeypatch.setattr(autonomous, "admit_batch", admit)

    async def fake_seed(urls, cfg=None):
        if "bad" in urls[0]:
            raise RuntimeError("host unreachable")
        calls.append("seed " + urls[0])
        return {urls[0]: True}
    monkeypatch.setattr(autonomous, "seed", fake_seed)
    with core.connect() as c:
        for u in ("https://good.localhost/", "https://bad.localhost/"):
            c.execute("INSERT INTO engine_state (key, value) VALUES (?, '1')", (f"seed:{u}",))
    eng = autonomous.Engine(acfg())
    with pytest.raises(autonomous.PartialCycle):  # the cycle ran to the end although two things failed, and says so
        asyncio.run(eng.scheduler())
    assert calls == ["seed https://good.localhost/", "release", "unreachable", "exhausted", "reactivate", "admit"]  # the failing op did not stop the rest
    assert [r["key"] for r in rows("SELECT key FROM engine_state WHERE key LIKE 'seed:%'")] == ["seed:https://bad.localhost/"]  # only the failed seed stays for a retry
    calls.clear()
    monkeypatch.setattr(autonomous, "seed", lambda urls, cfg=None: asyncio.sleep(0, {urls[0]: True}))  # recovered
    monkeypatch.setattr(autonomous, "retire_exhausted", lambda cfg: 0)
    asyncio.run(eng.scheduler())
    assert rows("SELECT COUNT(*) n FROM engine_state WHERE key LIKE 'seed:%'")[0]["n"] == 0  # retried and cleared, no duplicates left behind


def test_cycle_raises_only_when_nothing_worked(env, monkeypatch):
    def fail(*a, **k):
        raise RuntimeError("db down")
    for name in ("release", "retire_unreachable", "retire_exhausted", "reactivate_domains"):
        monkeypatch.setattr(autonomous, name, fail)

    async def admit(cfg):
        raise RuntimeError("db down")
    monkeypatch.setattr(autonomous, "admit_batch", admit)
    with pytest.raises(RuntimeError, match="every scheduler operation failed"):
        asyncio.run(autonomous.Engine(acfg()).scheduler())  # the supervisor then records the error and backs off


# ================= cycle outcomes, scheduler ownership, lease recovery, seeds, discovery alongside the scheduler =================
import json
import os
import subprocess
import sys
from pathlib import Path

from bs4 import BeautifulSoup

import business_jobs as bj
import crawler_api
import crawler_engine as engine
from test_business_jobs import cfg as bcfg, job_status, one_round, search_with
from test_crawler import SITES, a, page


def wstatus(name="scheduler") -> dict:
    return rows("SELECT * FROM worker_status WHERE name=?", name)[0]


def patch_ops(monkeypatch, fail=()):
    """Replace every scheduler operation by a stub; those named in `fail` raise."""
    def op(name, ret=0):
        def f(*a, **k):
            if name in fail:
                raise RuntimeError(f"{name} boom")
            return ret
        return f
    monkeypatch.setattr(autonomous, "release", op("release"))
    monkeypatch.setattr(autonomous, "retire_unreachable", op("unreachable"))
    monkeypatch.setattr(autonomous, "retire_exhausted", op("exhausted"))
    monkeypatch.setattr(autonomous, "reactivate_domains", op("reactivate"))

    async def admit(cfg):
        if "admit" in fail:
            raise RuntimeError("admit boom")
        return 2
    monkeypatch.setattr(autonomous, "admit_batch", admit)


def run_supervised_once(eng, name="scheduler"):
    """The real supervisor around one scheduler cycle (it stops itself after the first pass)."""
    async def fn():
        try:
            return await eng.scheduler()
        finally:
            eng.stop.set()
    asyncio.run(eng._supervise(name, fn, 0.01))


def test_cycle_outcomes_are_recorded_and_only_a_clean_cycle_counts_as_success(env, monkeypatch):
    patch_ops(monkeypatch)  # all operations succeed
    run_supervised_once(autonomous.Engine(acfg()))
    w = wstatus()
    assert w["last_cycle_status"] == "completed" and w["last_success_at"] and w["errors"] == 0 and w["restart_count"] == 0
    full = w["last_success_at"]
    detail = json.loads(w["last_cycle_detail"])
    assert detail["scheduler_cycle_status"] == "completed" and set(detail["scheduler_operation_duration_seconds"]) == {
        "SEEDS", "LEASE-RECOVERY", "UNREACHABLE-RETIREMENT", "EXHAUSTED-RETIREMENT", "DOMAIN-REACTIVATION", "DOMAIN-ADMISSION"} and detail["domains_admitted"] == 2
    time.sleep(1.1)  # core.now() has one-second resolution

    # one operation fails: a cycle that ran (runs+1) and recorded its error, NOT a success, NOT a crash
    patch_ops(monkeypatch, fail=("exhausted",))
    run_supervised_once(autonomous.Engine(acfg()))
    w = wstatus()
    assert w["last_cycle_status"] == "completed_with_errors" and w["last_partial_at"] and w["runs"] == 2
    assert w["last_success_at"] == full  # did not advance
    assert w["errors"] == 1 and "EXHAUSTED-RETIREMENT" in w["last_error"] and w["restart_count"] == 0 and w["status"] == "stopped"
    d = json.loads(w["last_cycle_detail"])
    assert list(d["scheduler_operation_failures"]) == ["EXHAUSTED-RETIREMENT"] and d["scheduler_operation_duration_seconds"]["DOMAIN-ADMISSION"] >= 0

    # several operations fail: still partial, the remaining ones ran
    patch_ops(monkeypatch, fail=("release", "reactivate", "admit"))
    run_supervised_once(autonomous.Engine(acfg()))
    w = wstatus()
    assert w["last_cycle_status"] == "completed_with_errors" and w["last_success_at"] == full and w["restart_count"] == 0

    # everything fails (the seed too): a failed cycle and a restart for the supervisor
    patch_ops(monkeypatch, fail=("release", "unreachable", "exhausted", "reactivate", "admit"))
    with core.connect() as c:
        c.execute("INSERT INTO engine_state (key, value) VALUES ('seed:https://x.localhost/', '1')")
    monkeypatch.setattr(autonomous, "seed", lambda urls, cfg=None: (_ for _ in ()).throw(RuntimeError("seed boom")))
    run_supervised_once(autonomous.Engine(acfg()))
    w = wstatus()
    assert w["last_cycle_status"] == "failed" and w["last_failed_at"] and w["restart_count"] == 1 and w["last_success_at"] == full


def test_scheduler_ownership_two_engines_one_cycle_and_release_paths(env, monkeypatch):
    calls = []
    patch_ops(monkeypatch)
    monkeypatch.setattr(autonomous, "release", lambda owner=None: calls.append("cycle") or 0)
    e1, e2 = autonomous.Engine(acfg()), autonomous.Engine(acfg())
    lock, owner = autonomous.try_scheduler_lock(e1.sched_owner)  # engine 1 is in the middle of its cycle
    assert lock is not None and owner == e1.sched_owner
    with pytest.raises(autonomous.CycleSkipped):
        asyncio.run(e2.scheduler())  # engine 2: skipped at once, nothing executed
    assert calls == [] and wstatus()["last_cycle_status"] == "skipped"
    assert rows("SELECT value FROM engine_state WHERE key='scheduler_lock_contention'")[0]["value"] == "1"
    run_supervised_once(autonomous.Engine(acfg()))  # a skipped cycle through the supervisor: no error, no restart, no success claimed
    w = wstatus()
    assert w["errors"] == 0 and w["restart_count"] == 0 and w["last_success_at"] is None and w["runs"] == 0
    autonomous.release_scheduler_lock(lock)  # normal end of engine 1's cycle
    asyncio.run(e2.scheduler())  # now engine 2 runs
    assert calls == ["cycle"]
    c, _ = autonomous.try_scheduler_lock("x")  # and released again after engine 2's cycle
    assert c is not None
    autonomous.release_scheduler_lock(c)


def test_scheduler_lock_is_released_when_the_owning_process_is_killed(env):
    code = "import autonomous, time\nc, o = autonomous.try_scheduler_lock('other-process')\nprint('held' if c else 'busy', flush=True)\ntime.sleep(120)\n"
    root = Path(__file__).resolve().parent.parent
    p = subprocess.Popen([sys.executable, "-c", code], cwd=root, env={**os.environ, "DB_SCHEMA": core.DB_SCHEMA, "CRAWLER_ALLOW_PRIVATE": "1"}, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        c, holder = autonomous.try_scheduler_lock("me")
        assert c is None and holder == "other-process"  # a second PROCESS cannot get it
        p.kill()  # abnormal termination: no unlock, no cleanup
        p.wait(10)
        for _ in range(50):  # the server notices the dead connection
            c, holder = autonomous.try_scheduler_lock("me")
            if c:
                break
            time.sleep(0.2)
        assert c is not None, f"lock still held by {holder} after the owner was killed"
        autonomous.release_scheduler_lock(c)
    finally:
        p.kill()


def test_lease_recovery_and_retirement_of_an_exhausted_domain(env):
    """worker leases a URL -> domain reaches its budget -> retirement must not touch the leased URL -> worker dies -> lease expires ->
    recovery returns it -> the NEXT cycle retires it. Nothing lost."""
    domain("busy.localhost", completed=2, pending=4)
    c = acfg(domain_page_budget=3, claim_batch=1)
    ((url, _),) = autonomous.claim_urls("dying-worker", c)
    with core.connect() as conn:  # another page of the domain completes: the budget (3) is now spent
        conn.execute("UPDATE discovered_urls SET crawl_status='completed' WHERE url=(SELECT url FROM discovered_urls WHERE crawl_status='pending' ORDER BY id LIMIT 1)")
    autonomous.retire_exhausted(c)
    st = {r["url"]: r["crawl_status"] for r in rows("SELECT url, crawl_status FROM discovered_urls WHERE url LIKE '%pending%'")}
    assert st[url] == "crawling"  # an actively leased URL is NOT marked skipped
    assert sum(1 for v in st.values() if v == "skipped") == 2 and sum(1 for v in st.values() if v == "completed") == 1
    with core.connect() as conn:  # the worker is killed: its lease runs out
        conn.execute("UPDATE discovered_urls SET lease_expires=? WHERE url=?", (core.later(seconds=-5), url))
    assert autonomous.release() == 1
    assert rows("SELECT crawl_status, lease_owner FROM discovered_urls WHERE url=?", url)[0] == {"crawl_status": "pending", "lease_owner": None}  # recovered, not lost
    autonomous.retire_exhausted(c)  # next cycle
    assert rows("SELECT crawl_status, skip_reason FROM discovered_urls WHERE url=?", url)[0] == {"crawl_status": "skipped", "skip_reason": "domain_page_budget_exhausted"}
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE domain='busy.localhost'")[0]["n"] == 6  # every URL still accounted for
    autonomous.retire_exhausted(c)  # repeated cycles change nothing
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE skip_reason='domain_page_budget_exhausted'")[0]["n"] == 3


def test_stale_autonomous_worker_cannot_overwrite_the_new_owner(env):
    """Regression: claim_urls did not bump the fencing token, so two workers leasing a URL one after the other held the SAME token."""
    domain("fence.localhost", pending=1)
    c = acfg(claim_batch=5, lease_seconds=1)
    ((url, _),) = autonomous.claim_urls("old", c)
    old = dict(engine.FENCE)
    with core.connect() as conn:
        conn.execute("UPDATE discovered_urls SET lease_expires=? WHERE url=?", (core.later(seconds=-5), url))
    assert autonomous.release() == 1
    ((url2, _),) = autonomous.claim_urls("new", c)
    new = dict(engine.FENCE)
    norm = core.normalize_url(url)
    assert url2 == url and old[norm] != new[norm]
    soup = BeautifulSoup("<html><body><a href='https://t.org/'>t</a></body></html>", "html.parser")

    def write(fence):
        engine.FENCE.clear()
        engine.FENCE.update(fence)
        with core.connect() as conn:
            engine.process_page(conn, url, url, 0, soup, 200, "text/html", c.crawl, set(), False, core.now())
    write(old)  # the stale worker finishes late
    assert rows("SELECT crawl_status, lease_owner FROM discovered_urls WHERE url=?", url)[0] == {"crawl_status": "crawling", "lease_owner": "new"}
    write(new)
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url=?", url)[0]["crawl_status"] == "completed"


def test_seed_timeout_and_restart_resume(env, monkeypatch):
    monkeypatch.setattr(autonomous, "SEED_TIMEOUT", 0.3)
    patch_ops(monkeypatch)
    started = []

    async def seed_stub(urls, cfg=None):
        started.append(urls[0])
        if "hang" in urls[0]:
            await asyncio.sleep(30)  # an unreachable host
        return {urls[0]: True}
    monkeypatch.setattr(autonomous, "seed", seed_stub)
    with core.connect() as c:
        for u in ("https://hang.localhost/", "https://ok1.localhost/", "https://ok2.localhost/"):
            c.execute("INSERT INTO engine_state (key, value) VALUES (?, '1')", (f"seed:{u}",))
    with pytest.raises(autonomous.PartialCycle):
        asyncio.run(autonomous.Engine(acfg()).scheduler())
    assert started == ["https://hang.localhost/", "https://ok1.localhost/", "https://ok2.localhost/"]  # the hung seed did not block the others
    assert [r["key"] for r in rows("SELECT key FROM engine_state WHERE key LIKE 'seed:%'")] == ["seed:https://hang.localhost/"]  # only it stays for a retry
    detail = json.loads(wstatus()["last_cycle_detail"])
    assert detail["pending_seed_count"] == 3 and "https://hang.localhost/" in detail["scheduler_operation_failures"]["SEEDS"]
    started.clear()  # engine restart: a new engine picks the leftover flag up again, and only that one
    monkeypatch.setattr(autonomous, "SEED_TIMEOUT", 5)

    async def ok(urls, cfg=None):
        started.append(urls[0])
        return {urls[0]: True}
    monkeypatch.setattr(autonomous, "seed", ok)
    asyncio.run(autonomous.Engine(acfg()).scheduler())
    assert started == ["https://hang.localhost/"] and rows("SELECT COUNT(*) n FROM engine_state WHERE key LIKE 'seed:%'")[0]["n"] == 0


def test_real_seed_is_idempotent_across_cycles(env, monkeypatch):
    u = env
    SITES["seedme.localhost"] = {"/": page(a("/p2")), "/p2": page()}
    monkeypatch.setattr(autonomous, "release", lambda owner=None: 0)  # keep the cycle small; seed() itself is the real one
    monkeypatch.setattr(autonomous, "admit_batch", lambda cfg: asyncio.sleep(0, 0))
    with core.connect() as c:
        c.execute("INSERT INTO engine_state (key, value) VALUES (?, '1')", (f"seed:{u('seedme')}",))
    eng = autonomous.Engine(acfg())
    for _ in range(2):
        asyncio.run(eng.scheduler())
    assert rows("SELECT COUNT(*) n FROM engine_state WHERE key LIKE 'seed:%'")[0]["n"] == 0
    assert rows("SELECT COUNT(*) n FROM discovered_domains WHERE domain='seedme.localhost'")[0]["n"] == 1
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE url=?", u("seedme"))[0]["n"] == 1


def test_business_and_competitor_discovery_progress_while_a_scheduler_operation_fails(env, monkeypatch):
    """One customer + two competitors = three simultaneous jobs; the scheduler's retirement fails EVERY cycle; the jobs still finish."""
    u = env
    for t in ("own", "ca", "cb"):
        SITES[f"{t}.localhost"] = {"/": page()}
    SITES["shared.localhost"] = {"/": page(a(u("own"), "O"), a(u("ca"), "A"), a(u("cb"), "B"))}
    SITES["caonly.localhost"] = {"/": page(a(u("ca"), "A2"))}
    search_with(monkeypatch, {"own.localhost": [u("shared")], "ca.localhost": [u("shared"), u("caonly")], "cb.localhost": [u("shared")]})
    monkeypatch.setattr(bj, "DDG_CONCURRENCY", 3)
    monkeypatch.setattr(bj, "MAX_ACTIVE_COMPETITOR_JOBS", 2)
    with core.connect() as c:
        bj.create_job(c, "own.localhost")
    crawler_api.add_business_competitors("own.localhost", crawler_api.CompetitorsRequest(domains=["ca.localhost", "cb.localhost"]))
    monkeypatch.setattr(autonomous, "retire_exhausted", lambda cfg: (_ for _ in ()).throw(RuntimeError("lock timeout")))
    monkeypatch.setattr(autonomous, "admit_batch", lambda cfg: asyncio.sleep(0, 0))
    eng, c, partial = autonomous.Engine(acfg()), bcfg(), 0
    domains = ["own.localhost", "ca.localhost", "cb.localhost"]
    for i in range(60):
        asyncio.run(bj.search_pending())
        bj.tick()
        try:
            asyncio.run(eng.scheduler())
        except autonomous.PartialCycle:
            partial += 1  # the scheduler is failing every single cycle...
        if all(job_status(d)["status"] == "done" for d in domains):
            break
        one_round(c, f"w{i % 3}")
    assert partial >= 1 and [job_status(d)["status"] for d in domains] == ["done"] * 3  # ...and discovery does not care
    assert crawler_api.competitor_backlinks("ca.localhost", False, 100, 0)["total_backlinks"] == 2
    assert crawler_api.backlinks("own.localhost", False, 100, 0)["total_backlinks"] == 1
    assert rows("SELECT COUNT(*) n FROM crawl_history WHERE source_url=?", u("shared"))[0]["n"] == 1  # shared URL fetched once for three jobs


def test_admission_is_bounded_per_domain_and_a_stuck_domain_is_retired_after_three_failures(env, monkeypatch):
    """Found by the staging run: one slow host in admit_domain (robots + sitemaps, many requests) held the whole scheduler cycle, and the lock, for minutes."""
    monkeypatch.setattr(autonomous, "ADMIT_TIMEOUT", 0.3)
    for d in ("stuck.localhost", "fine1.localhost", "fine2.localhost"):
        with core.connect() as c:
            c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES (?, 'discovered', ?)", (d, 1 if d == "stuck.localhost" else 5))
    asked = []

    async def fake_admit(cfg, job, dom, *a):
        asked.append(dom)
        if dom == "stuck.localhost":
            await asyncio.sleep(30)
        with core.connect() as c:
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (dom,))
        return 1
    monkeypatch.setattr(autonomous.engine, "admit_domain", fake_admit)
    cfg = acfg(admit_per_cycle=5, max_admitted=50)
    for n in (1, 2, 3):
        t0 = time.monotonic()
        got = asyncio.run(autonomous.admit_batch(cfg))
        assert time.monotonic() - t0 < 10  # bounded, not 30s
        st = {r["domain"]: r["discovery_status"] for r in rows("SELECT * FROM discovered_domains")}
        assert st["fine1.localhost"] == st["fine2.localhost"] == "admitted"  # the others were admitted in the same cycle
        assert st["stuck.localhost"] == ("unreachable" if n == 3 else "discovered")  # three strikes, then it no longer blocks the head of the queue
        assert got == (2 if n == 1 else 0)
    assert rows("SELECT value FROM engine_state WHERE key='admit_fail:stuck.localhost'")[0]["value"] == "3"


def test_admission_cycle_budget_stops_starting_new_domains(env, monkeypatch):
    monkeypatch.setattr(autonomous, "ADMIT_CYCLE_BUDGET", 0.2)
    for i in range(4):
        with core.connect() as c:
            c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES (?, 'discovered', 5)", (f"slow{i}.localhost",))
    started = []

    async def slowish(cfg, job, dom, *a):
        started.append(dom)
        await asyncio.sleep(0.25)
        return 1
    monkeypatch.setattr(autonomous.engine, "admit_domain", slowish)
    assert asyncio.run(autonomous.admit_batch(acfg(admit_per_cycle=5, max_admitted=50))) == 1
    assert len(started) == 1  # the budget ran out after the first one: the rest wait for the next cycle


def _hold_domain_row(name: str):
    """What a crawl does: a transaction that has the domain row locked (process_page upserts its domain inside a long transaction)."""
    locked, release = threading.Event(), threading.Event()

    def hold():
        with core.connect() as c:
            c.execute("SELECT id FROM discovered_domains WHERE domain=? FOR UPDATE", (name,))
            locked.set()
            release.wait(60)
    th = threading.Thread(target=hold)
    th.start()
    assert locked.wait(10)
    return release, th


def test_admission_and_retirement_skip_a_domain_row_locked_by_a_crawl(env, monkeypatch):
    """Found by the staging run: DOMAIN-ADMISSION waited 60s for a domain row a crawl held, hit the lock timeout and failed the cycle (8 of 14 cycles)."""
    for d, pr in (("locked.localhost", 1), ("free.localhost", 5)):
        with core.connect() as c:
            c.execute("INSERT INTO discovered_domains (domain, discovery_status, priority) VALUES (?, 'discovered', ?)", (d, pr))
    asked = []

    async def fake_admit(cfg, job, dom, *a):
        asked.append(dom)
        with core.connect() as c:
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (dom,))
        return 1
    monkeypatch.setattr(autonomous.engine, "admit_domain", fake_admit)
    cfg = acfg(admit_per_cycle=5, max_admitted=50)
    release, th = _hold_domain_row("locked.localhost")
    t0 = time.monotonic()
    assert asyncio.run(autonomous.admit_batch(cfg)) == 1  # the free domain was admitted, the locked one is left for later
    assert time.monotonic() - t0 < 15 and asked == ["free.localhost"]  # ~3s lock_timeout, not the 60s database-wide one
    release.set()
    th.join()
    assert asyncio.run(autonomous.admit_batch(cfg)) == 1 and asked == ["free.localhost", "locked.localhost"]  # next cycle

    # retirement/reactivation/unreachable updates of domain rows skip a locked row the same way
    domain("done.localhost", completed=3)  # admitted, 3 completed pages, nothing pending: it can be retired
    c3 = acfg(domain_page_budget=3)
    release, th = _hold_domain_row("done.localhost")
    t0 = time.monotonic()
    assert autonomous.retire_exhausted(c3) == 2 and time.monotonic() - t0 < 10  # the two empty domains admitted above are retired; the locked one is not waited for
    assert rows("SELECT discovery_status FROM discovered_domains WHERE domain='done.localhost'")[0]["discovery_status"] == "admitted"
    release.set()
    th.join()
    assert autonomous.retire_exhausted(c3) == 1
    assert rows("SELECT discovery_status FROM discovered_domains WHERE domain='done.localhost'")[0]["discovery_status"] == "crawled"

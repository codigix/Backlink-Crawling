"""Deadlock handling: the lookups write (discovered_domains upserts) uses one global (domain, URL) order and is retried whole on SQLSTATE 40P01;
lease renewal never waits for a row. Real PostgreSQL (isolated schema). Nothing is deleted anywhere."""

import threading
import time

import psycopg
import pytest

import autonomous
import business_jobs as bj
import crawler_core as core
from test_business_jobs import onboard
from test_crawler import env, port, rows  # noqa: F401 - fixtures


def claimed(*domains):
    onboard(*domains, max_pages=500)
    return bj._claim_search(len(domains))


def test_opposite_order_candidate_writes_do_not_deadlock_and_domain_counts_are_exact(env):
    """Two lookups workers store results over the SAME 60 domains, one listing them forward and one backward. Before the fix the per-URL
    upserts ran in search order, i.e. opposite lock orders = a deadlock cycle."""
    doms = [f"d{i:02d}example.com" for i in range(60)]
    for rnd in range(4):
        ja, jb = claimed(f"ba{rnd}.localhost", f"bb{rnd}.localhost")
        ua = [f"https://{d}/a{rnd}" for d in doms]
        ub = [f"https://{d}/b{rnd}" for d in reversed(doms)]
        errs, barrier = [], threading.Barrier(2)

        def run(job, urls):
            try:
                barrier.wait()
                bj._search_done(job, urls)
            except Exception as e:  # noqa: BLE001
                errs.append(repr(e))
        ts = [threading.Thread(target=run, args=(ja, ua)), threading.Thread(target=run, args=(jb, ub))]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert not errs, errs
    got = rows("SELECT domain, pages_discovered p FROM discovered_domains WHERE domain LIKE 'd__example.com' ORDER BY 1")
    assert len(got) == 60 and all(r["p"] == 8 for r in got)  # 4 rounds x 2 distinct URLs per domain: every upsert counted once
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE domain LIKE 'd__example.com'")[0]["n"] == 60 * 8


def test_deadlock_victim_is_rolled_back_whole_and_retried_without_duplicates(env, monkeypatch):
    (job,) = claimed("retry.localhost")
    urls = [f"https://r{i}.example.net/p" for i in range(5)]
    real, calls = core.save_url, []

    def flaky(*a, **kw):
        calls.append(1)
        if len(calls) == 3:  # fails in the middle of the transaction, after some rows were already written
            raise psycopg.errors.DeadlockDetected("deadlock detected")
        return real(*a, **kw)
    monkeypatch.setattr(core, "save_url", flaky)
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    before = core.BUSY_ERRORS
    bj._search_done(job, urls)
    assert core.BUSY_ERRORS == before + 1  # exactly one deadlock seen, one retry
    j = rows("SELECT * FROM business_jobs WHERE target_domain='retry.localhost'")[0]
    assert j["status"] == "crawling" and j["search_results"] == 5  # the job completed ONCE
    assert rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", j["id"])[0]["n"] == 5  # no duplicates from the failed first attempt
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE domain='example.net'")[0]["n"] == 5


def test_retry_exhaustion_raises_and_leaves_the_job_untouched(env, monkeypatch):
    (job,) = claimed("giveup.localhost")
    monkeypatch.setattr(core, "save_url", lambda *a, **k: (_ for _ in ()).throw(psycopg.errors.DeadlockDetected("deadlock detected")))
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    with pytest.raises(psycopg.errors.DeadlockDetected):  # the final failure is never swallowed
        bj._search_done(job, ["https://g.example.net/p"])
    j = rows("SELECT * FROM business_jobs WHERE target_domain='giveup.localhost'")[0]
    assert j["status"] == "searching"  # the fenced UPDATE was rolled back with the rest: no false 'done', the job can be searched again
    assert rows("SELECT COUNT(*) n FROM job_urls")[0]["n"] == 0 and rows("SELECT COUNT(*) n FROM discovered_urls")[0]["n"] == 0


def test_lease_renewal_skips_a_row_locked_by_a_page_transaction_and_never_touches_other_owners(env):
    with core.connect() as c:
        for i in range(4):
            core.save_url(c, f"http://lease{i}.localhost/", None, "t", 0, True, "crawling")
        c.execute("UPDATE discovered_urls SET lease_owner='mine', lease_expires=? WHERE url LIKE '%lease0%' OR url LIKE '%lease1%' OR url LIKE '%lease2%'", (core.later(seconds=5),))
        c.execute("UPDATE discovered_urls SET lease_owner='other', lease_expires=? WHERE url LIKE '%lease3%'", (core.later(seconds=5),))
    old = {r["url"]: r["lease_expires"] for r in rows("SELECT url, lease_expires FROM discovered_urls")}
    holder, release = threading.Event(), threading.Event()

    def page_txn():  # a page transaction holding the row lock of lease1 for a while
        with core.connect() as c:
            c.execute("SELECT 1 FROM discovered_urls WHERE url LIKE '%lease1%' FOR UPDATE")
            holder.set()
            release.wait(10)
    t = threading.Thread(target=page_txn)
    t.start()
    assert holder.wait(10)
    t0 = time.monotonic()
    n = autonomous.renew("mine", 900)
    took = time.monotonic() - t0
    release.set()
    t.join()
    new = {r["url"]: r["lease_expires"] for r in rows("SELECT url, lease_expires FROM discovered_urls")}
    assert took < 2 and n == 2  # did not wait for the locked row; renewed the other two
    assert new["http://lease1.localhost/"] == old["http://lease1.localhost/"]  # locked row: renewed next tick
    assert new["http://lease0.localhost/"] > old["http://lease0.localhost/"] and new["http://lease2.localhost/"] > old["http://lease2.localhost/"]
    assert new["http://lease3.localhost/"] == old["http://lease3.localhost/"]  # another owner's lease is never extended
    assert autonomous.renew("mine", 900) == 3  # next tick: all three of this owner's leases

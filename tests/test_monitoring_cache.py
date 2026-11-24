"""Monitoring-only full-table aggregates: one background refresh per TTL, never blocking, failure backs off a full TTL, values stay exact
(counts() / scheduler paths are untouched). Real PostgreSQL (isolated schema)."""

import threading
import time

import pytest

import crawler_core as core
import engine_stats
from test_crawler import env, port, rows  # noqa: F401 - fixtures


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    engine_stats._slow.clear()
    engine_stats._slow_running.clear()
    yield
    engine_stats._slow.clear()


def wait_idle():
    for _ in range(100):
        if not engine_stats._slow_running:
            return
        time.sleep(0.05)
    raise AssertionError("refresh thread did not finish")


def test_first_call_returns_none_then_the_exact_value_and_its_age(env):
    with core.connect() as c:
        for i in range(3):
            core.save_url(c, f"http://m{i}.localhost/", None, "t", 0, True, "pending")
    sql, shape = "SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1", lambda cur: {r[0]: r[1] for r in cur}
    assert engine_stats.slow_cached("u", sql, shape) == (None, None)  # never waits for the aggregate
    wait_idle()
    value, age = engine_stats.slow_cached("u", sql, shape)
    assert value == {"pending": 3} and age is not None and age < 5
    assert value == engine_stats.counts()["urls_by_status"]  # same numbers as the exact path


def test_concurrent_callers_start_one_refresh_and_a_fresh_value_is_not_recomputed(env, monkeypatch):
    calls = []
    real = core.connect

    def slow_connect():
        calls.append(1)
        time.sleep(0.3)
        return real()
    monkeypatch.setattr(core, "connect", slow_connect)
    ts = [threading.Thread(target=engine_stats.slow_cached, args=("k", "SELECT 1", lambda cur: 1)) for _ in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    wait_idle()
    engine_stats.slow_cached("k", "SELECT 1", lambda cur: 1)
    wait_idle()
    assert len(calls) == 1  # 12 simultaneous callers + one later call inside the TTL = a single query


def test_failed_refresh_backs_off_for_the_whole_ttl_and_keeps_the_old_value(env, monkeypatch):
    sql, shape = "SELECT COUNT(*) FROM discovered_urls", lambda cur: cur.fetchone()[0]
    engine_stats.slow_cached("f", sql, shape)
    wait_idle()
    good, _ = engine_stats.slow_cached("f", sql, shape)
    assert good == 0
    attempts = []
    monkeypatch.setattr(core, "connect", lambda: attempts.append(1) or (_ for _ in ()).throw(RuntimeError("statement timeout")))
    monkeypatch.setattr(engine_stats, "SLOW_TTL", 0.2)
    engine_stats._slow["f"]["next"] = 0.0  # due now
    for _ in range(20):  # a burst of callers while the refresh is failing
        engine_stats.slow_cached("f", sql, shape)
    wait_idle()
    assert len(attempts) == 1  # one attempt, not one per call
    assert engine_stats.slow_cached("f", sql, shape)[0] == 0  # the previous value is kept


def test_diagnostics_and_metrics_use_the_cache_and_do_not_wait(env):
    t0 = time.monotonic()
    snap = engine_stats.diagnostics({"done": 0, "started": 0, "total": 0, "retries": 0})
    assert snap["frontier_counts_age_s"] is None and snap["discovered"] == 0  # no aggregate yet: empty, not blocked
    m = engine_stats.metrics()
    wait_idle()
    assert time.monotonic() - t0 < 5 and m["urls"] in (None, {})
    assert engine_stats.metrics()["urls_age_s"] is not None  # filled by the background refresh

"""Phase 9.1 D4: an API request gives up on a missing database connection after API_DB_WAIT (not the 30 s worker wait) with HTTP 503,
never serves an old cached aggregate in place of the outage, and recovers without a restart. The pool is replaced by one that raises like
psycopg_pool does; nothing here touches a real server outage."""

import time

import psycopg
import pytest
from psycopg_pool import PoolTimeout

import api_timing
import crawler_api
import crawler_core as core
from api_data import TARGET, load
from read_cache import graph_cache
from test_api_perf import client, db  # noqa: F401 - fixtures


class DownPool:
    """Stands in for the pool while the database is unreachable: getconn waits `delay` then times out (the real pool waits the whole timeout)."""
    max_size = 6

    def __init__(self, real):
        self.real, self.asked, self.down = real, [], True

    def getconn(self, timeout):
        if self.down:
            self.asked.append(timeout)
            time.sleep(0.05)
            raise PoolTimeout("down")
        return self.real.getconn(timeout=timeout)

    def __getattr__(self, name):
        return getattr(self.real, name)


@pytest.fixture()
def outage(db, monkeypatch):  # noqa: F811
    with core.connect() as c:
        load(c, scale=0.1)
    pool = DownPool(core._get_pool())
    monkeypatch.setattr(core, "_get_pool", lambda: pool)
    monkeypatch.setattr(core, "DB_POOL_WAIT", 30.0)
    monkeypatch.setattr(api_timing, "API_DB_WAIT", 2.5)
    return pool


PATHS = [f"/backlinks/{TARGET}?limit=5", f"/backlinks/{TARGET}/referring-domains", f"/backlinks/{TARGET}/anchors", f"/backlinks/{TARGET}/history"]


def test_requests_get_503_with_the_api_wait_while_workers_keep_the_global_wait(outage, client):  # noqa: F811
    for p in PATHS:
        r = client.get(p)
        assert r.status_code == 503 and r.headers["Retry-After"] == "5" and "database unavailable" in r.json()["detail"], p
    assert set(outage.asked) == {2.5}  # the API's own wait, per request
    with pytest.raises(core.DatabaseUnavailable):
        core.connect()
    assert outage.asked[-1] == 30.0  # outside a request (worker, engine, task): the unchanged global wait
    assert issubclass(core.DatabaseUnavailable, TimeoutError)  # existing `except TimeoutError` handlers still match


def test_recovery_needs_no_restart(outage, client):  # noqa: F811
    assert client.get(PATHS[1]).status_code == 503
    outage.down = False
    r = client.get(PATHS[1])
    assert r.status_code == 200
    assert client.get(PATHS[0]).status_code == 200


def test_a_warm_cached_aggregate_is_not_served_in_place_of_an_outage(outage, client, monkeypatch):  # noqa: F811
    now = [1000.0]
    monkeypatch.setattr(graph_cache, "clock", lambda: now[0])
    monkeypatch.setattr(graph_cache, "enabled", True)
    graph_cache.clear()
    outage.down = False
    warm = client.get(PATHS[1])
    assert warm.status_code == 200 and warm.headers["X-Cache"] == "MISS"
    assert client.get(PATHS[1]).headers["X-Cache"] == "HIT"
    now[0] += graph_cache.ttl + 1  # expired but inside GRAPH_CACHE_MAX_STALE_SECONDS: before 9.1 a failing refresh answered 200 STALE
    def boom(*a, **k):  # the connection was handed out, then the server went away under the aggregate query
        raise psycopg.OperationalError("server closed the connection unexpectedly")
    monkeypatch.setattr(crawler_api, "_grouped_page", boom)
    r = client.get(PATHS[1])
    assert r.status_code == 503 and "X-Cache" not in r.headers
    monkeypatch.setattr(graph_cache, "no_stale", ())  # control: the old behaviour really was a stale 200
    assert client.get(PATHS[1]).headers.get("X-Cache") == "STALE"
    graph_cache.clear()

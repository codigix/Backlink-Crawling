"""Leases, Redis/Celery dispatch, crash recovery. Redis tests use db 14 of the local Redis and are skipped (NOT RUN) if it is down."""

import asyncio
import threading
import time

import pytest
import redis

import autonomous
import crawler_core as core
import tasks
from test_crawler import env, port, rows  # noqa: F401 - fixtures

TEST_REDIS = "redis://localhost:6379/14"


def redis_up():
    try:
        redis.Redis.from_url(TEST_REDIS, socket_connect_timeout=1).ping()
        return True
    except redis.RedisError:
        return False


needs_redis = pytest.mark.skipif(not redis_up(), reason="Redis not running")


def seed(n=30):
    with core.connect() as c:
        c.execute("INSERT INTO discovered_domains (domain, discovery_status) VALUES ('x.org','admitted')")
        for i in range(n):
            core.save_url(c, f"http://x.org/{i}", None, "t", 1, True)


def test_renewed_lease_survives_recovery_but_a_dead_workers_does_not(env):
    seed(5)
    cfg = autonomous.AutoConfig(lease_seconds=1, per_batch=10, claim_batch=10, domain_page_budget=100)
    assert len(autonomous.claim_urls("alive", cfg)) == 5
    released, stop = [], threading.Event()

    def reaper():
        while not stop.is_set():
            released.append(autonomous.release())  # what the scheduler does every cycle
            time.sleep(0.2)
    t = threading.Thread(target=reaper)
    t.start()
    asyncio.run(autonomous.with_lease("alive", cfg, asyncio.sleep(3)))  # work 3x longer than the lease
    stop.set()
    t.join()
    assert sum(released) == 0  # never reset while the round was alive
    time.sleep(2.5)  # the worker dies: no renewals, lease runs out
    assert autonomous.release() == 5 and rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='pending'")[0]["n"] == 5


def test_redis_unavailable_fails_fast_and_changes_nothing(env, monkeypatch):
    seed(5)
    monkeypatch.setattr(tasks, "_redis", redis.Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=1, socket_timeout=1))
    t0 = time.monotonic()
    with pytest.raises(redis.RedisError):
        tasks.dispatch(force=True)
    assert time.monotonic() - t0 < 10
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='pending'")[0]["n"] == 5  # PostgreSQL untouched
    assert tasks.recover() == 0  # lease recovery needs only PostgreSQL, so it still works with Redis down


@needs_redis
def test_concurrent_dispatchers_never_exceed_the_queue_limit(env, monkeypatch):
    seed(300)
    r = redis.Redis.from_url(TEST_REDIS)
    r.flushdb()
    monkeypatch.setattr(tasks, "_redis", r)
    monkeypatch.setattr(tasks.crawl_batch, "delay", lambda: r.lpush("celery", "task"))
    monkeypatch.setattr(tasks, "MAX_QUEUED", 4)
    errors = []

    def go():
        try:
            for _ in range(5):
                tasks.dispatch(force=True)
        except Exception as e:  # noqa: BLE001
            errors.append(e)
    ts = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in ts]
    [t.join(60) for t in ts]
    assert not errors and r.llen("celery") <= 4  # lock makes check-then-enqueue atomic
    r.flushdb()


@needs_redis
def test_dispatch_is_noop_when_queue_is_full_and_resumes_after_drain(env, monkeypatch):
    seed(600)  # 6 tasks wanted, 4 allowed
    r = redis.Redis.from_url(TEST_REDIS)
    r.flushdb()
    monkeypatch.setattr(tasks, "_redis", r)
    monkeypatch.setattr(tasks.crawl_batch, "delay", lambda: r.lpush("celery", "task"))
    real = autonomous.AutoConfig
    monkeypatch.setattr(autonomous, "AutoConfig", lambda: real(claim_batch=100))  # the default batch (500) would want only 2 tasks
    assert tasks.dispatch(force=True) == tasks.MAX_QUEUED
    assert tasks.dispatch(force=True) == 0
    r.lpop("celery")  # a worker took one
    assert tasks.dispatch(force=True) == 1
    r.flushdb()


def test_redelivered_task_after_worker_loss_claims_nothing_twice(env):
    seed(10)
    first = autonomous.claim_urls("celery:h:1", autonomous.AutoConfig(per_batch=10, claim_batch=10, domain_page_budget=100))
    assert len(first) == 10
    again = tasks.crawl_batch.apply()  # redelivered while the first lease is still valid
    assert again.result == 0  # nothing claimable: no duplicate crawl
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET lease_expires='2000-01-01 00:00:00'")
    assert tasks.recover() == 10  # the original worker really died: leases return to the frontier

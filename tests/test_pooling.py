"""Phase 3: PostgreSQL connection pooling (crawler_core.connect / connect_unpooled / close_pool). Real PostgreSQL in an isolated schema per test
(the `env` fixture); the local scratch database only. Covers: SELECT/INSERT, commit, rollback, exceptions in `with`, connection reuse, pool
exhaustion and timeout, state that must not leak between checkouts, concurrent API requests and crawler claims, transaction- and session-level
advisory locks, the scheduler's dedicated connection, pool shutdown/restart and fork safety."""

import gc
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api_timing
import autonomous
import business_jobs as bj
import crawler_api
import crawler_core as core
import engine_stats
from api_data import TARGET, load
from test_crawler import env, port, rows  # noqa: F401 - fixtures


@pytest.fixture()
def pool_of(monkeypatch, env):
    """pool_of(max, wait=..): a fresh pool of that size for this test (the pool is rebuilt whenever its settings change)."""
    def make(maximum: int, wait: float = 0.3, minimum: int = 1) -> None:
        monkeypatch.setattr(core, "POOL_MAX", maximum)
        monkeypatch.setattr(core, "POOL_MIN", minimum)
        monkeypatch.setattr(core, "DB_POOL_WAIT", wait)
    yield make
    core.close_pool()  # never leave connections of a dropped test schema open


def backend_pid(c) -> int:
    return c.execute("SELECT pg_backend_pid()").fetchone()[0]


def alive_backends(pids) -> int:
    """How many of these server backends are still connected, as PostgreSQL sees them. Only the given pids are counted, so another process using the
    same database (a second test run, a developer's engine) cannot change the answer."""
    with psycopg.connect(core.DATABASE_URL, autocommit=True, application_name="pool-test-observer") as o:
        return o.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE pid = ANY(%s)", (list(pids),)).fetchone()[0]


# ------------------------------------------------------------------ plain operations, transactions, exceptions ------------------------------------------------------------------

def test_select_insert_and_commit_are_visible_to_other_connections(env):
    with core.connect() as c:
        assert c.execute("SELECT 1 AS one").fetchone()["one"] == 1
        c.execute("INSERT INTO engine_state (key, value) VALUES ('k', 'v')")
    with psycopg.connect(core.DATABASE_URL) as raw:  # a connection outside the pool sees the committed row
        raw.execute(f"SET search_path={core.DB_SCHEMA}")
        assert raw.execute("SELECT value FROM engine_state WHERE key='k'").fetchone()[0] == "v"
    assert rows("SELECT value FROM engine_state WHERE key='k'")[0]["value"] == "v"


def test_exception_inside_with_rolls_back_and_returns_the_connection(env, pool_of):
    pool_of(1)
    with pytest.raises(RuntimeError):
        with core.connect() as c:
            c.execute("INSERT INTO engine_state (key, value) VALUES ('gone', 'x')")
            raise RuntimeError("boom")
    assert rows("SELECT 1 FROM engine_state WHERE key='gone'") == []  # rolled back, and the only connection is usable again
    assert core.OPEN_CONNS == 0
    with pytest.raises(psycopg.errors.UniqueViolation):  # a failing statement: the transaction is aborted, the connection must still come back clean
        with core.connect() as c:
            c.execute("INSERT INTO engine_state (key, value) VALUES ('d', 'x')")
            c.execute("INSERT INTO engine_state (key, value) VALUES ('d', 'y')")
    assert rows("SELECT 1 FROM engine_state WHERE key='d'") == [] and core.OPEN_CONNS == 0


def test_explicit_rollback_and_close_without_commit_discard_work(env):
    c = core.connect()
    c.execute("INSERT INTO engine_state (key, value) VALUES ('a', '1')")
    c.rollback()
    c.execute("INSERT INTO engine_state (key, value) VALUES ('b', '1')")
    c.close()  # like closing a plain connection: uncommitted work is discarded
    assert rows("SELECT key FROM engine_state WHERE key IN ('a','b')") == []


def test_a_failing_statement_inside_with_still_returns_the_connection(env, pool_of):
    pool_of(1)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):  # the statement itself fails: the transaction is aborted
        with core.connect() as c:
            c.execute("INSERT INTO job_urls (job_id, url_id, depth) VALUES (999, 999, 0)")
    assert core.OPEN_CONNS == 0
    with core.connect() as c:
        assert c.execute("SELECT 1").fetchone()[0] == 1


# ------------------------------------------------------------------ reuse, exhaustion, leaks ------------------------------------------------------------------

def test_connections_are_reused_not_reopened(env, pool_of):
    pool_of(3)
    pids = set()
    for _ in range(40):
        with core.connect() as c:
            pids.add(backend_pid(c))
    assert len(pids) <= 3  # one connection per pool slot at most, however many checkouts
    assert core.pool_stats()["connections_num"] <= 3 and core.pool_stats()["requests_num"] >= 40


def test_exhaustion_times_out_loudly_and_recovers(env, pool_of):
    pool_of(2, wait=0.2)
    c1, c2 = core.connect(), core.connect()
    t0 = time.monotonic()
    with pytest.raises(TimeoutError, match="connection limit"):
        core.connect()
    assert 0.15 <= time.monotonic() - t0 < 3  # waited its budget, did not hang
    c1.close()
    c3 = core.connect()  # a returned connection is handed out again
    with pytest.raises(TimeoutError):
        core.connect()
    c2.close(); c3.close()
    assert core.OPEN_CONNS == 0


def test_a_repeated_close_never_returns_somebody_elses_connection(env, pool_of):
    pool_of(2, wait=0.2)
    c1, c2 = core.connect(), core.connect()
    c1.close()
    c3 = core.connect()  # the connection c1 gave back, now owned by c3
    c1.close()  # stale second close of c1: must be a no-op
    c1.close()
    with pytest.raises(TimeoutError):  # c2 and c3 still hold both connections
        core.connect()
    assert c3.execute("SELECT 1").fetchone()[0] == 1  # and c3's connection was not returned or closed under it
    c2.close(); c3.close()


def test_a_released_lease_cannot_be_used(env):
    c = core.connect()
    c.close()
    assert c.closed
    with pytest.raises(psycopg.InterfaceError):
        c.execute("SELECT 1")


def test_a_dropped_lease_is_returned_by_the_garbage_collector(env, pool_of):
    pool_of(2, wait=0.2)
    for _ in range(4):  # four forgotten checkouts on a pool of two: would exhaust it if they leaked
        core.connect().execute("SELECT 1")
        gc.collect()
    assert core.OPEN_CONNS == 0
    with core.connect() as c:
        assert c.execute("SELECT 1").fetchone()[0] == 1


def test_a_closed_connection_is_replaced_not_reused(env, pool_of):
    pool_of(1)
    with core.connect() as c:
        c.pgconn.finish()  # the server side is gone (network drop, server restart)
    with core.connect() as c:  # the broken connection was discarded, a fresh one is opened
        assert c.execute("SELECT 1").fetchone()[0] == 1


# ------------------------------------------------------------------ no state leaks between checkouts ------------------------------------------------------------------

def test_session_settings_options_and_row_factory_are_preserved(env):
    with core.connect() as c:
        got = {k: c.execute(f"SHOW {k}").fetchone()[0] for k in ("statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout", "application_name")}
        assert got == {"statement_timeout": "5min", "lock_timeout": "1min", "idle_in_transaction_session_timeout": "5min", "application_name": "backlink-crawler"}
        assert core.DB_SCHEMA in c.execute("SHOW search_path").fetchone()[0]
        r = c.execute("SELECT 7 AS a, 8 AS b").fetchone()
        assert (r["a"], r[1]) == (7, 8) and type(r) is core.Row  # by name and by position
        assert type(c.cursor()) is core.Cursor and type(c) is core.Connection


def test_diagnostics_and_set_local_do_not_leak_into_the_next_checkout(env, pool_of):
    pool_of(1)  # one connection: the next checkout is guaranteed to be the same session
    with core.connect() as c:
        first = backend_pid(c)
    engine_stats.diagnostics({"done": 0, "started": 0, "retries": 0, "total": 0})  # used to run a session-level SET statement_timeout='5s'
    with core.connect() as c:
        c.execute("SET LOCAL lock_timeout='3s'")  # the admission code's pattern
        assert c.execute("SHOW lock_timeout").fetchone()[0] == "3s"
    with core.connect() as c:
        assert backend_pid(c) == first, "test is meaningless unless the same connection was reused"
        assert c.execute("SHOW statement_timeout").fetchone()[0] == "5min"
        assert c.execute("SHOW lock_timeout").fetchone()[0] == "1min"


def test_per_request_cursor_timing_does_not_stick_to_the_connection(env, pool_of):
    pool_of(1)
    m = {"connect_ms": 0.0, "sql_ms": 0.0, "fetch_ms": 0.0, "conns": 0}
    token = api_timing._ctx.set(m)
    try:
        with api_timing.connect() as c:
            assert type(c.cursor()) is api_timing._Cursor
            c.execute("SELECT 1")
    finally:
        api_timing._ctx.reset(token)
    assert m["conns"] == 1 and m["sql_ms"] > 0
    with core.connect() as c:  # the same connection, plain cursors again
        assert type(c.cursor()) is core.Cursor


def test_a_transaction_scoped_advisory_lock_is_released_at_commit(env, pool_of):
    pool_of(2)
    with core.connect() as a:
        a.execute("SELECT pg_advisory_xact_lock(424242)")
        held = rows("SELECT COUNT(*) n FROM pg_locks WHERE locktype='advisory' AND objid=424242 AND granted")[0]["n"]
        assert held == 1
        with core.connect() as b:  # a second pooled connection cannot take it while `a` is open
            assert b.execute("SELECT pg_try_advisory_xact_lock(424242)").fetchone()[0] is False
    with core.connect() as c:
        assert c.execute("SELECT pg_try_advisory_xact_lock(424242)").fetchone()[0] is True  # free again, nothing stayed on the pooled session
    assert rows("SELECT COUNT(*) n FROM pg_locks WHERE locktype='advisory' AND objid=424242")[0]["n"] == 0


# ------------------------------------------------------------------ session advisory lock: dedicated connection ------------------------------------------------------------------

def test_scheduler_lock_uses_a_dedicated_connection_that_is_never_pooled(env, pool_of):
    pool_of(2)
    with core.connect() as c:
        pooled = backend_pid(c)
    before = core.pool_stats()["pool_size"]
    lock, owner = autonomous.try_scheduler_lock("engine-A")
    assert lock is not None and type(lock) is core.Unpooled
    try:
        assert backend_pid(lock) != pooled  # not a pool connection
        assert core.pool_stats()["pool_size"] == before  # and the pool did not grow or shrink because of it
        for _ in range(30):  # heavy pooled traffic while the lock is held: the lock's session is never handed out
            with core.connect() as c:
                assert backend_pid(c) != backend_pid(lock)
        other, holder = autonomous.try_scheduler_lock("engine-B")
        assert other is None and holder == "engine-A"  # exclusive across connections
    finally:
        autonomous.release_scheduler_lock(lock)
    assert lock.closed
    again, _ = autonomous.try_scheduler_lock("engine-B")  # released: the next engine gets it
    assert again is not None
    autonomous.release_scheduler_lock(again)
    assert core.OPEN_CONNS == 0


def test_closing_the_pool_does_not_touch_the_scheduler_lock(env):
    lock, _ = autonomous.try_scheduler_lock("engine-A")
    try:
        with core.connect() as c:
            c.execute("SELECT 1")
        core.close_pool()
        assert not lock.closed and lock.execute("SELECT 1").fetchone()[0] == 1
        assert autonomous.try_scheduler_lock("engine-B")[0] is None  # still held
    finally:
        autonomous.release_scheduler_lock(lock)


# ------------------------------------------------------------------ concurrency ------------------------------------------------------------------

def test_concurrent_api_requests_stay_within_the_pool_and_leak_nothing(env, pool_of):
    pool_of(4, wait=5)
    engine_stats.reset_snapshot()  # the statistics snapshot is process-wide
    with core.connect() as c:
        load(c, scale=1)
    app = FastAPI()
    app.include_router(crawler_api.router)
    api_timing.install(app)
    client = TestClient(app)
    urls = [f"/backlinks/{TARGET}?limit=20", f"/backlinks/{TARGET}/referring-domains?limit=5", f"/backlinks/{TARGET}/anchors", "/crawler/outgoing-links?limit=10", "/crawler/discovered-urls?limit=10"]
    want = {u: client.get(u).json() for u in urls}
    peak, stop = [0], threading.Event()

    def watch():
        while not stop.is_set():
            peak[0] = max(peak[0], core.pool_stats()["pool_size"])  # connections this process's pool has open (idle + checked out)
            time.sleep(0.005)
    w = threading.Thread(target=watch)
    w.start()
    try:
        with ThreadPoolExecutor(24) as ex:
            results = list(ex.map(lambda u: (u, TestClient(app).get(u)), urls * 12))
    finally:
        stop.set(); w.join()
    assert all(r.status_code == 200 and r.json() == want[u] for u, r in results)
    assert 1 <= peak[0] <= 4, f"the pool grew to {peak[0]} connections with a maximum of 4"
    assert core.OPEN_CONNS == 0 and core.pool_stats()["pool_available"] == core.pool_stats()["pool_size"]  # everything came back
    engine_stats.reset_snapshot()


def test_api_request_fails_loudly_when_the_pool_is_exhausted(env, pool_of, monkeypatch):
    pool_of(1, wait=0.2)
    monkeypatch.setattr(api_timing, "API_DB_WAIT", 0.2)  # Phase 9.1: an API request has its own wait (default 5 s) and answers 503, it no longer raises
    app = FastAPI()
    app.include_router(crawler_api.router)
    api_timing.install(app)
    hold = core.connect()  # the only connection is busy
    try:
        t0 = time.monotonic()
        r = TestClient(app, raise_server_exceptions=True).get("/crawler/discovered-urls")
        assert r.status_code == 503 and r.headers["Retry-After"] and "database unavailable" in r.json()["detail"]
        assert time.monotonic() - t0 < 5  # it waited its budget and gave up, it did not hang
        with pytest.raises(TimeoutError, match="connection limit"):  # outside a request (worker, engine) the plain pool error is unchanged
            core.connect()
    finally:
        hold.close()


def test_concurrent_claims_lease_every_url_exactly_once_on_the_pool(env, pool_of):
    pool_of(6, wait=10)
    with core.connect() as c:
        for j in range(3):
            c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES (?,?, 'crawling', 3, 10000)", (f"b{j}.example", j))
        c.execute("INSERT INTO discovered_urls (url, normalized_url, domain, hostname, crawl_status) SELECT 'http://d'||(g%20)||'.example/p'||g, 'http://d'||(g%20)||'.example/p'||g,"
                  " 'd'||(g%20)||'.example', 'd'||(g%20)||'.example', 'pending' FROM generate_series(1,400) g")
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT 1+(id%3), id, id%3 FROM discovered_urls")
    cfg = autonomous.AutoConfig(sources=("test",), claim_batch=7, per_batch=3, max_crawl_workers=50, business_reserved_workers=1, max_jobs_per_business=50)
    got, errors = [], []

    def worker(n):
        try:
            while work := autonomous.claim_work(f"w{n}", cfg):  # each claim holds one connection and opens a second inside it
                got.extend(u for u, _ in work)
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
    ts = [threading.Thread(target=worker, args=(n,)) for n in range(3)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errors, errors
    assert len(got) == len(set(got)) == 400
    assert core.OPEN_CONNS == 0


# ------------------------------------------------------------------ lifecycle ------------------------------------------------------------------

def test_pool_shutdown_and_restart_leave_no_connections_and_no_duplicate_pools(env, pool_of):
    pool_of(3, minimum=2)
    for _ in range(4):
        held = [core.connect() for _ in range(3)]  # the whole pool at once: learn which server backends it owns
        pids = {backend_pid(c) for c in held}
        [c.close() for c in held]
        first = core._pool
        with core.connect() as c:
            c.execute("SELECT 1")
        assert core._pool is first  # repeated use never builds a second pool
        assert alive_backends(pids) == len(pids)
        core.close_pool()
        assert core._pool is None
        deadline = time.monotonic() + 5
        while alive_backends(pids) > 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert alive_backends(pids) == 0, "pool connections stayed open after close_pool"
    core.close_pool()  # idempotent
    pool_threads = [t for t in threading.enumerate() if t.name.startswith("backlink-db")]
    assert pool_threads == [], pool_threads  # no worker threads of closed pools linger


def test_close_does_not_pull_a_connection_out_from_under_a_running_operation(env, pool_of):
    pool_of(2)
    c = core.connect()
    c.execute("INSERT INTO engine_state (key, value) VALUES ('mid', '1')")
    core.close_pool()  # the application is shutting down while c is mid-transaction
    c.execute("INSERT INTO engine_state (key, value) VALUES ('mid2', '1')")  # still usable
    c.commit()
    c.close()  # returned to the (closed) pool: closed for real
    assert {r["key"] for r in rows("SELECT key FROM engine_state WHERE key LIKE 'mid%'")} == {"mid", "mid2"}
    assert core.OPEN_CONNS == 0


def test_a_forked_child_never_closes_or_reuses_the_parents_pool(env):
    with core.connect() as c:
        c.execute("SELECT 1")
    parent = core._pool
    core._forget_pool_after_fork()  # what os.register_at_fork runs in a child process
    try:
        assert core._pool is None and parent in core._inherited_pools and not parent.closed
        with core.connect() as c:  # the child builds its own pool lazily
            c.execute("SELECT 1")
        assert core._pool is not parent
    finally:
        core._inherited_pools.remove(parent)
        parent.close()


def test_changing_the_target_schema_gets_its_own_pool(env, monkeypatch):
    with core.connect() as c:
        first_pool = core._pool
        assert core.DB_SCHEMA in c.execute("SHOW search_path").fetchone()[0]
    other = "t_" + uuid.uuid4().hex[:12]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {other}")
        try:
            monkeypatch.setattr(core, "DB_SCHEMA", other)
            with core.connect() as c:
                assert other in c.execute("SHOW search_path").fetchone()[0]
            assert core._pool is not first_pool and first_pool.closed  # the old pool was closed, not leaked
        finally:
            core.close_pool()
            admin.execute(f"DROP SCHEMA {other} CASCADE")


def test_onboarding_and_rescan_work_through_the_pool(env, pool_of):
    pool_of(3)
    with core.connect() as c:
        job, created = bj.create_job(c, "pool-biz.example")
    assert created and job["target_domain"] == "pool-biz.example"
    with core.connect() as c:
        again, created2 = bj.create_job(c, "pool-biz.example")  # repeated onboarding never starts another scan
        assert not created2 and again["id"] == job["id"]
    with core.connect() as c:
        c.execute("UPDATE business_jobs SET status='done' WHERE id=?", (job["id"],))
    with core.connect() as c:
        forced, created3 = bj.create_job(c, "pool-biz.example", force=True)  # an explicit rescan does
    assert created3 and forced["id"] != job["id"] and core.OPEN_CONNS == 0

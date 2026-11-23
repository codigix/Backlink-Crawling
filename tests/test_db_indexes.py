"""Phase 4: db_indexes.py (CONCURRENTLY read-path indexes, built outside the migration runner). Local scratch database, one schema per test.
Checks the safety properties: nothing existing is dropped or changed, a rerun is a no-op, an invalid index is reported and left alone,
a failed build does not stop the others, and the planner uses the new indexes for the queries they were made for."""

import pytest

import crawler_core as core
import db_indexes
from api_data import load
from test_crawler import env, port, rows  # noqa: F401 - fixtures


def defs() -> dict:
    return {r["indexname"]: r["indexdef"] for r in rows("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=current_schema()")}


def run(**kw) -> dict:
    c = core.connect_unpooled()
    try:
        return db_indexes.apply(c, **kw)
    finally:
        c.close()


def test_dry_run_changes_nothing(env):
    before = defs()
    assert set(run(dry_run=True).values()) == {"missing"}
    assert defs() == before


def test_apply_builds_valid_indexes_keeps_every_existing_one_and_is_idempotent(env):
    before = defs()
    assert set(run().values()) == {"created"}
    after = defs()
    assert all(after[k] == v for k, v in before.items()), "an existing index was dropped or redefined"
    assert set(after) - set(before) == set(db_indexes.INDEXES)
    assert "(first_seen)" in after["idx_discovered_first_seen"] and "(crawl_completed)" in after["idx_crawl_history_completed"]
    assert set(run().values()) == {"exists"} and defs() == after


def test_an_identical_index_under_the_name_is_accepted_as_it_is(env):
    with core.connect() as c:  # e.g. idx_crawl_history_completed as another branch's migration 11 created it
        c.execute("CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_completed)")
    mine = defs()["idx_crawl_history_completed"]
    out = run()
    assert out["idx_crawl_history_completed"] == "exists" and defs()["idx_crawl_history_completed"] == mine
    assert out["idx_discovered_first_seen"] == "created"


@pytest.mark.parametrize("ddl", [
    "CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_completed, http_status)",         # extra column
    "CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_completed DESC)",                 # ordering
    "CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_completed) WHERE http_status=200",  # predicate
    "CREATE UNIQUE INDEX idx_crawl_history_completed ON crawl_history(crawl_completed)",               # uniqueness
    "CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_started)",                        # other column
    "CREATE INDEX idx_crawl_history_completed ON crawl_history(lower(crawl_completed))",               # expression
])
def test_a_same_name_index_that_differs_in_columns_order_predicate_or_uniqueness_is_reported_not_trusted(env, ddl):
    with core.connect() as c:
        c.execute(ddl)
    mine = defs()["idx_crawl_history_completed"]
    out = run()
    assert out["idx_crawl_history_completed"].startswith("DIFFERENT") and defs()["idx_crawl_history_completed"] == mine
    assert out["idx_discovered_first_seen"] == "created"  # the other index is independent


def test_dry_run_also_reports_a_different_definition(env):
    with core.connect() as c:
        c.execute("CREATE INDEX idx_crawl_history_completed ON crawl_history(crawl_started)")
    assert run(dry_run=True)["idx_crawl_history_completed"].startswith("DIFFERENT")


class Spy:
    """Wraps the connection: records every statement and the transaction status it was sent in."""
    def __init__(self, conn):
        self.conn, self.log = conn, []

    def execute(self, sql, *a, **kw):
        self.log.append((sql, self.conn.info.transaction_status.name))
        return self.conn.execute(sql, *a, **kw)

    def __getattr__(self, k):
        return getattr(self.conn, k)

    def __setattr__(self, k, v):
        if k in ("conn", "log"):
            object.__setattr__(self, k, v)
        else:
            setattr(self.conn, k, v)


def spied(**kw):
    c = core.connect_unpooled()
    try:
        spy = Spy(c)
        return db_indexes.apply(spy, **kw), spy.log
    finally:
        c.close()


def test_every_create_is_concurrent_and_runs_outside_a_transaction(env):
    out, log = spied()
    builds = [(sql, tx) for sql, tx in log if sql.startswith("CREATE")]
    assert len(builds) == len(db_indexes.INDEXES) and set(out.values()) == {"created"}
    assert all("CONCURRENTLY" in sql and tx == "IDLE" for sql, tx in builds)  # IDLE = no transaction open (INTRANS would be inside one)


def test_nothing_is_ever_dropped_not_even_after_an_invalid_or_failed_index(env, monkeypatch):
    run()
    with core.connect() as c:
        c.execute("UPDATE pg_index SET indisvalid=false WHERE indexrelid=?::regclass", ("idx_discovered_first_seen",))
        c.execute("CREATE INDEX idx_crawl_history_completed_x ON crawl_history(crawl_started)")
    bad = dict(db_indexes.INDEXES)
    bad["idx_broken"] = ("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_broken ON outgoing_links(no_such_column)", "outgoing_links USING btree (no_such_column)", "")
    monkeypatch.setattr(db_indexes, "INDEXES", bad)
    before = defs()
    out, log = spied()
    assert out["idx_broken"].startswith("FAILED") and out["idx_discovered_first_seen"].startswith("INVALID")
    assert not [sql for sql, _ in log if "DROP" in sql.upper() or "DELETE" in sql.upper() or "REINDEX" in sql.upper()]
    assert defs() == before


def test_invalid_index_is_reported_and_never_dropped(env):
    run()
    with core.connect() as c:  # what a cancelled CREATE INDEX CONCURRENTLY leaves behind
        c.execute("UPDATE pg_index SET indisvalid=false WHERE indexrelid=?::regclass", ("idx_discovered_first_seen",))
    out = run()
    assert out["idx_discovered_first_seen"].startswith("INVALID") and "idx_discovered_first_seen" in defs()
    assert all(v == "exists" for k, v in out.items() if k != "idx_discovered_first_seen")


def test_a_failed_build_is_reported_and_the_other_indexes_are_still_built(env, monkeypatch):
    bad = dict(db_indexes.INDEXES)
    first = next(iter(bad))
    bad[first] = ("CREATE INDEX CONCURRENTLY IF NOT EXISTS " + first + " ON outgoing_links(no_such_column)", "outgoing_links USING btree (no_such_column)", "broken on purpose")
    monkeypatch.setattr(db_indexes, "INDEXES", bad)
    out = run()
    assert out[first].startswith("FAILED") and first not in defs()
    assert [v for k, v in out.items() if k != first] == ["created"] * (len(bad) - 1)


def test_planner_uses_the_new_indexes_for_the_statistics_queries(env):
    with core.connect() as c:
        load(c, scale=20)
        c.execute("INSERT INTO crawl_history (source_url, crawl_completed) SELECT 'u'||g, to_char(timestamp '2026-01-01' + (g||' seconds')::interval,'YYYY-MM-DD HH24:MI:SS') FROM generate_series(1,20000) g")
    run()
    with core.connect() as c:
        c.execute("ANALYZE")
        plans = [" ".join(r[0] for r in c.execute("EXPLAIN " + sql, ("2030-01-01",))) for sql in (
            "SELECT COUNT(*) FROM discovered_urls WHERE first_seen>=?",
            "SELECT COUNT(*) FROM crawl_history WHERE crawl_completed>=? AND error_message IS NULL")]
    assert "idx_discovered_first_seen" in plans[0] and "idx_crawl_history_completed" in plans[1]


def test_planner_uses_the_history_indexes(env):
    """The three /history statements (hot-domain shapes), with sequential scans discouraged because the fixture is tiny."""
    with core.connect() as c:
        load(c, scale=20)
        c.execute("INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at) "
                  "SELECT 's'||g, 'd'||g, 'codigix.co', '', 'lost', to_char(timestamp '2026-01-01' + (g||' minutes')::interval,'YYYY-MM-DD HH24:MI:SS') FROM generate_series(1,3000) g")
        c.execute("UPDATE outgoing_links SET is_active=0, lost_at='2026-02-01 00:00:00' WHERE id % 7 = 0")
    run()
    with core.connect() as c:
        c.execute("ANALYZE")
        c.execute("SET LOCAL enable_seqscan = off")
        cond = " WHERE destination_domain=? AND link_type='html' AND source_domain!=?"
        new = " ".join(r[0] for r in c.execute('EXPLAIN SELECT source_url, destination_url, first_seen FROM outgoing_links' + cond + ' ORDER BY first_seen COLLATE "C" DESC, id LIMIT ?', ("codigix.co", "codigix.co", 200)))
        rem = " ".join(r[0] for r in c.execute("EXPLAIN SELECT source_url, destination_url, anchor_text, lost_at FROM outgoing_links" + cond +
                                               " AND COALESCE(lost_at,'')<>'' AND COALESCE(is_active,0)=0 ORDER BY lost_at COLLATE \"C\" DESC, id LIMIT ?", ("codigix.co", "codigix.co", 200)))
        hist = " ".join(r[0] for r in c.execute('EXPLAIN SELECT id FROM link_history WHERE destination_domain=? ORDER BY detected_at COLLATE "C" DESC, id DESC LIMIT ?', ("codigix.co", 400)))
    assert "idx_outgoing_dest_first_seen" in new and "idx_outgoing_removed" in rem and "idx_link_history_dest_detected_c" in hist

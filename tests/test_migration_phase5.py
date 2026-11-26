"""Migration qualification for versions 12 and 13, on real PostgreSQL (one scratch schema per test, never the production tunnel):
fresh database, upgrade from v9 with data, a database that already holds versions 10/11 this branch does not know, v12-only database, repeated runs.
Existing rows must come out unchanged (new columns aside) and the business flow must run on every upgraded shape."""

import asyncio
import json
import uuid

import psycopg
import pytest

import business_jobs as bj
import crawler_core as core
import ddg_lookup
from test_crawler import port, rows  # noqa: F401 - fixture

FULL = list(core.MIGRATIONS)


@pytest.fixture()
def schema(monkeypatch):
    name = "mig_" + uuid.uuid4().hex[:8]
    assert ":5433" not in core.DATABASE_URL, "never run migration tests against the production tunnel"
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {name}")
        monkeypatch.setattr(core, "DB_SCHEMA", name)
        yield name
        monkeypatch.setattr(core, "MIGRATIONS", FULL)
        admin.execute(f"DROP SCHEMA {name} CASCADE")


def migrate_to(monkeypatch, version: int | None) -> None:
    monkeypatch.setattr(core, "MIGRATIONS", [m for m in FULL if version is None or m[0] <= version])
    with core.connect() as c:
        core.migrate(c, recover=False)


def versions() -> list[int]:
    return [r["version"] for r in rows("SELECT version FROM schema_migrations ORDER BY 1")]


def snapshot() -> dict[str, list[dict]]:
    out = {}
    with core.connect() as c:
        for t in [r[0] for r in c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE'")]:
            out[t] = sorted((json.loads(r[0]) for r in c.execute(f'SELECT row_to_json(x)::text FROM "{t}" x')), key=lambda d: json.dumps(d, sort_keys=True))
    return out


def seed_v9() -> None:
    """Rows of every kind the business feature wrote before this phase (raw SQL: the v9 schema has no parent_domain)."""
    with core.connect() as c:
        for d, typ in (("own.example", "own_business"), ("rival.example", "competitor"), ("second.example", "own_business")):
            c.execute("INSERT INTO businesses (domain, business_name, search_interval_seconds, next_run_at, target_type) VALUES (?,?,?,?,?)", (d, d, 21600, "2026-01-01 00:00:00", typ))
        c.execute("INSERT INTO business_competitors (business_domain, competitor_domain) VALUES ('own.example','rival.example'), ('second.example','rival.example')")
        for d in ("own.example", "rival.example"):
            jid = c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES (?,2,'done',3,150) RETURNING id", (d,)).fetchone()[0]
            for i in range(3):
                core.save_url(c, f"https://src{i}.{d}/p", None, "ddg", 0, False)
            c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls WHERE domain LIKE ?", (jid, f"src%.{d}"))


def assert_unchanged(before: dict, after: dict) -> None:
    for t, old in before.items():
        new = after[t]
        assert len(new) >= len(old), f"{t}: rows were lost"
        if t == "schema_migrations":
            continue
        for o, n in zip(old, new):  # same order key set: every old column keeps its value (new columns are additions)
            assert {k: n[k] for k in o} == o, f"{t}: an existing row changed: {o} -> {n}"


def flow_works() -> None:
    """The business feature on the upgraded shape: onboard, a cursor-based search, close the job."""
    ddg_lookup._page = lambda q, p, a, end=float("inf"), backend="auto": [{"href": f"https://x{p}{i}.flow.net/p"} for i in range(3)] if p == 1 else []
    with core.connect() as c:
        bj.create_job(c, "flow.example")
    asyncio.run(bj.search_pending())
    bj.tick()
    r = rows("SELECT status, search_results FROM business_jobs WHERE target_domain='flow.example'")[0]
    assert r["search_results"] == 3 and rows("SELECT COUNT(*) n FROM discovery_cursors WHERE domain='flow.example'")[0]["n"] >= 1


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    real = ddg_lookup._page
    monkeypatch.setattr(ddg_lookup.time, "sleep", lambda s: None)
    monkeypatch.setattr(ddg_lookup, "BACKENDS", ["b1"])
    yield
    ddg_lookup._page = real


def test_fresh_database_gets_every_version_and_no_schema_problems(schema, monkeypatch):
    migrate_to(monkeypatch, None)
    assert versions() == [1, 2, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17]  # 3/4 belong to the common-crawl branch, 10/11 to other work: deliberately not reused
    with core.connect() as c:
        assert core.schema_problems(c) == []
        cols = {(r[0], r[1]) for r in c.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema=current_schema()")}
    assert {("businesses", "parent_domain"), ("businesses", "last_search_at"), ("businesses", "discovery_state"), ("discovery_cursors", "empty_streak"),
            ("url_discoveries", "query_hash")} <= cols
    flow_works()


def test_upgrade_from_v9_keeps_every_existing_row_and_backfills(schema, monkeypatch):
    migrate_to(monkeypatch, 9)
    assert versions() == [1, 2, 5, 6, 7, 8, 9]
    seed_v9()
    before = snapshot()
    migrate_to(monkeypatch, None)
    assert versions() == [1, 2, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17]
    assert_unchanged(before, snapshot())
    assert rows("SELECT COUNT(*) n FROM url_discoveries WHERE query_hash='legacy'")[0]["n"] == 6  # 3 candidates for each of two targets
    assert rows("SELECT domain, parent_domain FROM businesses ORDER BY domain") == [
        {"domain": "own.example", "parent_domain": None}, {"domain": "rival.example", "parent_domain": "own.example"},  # lowest-sorting active owner
        {"domain": "second.example", "parent_domain": None}]
    flow_works()


def test_database_already_at_v11_with_history_this_branch_does_not_know(schema, monkeypatch):
    migrate_to(monkeypatch, 9)
    seed_v9()
    with core.connect() as c:  # what the scratch database holds: v10 (a column) and v11 (indexes) from work that is not in this branch
        c.execute("ALTER TABLE business_jobs ADD COLUMN search_lease_expires TEXT")
        c.execute("CREATE INDEX idx_unknown_v11 ON discovered_urls(crawl_status, crawl_depth, id)")
        c.execute("INSERT INTO schema_migrations (version, name) VALUES (10, 'search_lease_expires'), (11, 'indexes for scheduler, metrics and retention')")
    before = snapshot()
    migrate_to(monkeypatch, None)
    assert versions() == [1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]  # 10 and 11 are left exactly as they were, 12 and 13 are added on top
    after = snapshot()
    assert_unchanged(before, after)
    assert rows("SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_unknown_v11'")
    assert any("search_lease_expires" in j for j in (json.dumps(r) for r in after["business_jobs"]))  # the foreign column survives
    with core.connect() as c:
        assert core.schema_problems(c) == []
    flow_works()  # and the feature runs on that shape: nothing here depends on v10/v11


def test_database_at_v12_only_gets_v13(schema, monkeypatch):
    migrate_to(monkeypatch, 12)
    with core.connect() as c:  # a v12 database as Phase 5 first left it: a competitor already registered, no parent column yet
        c.execute("INSERT INTO businesses (domain, search_interval_seconds, next_run_at, target_type) VALUES ('c.example', 86400, '2026-01-01 00:00:00', 'competitor'), ('o.example', 21600, '2026-01-01 00:00:00', 'own_business')")
        c.execute("INSERT INTO business_competitors (business_domain, competitor_domain) VALUES ('o.example', 'c.example')")
    before = snapshot()
    migrate_to(monkeypatch, None)
    assert versions()[-4:] == [14, 15, 16, 17]
    assert_unchanged(before, snapshot())
    assert rows("SELECT parent_domain FROM businesses WHERE domain='c.example'")[0]["parent_domain"] == "o.example"


def test_repeated_migration_changes_nothing(schema, monkeypatch):
    migrate_to(monkeypatch, None)
    seed_after = rows("SELECT version, name, applied_at FROM schema_migrations ORDER BY 1")
    snap = snapshot()
    for _ in range(3):
        migrate_to(monkeypatch, None)
    assert rows("SELECT version, name, applied_at FROM schema_migrations ORDER BY 1") == seed_after  # no version was re-applied
    assert snapshot() == snap
    with core.connect() as c:  # re-running a version's statements themselves (its row removed) is also harmless
        c.execute("DELETE FROM schema_migrations WHERE version IN (12, 13)")
        core.migrate(c, recover=False)
        assert core.schema_problems(c) == []
    assert versions()[-4:] == [14, 15, 16, 17]

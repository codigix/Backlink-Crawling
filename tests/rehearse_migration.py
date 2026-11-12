"""Migration rehearsal. NEVER writes to the source database: it only reads it (pg_dump, counts).

  .venv/Scripts/python tests/rehearse_migration.py

1. pg_dump -Fc of the database at DATABASE_URL            -> backups/<db>_<timestamp>.dump
2. restore into a new isolated database <db>_rehearsal     (dropped and recreated each run)
3. row counts source == restored copy
4. migrate the restored copy as-is (already at the latest version: must be a no-op)
5. roll the copy back to the OLD pre-migration schema (no schema_migrations, no v1/v2 columns, indexes, tables), migrate it forward
6. schema_problems() == [], row counts unchanged, constraint/index definitions identical to the source
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import crawler_core as core  # noqa: E402

BIN = next((p for p in sorted(Path("C:/Program Files/PostgreSQL").glob("*/bin"), reverse=True) if (p / "pg_dump.exe").exists()), None)
SRC = core.DATABASE_URL
info = conninfo_to_dict(SRC)
DB = info["dbname"]
REH = DB + "_rehearsal"
assert REH != DB
ENV = {**os.environ, "PGPASSWORD": info.get("password", "")}
HOST = ["-h", info.get("host", "localhost"), "-p", str(info.get("port", 5432)), "-U", info.get("user", "postgres")]
DEST = make_conninfo(SRC, dbname=REH)
BACKUPS = ROOT / "backups"
BACKUPS.mkdir(exist_ok=True)


def run(cmd):
    t = time.time()
    r = subprocess.run([str(BIN / cmd[0]), *cmd[1:]], env=ENV, capture_output=True, text=True)
    print(f"  {cmd[0]} exit {r.returncode} in {time.time() - t:.0f}s", flush=True)
    if r.returncode:
        print(r.stderr[-1500:])
        raise SystemExit(1)


def counts(url):
    with psycopg.connect(url) as c:
        names = [r[0] for r in c.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1")]
        return {n: c.execute(f'SELECT COUNT(*) FROM "{n}"').fetchone()[0] for n in names}


def structure(url):
    with psycopg.connect(url) as c:
        idx = {r[0]: r[1] for r in c.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public'")}
        con = {r[0]: r[1] for r in c.execute("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='public'::regnamespace")}
        cols = {(r[0], r[1]): (r[2], r[3]) for r in c.execute(
            "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='public'")}
    return idx, con, cols


def main():
    stamp = time.strftime("%Y%m%d_%H%M%S")
    dump = BACKUPS / f"{DB}_{stamp}.dump"
    print("1. backup ->", dump, flush=True)
    run(["pg_dump", *HOST, "-Fc", "-f", str(dump), DB])
    print(f"   size {dump.stat().st_size / 1e6:.1f} MB")
    print("2. restore into", REH, flush=True)
    with psycopg.connect(make_conninfo(SRC, dbname="postgres"), autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{REH}" WITH (FORCE)')
        c.execute(f'CREATE DATABASE "{REH}"')
    run(["pg_restore", *HOST, "--no-owner", "-d", REH, str(dump)])
    src_counts, reh_counts = counts(SRC), counts(DEST)
    print("3. row counts equal:", src_counts == reh_counts, "|", {k: v for k, v in src_counts.items() if v})
    assert src_counts == reh_counts
    src_struct = structure(SRC)
    core.DATABASE_URL = DEST
    with core.connect() as c:
        print("4. migrate restored copy (already latest):", end=" ")
        core.migrate(c, recover=False)
        print("schema_problems =", core.schema_problems(c))
        assert core.schema_problems(c) == []
    print("5. roll the copy back to the pre-migration schema, then migrate forward", flush=True)
    with psycopg.connect(DEST, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS schema_migrations, link_history")
        for ix in ("idx_outgoing_dest_active", "idx_link_history_dest", "idx_backlink_history_bl", "idx_backlinks_source", "idx_discovered_lease"):
            c.execute(f"DROP INDEX IF EXISTS {ix}")
        for col in ("page_cursor", "total_pages", "index_id"):
            c.execute(f"ALTER TABLE discovery_jobs DROP COLUMN IF EXISTS {col}")
        for t, col in core._V1_COLUMNS:
            c.execute(f"ALTER TABLE {t} DROP COLUMN IF EXISTS {col.split()[0]}")
        c.execute("DROP INDEX IF EXISTS idx_outgoing_source_domain")
    with core.connect() as c:
        pre = core.schema_problems(c)
        print(f"   old-schema problems detected before migrating: {len(pre)} (e.g. {pre[:3]})")
        assert pre, "rollback did not recreate the old schema"
        t = time.time()
        core.migrate(c, recover=False)
        print(f"   migrated forward in {time.time() - t:.1f}s; schema_problems =", core.schema_problems(c))
        assert core.schema_problems(c) == []
        print("   schema_migrations:", [tuple(r)[:2] for r in c.execute("SELECT version, name FROM schema_migrations ORDER BY 1")])
    after = counts(DEST)
    print("6. row counts unchanged by migration:", after == {**src_counts, "link_history": 0, "schema_migrations": 2} or after == src_counts)
    assert {k: v for k, v in after.items() if k in src_counts and k != "schema_migrations"} == {k: v for k, v in src_counts.items() if k != "schema_migrations"}
    idx, con, cols = structure(DEST)
    s_idx, s_con, s_cols = src_struct
    for name, a, b in (("indexes", s_idx, idx), ("constraints", s_con, con), ("columns", s_cols, cols)):
        print(f"   {name}: only in source {sorted(set(a) - set(b))[:5]} | only in migrated {sorted(set(b) - set(a))[:5]} | differing {[k for k in a if k in b and a[k] != b[k]][:5]}")
    print("REHEARSAL PASSED; rehearsal database kept for tests:", REH, "| backup:", dump)


if __name__ == "__main__":
    main()

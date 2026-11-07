"""One-off copy of the old SQLite file into PostgreSQL (DATABASE_URL).

  python migrate_sqlite_to_postgres.py [path/to/backlinks.db]

Creates the schema, copies the graph tables with their ids, then resets the id sequences. Run once on an empty database.
legacy_backlinks / jobs / archived_links (old per-job results, read by no endpoint) are not copied.
"""
import sqlite3
import sys

import crawler_core as core

TABLES = ("discovered_urls", "discovered_domains", "outgoing_links", "target_domains", "backlinks", "crawl_history",
          "backlink_history", "discovery_sources", "engine_state", "worker_status", "discovery_jobs", "crawl_jobs")

src = sqlite3.connect(f"file:{sys.argv[1] if len(sys.argv) > 1 else core.__file__.replace('crawler_core.py', 'backlinks.db')}?mode=ro", uri=True)
src.row_factory = sqlite3.Row
with core.connect() as pg:
    core.migrate(pg, recover=False)
    for t in TABLES:
        try:
            cur = src.execute(f"SELECT * FROM {t}")
        except sqlite3.OperationalError:
            continue
        cols = [d[0] for d in cur.description]
        sql = f"INSERT INTO {t} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) ON CONFLICT DO NOTHING"
        n = 0
        while batch := cur.fetchmany(5000):
            pg.executemany(sql, [tuple(r) for r in batch])
            n += len(batch)
        if "id" in cols:
            pg.execute(f"SELECT setval(pg_get_serial_sequence('{t}','id'), COALESCE((SELECT MAX(id) FROM {t}), 1))")
        print(f"{t}: {n}", flush=True)

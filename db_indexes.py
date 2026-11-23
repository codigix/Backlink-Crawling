"""Read-path indexes, built CONCURRENTLY (no write lock on a live table) outside the migration runner.

migrate() runs every migration in ONE transaction, and CREATE INDEX CONCURRENTLY cannot run inside one, so these live here: a separate step an
operator runs on purpose.  Nothing here drops, renames or rewrites anything:
  * an index of the same name is accepted only if valid AND its table, columns, ordering, uniqueness and predicate match; a different one is
    reported (with its definition) and left alone,
  * an INVALID one (an earlier build failed or was cancelled) is REPORTED, never dropped; the DROP to run by hand is printed,
  * a failed build is reported and the next index is still tried.

The target is verified before anything connects (fail closed). On a development machine DATABASE_URL in .env is the SSH tunnel to production, so:
  * --apply never reads DATABASE_URL. It needs INDEX_DATABASE_URL (set for this one command) AND --confirm-target 'host:port/dbname' that matches it;
  * port 5433 (the tunnel) is refused for every mode, including the dry run; the effective port follows libpq (URL port, else PGPORT, else 5432);
  * the target is printed as host:port/dbname only: never the user, the password or any option.

    python db_indexes.py                                   # dry run (reads INDEX_DATABASE_URL, else DATABASE_URL): what exists / is missing
    INDEX_DATABASE_URL=postgresql://... python db_indexes.py --apply --confirm-target host:5432/dbname
"""
import argparse
import os
import re
import sys

import crawler_core as core

FORBIDDEN_PORTS = {5433}  # the development machine's SSH tunnel to the production database
EXIT_PROBLEM, EXIT_REFUSED = 1, 2

# name -> (CREATE statement, expected definition, why). The expected definition is what pg_get_indexdef() prints after "CREATE INDEX name ON [schema.]":
# table, method, columns with their ordering, predicate. An index of that NAME counts as present only if it matches it exactly (and is valid and not
# unique); otherwise it is reported and left alone. idx_crawl_history_completed came from another branch's migration 11, so it may already exist.
# Only indexes whose benefit was MEASURED are here: (destination_domain, id) on outgoing_links and link_history were tried and the planner kept
# walking the primary key for hot domains (no gain), so they are not built.
INDEXES: dict[str, tuple[str, str, str]] = {
    "idx_discovered_first_seen": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_discovered_first_seen ON discovered_urls(first_seen)",
        "discovered_urls USING btree (first_seen)",
        "statistics: urls discovered in the last minute (first_seen >= ?)"),
    "idx_crawl_history_completed": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crawl_history_completed ON crawl_history(crawl_completed)",
        "crawl_history USING btree (crawl_completed)",
        "statistics: pages crawled in the last minute (crawl_completed >= ?)"),
    "idx_link_history_dest_detected_c": (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_link_history_dest_detected_c ON link_history(destination_domain, detected_at COLLATE "C" DESC, id DESC)',
        'link_history USING btree (destination_domain, detected_at COLLATE "C" DESC, id DESC)',
        "/history: the newest events of one domain in detected_at order, read in batches (COLLATE C = Python's string order)"),
    "idx_outgoing_removed": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outgoing_removed ON outgoing_links(destination_domain, lost_at COLLATE \"C\" DESC, id)"
        " WHERE link_type='html' AND COALESCE(is_active,0)=0 AND COALESCE(lost_at,'')<>''",
        "outgoing_links USING btree (destination_domain, lost_at COLLATE \"C\" DESC, id) WHERE ((link_type = 'html'::text) AND (COALESCE(is_active, 0) = 0)"
        " AND (COALESCE(lost_at, ''::text) <> ''::text))",
        "/history: removed links known only by lost_at; holds removed rows only (tiny), so a domain without any is answered without scanning its links"),
    "idx_outgoing_dest_first_seen": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outgoing_dest_first_seen ON outgoing_links(destination_domain, first_seen COLLATE \"C\" DESC, id)"
        " WHERE link_type='html'",
        "outgoing_links USING btree (destination_domain, first_seen COLLATE \"C\" DESC, id) WHERE (link_type = 'html'::text)",
        "/history: the newest 'new' link events of one domain"),
}


def state(conn, name: str) -> tuple[str, str]:
    """('missing' | 'valid' | 'invalid', definition) of an index in the connection's current schema; definition = pg_get_indexdef() without the
    'CREATE INDEX name ON schema.' prefix, with 'UNIQUE ' kept in front when the index is unique."""
    r = conn.execute("SELECT i.indisvalid, i.indisunique, pg_get_indexdef(i.indexrelid) AS d FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid"
                     " WHERE c.relname=? AND c.relnamespace=current_schema()::regnamespace", (name,)).fetchone()
    if not r:
        return "missing", ""
    d = re.sub(r"^CREATE (?:UNIQUE )?INDEX \S+ ON (?:ONLY )?(?:\S+\.)?", "", r["d"])
    return ("valid" if r["indisvalid"] else "invalid"), ("UNIQUE " if r["indisunique"] else "") + d


def apply(conn, dry_run: bool = False) -> dict[str, str]:
    """Build every missing index. Returns name -> 'created' | 'exists' | 'missing' (dry run) | 'DIFFERENT ...' | 'INVALID ...' | 'FAILED ...'.
    Never issues a DROP."""
    conn.autocommit = True  # CONCURRENTLY refuses a transaction block
    conn.execute("SET statement_timeout = 0")  # the pool's 5-minute cap would kill the build of a big table half way and leave an INVALID index
    out = {}
    for name, (ddl, expected, _why) in INDEXES.items():
        st, have = state(conn, name)
        if st == "invalid":
            out[name] = f"INVALID (an earlier build failed); not touched. To rebuild, review it, then run by hand: DROP INDEX CONCURRENTLY {name}"
        elif st == "valid" and have == expected:
            out[name] = "exists"
        elif st == "valid":
            out[name] = f"DIFFERENT definition under this name: [{have}], expected [{expected}]; not touched, nothing built"
        elif dry_run:
            out[name] = "missing"
        else:
            try:
                conn.execute(ddl)
                got = state(conn, name)
                out[name] = "created" if got == ("valid", expected) else f"INVALID after build ({got[0]}: {got[1]}); not touched"
            except Exception as e:  # report and carry on: the other indexes are independent
                out[name] = f"FAILED {type(e).__name__}: {e}".splitlines()[0]
    return out


# ---------------------------------------------------------------- target verification (before anything connects) ----------------------------------------------------------------

class Refused(Exception):
    """The target was not verified; the message is safe to print (it never contains the URL, user or password)."""


_HOST_OK = re.compile(r"^(?:[A-Za-z0-9.-]+|[0-9A-Fa-f:.]+|/[A-Za-z0-9_./-]+)$")  # a host name, an IPv4/IPv6 address or a socket directory
_DBNAME_OK = re.compile(r"^[A-Za-z0-9_.$-]+$")


def target_of(url: str, environ) -> tuple[str, list[int]]:
    """('host:port/dbname', [ports]) of a PostgreSQL URL or key=value string, for display and checking. Only host, port and database name are used
    (a multi-host URL lists every host); the port is what libpq would use: the URL's, else PGPORT, else 5432."""
    from psycopg.conninfo import conninfo_to_dict
    try:
        d = conninfo_to_dict(url)
    except Exception:  # noqa: BLE001 - never echo the text: it may hold a password
        raise Refused("the database URL cannot be parsed") from None
    hosts = [h for h in (d.get("host") or d.get("hostaddr") or environ.get("PGHOST") or "").split(",") if h]
    if not hosts:
        raise Refused("the database URL names no host")
    if not all(_HOST_OK.match(h) for h in hosts):  # e.g. a password with a raw '@' or '/' makes libpq read part of it as the host: never display that
        raise Refused("the database URL has a host that is not a plain host name or address (escape special characters in the password, e.g. @ as %40)")
    ports = (d.get("port") or environ.get("PGPORT") or "5432").split(",")
    if len(ports) == 1:
        ports = ports * len(hosts)
    if len(ports) != len(hosts):
        raise Refused("the database URL has a different number of hosts and ports")
    try:
        nums = [int(p) for p in ports]
    except ValueError:
        raise Refused("the database URL has a port that is not a number") from None
    dbname = d.get("dbname") or environ.get("PGDATABASE")
    if not dbname:
        raise Refused("the database URL names no database")
    if not _DBNAME_OK.match(dbname):
        raise Refused("the database name is not a plain identifier (escape special characters in the password, e.g. / as %2F)")
    return ",".join(f"{h.lower()}:{p}" for h, p in zip(hosts, nums)) + "/" + dbname, nums


def verified_target(apply_mode: bool, confirm: str | None, environ) -> tuple[str, str, str]:
    """(url, label, source) of the database to use, or Refused. --apply uses INDEX_DATABASE_URL only and needs --confirm-target equal to its label;
    the dry run may fall back to DATABASE_URL. Port 5433 is refused in both."""
    explicit, inherited = environ.get("INDEX_DATABASE_URL"), environ.get("DATABASE_URL")
    if apply_mode and not explicit:
        raise Refused("--apply needs INDEX_DATABASE_URL set for this command; DATABASE_URL is never used to build indexes (on a development machine it is the production tunnel)")
    url, source = (explicit, "INDEX_DATABASE_URL") if explicit else (inherited, "DATABASE_URL")
    if not url:
        raise Refused("set INDEX_DATABASE_URL (or DATABASE_URL for a dry run)")
    label, ports = target_of(url, environ)
    bad = sorted(set(ports) & FORBIDDEN_PORTS)
    if bad:
        raise Refused(f"target {label} uses port {bad[0]}, the SSH tunnel to the production database; refused (also for a dry run)")
    if apply_mode and (confirm or "").strip().lower() != label.lower():
        raise Refused(f"target {label} was not confirmed; to build indexes there re-run with --confirm-target {label}")
    return url, label, source


def main(argv=None, environ=None, connect=None) -> int:
    """Exit codes: 0 done / nothing to do, 1 an index is INVALID, DIFFERENT or FAILED, 2 refused (nothing was connected to)."""
    environ = os.environ if environ is None else environ
    ap = argparse.ArgumentParser(description="Build the read-path indexes concurrently; dry run unless --apply.")
    ap.add_argument("--apply", action="store_true", help="build what is missing (needs INDEX_DATABASE_URL and --confirm-target)")
    ap.add_argument("--confirm-target", help="the target as printed: host:port/dbname")
    args = ap.parse_args(argv)
    try:
        url, label, source = verified_target(args.apply, args.confirm_target, environ)
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_REFUSED
    print(f"target: {label}" + (f" schema {core.DB_SCHEMA}" if core.DB_SCHEMA else "") + f" (from {source}; "
          + ("building missing indexes)" if args.apply else "dry run, nothing is built; add --apply with INDEX_DATABASE_URL and --confirm-target to build)"))
    if connect is None:
        def connect(u):
            core.DATABASE_URL = u  # connect_unpooled reads it: the verified URL, never whatever .env said
            return core.connect_unpooled()
    conn = connect(url)
    try:
        res = apply(conn, dry_run=not args.apply)
        for n, r in res.items():
            print(f"  {n}: {r}")
        return EXIT_PROBLEM if any(r.startswith(("INVALID", "FAILED", "DIFFERENT")) for r in res.values()) else 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

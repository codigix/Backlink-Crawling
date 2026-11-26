"""Read-only FastAPI router over the link graph (backlink search + inspection). Mounted by backlink_api.py.
Crawling is done only by the standalone autonomous.py process."""

import os
import re
from contextlib import nullcontext
from typing import Annotated

import psycopg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

import business_jobs
import competitors
import crawler_core as core
import api_timing
import engine_stats
from api_timing import connect
from read_cache import graph_cache

graph_cache.no_stale = (core.DatabaseUnavailable, psycopg.OperationalError)  # an outage is an error, never a "successful" old aggregate

router = APIRouter()

# Endpoints that used to return everything are bounded: a page is DEFAULT_LIMIT rows unless the client asks for more, never more than MAX_LIMIT.
# Older clients that sent an explicit limit keep working; clients that relied on 'no limit = all rows' now page with after_id / offset.
DEFAULT_LIMIT = int(os.environ.get("API_DEFAULT_LIMIT", 1000))
MAX_LIMIT = int(os.environ.get("API_MAX_LIMIT", 10000))
# Annotated (not `= Query(...)`) so a direct Python call of an endpoint function sees a plain int default, as the tests do.
Limit = Annotated[int, Query(ge=1, le=MAX_LIMIT, description="page size (default API_DEFAULT_LIMIT, max API_MAX_LIMIT)")]
Offset = Annotated[int, Query(ge=0)]
Cursor = Annotated[str | None, Query(description="keyset paging instead of offset: the next_cursor of the previous page")]
AfterId = Annotated[int | None, Query(ge=0, description="keyset paging: only rows with id greater than this (the next_after_id of the previous page)")]


def link_status(r) -> str:
    """One definition of a link's state, shared by every endpoint (matches the backlinks table's states)."""
    if not r["is_active"]:
        return "removed"  # two consecutive successful fetches without the link
    if r["lost_at"]:
        return "suspected_removed"  # one miss so far
    return "fetch_failed" if r["src_status"] in ("failed", "retry_pending") else "verified"


# Explicit column lists (never SELECT *): a column added to a table later does not silently change, or widen, an API response.
_URL_COLS = ("id, url, normalized_url, domain, hostname, discovered_from, discovery_method, first_seen, last_seen, crawl_status, crawl_depth,"
             " is_internal, is_external, http_status, content_type, last_crawled, crawl_error, next_crawl_at, retry_count, sitemap_lastmod,"
             " canonical_url, last_successful_crawl, content_hash, title, lease_owner, lease_expires, skip_reason, lease_gen")
_DOMAIN_COLS = "id, domain, first_seen, last_seen, discovery_source, discovery_status, pages_discovered, last_crawled, priority"
_OUT_COLS = ("id, source_url, destination_url, destination_domain, anchor_text, rel_attributes, link_type, first_seen, last_seen, is_active,"
             " is_nofollow, is_sponsored, is_ugc, position, source_domain, lost_at")
_OUT_DETAIL_COLS = ", img_alt, context"  # free text up to 300 chars per row: only sent when the client asks (include_details=true)


def _page(conn, sql: str, args: list, limit: int, offset: int) -> list[dict]:
    return [dict(r) for r in conn.execute(sql + " LIMIT ? OFFSET ?", (*args, limit, offset))]


def _snapshot_total(c, key: str) -> int | None:
    """An unfiltered total from the shared statistics snapshot (at most STATS_TTL old, age in X-Stats-Age) instead of a full-table COUNT(*);
    None when no snapshot can be had, then the caller counts exactly."""
    try:
        return engine_stats.snapshot_counts(lambda: nullcontext(c))[key]
    except engine_stats.StatsUnavailable:
        return None


def _keyset(after_id: int | None, offset: int) -> tuple[str, list]:
    if after_id is not None and offset:
        raise HTTPException(422, "use either after_id or offset, not both")
    return ("id>?", [after_id]) if after_id is not None else ("", [])


@router.get("/crawler/discovered-urls")
def discovered_urls(domain: str | None = None, is_external: bool | None = None, limit: int = Query(100, ge=1, le=1000),
                    after_id: int | None = Query(None, ge=0, description="keyset paging: only rows with id greater than this (the last id of the previous page)")) -> dict:
    where, args = [], []
    for col, v in (("domain", domain), ("is_external", None if is_external is None else int(is_external))):
        if v is not None:
            where.append(f"{col}=?")
            args.append(v)
    clause = " WHERE " + " AND ".join(where) if where else ""
    key, kargs = _keyset(after_id, 0)
    page_clause = " WHERE " + " AND ".join(where + ([key] if key else [])) if where or key else ""
    with connect() as c:
        total = None if where else _snapshot_total(c, "discovered_urls")
        if total is None:
            total = c.execute("SELECT COUNT(*) n FROM discovered_urls" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, f"SELECT {_URL_COLS} FROM discovered_urls" + page_clause + " ORDER BY id", args + kargs, limit, 0)}


@router.get("/crawler/discovered-domains")
def discovered_domains(discovery_status: str | None = None, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0),
                       after_id: int | None = Query(None, ge=0, description="keyset paging instead of offset: only rows with id greater than this")) -> dict:
    clause, args = (" WHERE discovery_status=?", [discovery_status]) if discovery_status else ("", [])
    key, kargs = _keyset(after_id, offset)
    page_clause = clause + (" AND " if clause else " WHERE ") + key if key else clause
    with connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM discovered_domains" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, f"SELECT {_DOMAIN_COLS} FROM discovered_domains" + page_clause + " ORDER BY id", args + kargs, limit, offset)}


@router.get("/crawler/outgoing-links")
def outgoing_links(source_url: str | None = None, destination_domain: str | None = None, active_only: bool = True,
                         limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0),
                         after_id: int | None = Query(None, ge=0, description="keyset paging instead of offset: only rows with id greater than this"),
                         include_details: bool = False) -> dict:
    where, args = [], []
    for col, v in (("source_url", source_url), ("destination_domain", destination_domain)):
        if v:
            where.append(f"{col}=?")
            args.append(v)
    if active_only:
        where.append("is_active=1")
    clause = " WHERE " + " AND ".join(where) if where else ""
    key, kargs = _keyset(after_id, offset)
    page_clause = " WHERE " + " AND ".join(where + ([key] if key else [])) if where or key else ""
    with connect() as c:
        total = _snapshot_total(c, "outgoing_links") if where == ["is_active=1"] else None  # exactly the statistics' "active links" count
        if total is None:
            total = c.execute("SELECT COUNT(*) n FROM outgoing_links" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, f"SELECT {_OUT_COLS}{_OUT_DETAIL_COLS if include_details else ''} FROM outgoing_links" + page_clause + " ORDER BY id", args + kargs, limit, offset)}


def _clean(domain: str) -> str:
    try:
        return core.clean_target(domain)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e


def _host_re(host: str) -> str:
    """Postgres regex matching URLs whose host is `host` or one of its subdomains (never a longer name like evilhost)."""
    return r"^https?://(?:[^/?#@]*\.)?" + re.escape(host) + r"(?::[0-9]+)?(?:[/?#]|$)"


def _graph_cond(target: str, active_only: bool = True) -> tuple[str, list]:
    """WHERE clause + args selecting the html links into `target` from the global outgoing_links graph (no crawl, no network, no
    registration needed). Registrable-domain and subdomain targets are both filtered in SQL, so totals are exact."""
    reg = core.registrable_domain(target)
    cond = " WHERE destination_domain=? AND link_type='html' AND source_domain!=?" + (" AND is_active=1" if active_only else "")
    args: list = [reg, reg]
    if target != reg:  # subdomain: the destination host must match, and links from the target's own host do not count
        cond = cond.replace("source_domain!=?", "TRUE") + " AND destination_url ~* ? AND source_url !~* ?"
        args = [reg, _host_re(target), _host_re(target)]
    return cond, args


_PLAIN_HOST = re.compile(r"https?://([a-z0-9.-]+)(?:[/?#]|$)")  # no userinfo/port/uppercase/unicode: such a URL's hostname() is exactly this group (minus a trailing dot)
HISTORY_WINDOW = 100000  # /history reads at most this many (newest) link_history rows of a domain
_SRC_STATUS = "(SELECT crawl_status FROM discovered_urls u WHERE u.url=outgoing_links.source_url)"
_LINK_COLS = "source_url, destination_url, anchor_text, rel_attributes, source_domain, first_seen, last_seen, is_active, lost_at"  # what the responses use


def _graph_page(c, target: str, active_only: bool, limit: int, offset: int, after_id: int | None = None) -> list:
    """One page of links (only the columns the API returns, plus the source page's crawl status), ordered by id. after_id = keyset paging."""
    cond, args = _graph_cond(target, active_only)
    if after_id is not None:
        cond, args = cond + " AND id>?", [*args, after_id]
    return c.execute(f"SELECT id, {_LINK_COLS}, {_SRC_STATUS} AS src_status FROM outgoing_links" + cond + " ORDER BY id LIMIT ? OFFSET ?",
                     (*args, limit, offset)).fetchall()


def _cached(kind: str, target: str, params: tuple, compute):
    """graph_cache lookup for one graph aggregate. The key is the query kind, the CLEANED target (a subdomain target and its registrable domain
    select different links, so they never share a key) and every parameter that changes the result. Authorization (_comp, _owner), live job
    state and the link rows of a page stay outside: callers run them on every request. Adds X-Cache / X-Cache-Age when the cache is on."""
    value, status, age = graph_cache.get((kind, target, *params), compute)
    if status != "OFF":
        api_timing.note_cache(status, age)
    return value


def _graph_summary(c, target: str, active_only: bool) -> tuple[int, int, int]:
    """(total links, verified links, referring domains) in ONE scan; same rules as link_status() and as counting the distinct source_domain values
    in Python (a NULL source_domain is one group there, as it is for GROUP BY here). The referring domains are the groups of a hash aggregate: the
    former COUNT(DISTINCT source_domain) sorted every row (an external merge sort on disk for a hot target) and was ~4x slower."""
    cond, args = _graph_cond(target, active_only)
    # LEFT JOIN, not a per-row subquery, so the planner can hash-join the failed pages; url is UNIQUE, so the join never multiplies rows
    r = c.execute(
        "SELECT COALESCE(SUM(n), 0)::bigint AS total, COALESCE(SUM(v), 0)::bigint AS verified, COUNT(*) AS refs FROM ("
        "SELECT t.source_domain, COUNT(*) AS n, COUNT(*) FILTER (WHERE COALESCE(t.is_active,0)<>0 AND COALESCE(t.lost_at,'')='' AND bad.url IS NULL) AS v"
        " FROM (SELECT is_active, lost_at, source_domain, source_url FROM outgoing_links" + cond + ") t"
        " LEFT JOIN discovered_urls bad ON bad.url=t.source_url AND bad.crawl_status IN ('failed','retry_pending') GROUP BY t.source_domain) g", args).fetchone()
    return r["total"], r["verified"], r["refs"]


def _all_cursor(cursor: str | None, offset: int) -> tuple[str, list]:
    """`cursor` of GET /backlinks = '<target_type>:<destination_domain>:<id>' of the last row of the previous page (its next_cursor)."""
    if cursor is None:
        return "", []
    if offset:
        raise HTTPException(422, "use either cursor or offset, not both")
    ttype, _, rest = cursor.partition(":")
    dom, _, rid = rest.rpartition(":")
    if ttype not in ("own_business", "competitor") or not dom or not rid.isdigit():
        raise HTTPException(422, "cursor must be the next_cursor of a previous page")
    return " AND (b.target_type, l.destination_domain, l.id) > (?, ?, ?)", [ttype, dom, int(rid)]


def _all_page_sql(cols: str, active: str, target_type: str | None, cursor: str | None, kargs: list, n: int, offset: int) -> tuple[str, list]:
    """The page query of GET /backlinks, target by target. The former join + global sort read and sorted EVERY link of every monitored target
    (~2.3M rows, a full scan of the 38M-row table) to return 100 of them. Here the targets are walked in their final order and, for each, only
    its first `offset+n` links by id are read through idx_outgoing_dest_active, so the work stops after the first target(s) that fill the page.
    Same rows, same order (target_type, target domain, link id), same cursor and offset meaning as before."""
    tcond, targs = [], []
    if target_type:
        tcond.append("target_type=?")
        targs.append(target_type)
    lcond, largs = "", []
    if cursor is not None:  # kargs = [type, domain, id]: targets from the cursor's one on; inside that target only links after the cursor's id
        tcond.append("(target_type, domain) >= (?, ?)")
        targs += [kargs[0], kargs[1]]
        lcond, largs = " AND (t.domain<>? OR l.id>?)", [kargs[1], kargs[2]]
    where = (" WHERE " + " AND ".join(tcond)) if tcond else ""
    order = "domain" if target_type else "target_type, domain"  # with a type filter target_type is constant: leaving it out lets the planner see that the outer order is already the final one
    sql = (f"SELECT {cols.replace('l.', 'x.')}, t.target_type, (SELECT crawl_status FROM discovered_urls u WHERE u.url=x.source_url) AS src_status FROM"
           f" (SELECT domain, target_type FROM businesses{where} ORDER BY {order}) t"
           f" CROSS JOIN LATERAL (SELECT {cols} FROM outgoing_links l WHERE l.destination_domain=t.domain AND l.link_type='html' AND l.source_domain<>t.domain"
           f"{active}{lcond} ORDER BY l.id LIMIT ?) x ORDER BY {'t.' + order.replace(', ', ', t.')}, x.id LIMIT ? OFFSET ?")
    return sql, [*targs, *largs, offset + n, n, offset]


@router.get("/backlinks")
def all_backlinks(target_type: str | None = Query(None, pattern="^(own_business|competitor)$"), include_removed: bool = False,
                  limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, cursor: Cursor = None) -> dict:
    """Backlinks of EVERY monitored target, our businesses and all competitors together (filter with target_type), straight from the link graph.
    Ordered by (target_type, target domain, link id), a unique key: with `cursor` a deep page costs the same as the first one, `offset` still works."""
    active = "" if include_removed else " AND l.is_active=1"
    args = [target_type] if target_type else []
    kcond, kargs = _all_cursor(cursor, offset)
    cols = ", ".join("l." + x for x in ("id", "destination_domain", "source_url", "destination_url", "anchor_text", "rel_attributes", "source_domain",
                                        "first_seen", "last_seen", "is_active", "lost_at"))
    page_sql, page_args = _all_page_sql(cols, active, target_type, cursor, kargs, limit + 1, offset)

    def per_target(c) -> list:
        """backlinks and distinct referring domains of every target: a hash aggregate over (target, source domain) pairs (COUNT(DISTINCT) sorted every link on disk)."""
        return [dict(r) for r in c.execute(
            "SELECT b.domain, b.target_type, COALESCE(g.n, 0) AS backlinks, COALESCE(g.r, 0) AS referring_domains FROM businesses b LEFT JOIN ("
            "SELECT d, SUM(n)::bigint AS n, COUNT(*) AS r FROM (SELECT l.destination_domain AS d, l.source_domain AS s, COUNT(*) AS n FROM outgoing_links l"
            " JOIN businesses b2 ON b2.domain=l.destination_domain WHERE l.link_type='html' AND l.source_domain<>b2.domain" + active
            + (" AND b2.target_type=?" if target_type else "") + " GROUP BY 1, 2) x GROUP BY d) g ON g.d=b.domain"
            + (" WHERE b.target_type=?" if target_type else "") + " ORDER BY b.target_type, b.domain", args + args).fetchall()]

    with connect() as c:
        per = per_target(c)  # live, like the page (the read cache covers only the per-target summary, referring-domain and anchor reads)
        rows = c.execute(page_sql, page_args).fetchall()
    more, rows = len(rows) > limit, rows[:limit]
    return {"total_backlinks": sum(r["backlinks"] for r in per), "limit": limit, "offset": offset, "has_more": more,
            "next_offset": offset + limit if more else None,
            "next_cursor": f"{rows[-1]['target_type']}:{rows[-1]['destination_domain']}:{rows[-1]['id']}" if more else None,
            "targets": per,
            "backlinks": [{"target_domain": r["destination_domain"], "target_type": r["target_type"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                           "anchor_text": r["anchor_text"], "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": link_status(r),
                           "first_seen": r["first_seen"], "last_verified": r["last_seen"], "is_active": bool(r["is_active"])} for r in rows]}


@router.get("/backlinks/{target_domain}")
def backlinks(target_domain: str, include_removed: bool = False, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, after_id: AfterId = None) -> dict:
    """Backlinks for ANY domain, read from our own link graph; no network call here. A domain with nothing yet is queued for the
    engine's discovery for a business is started by POST /discovery/onboard, never by this GET."""
    target = _clean(target_domain)
    reg = core.registrable_domain(target)
    _keyset(after_id, offset)  # 422 when both are given
    with connect() as c:
        # totals come from SQL aggregates, never from loading the graph; with GRAPH_CACHE_ENABLED they may be up to the cache TTL old, the page and the job state are always live
        total, active, refs = _cached("summary", target, (not include_removed,), lambda: _graph_summary(c, target, not include_removed))
        rows = _graph_page(c, target, not include_removed, limit + 1, offset, after_id) if total else []  # one extra row tells whether another page exists
        more, rows = len(rows) > limit, rows[:limit]
        job = business_jobs.latest(c, reg)
    state = business_jobs.api_state(job)
    if total == 0:
        return {"domain": target, "target_domain": target, "total_backlinks": 0, "referring_domains": 0, "total_referring_domains": 0,
                "status": state, "job_id": job and job["id"], "backlinks": [],
                "message": "Not onboarded: POST /discovery/onboard to start discovery." if not job else
                f"No backlink found yet ({job['pages_crawled']} page(s) crawled" + (f"; last error: {job['last_error']}" if job["last_error"] else "") + ")."}
    return {
        "domain": target, "target_domain": target, "status": "ok", "discovery_status": state, "total_backlinks": total, "verified_backlinks": active,
        "referring_domains": refs, "total_referring_domains": refs, "limit": limit, "offset": offset, "has_more": more,
        "next_offset": offset + limit if more and after_id is None else None,
        "next_after_id": rows[-1]["id"] if more else None,  # pass as after_id for the next page; null = last page
        "backlinks": [{
            "source_url": r["source_url"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"],
            "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": link_status(r),
            "verification_status": link_status(r), "discovery_method": "forward_crawl",
            "first_seen": r["first_seen"], "last_verified": r["last_seen"], "is_active": bool(r["is_active"]),
        } for r in rows],
    }


def _group_cursor(cursor: str | None, offset: int) -> tuple[int, int] | None:
    """`cursor` of the grouped routes = '<count>:<first id>' of the last group of the previous page (its next_cursor): the groups' sort key
    (size DESC, lowest link id ASC) is unique, because every link belongs to exactly one group."""
    if cursor is None:
        return None
    if offset:
        raise HTTPException(422, "use either cursor or offset, not both")
    n, _, mid = cursor.partition(":")
    if not (n.isdigit() and mid.isdigit()):
        raise HTTPException(422, "cursor must be the next_cursor of a previous page")
    return int(n), int(mid)


def _grouped_page(c, select: str, cond: str, args: list, group_by: str, limit: int, offset: int, after: tuple[int, int] | None = None) -> tuple[list, int]:
    """One page of a GROUP BY over the link graph, ordered by size then first appearance (lowest id), and the TOTAL number of groups. Up to limit+1 rows
    come back (the extra one tells the caller whether a page follows); every row carries _n and _mid, the key of the keyset cursor. The total comes
    from a window count in the same statement (it counts groups, not rows); with a cursor the groups are aggregated once in a CTE so the total still
    counts all of them. Only a page past the end needs a second query."""
    if after is None:
        rows = c.execute(f"SELECT {select}, COUNT(*) AS _n, MIN(id) AS _mid, COUNT(*) OVER () AS n_groups FROM outgoing_links" + cond +
                         f" GROUP BY {group_by} ORDER BY COUNT(*) DESC, MIN(id) LIMIT ? OFFSET ?", (*args, limit + 1, offset)).fetchall()
    else:
        rows = c.execute(f"WITH g AS MATERIALIZED (SELECT {select}, COUNT(*) AS _n, MIN(id) AS _mid FROM outgoing_links" + cond + f" GROUP BY {group_by})"
                         " SELECT g.*, (SELECT COUNT(*) FROM g) AS n_groups FROM g WHERE _n < ? OR (_n = ? AND _mid > ?) ORDER BY _n DESC, _mid LIMIT ?",
                         (*args, after[0], after[0], after[1], limit + 1)).fetchall()
    if rows or not (offset or after):
        return rows, rows[0]["n_groups"] if rows else 0
    return rows, c.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM outgoing_links" + cond + f" GROUP BY {group_by}) g", args).fetchone()[0]


def _group_next(rows: list, limit: int, offset: int) -> dict:
    """has_more / next_offset / next_cursor of a grouped page that was fetched with limit+1 rows (rows is the full fetch)."""
    more = len(rows) > limit
    return {"has_more": more, "next_offset": offset + limit if more else None, "next_cursor": f"{rows[limit - 1]['_n']}:{rows[limit - 1]['_mid']}" if more else None}



@router.get("/backlinks/{target_domain}/referring-domains")
def referring_domains(target_domain: str, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, cursor: Cursor = None) -> dict:
    target = _clean(target_domain)
    after = _group_cursor(cursor, offset)
    cond, args = _graph_cond(target)
    def compute(c):  # grouped in PostgreSQL; COLLATE "C" = Python's code-point min/max; ties keep first-appearance (lowest id) order like the old stable sort
        rows, total = _grouped_page(c, 'source_domain AS domain, COUNT(*) AS backlinks, MIN(first_seen COLLATE "C") AS first_seen,'
                                    ' MAX(last_seen COLLATE "C") AS last_verified', cond, args, "source_domain", limit, offset, after)
        return [{"domain": r["domain"], "backlinks": r["backlinks"], "first_seen": r["first_seen"], "last_verified": r["last_verified"]} for r in rows[:limit]], total, _group_next(rows, limit, offset)

    with connect() as c:
        items, total, nxt = _cached("referring_domains", target, (limit, offset, after), lambda: compute(c))
    return {"domain": target, "status": "ok" if total else "not_found_in_current_index", "total_referring_domains": total, "limit": limit, "offset": offset,
            **nxt, "referring_domains": items}


@router.get("/backlinks/{target_domain}/anchors")
def backlink_anchors(target_domain: str, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, cursor: Cursor = None) -> dict:
    """Anchor-text distribution of the active backlinks, most used first (paged: total_anchors is the number of distinct anchors)."""
    target = _clean(target_domain)
    after = _group_cursor(cursor, offset)
    cond, args = _graph_cond(target)
    def compute(c):
        rows, total = _grouped_page(c, "COALESCE(anchor_text,'') AS anchor_text, COUNT(*) AS n", cond, args, "COALESCE(anchor_text,'')", limit, offset, after)
        return [{"anchor_text": r["anchor_text"], "count": r["n"]} for r in rows[:limit]], total, _group_next(rows, limit, offset)

    with connect() as c:
        anchors, total, nxt = _cached("anchors", target, (limit, offset, after), lambda: compute(c))
    return {"target_domain": target, "domain": target, "total_anchors": total, "limit": limit, "offset": offset, **nxt, "anchors": anchors}


@router.get("/backlinks/{target_domain}/history")
def backlink_history(target_domain: str, limit: int = Query(200, ge=1, le=1000)) -> dict:
    """Every state change, newest first: `new` (first_seen), then the append-only link_history rows (suspected_removed, lost,
    reappeared; repeated removals and returns all stay), plus backlink_history rows for registered targets (`origin`=backlink).
    Works for domains nobody registered.
    Only the newest `limit` events can be in the answer, and each of the four sources is already ordered, so every source is cut to its own
    newest `limit` (same tie order as the old full sort) instead of loading the whole history into Python."""
    target = _clean(target_domain)
    reg = core.registrable_domain(target)
    cond, args = _graph_cond(target, False)

    verdicts: dict = {}

    def on_target(url: str) -> bool:
        """== core.host_matches(core.hostname(url), target), decided once per distinct host: a plain lowercase ASCII host (the common case) is its
        own hostname() (nothing for urlsplit/idna to change), so the verdict is cached by host; any other URL takes the original path."""
        m = _PLAIN_HOST.match(url)
        if not m:
            return core.host_matches(core.hostname(url), target)
        host = m.group(1).rstrip(".")
        if host not in verdicts:
            verdicts[host] = core.host_matches(host, target)
        return verdicts[host]

    window: list = []  # [" AND id>=?", [id]] once known; [] -> not read yet

    def in_window(c) -> tuple[str, list]:
        """The old loop read only the newest HISTORY_WINDOW link_history rows (by id) of the registrable domain. That window cuts anything off only
        when the domain has more rows than that; then it is `id >= the HISTORY_WINDOW-th largest id`."""
        if not window:
            r = c.execute("SELECT id FROM link_history WHERE destination_domain=? ORDER BY id DESC OFFSET ? LIMIT 1", (reg, HISTORY_WINDOW - 1)).fetchone()
            window.extend((" AND id>=?", [r["id"]]) if r else ("", []))
        return window[0], window[1]

    def logged(c) -> list:
        """The newest `limit` link_history events on the target, exactly the ones the old loop kept: it took the window above, kept the rows whose
        destination is on the target and whose source is not (on_target, decided in Python as before), and cut the newest `limit` by detected_at (ties: the
        higher id first). PostgreSQL now hands the window over in that order, newest first, a batch at a time; the first `limit` rows on_target accepts are
        the answer, and the scan stops there. Batches grow x4, so a domain whose rows are mostly rejected still ends after reading each row once or twice."""
        win, wargs = in_window(c)
        sql = ("SELECT id, source_url, destination_url, anchor_text, event_type, detected_at FROM link_history WHERE destination_domain=?" + win +
               ' ORDER BY detected_at COLLATE "C" DESC, id DESC LIMIT ?')  # COLLATE "C" = Python's code-point order
        batch = max(2 * limit, 256)
        while True:
            rows = c.execute(sql, (reg, *wargs, batch)).fetchall()
            kept = [r for r in rows if on_target(r["destination_url"]) and not on_target(r["source_url"])]
            if len(kept) >= limit or len(rows) < batch:
                return [{"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                         "anchor_text": r["anchor_text"], "origin": "graph"} for r in kept[:limit]]
            batch *= 4

    def removed_without_history(c) -> list:
        """Rows removed before link_history existed: their single lost_at is all we know. A removed link is skipped when link_history already holds a 'lost'
        event for the same (source_url, destination_url, anchor_text) inside the window and on the target (what the old code kept in lost_seen). Candidates
        come newest first in batches; only THEIR lost events are looked up, so a domain with thousands of lost events ships none of them."""
        out, offset, batch = [], 0, limit
        while True:
            cands = c.execute("SELECT source_url, destination_url, anchor_text, lost_at FROM outgoing_links" + cond +
                              " AND COALESCE(lost_at,'')<>'' AND COALESCE(is_active,0)=0 ORDER BY lost_at COLLATE \"C\" DESC, id LIMIT ? OFFSET ?", (*args, batch, offset)).fetchall()
            if cands:
                win, wargs = in_window(c)
                known = {(r["source_url"], r["destination_url"], r["anchor_text"]) for r in c.execute(
                    "SELECT source_url, destination_url, anchor_text FROM link_history WHERE destination_domain=?" + win + " AND event_type='lost'"
                    " AND (source_url, destination_url, anchor_text) IN (SELECT * FROM unnest(?::text[], ?::text[], ?::text[]))",
                    (reg, *wargs, [r["source_url"] for r in cands], [r["destination_url"] for r in cands], [r["anchor_text"] for r in cands]))
                    if on_target(r["destination_url"]) and not on_target(r["source_url"])}
                out += [{"event": "lost", "at": r["lost_at"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"}
                        for r in cands if (r["source_url"], r["destination_url"], r["anchor_text"]) not in known]
            offset += len(cands)
            if len(out) >= limit or len(cands) < batch:
                return out[:limit]
            batch *= 4

    with connect() as c:
        new = [{"event": "new", "at": r["first_seen"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"}
               for r in c.execute('SELECT source_url, destination_url, first_seen FROM outgoing_links' + cond + ' ORDER BY first_seen COLLATE "C" DESC, id LIMIT ?', (*args, limit))]
        logged_events = logged(c)
        fallback = removed_without_history(c)
        registered = [{"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                       "details": r["details"], "origin": "backlink"}
                      for r in c.execute("SELECT event_type, detected_at, details, source_url, destination_url FROM (SELECT h.id, h.event_type, h.detected_at, h.details,"
                                         " b.source_url, b.destination_url FROM backlink_history h JOIN backlinks b ON b.id=h.backlink_id"
                                         " JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=? ORDER BY h.id DESC LIMIT 100000) x"
                                         ' ORDER BY detected_at COLLATE "C" DESC, id DESC LIMIT ?', (target, limit))]
    ev = new + logged_events + fallback + registered  # the old append order, so equal timestamps tie-break identically
    ev.sort(key=lambda e: e["at"], reverse=True)
    return {"domain": target, "events": ev[:limit]}


class OnboardRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"domain": "codigix.co"}]})
    domain: str = Field(min_length=3, max_length=2048, description="The business's domain or website URL, e.g. codigix.co or https://www.codigix.co/")


@router.post("/discovery/onboard", status_code=202)
def discovery_onboard(body: OnboardRequest) -> dict:
    """Register a business and queue its FIRST discovery job (P0); returns at once (the engine searches and crawls). Calling it again never
    starts another scan: it returns the active or latest job (created=false) and the scheduler runs the business every interval
    (next_run_at). To show progress use GET /discovery/business/{domain}/status; to scan again now, POST .../rescan."""
    domain = _clean(body.domain)
    with connect() as c:
        job, created = business_jobs.create_job(c, domain)
        nxt = c.execute("SELECT next_run_at FROM businesses WHERE domain=?", (job["target_domain"],)).fetchone()[0]
    return {"domain": job["target_domain"], "job_id": job["id"], "status": "queued" if created else business_jobs.api_state(job),
            "priority": f"P{job['priority']}", "created": created, "next_run_at": nxt}


@router.post("/discovery/business/{domain}/rescan", status_code=202)
def discovery_rescan(domain: str) -> dict:
    """Deliberate manual rescan of a registered business or competitor: one new job now, unless one is already active (then that one).
    The only way besides the scheduler to search a target again; ordinary onboarding calls never do."""
    with connect() as c:
        reg = core.registrable_domain(_clean(domain))
        b = c.execute("SELECT target_type, active FROM businesses WHERE domain=?", (reg,)).fetchone()
        if not b:
            raise HTTPException(404, "not onboarded")
        if not b["active"]:
            raise HTTPException(409, "not monitored (inactive): register it again to scan it")
        job, created = business_jobs.create_job(c, reg, target_type=b["target_type"], force=True)
    return {"domain": reg, "job_id": job["id"], "status": "queued" if created else business_jobs.api_state(job),
            "priority": f"P{job['priority']}", "created": created}


@router.post("/businesses", status_code=202)
def onboard_business(body: OnboardRequest) -> dict:
    """Older form of /discovery/onboard (returns the whole job)."""
    with connect() as c:
        job, created = business_jobs.create_job(c, _clean(body.domain))
    return {"created": created, "job": job}


@router.get("/discovery/business/{domain}/status")
def discovery_business_status(domain: str) -> dict:
    with connect() as c:
        reg = core.registrable_domain(_clean(domain))
        job, sched = business_jobs.latest(c, reg), c.execute("SELECT * FROM businesses WHERE domain=?", (reg,)).fetchone()
        disc = business_jobs.discovery_status(c, reg)
    if job is None:
        raise HTTPException(404, "not onboarded")
    return {"domain": reg, "status": business_jobs.api_state(job), "job": job, "schedule": dict(sched) if sched else None, "discovery": disc}


@router.get("/discovery/business/{domain}/runs")
def discovery_business_runs(domain: str, limit: int = Query(50, ge=1, le=500)) -> dict:
    with connect() as c:
        return {"runs": _page(c, "SELECT * FROM business_jobs WHERE target_domain=? ORDER BY id DESC", [core.registrable_domain(_clean(domain))], limit, 0)}


@router.get("/businesses")
def list_businesses(status: str | None = None, limit: int = Query(100, ge=1, le=1000)) -> dict:
    with connect() as c:
        where, args = (" WHERE status=?", [status]) if status else ("", [])
        return {"items": _page(c, "SELECT * FROM business_jobs" + where + " ORDER BY id DESC", args, limit, 0)}


@router.get("/businesses/{domain}")
def business_status(domain: str) -> dict:
    """Progress of this business's latest discovery job (counters are refreshed by the engine every few seconds)."""
    with connect() as c:
        job = business_jobs.latest(c, _clean(domain))
    if job is None:
        raise HTTPException(404, "not onboarded")
    return job


# ---------- competitors: registered per business; backlinks come from the same shared graph as everyone else's ----------

class CompetitorsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"domains": ["competitor-a.com"]}]})
    domains: list[str] = Field(min_length=1, max_length=20, description="Competitor domains or URLs")


def _owner(business_domain: str) -> str:
    return core.registrable_domain(_clean(business_domain))


@router.post("/businesses/{business_domain}/competitors", status_code=202)
def add_business_competitors(business_domain: str, body: CompetitorsRequest) -> dict:
    """Register competitors of ONE business (onboards it first if needed); only that business's gap includes them."""
    owner, out = _owner(business_domain), []
    with connect() as c:
        if not competitors.own_business(c, owner):
            business_jobs.create_job(c, owner)
        for d in body.domains:
            try:
                out.append(competitors.register(c, owner, d))
            except ValueError as e:
                out.append({"domain": d, "status": "rejected", "error": str(e)})
    graph_cache.clear()  # the competitor set is part of every cached gap
    return {"business": owner, "results": out}


@router.get("/businesses/{business_domain}/competitors")
def list_business_competitors(business_domain: str, limit: Annotated[int | None, Query(ge=1, le=1000, description="page size; omitted = all competitors")] = None,
                              offset: Offset = 0,
                              counts: Annotated[bool, Query(description="false skips the known-backlink count (the slow part): each competitor's `known_backlinks` is then null")] = True) -> dict:
    with connect() as c:
        return {"business": _owner(business_domain), "competitors": competitors.listing(c, _owner(business_domain), limit, offset, counts)}


@router.delete("/businesses/{business_domain}/competitors/{competitor_domain}")
def remove_business_competitor(business_domain: str, competitor_domain: str) -> dict:
    with connect() as c:
        if not competitors.remove(c, _owner(business_domain), _clean(competitor_domain)):
            raise HTTPException(404, "not a competitor of this business")
    graph_cache.clear()  # the competitor set is part of every cached gap
    return {"removed": True, "data_kept": True}


@router.get("/businesses/{business_domain}/backlink-gap")
def backlink_gap(business_domain: str, competitor: str | None = None, limit: Annotated[int, Query(ge=1, le=2000)] = 200, offset: Offset = 0,
                 after_opportunity: Annotated[str | None, Query(description="keyset paging of `opportunities`: pagination.lists.opportunities.next_after of the previous page")] = None,
                 after_shared: Annotated[str | None, Query(description="keyset paging of `shared_domains` (next_after of the previous page)")] = None,
                 after_own_only: Annotated[str | None, Query(description="keyset paging of `own_only_domains` (next_after of the previous page)")] = None,
                 after_page: Annotated[str | None, Query(description="keyset paging of `source_page_gap` (next_after of the previous page)")] = None,
                 refresh: Annotated[bool, Query(description="bypass the read cache (GRAPH_CACHE_ENABLED) and recompute; the fresh result replaces the cached one")] = False) -> dict:
    """Referring domains and pages that link the competitors but not this business, over the discovered index (not the whole internet). Every list is
    cut in PostgreSQL to `limit` entries; `summary` and `pagination.lists.*.total` always describe the whole comparison. Page with `offset` or with the
    per-list `after_*` cursors (not both); with cursors each list advances on its own, so stop asking for a list once its has_more is false."""
    after = {k: v for k, v in (("opportunities", after_opportunity), ("shared_domains", after_shared), ("own_only_domains", after_own_only),
                               ("source_page_gap", after_page)) if v is not None}
    if after and offset:
        raise HTTPException(422, "use either the after_* cursors or offset, not both")
    owner, comp = _owner(business_domain), _clean(competitor) if competitor else None

    def compute():
        with connect() as c:
            return competitors.gap(c, owner, comp, limit, offset, after)
    try:
        if refresh:
            graph_cache.drop(("gap", owner, comp, limit, offset, tuple(sorted(after.items()))))  # a Refresh must never be answered from (or re-stamp) an old entry
        return _cached("gap", owner, (comp, limit, offset, tuple(sorted(after.items()))), compute)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e


def _comp(domain: str) -> str:
    d = _clean(domain)
    with connect() as c:
        if not competitors.is_competitor(c, d):
            raise HTTPException(404, "not a registered competitor")
    return d


@router.get("/competitors/{competitor_domain}/backlinks")
def competitor_backlinks(competitor_domain: str, include_removed: bool = False, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, after_id: AfterId = None) -> dict:
    return {**backlinks(_comp(competitor_domain), include_removed, limit, offset, after_id), "target_type": "competitor"}


@router.get("/competitors/{competitor_domain}/referring-domains")
def competitor_referring_domains(competitor_domain: str, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, cursor: Cursor = None) -> dict:
    return referring_domains(_comp(competitor_domain), limit, offset, cursor)


@router.get("/competitors/{competitor_domain}/anchors")
def competitor_anchors(competitor_domain: str, limit: Limit = DEFAULT_LIMIT, offset: Offset = 0, cursor: Cursor = None) -> dict:
    return backlink_anchors(_comp(competitor_domain), limit, offset, cursor)


@router.get("/competitors/{competitor_domain}/history")
def competitor_history(competitor_domain: str, limit: int = Query(200, ge=1, le=1000)) -> dict:
    return backlink_history(_comp(competitor_domain), limit)


@router.get("/competitors/{competitor_domain}/discovery/status")
def competitor_discovery_status(competitor_domain: str) -> dict:
    return discovery_business_status(_comp(competitor_domain))

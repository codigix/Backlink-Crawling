"""Read-only FastAPI router over the link graph (backlink search + inspection). Mounted by backlink_api.py.
Crawling is done only by the standalone autonomous.py process."""

import re

from fastapi import APIRouter, HTTPException, Query

import crawler_core as core

router = APIRouter()


def link_status(r) -> str:
    """One definition of a link's state, shared by every endpoint (matches the backlinks table's states)."""
    if not r["is_active"]:
        return "removed"  # two consecutive successful fetches without the link
    if r["lost_at"]:
        return "suspected_removed"  # one miss so far
    return "fetch_failed" if r["src_status"] in ("failed", "retry_pending") else "verified"


def _page(conn, sql: str, args: list, limit: int, offset: int) -> list[dict]:
    return [dict(r) for r in conn.execute(sql + " LIMIT ? OFFSET ?", (*args, limit, offset))]


@router.get("/crawler/discovered-urls")
def discovered_urls(domain: str | None = None, is_external: bool | None = None, limit: int = Query(100, ge=1, le=1000)) -> dict:
    where, args = [], []
    for col, v in (("domain", domain), ("is_external", None if is_external is None else int(is_external))):
        if v is not None:
            where.append(f"{col}=?")
            args.append(v)
    clause = " WHERE " + " AND ".join(where) if where else ""
    with core.connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM discovered_urls" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, "SELECT * FROM discovered_urls" + clause + " ORDER BY id", args, limit, 0)}


@router.get("/crawler/discovered-domains")
def discovered_domains(discovery_status: str | None = None, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    clause, args = (" WHERE discovery_status=?", [discovery_status]) if discovery_status else ("", [])
    with core.connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM discovered_domains" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, "SELECT * FROM discovered_domains" + clause + " ORDER BY id", args, limit, offset)}


@router.get("/crawler/outgoing-links")
def outgoing_links(source_url: str | None = None, destination_domain: str | None = None, active_only: bool = True,
                         limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    where, args = [], []
    for col, v in (("source_url", source_url), ("destination_domain", destination_domain)):
        if v:
            where.append(f"{col}=?")
            args.append(v)
    if active_only:
        where.append("is_active=1")
    clause = " WHERE " + " AND ".join(where) if where else ""
    with core.connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM outgoing_links" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, "SELECT * FROM outgoing_links" + clause + " ORDER BY id", args, limit, offset)}


def _clean(domain: str) -> str:
    try:
        return core.clean_target(domain)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e


def _host_re(host: str) -> str:
    """Postgres regex matching URLs whose host is `host` or one of its subdomains (never a longer name like evilhost)."""
    return r"^https?://(?:[^/?#@]*\.)?" + re.escape(host) + r"(?::[0-9]+)?(?:[/?#]|$)"


def _graph(c, target: str, active_only: bool = True, limit: int | None = None, offset: int = 0) -> tuple[int, list]:
    """(total, rows) of html links into `target` straight from the global outgoing_links graph: no crawl, no network, no
    registration needed. Both registrable-domain and subdomain targets are filtered and paged in SQL, so totals are exact."""
    reg = core.registrable_domain(target)
    cond = " WHERE destination_domain=? AND link_type='html' AND source_domain!=?" + (" AND is_active=1" if active_only else "")
    args: list = [reg, reg]
    if target != reg:  # subdomain: the destination host must match, and links from the target's own host do not count
        cond = cond.replace("source_domain!=?", "TRUE") + " AND destination_url ~* ? AND source_url !~* ?"
        args = [reg, _host_re(target), _host_re(target)]
    total = c.execute("SELECT COUNT(*) FROM outgoing_links" + cond, args).fetchone()[0]
    rows = c.execute("SELECT *, (SELECT crawl_status FROM discovered_urls u WHERE u.url=outgoing_links.source_url) AS src_status FROM outgoing_links"
                     + cond + " ORDER BY id" + (" LIMIT ? OFFSET ?" if limit else ""), (*args, limit, offset) if limit else args).fetchall()
    return total, rows


@router.get("/backlinks/{target_domain}")
def backlinks(target_domain: str, include_removed: bool = False, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    """Backlinks for ANY domain, read from our own link graph; no network call here. A domain with nothing yet is queued for the
    engine, which looks it up in Common Crawl and crawls the linking sites: call again later (status 'discovering')."""
    target = _clean(target_domain)
    reg = core.registrable_domain(target)
    with core.connect() as c:
        total, rows = _graph(c, target, not include_removed, limit, offset)
        every = _graph(c, target, not include_removed)[1]
        active = sum(link_status(r) == "verified" for r in every)
        refs = len({r["source_domain"] for r in every})
        state = None
        if total == 0:
            done = c.execute("SELECT value FROM engine_state WHERE key=?", (f"cc_done:{reg}",)).fetchone()
            if done is None:  # never looked up yet (or lookup pending): make sure the engine has the request
                c.execute("INSERT INTO engine_state (key, value) VALUES (?, '1') ON CONFLICT (key) DO NOTHING", (f"cc_lookup:{reg}",))
            state = done[0] if done else None
    if total == 0:
        crawling = state is None or state.startswith("ok:")  # lookup pending, or sources seeded and the crawler is on them
        return {"domain": target, "target_domain": target, "total_backlinks": 0, "referring_domains": 0, "total_referring_domains": 0,
                "status": "discovering" if crawling else "not_found_in_current_index",
                "message": "Discovery started: Common Crawl lookup and crawl in progress, check again shortly." if crawling
                else f"No backlinks found ({state}).", "backlinks": []}
    return {
        "domain": target, "target_domain": target, "status": "ok", "total_backlinks": total, "verified_backlinks": active,
        "referring_domains": refs, "total_referring_domains": refs, "limit": limit, "offset": offset,
        "backlinks": [{
            "source_url": r["source_url"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"],
            "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": link_status(r),
            "verification_status": link_status(r), "discovery_method": "forward_crawl",
            "first_seen": r["first_seen"], "last_verified": r["last_seen"], "is_active": bool(r["is_active"]),
        } for r in rows],
    }


@router.get("/backlinks/{target_domain}/referring-domains")
def referring_domains(target_domain: str) -> dict:
    target = _clean(target_domain)
    agg: dict[str, dict] = {}
    with core.connect() as c:
        for r in _graph(c, target)[1]:  # shortcut: aggregated in Python, fine to ~100k links per target
            d = agg.setdefault(r["source_domain"], {"domain": r["source_domain"], "backlinks": 0, "first_seen": r["first_seen"], "last_verified": r["last_seen"]})
            d["backlinks"] += 1
            d["first_seen"], d["last_verified"] = min(d["first_seen"], r["first_seen"]), max(d["last_verified"], r["last_seen"])
    items = sorted(agg.values(), key=lambda d: -d["backlinks"])
    return {"domain": target, "status": "ok" if items else "not_found_in_current_index", "total_referring_domains": len(items), "referring_domains": items}


@router.get("/backlinks/{target_domain}/anchors")
def backlink_anchors(target_domain: str) -> dict:
    """Anchor-text distribution of the active backlinks."""
    target = _clean(target_domain)
    counts: dict[str, int] = {}
    with core.connect() as c:
        for r in _graph(c, target)[1]:
            counts[r["anchor_text"] or ""] = counts.get(r["anchor_text"] or "", 0) + 1
    return {"target_domain": target, "domain": target, "anchors": [{"anchor_text": a, "count": n} for a, n in sorted(counts.items(), key=lambda x: -x[1])]}


@router.get("/backlinks/{target_domain}/history")
def backlink_history(target_domain: str, limit: int = Query(200, ge=1, le=1000)) -> dict:
    """Every state change, newest first: `new` (first_seen), then the append-only link_history rows (suspected_removed, lost,
    reappeared; repeated removals and returns all stay), plus backlink_history rows for registered targets (`origin`=backlink).
    Works for domains nobody registered."""
    target = _clean(target_domain)
    reg = core.registrable_domain(target)
    ev, lost_seen = [], set()
    with core.connect() as c:
        for r in _graph(c, target, False)[1]:
            ev.append({"event": "new", "at": r["first_seen"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"})
        for r in c.execute("SELECT * FROM link_history WHERE destination_domain=? ORDER BY id DESC LIMIT 100000", (reg,)):
            if core.host_matches(core.hostname(r["destination_url"]), target) and not core.host_matches(core.hostname(r["source_url"]), target):
                ev.append({"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                           "anchor_text": r["anchor_text"], "origin": "graph"})
                if r["event_type"] == "lost":
                    lost_seen.add((r["source_url"], r["destination_url"], r["anchor_text"]))
        for r in _graph(c, target, False)[1]:  # rows removed before link_history existed: their single lost_at is all we know
            if r["lost_at"] and not r["is_active"] and (r["source_url"], r["destination_url"], r["anchor_text"]) not in lost_seen:
                ev.append({"event": "lost", "at": r["lost_at"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"})
        for r in c.execute("SELECT h.event_type, h.detected_at, h.details, b.source_url, b.destination_url FROM backlink_history h"
                           " JOIN backlinks b ON b.id=h.backlink_id JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=?"
                           " ORDER BY h.id DESC LIMIT 100000", (target,)):
            ev.append({"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                       "details": r["details"], "origin": "backlink"})
    ev.sort(key=lambda e: e["at"], reverse=True)
    return {"domain": target, "events": ev[:limit]}

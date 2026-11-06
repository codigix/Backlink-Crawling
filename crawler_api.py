"""Read-only FastAPI router over the link graph (backlink search + inspection). Mounted by backlink_api.py.
Crawling is done only by the standalone autonomous.py process."""

from fastapi import APIRouter, HTTPException, Query

import crawler_core as core

router = APIRouter()


def _page(conn, sql: str, args: list, limit: int, offset: int) -> list[dict]:
    return [dict(r) for r in conn.execute(sql + " LIMIT ? OFFSET ?", (*args, limit, offset))]


@router.get("/crawler/discovered-urls")
def discovered_urls(domain: str | None = None, is_external: bool | None = None, limit: int = Query(100, le=1000)) -> dict:
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
def discovered_domains(discovery_status: str | None = None, limit: int = Query(100, le=1000), offset: int = 0) -> dict:
    clause, args = (" WHERE discovery_status=?", [discovery_status]) if discovery_status else ("", [])
    with core.connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM discovered_domains" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, "SELECT * FROM discovered_domains" + clause + " ORDER BY id", args, limit, offset)}


@router.get("/crawler/outgoing-links")
def outgoing_links(source_url: str | None = None, destination_domain: str | None = None, active_only: bool = True,
                         limit: int = Query(100, le=1000), offset: int = 0) -> dict:
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


def _graph(c, target: str, active_only: bool = True, limit: int | None = None, offset: int = 0) -> tuple[int, list]:
    """(total, rows) of html links into `target` straight from the global outgoing_links graph: no crawl, no network, no
    registration needed. Registrable-domain targets page in SQL; subdomain targets are host-filtered in Python (capped)."""
    reg = core.registrable_domain(target)
    where = " FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND source_domain!=?" + (" AND is_active=1" if active_only else "")
    if target == reg:
        total = c.execute("SELECT COUNT(*)" + where, (reg, reg)).fetchone()[0]
        return total, c.execute("SELECT *" + where + " ORDER BY id LIMIT ? OFFSET ?", (reg, reg, limit or 100_000, offset)).fetchall()
    rows = [r for r in c.execute("SELECT *" + where + " ORDER BY id LIMIT 100000", (reg, ""))  # shortcut: 100k cap for subdomain targets
            if core.host_matches(core.hostname(r["destination_url"]), target) and not core.host_matches(core.hostname(r["source_url"]), target)]
    return len(rows), rows[offset:offset + limit] if limit else rows


@router.get("/backlinks/{target_domain}")
def backlinks(target_domain: str, include_removed: bool = False, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    """Backlinks for ANY domain, read from our own link graph. Never starts a crawl or calls an external service."""
    target = _clean(target_domain)
    with core.connect() as c:
        total, rows = _graph(c, target, not include_removed, limit, offset)
        active = total if not include_removed else _graph(c, target)[0]
        refs = len({r["source_domain"] for r in _graph(c, target, not include_removed)[1]})
    if total == 0:
        return {"domain": target, "target_domain": target, "total_backlinks": 0, "referring_domains": 0, "total_referring_domains": 0,
                "status": "not_found_in_current_index", "message": "No discovered backlinks are currently available.", "backlinks": []}
    return {
        "domain": target, "target_domain": target, "status": "ok", "total_backlinks": total, "verified_backlinks": active,
        "referring_domains": refs, "total_referring_domains": refs, "limit": limit, "offset": offset,
        "backlinks": [{
            "source_url": r["source_url"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"],
            "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": "verified" if r["is_active"] else "removed",
            "verification_status": "verified" if r["is_active"] else "removed", "discovery_method": "forward_crawl",
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
def backlink_history(target_domain: str, limit: int = Query(200, le=1000)) -> dict:
    """New and lost backlinks, newest first, from the graph itself (so it works for domains nobody registered)."""
    target = _clean(target_domain)
    ev = []
    with core.connect() as c:
        for r in _graph(c, target, False)[1]:
            ev.append({"event": "new", "at": r["first_seen"], "source_url": r["source_url"], "destination_url": r["destination_url"]})
            if r["lost_at"]:
                ev.append({"event": "lost", "at": r["lost_at"], "source_url": r["source_url"], "destination_url": r["destination_url"]})
    ev.sort(key=lambda e: e["at"], reverse=True)
    return {"domain": target, "events": ev[:limit]}



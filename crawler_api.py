"""Read-only FastAPI router over the link graph (backlink search + inspection). Mounted by backlink_api.py.
Crawling is done only by the standalone autonomous.py process."""

import re

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

import business_jobs
import competitors
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


@router.get("/backlinks")
def all_backlinks(target_type: str | None = Query(None, pattern="^(own_business|competitor)$"), include_removed: bool = False,
                  limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    """Backlinks of EVERY monitored target, our businesses and all competitors together (filter with target_type), straight from the link graph."""
    cond = " FROM outgoing_links l JOIN businesses b ON b.domain=l.destination_domain WHERE l.link_type='html' AND l.source_domain<>b.domain"            + ("" if include_removed else " AND l.is_active=1") + (" AND b.target_type=?" if target_type else "")
    args = [target_type] if target_type else []
    with core.connect() as c:
        total = c.execute("SELECT COUNT(*)" + cond, args).fetchone()[0]
        rows = c.execute("SELECT l.*, b.target_type, (SELECT crawl_status FROM discovered_urls u WHERE u.url=l.source_url) AS src_status" + cond + " ORDER BY b.target_type, l.destination_domain, l.id LIMIT ? OFFSET ?", (*args, limit, offset)).fetchall()
        per = c.execute("SELECT b.domain, b.target_type, COUNT(l.id) AS backlinks, COUNT(DISTINCT l.source_domain) AS referring_domains FROM businesses b"
                        " LEFT JOIN outgoing_links l ON l.destination_domain=b.domain AND l.link_type='html' AND l.source_domain<>b.domain"
                        + ("" if include_removed else " AND l.is_active=1") + (" WHERE b.target_type=?" if target_type else "")
                        + " GROUP BY b.domain, b.target_type ORDER BY b.target_type, b.domain", args).fetchall()
    return {"total_backlinks": total, "limit": limit, "offset": offset, "targets": [dict(r) for r in per],
            "backlinks": [{"target_domain": r["destination_domain"], "target_type": r["target_type"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                           "anchor_text": r["anchor_text"], "rel": r["rel_attributes"], "source_domain": r["source_domain"], "link_status": link_status(r),
                           "first_seen": r["first_seen"], "last_verified": r["last_seen"], "is_active": bool(r["is_active"])} for r in rows]}


@router.get("/backlinks/{target_domain}")
def backlinks(target_domain: str, include_removed: bool = False, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    """Backlinks for ANY domain, read from our own link graph; no network call here. A domain with nothing yet is queued for the
    engine's discovery for a business is started by POST /discovery/onboard, never by this GET."""
    target = _clean(target_domain)
    reg = core.registrable_domain(target)
    with core.connect() as c:
        total, rows = _graph(c, target, not include_removed, limit, offset)
        every = _graph(c, target, not include_removed)[1]
        active = sum(link_status(r) == "verified" for r in every)
        refs = len({r["source_domain"] for r in every})
        job = business_jobs.latest(c, reg)
    state = business_jobs.api_state(job)
    if total == 0:
        return {"domain": target, "target_domain": target, "total_backlinks": 0, "referring_domains": 0, "total_referring_domains": 0,
                "status": state, "job_id": job and job["id"], "backlinks": [],
                "message": "Not onboarded: POST /discovery/onboard to start discovery." if not job else
                f"No backlink found yet ({job['pages_crawled']} page(s) crawled" + (f"; last error: {job['last_error']}" if job["last_error"] else "") + ")."}
    return {
        "domain": target, "target_domain": target, "status": "ok", "discovery_status": state, "total_backlinks": total, "verified_backlinks": active,
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


class OnboardRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"domain": "codigix.co"}]})
    domain: str = Field(min_length=3, max_length=2048, description="The business's domain or website URL, e.g. codigix.co or https://www.codigix.co/")


@router.post("/discovery/onboard", status_code=202)
def discovery_onboard(body: OnboardRequest) -> dict:
    """Register a business and queue its P0 discovery job; returns at once (the engine searches and crawls). Idempotent per active job."""
    domain = _clean(body.domain)
    with core.connect() as c:
        job, created = business_jobs.create_job(c, domain)
    return {"domain": job["target_domain"], "job_id": job["id"], "status": "queued" if created else business_jobs.api_state(job),
            "priority": f"P{job['priority']}", "created": created}


@router.post("/businesses", status_code=202)
def onboard_business(body: OnboardRequest) -> dict:
    """Older form of /discovery/onboard (returns the whole job)."""
    with core.connect() as c:
        job, created = business_jobs.create_job(c, _clean(body.domain))
    return {"created": created, "job": job}


@router.get("/discovery/business/{domain}/status")
def discovery_business_status(domain: str) -> dict:
    with core.connect() as c:
        reg = core.registrable_domain(_clean(domain))
        job, sched = business_jobs.latest(c, reg), c.execute("SELECT * FROM businesses WHERE domain=?", (reg,)).fetchone()
    if job is None:
        raise HTTPException(404, "not onboarded")
    return {"domain": reg, "status": business_jobs.api_state(job), "job": job, "schedule": dict(sched) if sched else None}


@router.get("/discovery/business/{domain}/runs")
def discovery_business_runs(domain: str, limit: int = Query(50, ge=1, le=500)) -> dict:
    with core.connect() as c:
        return {"runs": _page(c, "SELECT * FROM business_jobs WHERE target_domain=? ORDER BY id DESC", [core.registrable_domain(_clean(domain))], limit, 0)}


@router.get("/businesses")
def list_businesses(status: str | None = None, limit: int = Query(100, ge=1, le=1000)) -> dict:
    with core.connect() as c:
        where, args = (" WHERE status=?", [status]) if status else ("", [])
        return {"items": _page(c, "SELECT * FROM business_jobs" + where + " ORDER BY id DESC", args, limit, 0)}


@router.get("/businesses/{domain}")
def business_status(domain: str) -> dict:
    """Progress of this business's latest discovery job (counters are refreshed by the engine every few seconds)."""
    with core.connect() as c:
        job = business_jobs.latest(c, _clean(domain))
    if job is None:
        raise HTTPException(404, "not onboarded")
    return job


# ---------- competitors: registered once for all businesses; backlinks come from the same shared graph as everyone else's ----------

class CompetitorsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"domains": ["competitor-a.com"]}]})
    domains: list[str] = Field(min_length=1, max_length=20, description="Competitor domains or URLs")


class ActiveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    active: bool


def _owner(business_domain: str) -> str:
    return core.registrable_domain(_clean(business_domain))


@router.post("/competitors", status_code=202)
def add_global_competitors(body: CompetitorsRequest) -> dict:
    """Register competitors once for ALL businesses: only a domain or URL is needed. Each is crawled once and is part of every business's backlink gap."""
    out = []
    with core.connect() as c:
        for d in body.domains:
            try:
                out.append(competitors.register(c, competitors.GLOBAL, d))
            except ValueError as e:
                out.append({"domain": d, "status": "rejected", "error": str(e)})
    return {"results": out}


@router.get("/competitors")
def list_global_competitors() -> dict:
    with core.connect() as c:
        return {"competitors": competitors.listing(c, competitors.GLOBAL)}


@router.get("/competitors/{competitor_domain}")
def get_global_competitor(competitor_domain: str) -> dict:
    with core.connect() as c:
        d = competitors.detail(c, competitors.GLOBAL, _clean(competitor_domain))
    if d is None:
        raise HTTPException(404, "not a global competitor")
    return d


@router.patch("/competitors/{competitor_domain}")
def set_global_competitor_active(competitor_domain: str, body: ActiveRequest) -> dict:
    with core.connect() as c:
        if not competitors.set_active(c, competitors.GLOBAL, _clean(competitor_domain), body.active):
            raise HTTPException(404, "not a global competitor")
    return {"competitor_domain": core.registrable_domain(_clean(competitor_domain)), "active": body.active}


@router.delete("/competitors/{competitor_domain}")
def remove_global_competitor(competitor_domain: str) -> dict:
    with core.connect() as c:
        if not competitors.remove(c, competitors.GLOBAL, _clean(competitor_domain)):
            raise HTTPException(404, "not a global competitor")
    return {"removed": True, "data_kept": True}


@router.get("/businesses/{business_domain}/backlink-gap")
def backlink_gap(business_domain: str, competitor: str | None = None, limit: int = Query(200, ge=1, le=2000)) -> dict:
    """Referring domains and pages that link the competitors but not this business, over the discovered index (not the whole internet)."""
    try:
        with core.connect() as c:
            return competitors.gap(c, _owner(business_domain), _clean(competitor) if competitor else None, limit)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e


def _comp(domain: str) -> str:
    d = _clean(domain)
    with core.connect() as c:
        if not competitors.is_competitor(c, d):
            raise HTTPException(404, "not a registered competitor")
    return d


@router.get("/competitors/{competitor_domain}/backlinks")
def competitor_backlinks(competitor_domain: str, include_removed: bool = False, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    return {**backlinks(_comp(competitor_domain), include_removed, limit, offset), "target_type": "competitor"}


@router.get("/competitors/{competitor_domain}/referring-domains")
def competitor_referring_domains(competitor_domain: str) -> dict:
    return referring_domains(_comp(competitor_domain))


@router.get("/competitors/{competitor_domain}/anchors")
def competitor_anchors(competitor_domain: str) -> dict:
    return backlink_anchors(_comp(competitor_domain))


@router.get("/competitors/{competitor_domain}/history")
def competitor_history(competitor_domain: str, limit: int = Query(200, ge=1, le=1000)) -> dict:
    return backlink_history(_comp(competitor_domain), limit)


@router.get("/competitors/{competitor_domain}/discovery/status")
def competitor_discovery_status(competitor_domain: str) -> dict:
    return discovery_business_status(_comp(competitor_domain))

"""Read-only FastAPI router over the link graph (backlink search + inspection). Mounted by backlink_api.py.
Crawling is done only by the standalone autonomous.py process."""

import heapq
import re
from contextlib import nullcontext

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

import business_jobs
import competitors
import crawler_core as core
import engine_stats
from api_timing import connect

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
        return {"total": total, "items": _page(c, "SELECT * FROM discovered_urls" + page_clause + " ORDER BY id", args + kargs, limit, 0)}


@router.get("/crawler/discovered-domains")
def discovered_domains(discovery_status: str | None = None, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    clause, args = (" WHERE discovery_status=?", [discovery_status]) if discovery_status else ("", [])
    with connect() as c:
        total = c.execute("SELECT COUNT(*) n FROM discovered_domains" + clause, args).fetchone()["n"]
        return {"total": total, "items": _page(c, "SELECT * FROM discovered_domains" + clause + " ORDER BY id", args, limit, offset)}


@router.get("/crawler/outgoing-links")
def outgoing_links(source_url: str | None = None, destination_domain: str | None = None, active_only: bool = True,
                         limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0),
                         after_id: int | None = Query(None, ge=0, description="keyset paging instead of offset: only rows with id greater than this")) -> dict:
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
        return {"total": total, "items": _page(c, "SELECT * FROM outgoing_links" + page_clause + " ORDER BY id", args + kargs, limit, offset)}


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
_SRC_STATUS = "(SELECT crawl_status FROM discovered_urls u WHERE u.url=outgoing_links.source_url)"
_LINK_COLS = "source_url, destination_url, anchor_text, rel_attributes, source_domain, first_seen, last_seen, is_active, lost_at"  # what the responses use


def _graph_page(c, target: str, active_only: bool, limit: int, offset: int) -> list:
    """One page of links (only the columns the API returns, plus the source page's crawl status), ordered by id."""
    cond, args = _graph_cond(target, active_only)
    return c.execute(f"SELECT {_LINK_COLS}, {_SRC_STATUS} AS src_status FROM outgoing_links" + cond + " ORDER BY id LIMIT ? OFFSET ?",
                     (*args, limit, offset)).fetchall()


def _graph_summary(c, target: str, active_only: bool) -> tuple[int, int, int]:
    """(total links, verified links, referring domains) in ONE aggregate query; same rules as link_status() and as counting the distinct
    source_domain values in Python (a NULL source_domain counts as one domain there, so it does here)."""
    cond, args = _graph_cond(target, active_only)
    # LEFT JOIN, not a per-row subquery, so the planner can hash-join the failed pages; url is UNIQUE, so the join never multiplies rows
    r = c.execute(
        "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE COALESCE(t.is_active,0)<>0 AND COALESCE(t.lost_at,'')='' AND bad.url IS NULL) AS verified,"
        " COUNT(DISTINCT t.source_domain) + CASE WHEN COUNT(*) FILTER (WHERE t.source_domain IS NULL)>0 THEN 1 ELSE 0 END AS refs"
        " FROM (SELECT is_active, lost_at, source_domain, source_url FROM outgoing_links" + cond + ") t"
        " LEFT JOIN discovered_urls bad ON bad.url=t.source_url AND bad.crawl_status IN ('failed','retry_pending')", args).fetchone()
    return r["total"], r["verified"], r["refs"]


@router.get("/backlinks")
def all_backlinks(target_type: str | None = Query(None, pattern="^(own_business|competitor)$"), include_removed: bool = False,
                  limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict:
    """Backlinks of EVERY monitored target, our businesses and all competitors together (filter with target_type), straight from the link graph."""
    cond = " FROM outgoing_links l JOIN businesses b ON b.domain=l.destination_domain WHERE l.link_type='html' AND l.source_domain<>b.domain"            + ("" if include_removed else " AND l.is_active=1") + (" AND b.target_type=?" if target_type else "")
    args = [target_type] if target_type else []
    cols = ", ".join("l." + x for x in ("destination_domain", "source_url", "destination_url", "anchor_text", "rel_attributes", "source_domain",
                                        "first_seen", "last_seen", "is_active", "lost_at"))
    with connect() as c:
        total = c.execute("SELECT COUNT(*)" + cond, args).fetchone()[0]
        rows = c.execute(f"SELECT {cols}, b.target_type, (SELECT crawl_status FROM discovered_urls u WHERE u.url=l.source_url) AS src_status" + cond + " ORDER BY b.target_type, l.destination_domain, l.id LIMIT ? OFFSET ?", (*args, limit, offset)).fetchall()
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
    with connect() as c:
        total, active, refs = _graph_summary(c, target, not include_removed)  # totals come from SQL aggregates, never from loading the graph
        rows = _graph_page(c, target, not include_removed, limit, offset) if total else []
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
    cond, args = _graph_cond(target)
    with connect() as c:  # grouped in PostgreSQL; COLLATE "C" = Python's code-point min/max; ties keep first-appearance (lowest id) order like the old stable sort
        items = [{"domain": r["domain"], "backlinks": r["backlinks"], "first_seen": r["first_seen"], "last_verified": r["last_verified"]}
                 for r in c.execute('SELECT source_domain AS domain, COUNT(*) AS backlinks, MIN(first_seen COLLATE "C") AS first_seen,'
                                    ' MAX(last_seen COLLATE "C") AS last_verified FROM outgoing_links' + cond +
                                    " GROUP BY source_domain ORDER BY COUNT(*) DESC, MIN(id)", args)]
    return {"domain": target, "status": "ok" if items else "not_found_in_current_index", "total_referring_domains": len(items), "referring_domains": items}


@router.get("/backlinks/{target_domain}/anchors")
def backlink_anchors(target_domain: str) -> dict:
    """Anchor-text distribution of the active backlinks."""
    target = _clean(target_domain)
    cond, args = _graph_cond(target)
    with connect() as c:
        anchors = [{"anchor_text": r["anchor_text"], "count": r["n"]}
                   for r in c.execute("SELECT COALESCE(anchor_text,'') AS anchor_text, COUNT(*) AS n FROM outgoing_links" + cond +
                                      " GROUP BY 1 ORDER BY COUNT(*) DESC, MIN(id)", args)]
    return {"target_domain": target, "domain": target, "anchors": anchors}


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
    newest = lambda events: heapq.nlargest(limit, events, key=lambda e: e["at"])  # noqa: E731 - same order as sorted(reverse=True)[:limit]
    lost_seen: set = set()

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

    def logged(c):  # link_history rows: the 100000-row window and the host filter are exactly as before; rows are streamed, only `limit` kept
        for r in c.execute("SELECT source_url, destination_url, anchor_text, event_type, detected_at FROM link_history WHERE destination_domain=? ORDER BY id DESC LIMIT 100000", (reg,)):
            if on_target(r["destination_url"]) and not on_target(r["source_url"]):
                if r["event_type"] == "lost":
                    lost_seen.add((r["source_url"], r["destination_url"], r["anchor_text"]))
                yield {"event": r["event_type"], "at": r["detected_at"], "source_url": r["source_url"], "destination_url": r["destination_url"],
                       "anchor_text": r["anchor_text"], "origin": "graph"}

    with connect() as c:
        new = [{"event": "new", "at": r["first_seen"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"}
               for r in c.execute('SELECT source_url, destination_url, first_seen FROM outgoing_links' + cond + ' ORDER BY first_seen COLLATE "C" DESC, id LIMIT ?', (*args, limit))]
        logged_events = newest(logged(c))
        # rows removed before link_history existed: their single lost_at is all we know. At most len(lost_seen) of the candidates are skipped,
        # so the newest limit + len(lost_seen) always contain the newest `limit` that are kept.
        fallback = [{"event": "lost", "at": r["lost_at"], "source_url": r["source_url"], "destination_url": r["destination_url"], "origin": "graph"}
                    for r in c.execute("SELECT source_url, destination_url, anchor_text, lost_at FROM outgoing_links" + cond +
                                       " AND COALESCE(lost_at,'')<>'' AND COALESCE(is_active,0)=0 ORDER BY lost_at COLLATE \"C\" DESC, id LIMIT ?", (*args, limit + len(lost_seen)))
                    if (r["source_url"], r["destination_url"], r["anchor_text"]) not in lost_seen][:limit]
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
    """Register a business and queue its P0 discovery job; returns at once (the engine searches and crawls). Idempotent per active job."""
    domain = _clean(body.domain)
    with connect() as c:
        job, created = business_jobs.create_job(c, domain)
    return {"domain": job["target_domain"], "job_id": job["id"], "status": "queued" if created else business_jobs.api_state(job),
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
    if job is None:
        raise HTTPException(404, "not onboarded")
    return {"domain": reg, "status": business_jobs.api_state(job), "job": job, "schedule": dict(sched) if sched else None}


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
    return {"business": owner, "results": out}


@router.get("/businesses/{business_domain}/competitors")
def list_business_competitors(business_domain: str) -> dict:
    with connect() as c:
        return {"business": _owner(business_domain), "competitors": competitors.listing(c, _owner(business_domain))}


@router.delete("/businesses/{business_domain}/competitors/{competitor_domain}")
def remove_business_competitor(business_domain: str, competitor_domain: str) -> dict:
    with connect() as c:
        if not competitors.remove(c, _owner(business_domain), _clean(competitor_domain)):
            raise HTTPException(404, "not a competitor of this business")
    return {"removed": True, "data_kept": True}


@router.get("/businesses/{business_domain}/backlink-gap")
def backlink_gap(business_domain: str, competitor: str | None = None, limit: int = Query(200, ge=1, le=2000)) -> dict:
    """Referring domains and pages that link the competitors but not this business, over the discovered index (not the whole internet)."""
    try:
        with connect() as c:
            return competitors.gap(c, _owner(business_domain), _clean(competitor) if competitor else None, limit)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e


def _comp(domain: str) -> str:
    d = _clean(domain)
    with connect() as c:
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

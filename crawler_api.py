"""FastAPI router for the universal forward crawler / link graph. Mounted by backlink_api.py."""

import asyncio
import json
import logging
import sys
import uuid

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

import crawler_core as core
import crawler_engine as engine

router = APIRouter()
_tasks: set[asyncio.Task] = set()  # strong refs so background jobs are not garbage-collected


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_domain: str | None = Field(default=None, examples=["codigix.co"])
    source_urls: list[HttpUrl] = Field(default_factory=list, max_length=1000)
    max_pages: int = Field(default=50_000, ge=1, le=1_000_000)
    max_depth: int = Field(default=15, ge=0, le=50)
    concurrency: int = Field(default=5, ge=1, le=50)
    use_playwright: bool = False
    enable_sitemap_discovery: bool = True
    enable_external_domain_discovery: bool = False
    max_domains: int = Field(default=20, ge=1, le=10_000)
    timeout: int = Field(default=30, ge=1, le=300)
    max_retries: int = Field(default=3, ge=1, le=10)


class DomainRequest(BaseModel):
    domain: str


class SeedsRequest(BaseModel):
    urls: list[HttpUrl] = Field(min_length=1, max_length=1000)


class RecrawlRequest(BaseModel):
    urls: list[HttpUrl] | None = Field(default=None, description="Omit to recrawl everything that is due")
    use_playwright: bool = False
    concurrency: int = Field(default=5, ge=1, le=50)


def _launch(job_id: str, coro_factory) -> None:
    """Run a crawl in its own thread + Proactor loop (Playwright cannot start under uvicorn's selector loop on Windows)."""
    async def job() -> None:
        try:
            await asyncio.to_thread(asyncio.run, coro_factory(),
                                    loop_factory=asyncio.ProactorEventLoop if sys.platform == "win32" else None)
            status, err = "finished", None
        except Exception as e:  # noqa: BLE001 - recorded on the job
            status, err = "failed", repr(e)
        with core.connect() as c:
            c.execute("UPDATE crawl_jobs SET status=?, error=?, finished_at=? WHERE job_id=?", (status, err, core.now(), job_id))
        logging.getLogger("crawler").info("job %s %s %s", job_id, status.upper(), err or "")

    t = asyncio.create_task(job())
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _new_job(target: str | None, cfg: dict) -> str:
    job_id = uuid.uuid4().hex
    with core.connect() as c:
        c.execute("INSERT INTO crawl_jobs (job_id, target_domain, status, config, created_at) VALUES (?,?,'running',?,?)",
                  (job_id, target, json.dumps(cfg), core.now()))
    return job_id


def _page(conn, sql: str, args: list, limit: int, offset: int) -> list[dict]:
    return [dict(r) for r in conn.execute(sql + " LIMIT ? OFFSET ?", (*args, limit, offset))]


@router.post("/crawler/start", status_code=202)
async def start(req: StartRequest) -> dict:
    """Start (or resume) forward crawling. With no source_urls it continues pending URLs already in the database."""
    try:
        target = core.clean_target(req.target_domain) if req.target_domain else None
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    cfg = engine.Config(target_domain=target, **req.model_dump(exclude={"target_domain", "source_urls"}))
    job_id = _new_job(target, engine.config_dict(cfg))
    _launch(job_id, lambda: engine.run_crawl(cfg, [str(u) for u in req.source_urls], job_id))
    return {"job_id": job_id, "status": "running"}


@router.post("/crawler/add-domain")
async def add_domain(req: DomainRequest) -> dict:
    """Register a source domain as approved for crawling (seeds its homepage and sitemap URLs)."""
    try:
        domain = core.registrable_domain(core.clean_target(req.domain))
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    ok = await engine.admit_domain(engine.Config(), None, domain)
    return {"domain": domain, "admitted": ok, **({} if ok else {"reason": "blocked by SSRF guard"})}


@router.post("/crawler/add-seeds")
async def add_seeds(req: SeedsRequest) -> dict:
    """Add seed URLs; their domains are approved. Crawl them with POST /crawler/start."""
    results = {}
    for u in map(str, req.urls):
        results[u] = await engine.admit_domain(engine.Config(enable_sitemap_discovery=False), None,
                                               core.registrable_domain(core.hostname(u)), u)
    return {"admitted": results}


@router.get("/crawler/status/{job_id}")
def status(job_id: str) -> dict:
    with core.connect() as c:
        job = c.execute("SELECT * FROM crawl_jobs WHERE job_id=?", (job_id,)).fetchone()
        pending = c.execute("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('pending','crawling')").fetchone()["n"]
    if job is None:
        raise HTTPException(404, "job not found")
    return {**dict(job), "config": json.loads(job["config"] or "{}"), "pending_urls": pending}


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


def _ensure_target(c, target: str) -> None:
    """Backfill from the link graph only the first time a target is seen; the crawler records its backlinks afterwards,
    so repeat GETs stay read-only and never wait for the crawler's write lock."""
    if c.execute("SELECT 1 FROM target_domains WHERE domain=? AND is_active=1", (target,)).fetchone() is None:
        core.match_target_from_graph(c, target)


def _backlink_rows(c, target: str, active_only: bool = True) -> list:
    sql = ("SELECT b.* FROM backlinks b JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=?"
           + (" AND b.is_active=1" if active_only else "") + " ORDER BY b.id")
    return c.execute(sql, (target,)).fetchall()


@router.get("/backlinks/{target_domain}")
def backlinks(target_domain: str, include_removed: bool = False) -> dict:
    """Backlinks for ANY domain. Unknown targets are registered and matched against the existing link graph."""
    try:
        target = core.clean_target(target_domain)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    with core.connect() as c:
        _ensure_target(c, target)
        rows = _backlink_rows(c, target, not include_removed)
    return {
        "target_domain": target,
        "total_backlinks": len(rows),
        "verified_backlinks": sum(r["link_status"] == "verified" for r in rows),
        "total_referring_domains": len({core.registrable_domain(core.hostname(r["source_url"])) for r in rows}),
        "backlinks": [{
            "source_url": r["source_url"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"],
            "rel": r["rel_attributes"], "verification_status": r["link_status"], "discovery_method": r["discovery_method"],
            "last_verified": r["last_verified"], "is_active": bool(r["is_active"]),
        } for r in rows],
    }


@router.get("/backlinks/{target_domain}/anchors")
def backlink_anchors(target_domain: str) -> dict:
    """Anchor-text distribution of the active backlinks."""
    try:
        target = core.clean_target(target_domain)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    with core.connect() as c:
        _ensure_target(c, target)
        rows = _backlink_rows(c, target)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["anchor_text"] or ""] = counts.get(r["anchor_text"] or "", 0) + 1
    return {"target_domain": target, "anchors": [{"anchor_text": a, "count": n} for a, n in sorted(counts.items(), key=lambda x: -x[1])]}


@router.get("/backlinks/{target_domain}/changes")
def backlink_changes(target_domain: str, event_type: str | None = None, limit: int = Query(200, le=1000)) -> dict:
    """History: first_discovered / verified / link_changed / confirmed_removed / reappeared."""
    try:
        target = core.clean_target(target_domain)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    sql = ("SELECT h.event_type, h.detected_at, h.details, b.source_url, b.destination_url FROM backlink_history h"
           " JOIN backlinks b ON b.id=h.backlink_id JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=?")
    args: list = [target]
    if event_type:
        sql += " AND h.event_type=?"
        args.append(event_type)
    with core.connect() as c:
        return {"target_domain": target, "events": [dict(r) for r in c.execute(sql + " ORDER BY h.id DESC LIMIT ?", (*args, limit))]}


@router.post("/crawler/recrawl", status_code=202)
async def recrawl(req: RecrawlRequest) -> dict:
    """Refetch pages that are due (or the given URLs) and diff their links against history."""
    cfg = engine.Config(use_playwright=req.use_playwright, concurrency=req.concurrency)
    job_id = _new_job(None, engine.config_dict(cfg))
    urls = [str(u) for u in req.urls] if req.urls else None
    _launch(job_id, lambda: engine.recrawl_due(cfg, job_id, urls))
    return {"job_id": job_id, "status": "running"}


@router.get("/crawler/statistics")
def statistics() -> dict:
    with core.connect() as c:
        one = lambda sql: c.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "discovered_urls": one("SELECT COUNT(*) FROM discovered_urls"),
            "discovered_domains": one("SELECT COUNT(*) FROM discovered_domains"),
            "outgoing_links": one("SELECT COUNT(*) FROM outgoing_links"),
            "backlinks_verified": one("SELECT COUNT(*) FROM backlinks WHERE link_status='verified' AND is_active=1"),
            "backlinks_suspected_removed": one("SELECT COUNT(*) FROM backlinks WHERE link_status='suspected_removed'"),
            "backlinks_removed": one("SELECT COUNT(*) FROM backlinks WHERE link_status='removed'"),
            "urls_by_status": {r[0]: r[1] for r in c.execute("SELECT crawl_status, COUNT(*) FROM discovered_urls GROUP BY 1")},
            "domains_by_status": {r[0]: r[1] for r in c.execute("SELECT discovery_status, COUNT(*) FROM discovered_domains GROUP BY 1")},
            "recrawl_due_now": len(engine.due_urls()),
        }

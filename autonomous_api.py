"""Discovery / global-crawler endpoints. Read-only views of PostgreSQL plus flags (pause, bootstrap) for the standalone
`python autonomous.py` engine."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import autonomous
import crawler_core as core
import engine_stats

router = APIRouter()


@router.post("/discovery/bootstrap", status_code=202)
def discovery_bootstrap() -> dict:
    """Ask the standalone engine to run every enabled source now (within ~30s). The API itself never fetches anything."""
    with core.connect() as c:
        c.execute("INSERT INTO engine_state (key, value) VALUES ('bootstrap_requested', '1') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
    return {"requested": True}


class SeedRequest(BaseModel):
    urls: list[str]  # "example.com" or a full URL; crawled first (priority 1)
    directories: bool = False  # also seed the built-in agency directories


@router.post("/crawler/seed", status_code=202)
def seed(req: SeedRequest) -> dict:
    """Queue URLs/domains for the standalone engine to admit at top priority (picked up within ~30s). Fetches nothing itself."""
    urls = [u.strip() if "://" in u else f"https://{u.strip()}/" for u in req.urls + (list(autonomous.DIRECTORIES) if req.directories else [])]
    bad = [u for u in urls if not core.normalize_url(u)]
    if bad or not urls:
        raise HTTPException(422, f"invalid or empty urls: {bad}")
    with core.connect() as c:
        c.executemany("INSERT INTO engine_state (key, value) VALUES (?, '1') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                      [(f"seed:{u}",) for u in urls])
    return {"queued": urls}


@router.get("/discovery/status")
def discovery_status() -> dict:
    with core.connect() as c:
        return {"sources": [dict(r) for r in c.execute("SELECT * FROM discovery_sources ORDER BY name")],
                "bootstrapped": c.execute("SELECT COUNT(*) FROM discovery_sources WHERE runs>0").fetchone()[0] > 0}


@router.get("/discovery/statistics")
def discovery_statistics() -> dict:
    return engine_stats.counts()


@router.get("/crawler/global-status")
def global_status() -> dict:
    with core.connect() as c:
        workers = [{**r, "healthy": autonomous.worker_health(r)} for r in map(dict, c.execute("SELECT * FROM worker_status ORDER BY name"))]
    return {"paused": autonomous.is_paused(), "workers": workers, **engine_stats.counts()}


@router.post("/crawler/pause")
def pause() -> dict:
    autonomous.set_paused(True)
    return {"paused": True}


@router.post("/crawler/resume")
def resume() -> dict:
    autonomous.set_paused(False)
    return {"paused": False}

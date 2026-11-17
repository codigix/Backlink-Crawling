"""Discovery / global-crawler endpoints. Read-only views of PostgreSQL plus flags (pause, bootstrap) for the standalone
`python autonomous.py` engine."""

from contextlib import nullcontext

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import autonomous
import crawler_core as core
import engine_stats
from api_timing import connect

router = APIRouter()


def _stats(connect_fn=None) -> dict:
    """The shared statistics snapshot (see engine_stats.snapshot_counts); 503 only when there is none at all and the database cannot make one."""
    try:
        return engine_stats.snapshot_counts(connect_fn)
    except engine_stats.StatsUnavailable as e:
        raise HTTPException(503, "statistics temporarily unavailable") from e


@router.post("/discovery/bootstrap", status_code=202)
def discovery_bootstrap() -> dict:
    """Ask the standalone engine to run every enabled source now (within ~30s). The API itself never fetches anything."""
    with connect() as c:
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
    with connect() as c:
        c.executemany("INSERT INTO engine_state (key, value) VALUES (?, '1') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                      [(f"seed:{u}",) for u in urls])
    return {"queued": urls}


@router.get("/discovery/status")
def discovery_status() -> dict:
    with connect() as c:
        return {"sources": [dict(r) for r in c.execute("SELECT * FROM discovery_sources ORDER BY name")],
                "bootstrapped": c.execute("SELECT COUNT(*) FROM discovery_sources WHERE runs>0").fetchone()[0] > 0}


@router.get("/discovery/statistics")
def discovery_statistics() -> dict:
    return _stats(connect)


@router.get("/crawler/global-status")
def global_status() -> dict:
    with connect() as c:  # one connection for workers, the paused flag and (when a refresh is due) the statistics
        workers = [{**r, "healthy": autonomous.worker_health(r)} for r in map(dict, c.execute("SELECT * FROM worker_status ORDER BY name"))]
        row = c.execute("SELECT value FROM engine_state WHERE key='paused'").fetchone()  # same rule as autonomous.is_paused()
        stats = _stats(lambda: nullcontext(c))
    return {"paused": bool(row and row["value"] == "1"), "workers": workers, **stats}


@router.post("/crawler/pause")
def pause() -> dict:
    autonomous.set_paused(True)
    return {"paused": True}


@router.post("/crawler/resume")
def resume() -> dict:
    autonomous.set_paused(False)
    return {"paused": False}

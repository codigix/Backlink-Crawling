"""FastAPI backend for the backlink checker.

Run: .venv/Scripts/python -m uvicorn backlink_api:app --reload
Docs: http://127.0.0.1:8000/docs
"""

import asyncio
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from backlink_checker import check_backlinks

DB_FILE = Path(__file__).parent / "backlinks.db"

app = FastAPI(title="Backlink Checker")
_tasks: set[asyncio.Task] = set()  # keep strong refs so running crawls aren't garbage-collected


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


with db() as conn:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, target_domain TEXT, status TEXT, error TEXT,
            created_at TEXT, finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS backlinks (
            job_id TEXT, target_domain TEXT, source_url TEXT, backlink_found INTEGER,
            matching_links TEXT, error TEXT, checked_at TEXT, source_site TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_backlinks_domain ON backlinks(target_domain);
        CREATE INDEX IF NOT EXISTS idx_backlinks_job ON backlinks(job_id);
    """)
    # Databases created by the earlier version lack source_site.
    if "source_site" not in {c["name"] for c in conn.execute("PRAGMA table_info(backlinks)")}:
        conn.execute("ALTER TABLE backlinks ADD COLUMN source_site TEXT")
    # Crawls don't survive a restart; don't leave them stuck as "running".
    conn.execute("UPDATE jobs SET status = 'failed', error = 'server restarted' WHERE status = 'running'")


class CrawlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_domain: str = Field(min_length=3, max_length=253, examples=["codigix.co"])
    source_urls: list[HttpUrl] = Field(min_length=1, max_length=1000)
    max_pages: int | None = Field(default=None, ge=1, description="Optional cap; omit to crawl every page")
    concurrency: int = Field(default=5, ge=1, le=50)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def summary(rows: list[sqlite3.Row]) -> dict:
    """Per source site: pages crawled, pages linking to the target, and every backlink found."""
    sites: dict[str, dict] = {}
    for row in rows:
        site = sites.setdefault(
            row["source_site"] or row["source_url"],
            {"pages_crawled": 0, "pages_with_backlink": 0, "backlinks": [], "failed_pages": []},
        )
        site["pages_crawled"] += 1
        if row["error"] is not None:
            site["failed_pages"].append({"page_url": row["source_url"], "error": row["error"]})
        elif row["backlink_found"]:
            site["pages_with_backlink"] += 1
            site["backlinks"] += [{"page_url": row["source_url"], **link} for link in json.loads(row["matching_links"])]
    for site in sites.values():
        site["has_backlink"] = site["pages_with_backlink"] > 0
    return {
        "pages_crawled": len(rows),
        "pages_with_backlink": sum(s["pages_with_backlink"] for s in sites.values()),
        "total_backlinks": sum(len(s["backlinks"]) for s in sites.values()),
        "sites_with_backlink": sum(s["has_backlink"] for s in sites.values()),
        "failed_pages": sum(len(s["failed_pages"]) for s in sites.values()),
        "sites": sites,
    }


async def run_job(job_id: str, req: CrawlRequest) -> None:
    def save(r: dict) -> None:
        # Saved per page, so progress is visible while crawling and survives a crash.
        with db() as conn:
            conn.execute(
                "INSERT INTO backlinks (job_id, target_domain, source_site, source_url, backlink_found,"
                " matching_links, error, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (job_id, req.target_domain, r["source_site"], r["source_url"], r["backlink_found"],
                 json.dumps(r["matching_links"]), r["error"], now()),
            )

    try:
        crawl = check_backlinks(
            req.target_domain, [str(u) for u in req.source_urls], req.max_pages, req.concurrency, on_result=save
        )
        # uvicorn --reload on Windows uses a selector loop, which can't start Playwright's browser process;
        # so each crawl gets its own thread with a Proactor loop there.
        loop_factory = asyncio.ProactorEventLoop if sys.platform == "win32" else None
        await asyncio.to_thread(asyncio.run, crawl, loop_factory=loop_factory)
        with db() as conn:
            conn.execute("UPDATE jobs SET status = 'finished', finished_at = ? WHERE job_id = ?", (now(), job_id))
    except Exception as e:  # noqa: BLE001 - record any crawl failure on the job instead of losing it
        with db() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'failed', error = ?, finished_at = ? WHERE job_id = ?",
                (repr(e), now(), job_id),
            )


@app.post("/crawl", status_code=202)
async def start_crawl(req: CrawlRequest) -> dict:
    """1. Start a backlink crawl in the background."""
    req.target_domain = req.target_domain.strip().lower()
    job_id = uuid.uuid4().hex
    with db() as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, target_domain, status, created_at) VALUES (?, ?, 'running', ?)",
            (job_id, req.target_domain, now()),
        )
    task = asyncio.create_task(run_job(job_id, req))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return {"job_id": job_id, "status": "running"}


@app.get("/crawl/{job_id}/status")
async def crawl_status(job_id: str) -> dict:
    """2. Status of a crawl (running, finished or failed) with live progress."""
    with db() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        progress = conn.execute(
            "SELECT COUNT(*) AS pages_crawled, COALESCE(SUM(backlink_found = 1), 0) AS pages_with_backlink,"
            " COALESCE(SUM(error IS NOT NULL), 0) AS failed_pages FROM backlinks WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    if job is None:
        raise HTTPException(404, "job not found")
    return {**dict(job), **dict(progress)}


@app.get("/crawl/{job_id}/backlinks")
async def crawl_backlinks(job_id: str) -> dict:
    """3. Backlinks found so far, grouped by source site (works while the crawl is still running)."""
    with db() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        rows = conn.execute("SELECT * FROM backlinks WHERE job_id = ?", (job_id,)).fetchall()
    if job is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "target_domain": job["target_domain"], "status": job["status"], **summary(rows)}


@app.get("/domain/{domain}")
async def domain_backlinks(domain: str) -> dict:
    """4. Stored backlink results for a domain (latest finished crawl)."""
    domain = domain.strip().lower()
    with db() as conn:
        job = conn.execute(
            "SELECT job_id FROM jobs WHERE target_domain = ? AND status = 'finished' ORDER BY finished_at DESC LIMIT 1",
            (domain,),
        ).fetchone()
        if job is None:
            raise HTTPException(404, f"no stored results for {domain}; start a crawl with POST /crawl")
        rows = conn.execute("SELECT * FROM backlinks WHERE job_id = ?", (job["job_id"],)).fetchall()
    return {"target_domain": domain, "job_id": job["job_id"], **summary(rows)}

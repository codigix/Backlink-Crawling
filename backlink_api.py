"""FastAPI backend for the backlink crawler.

Run: .venv/Scripts/python -m uvicorn backlink_api:app --reload
Docs: http://127.0.0.1:8000/docs
"""

import logging

from fastapi import FastAPI, Security
from fastapi.security import APIKeyHeader

import crawler_core
from autonomous_api import router as discovery_router
from crawler_api import router as crawler_router

# Progress of crawl jobs shows in this terminal (extraction = each page, verification = backlinks found/removed).
_h = logging.StreamHandler()
_h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%H:%M:%S"))
logging.getLogger("crawler").addHandler(_h)
logging.getLogger("crawler").setLevel(logging.INFO)
logging.getLogger("crawler").propagate = False  # Crawlee adds its own root handler; avoid duplicate lines

# Creates the link-graph tables. Rows from the old per-job results table are kept in legacy_backlinks (no endpoint reads them).
with crawler_core.connect() as conn:
    crawler_core.migrate(conn, recover=False)  # a separate crawler_worker process may be mid-crawl



# Only adds an "Authorize" button to /docs so the X-API-Key can be typed once. auto_error=False: nginx does the real key check.
app = FastAPI(title="Backlink Crawler", dependencies=[Security(APIKeyHeader(name="X-API-Key", auto_error=False))])
app.include_router(crawler_router)
app.include_router(discovery_router)

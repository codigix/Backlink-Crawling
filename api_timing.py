"""Per-request timing for the read API: connection time, SQL execution, result fetching, total -> `Server-Timing` header.

API-only: the crawler/engine never imports this module. Inside an HTTP request (see `install`)
`connect()` returns a pooled connection whose cursors add their durations to that request's totals (the cursor class is reset when the
connection goes back to the pool); anywhere else (direct calls, tests, the
engine) it is plain `core.connect()`. SQL text and parameters are never recorded, and the slow-request log line carries the route
template, never the query string."""

import contextvars
import logging
import os
import time

import psycopg
from fastapi.responses import JSONResponse

import crawler_core as core

log = logging.getLogger("crawler.api")
SLOW_MS = float(os.environ.get("API_SLOW_MS", 1000))  # one WARNING line per request slower than this; nothing is logged below it
API_DB_WAIT = float(os.environ.get("API_DB_WAIT", 5))  # an API request waits this long for a pooled connection; workers keep DB_POOL_WAIT
_ctx: contextvars.ContextVar = contextvars.ContextVar("api_timing", default=None)


def _add(key: str, t0: float) -> None:
    m = _ctx.get()
    if m is not None:
        m[key] += (time.perf_counter() - t0) * 1000


class _Cursor(core.Cursor):
    """psycopg runs the query and receives every row inside execute(); fetch*() only builds the Python rows."""

    def execute(self, *a, **kw):
        t0 = time.perf_counter()
        try:
            return super().execute(*a, **kw)
        finally:
            _add("sql_ms", t0)

    def fetchone(self):
        t0 = time.perf_counter()
        try:
            return super().fetchone()
        finally:
            _add("fetch_ms", t0)

    def fetchall(self):
        t0 = time.perf_counter()
        try:
            return super().fetchall()
        finally:
            _add("fetch_ms", t0)

    def __iter__(self):
        it = super().__iter__()
        while True:
            t0 = time.perf_counter()
            try:
                row = next(it)
            except StopIteration:
                _add("fetch_ms", t0)
                return
            _add("fetch_ms", t0)
            yield row


def connect():
    """Drop-in for core.connect() in API code."""
    m = _ctx.get()
    if m is None:
        return core.connect()
    t0 = time.perf_counter()
    try:
        conn = core.connect()
    finally:
        _add("connect_ms", t0)
    m["conns"] += 1
    conn.cursor_factory = _Cursor
    return conn


def note(key: str, value) -> None:
    """Attach a small fact (e.g. statistics age) to the current request's response headers; no-op outside a request."""
    m = _ctx.get()
    if m is not None:
        m.setdefault("notes", {})[key] = value


_CACHE_RANK = {"HIT": 0, "STALE": 1, "MISS": 2}


def note_cache(status: str, age: float) -> None:
    """X-Cache (HIT | STALE | MISS; the worst of the request's cache lookups) and X-Cache-Age (whole seconds, the oldest) on this response. Only the
    status word and a number: no key, domain or data. No-op outside a request."""
    m = _ctx.get()
    if m is not None:
        n = m.setdefault("notes", {})
        if _CACHE_RANK.get(status, 0) >= _CACHE_RANK.get(n.get("X-Cache"), -1):
            n["X-Cache"] = status
        n["X-Cache-Age"] = max(int(age), int(n.get("X-Cache-Age", 0)))


def _unavailable(request, exc):
    log.warning("database unavailable for %s %s: %s", request.method, request.url.path, type(exc).__name__)
    return JSONResponse({"detail": "database unavailable, retry shortly"}, status_code=503, headers={"Retry-After": "5"})


def install(app) -> None:
    app.add_exception_handler(core.DatabaseUnavailable, _unavailable)  # never an empty or partial 200
    app.add_exception_handler(psycopg.OperationalError, _unavailable)
    @app.middleware("http")
    async def server_timing(request, call_next):
        m = {"connect_ms": 0.0, "sql_ms": 0.0, "fetch_ms": 0.0, "conns": 0}
        token, wtoken, t0 = _ctx.set(m), core._WAIT.set(API_DB_WAIT), time.perf_counter()
        try:
            resp = await call_next(request)  # returns once the endpoint ran and its JSON body is rendered
        finally:
            _ctx.reset(token)
            core._WAIT.reset(wtoken)
        total = (time.perf_counter() - t0) * 1000
        other = max(0.0, total - m["connect_ms"] - m["sql_ms"] - m["fetch_ms"])
        resp.headers["Server-Timing"] = (f'db_connect;dur={m["connect_ms"]:.1f};desc="{m["conns"]} connection(s)", db_sql;dur={m["sql_ms"]:.1f}, '
                                         f'db_fetch;dur={m["fetch_ms"]:.1f}, app;dur={other:.1f};desc="python, serialization", total;dur={total:.1f}')
        for k, v in m.get("notes", {}).items():
            resp.headers[k] = str(v)
        if total >= SLOW_MS:
            route = getattr(request.scope.get("route"), "path", "unmatched")
            log.warning("SLOW-API %s %s %.0fms conns=%d connect=%.0f sql=%.0f fetch=%.0f", request.method, route, total, m["conns"],
                        m["connect_ms"], m["sql_ms"], m["fetch_ms"])
        return resp

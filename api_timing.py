"""Per-request timing for the read API: connection time, SQL execution, result fetching, total -> `Server-Timing` header.

API-only: the crawler/engine never imports this module, and `core.connect()` itself is untouched. Inside an HTTP request (see `install`)
`connect()` returns a connection whose cursors add their durations to that request's totals; anywhere else (direct calls, tests, the
engine) it is plain `core.connect()`. SQL text and parameters are never recorded, and the slow-request log line carries the route
template, never the query string."""

import contextvars
import logging
import os
import time

import crawler_core as core

log = logging.getLogger("crawler.api")
SLOW_MS = float(os.environ.get("API_SLOW_MS", 1000))  # one WARNING line per request slower than this; nothing is logged below it
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


def install(app) -> None:
    @app.middleware("http")
    async def server_timing(request, call_next):
        m = {"connect_ms": 0.0, "sql_ms": 0.0, "fetch_ms": 0.0, "conns": 0}
        token, t0 = _ctx.set(m), time.perf_counter()
        try:
            resp = await call_next(request)  # returns once the endpoint ran and its JSON body is rendered
        finally:
            _ctx.reset(token)
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

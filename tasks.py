"""Celery tasks around the existing crawler. They add distribution and recovery, not new crawling logic:
claim_urls (autonomous.py) leases a batch in PostgreSQL, engine.crawl_round fetches/parses/stores it, release() returns leftovers.

Idempotent by construction: a task never carries URLs, it claims whatever is pending *now* from PostgreSQL. A duplicate or
redelivered task therefore claims nothing it should not, and a worker that died mid-task leaves leases that expire and are
released by recover() (never blindly re-queued)."""

import asyncio
import logging
import time

import psycopg
import redis
from celery import signals

import autonomous
import crawler_core as core
import crawler_engine as engine
from celery_app import BROKER_URL, MAX_QUEUED, MAX_RETRIES, RETRY_BACKOFF_MAX, TASK_TIME_LIMIT, app

log = logging.getLogger("crawler.celery")
RETRYABLE = (psycopg.OperationalError, TimeoutError, *core.TRANSIENT)  # temporary database trouble only
_redis: redis.Redis | None = None
_t0: dict[str, float] = {}


def r() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(BROKER_URL, socket_timeout=5, socket_connect_timeout=5)
    return _redis


def queue_length() -> int:
    return int(r().llen("celery"))


def dispatch(force: bool = False) -> int:
    """Enqueue crawl tasks for pending work, never more than MAX_QUEUED waiting. Cheap and safe to call from anywhere (the Engine's
    crawler loop, the end of a crawl task): PostgreSQL decides what each task actually gets."""
    if autonomous.is_paused() and not force:
        return 0
    try:  # one dispatcher at a time: the queue-length check and the enqueue below must not interleave with another process's
        with r().lock("bl:dispatch", timeout=30, blocking_timeout=2):
            return _dispatch()
    except redis.exceptions.LockError:
        return 0  # someone else is dispatching right now


def _dispatch() -> int:
    with core.connect() as c:  # LIMIT keeps this cheap on a frontier of hundreds of thousands of rows
        avail = c.execute("SELECT COUNT(*) n FROM (SELECT 1 FROM discovered_urls u JOIN discovered_domains d ON d.domain=u.domain"
                          " WHERE (u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?))"
                          " AND d.discovery_status='admitted' LIMIT 1000) t", (core.now(),)).fetchone()["n"]
    queued = queue_length()
    n = min(max(0, MAX_QUEUED - queued), -(-avail // autonomous.AutoConfig().claim_batch))
    for _ in range(n):
        crawl_batch.delay()
    if n:
        log.info("dispatched %d crawl task(s) (pending>=%d, queued before=%d)", n, avail, queued)
    return n


@app.task(bind=True, name="crawl.batch", autoretry_for=RETRYABLE, retry_backoff=True, retry_backoff_max=RETRY_BACKOFF_MAX,
          retry_jitter=True, max_retries=MAX_RETRIES)
def crawl_batch(self) -> int:
    """Claim one batch (FOR UPDATE SKIP LOCKED), crawl it with the existing engine, release whatever was not finished."""
    owner = f"celery:{self.request.hostname}:{self.request.id}"
    cfg = autonomous.AutoConfig()
    cfg.lease_seconds = TASK_TIME_LIMIT  # a lease outlives its task only if the worker died
    work = []
    try:
        work = autonomous.claim_urls(owner, cfg)
        if work:
            # The pool may not enforce time limits (Windows solo), so enforce the soft limit here, on every platform.
            asyncio.run(asyncio.wait_for(autonomous.with_lease(  # lease renewal: a slow batch keeps its URLs
                owner, cfg, engine.crawl_round(cfg.crawl, None, work, engine._approved(), False, len(work))), TASK_TIME_LIMIT - 60))
        return len(work)
    finally:
        left = autonomous.release(owner)  # on success nothing is left; on error/timeout unfinished URLs go back to pending
        log.info("crawl.batch %s: claimed %d, returned unfinished %d", self.request.id, len(work), left)
        if work:  # more may be waiting: keep the pipeline full without an external scheduler
            try:
                dispatch()
            except Exception:  # noqa: BLE001 - the Engine's dispatcher will catch up
                log.warning("re-dispatch failed", exc_info=True)


@app.task(name="crawl.recover")
def recover() -> int:
    """Give back leases of dead workers (expired and still 'crawling'/'recrawling'). Checks PostgreSQL; requeues nothing blindly."""
    n = autonomous.release()
    if n:
        log.warning("recovered %d URL(s) leased by dead workers", n)
    return n


# ---------- monitoring: counters in Redis, one log line per event ----------

def _count(key: str, by: float = 1) -> None:
    try:
        r().incrbyfloat(f"bl:{key}", by)
    except redis.RedisError:
        pass


@signals.task_prerun.connect
def _pre(task_id=None, **kw):
    _t0[task_id] = time.monotonic()


@signals.task_postrun.connect
def _post(task_id=None, task=None, state=None, **kw):
    if task is not None and task.name == "crawl.batch":
        _count("done")
        _count("seconds", time.monotonic() - _t0.pop(task_id, time.monotonic()))


@signals.task_retry.connect
def _retry(request=None, reason=None, **kw):
    _count("retries")
    log.warning("task %s retry: %r", getattr(request, "id", "?"), reason)


@signals.task_failure.connect
def _fail(task_id=None, exception=None, **kw):
    _count("failures")
    log.error("task %s FAILED: %r", task_id, exception)


@signals.worker_shutting_down.connect
def _bye(sig=None, how=None, exitcode=None, **kw):
    """Graceful shutdown: hand back this worker's unfinished leases right away instead of waiting for them to expire."""
    from celery.utils.nodenames import gethostname
    try:
        with core.connect() as c:
            n = c.execute("UPDATE discovered_urls SET crawl_status='pending', lease_owner=NULL, lease_expires=NULL"
                          " WHERE crawl_status='crawling' AND lease_owner LIKE ?", (f"celery:%{gethostname()}%",)).rowcount
        log.info("worker shutting down (%s): released %d lease(s)", how, n)
    except Exception:  # noqa: BLE001
        log.warning("could not release leases on shutdown; they expire on their own", exc_info=True)


def celery_stats() -> dict:
    """For the 10-second DIAG line: queue length, workers/active tasks (Celery inspect, 1s), retries, failures, average task time."""
    out = {"queue_length": queue_length()}
    g = lambda k: float(r().get(f"bl:{k}") or 0)  # noqa: E731
    out.update(retries=int(g("retries")), failures=int(g("failures")), tasks_done=int(g("done")),
               avg_task_s=round(g("seconds") / g("done"), 1) if g("done") else None)
    active = app.control.inspect(timeout=1).active() or {}
    out.update(workers=len(active), active_tasks=sum(len(v) for v in active.values()))
    return out

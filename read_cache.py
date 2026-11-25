"""Per-process, in-memory read cache for the backlink graph aggregates (summary, referring-domain pages, anchor pages).

Cache-aside with a TTL: the API asks `graph_cache.get(key, compute)`; a fresh entry is returned without touching PostgreSQL, otherwise ONE thread per
key runs `compute()` (single-flight) while the others for that key wait for its result. Different keys never wait for each other.

Design limits, on purpose:
  * per process: nothing is shared or signalled between API processes, and writers (engine, Celery workers) never talk to it. A total can therefore be
    up to GRAPH_CACHE_TTL_SECONDS behind the latest crawl write, and two API processes can differ by up to that long. There is no invalidation.
  * disabled unless GRAPH_CACHE_ENABLED is true; disabled, `get` is exactly `compute()`.
  * bounded: GRAPH_CACHE_MAX_ENTRIES entries AND GRAPH_CACHE_MAX_MB of (estimated) memory, least-recently-used out first; one value bigger than the
    whole budget is not cached at all.
  * stale on failure only: when a refresh raises and the old entry is younger than GRAPH_CACHE_MAX_STALE_SECONDS, the old value is served (status STALE)
    and the database is not retried for GRAPH_CACHE_RETRY_SECONDS; older than that, the exception reaches the caller. A successful refresh resets the age.
  * values are deep-copied in and out: a caller can never change what the cache holds.
Memory estimate: sys.getsizeof summed over the value's containers and scalars (an estimate, not an exact RSS figure)."""

import copy
import logging
import os
import sys
import threading
import time
from collections import OrderedDict

log = logging.getLogger("crawler.cache")

HIT, MISS, STALE, OFF = "HIT", "MISS", "STALE", "OFF"


def _flag(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "on")


def estimate_size(obj, _seen=None) -> int:
    """Approximate bytes of a JSON-like value (dict / list / tuple / set of scalars): getsizeof of every distinct object."""
    seen = _seen if _seen is not None else set()
    if id(obj) in seen:
        return 0
    seen.add(id(obj))
    n = sys.getsizeof(obj)
    if isinstance(obj, dict):
        n += sum(estimate_size(k, seen) + estimate_size(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        n += sum(estimate_size(v, seen) for v in obj)
    return n


class _Entry:
    __slots__ = ("value", "born", "size", "retry_at")

    def __init__(self, value, born: float, size: int):
        self.value, self.born, self.size, self.retry_at = value, born, size, 0.0


class _Flight:
    """One refresh in progress for one key; followers wait on `done`."""
    __slots__ = ("done", "value", "error")

    def __init__(self):
        self.done, self.value, self.error = threading.Event(), None, None


class TTLCache:
    def __init__(self, enabled: bool = False, ttl: float = 15, max_ttl: float = 30, max_entries: int = 1000, max_bytes: int = 32 * 1024 * 1024,
                 max_stale: float = 120, retry: float = 5, clock=time.monotonic):
        self.max_ttl = max(0.0, float(max_ttl))
        self.ttl = min(max(0.0, float(ttl)), self.max_ttl)  # never above the configured ceiling
        self.enabled = bool(enabled) and self.ttl > 0
        self.max_entries, self.max_bytes = max(1, int(max_entries)), max(1, int(max_bytes))
        self.max_stale, self.retry, self.clock = float(max_stale), float(retry), clock
        self.no_stale: tuple = ()  # exception types that must reach the caller even when an old entry exists (the API: database unavailable)
        self._init_state()

    @classmethod
    def from_env(cls) -> "TTLCache":
        e = os.environ.get
        return cls(enabled=_flag("GRAPH_CACHE_ENABLED", False), ttl=float(e("GRAPH_CACHE_TTL_SECONDS", 15)), max_ttl=float(e("GRAPH_CACHE_MAX_TTL_SECONDS", 30)),
                   max_entries=int(e("GRAPH_CACHE_MAX_ENTRIES", 1000)), max_bytes=int(float(e("GRAPH_CACHE_MAX_MB", 32)) * 1024 * 1024),
                   max_stale=float(e("GRAPH_CACHE_MAX_STALE_SECONDS", 120)), retry=float(e("GRAPH_CACHE_RETRY_SECONDS", 5)))

    def _init_state(self) -> None:
        self._lock = threading.Lock()
        self._entries: OrderedDict = OrderedDict()  # key -> _Entry, least recently used first
        self._flights: dict = {}
        self._bytes = 0
        self._c = dict(hits=0, misses=0, stale=0, errors=0, evictions=0, expired=0, oversize=0, refreshes=0, coalesced=0)

    def reset_after_fork(self) -> None:
        """A forked child must not inherit a lock another thread of the parent held, nor the parent's entries: it starts empty and independent."""
        self._init_state()

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes = 0

    def drop(self, key) -> None:
        with self._lock:
            self._drop(key)

    # ---- public ----
    def get(self, key, compute):
        """(value, status, age_seconds). status: HIT | MISS | STALE | OFF (cache disabled: compute() every time, nothing recorded).
        compute() runs at most once per key at a time; its exception propagates unless a young enough old value can be served instead."""
        if not self.enabled:
            return compute(), OFF, 0.0
        while True:
            with self._lock:
                now = self.clock()
                e = self._entries.get(key)
                if e is not None and now - e.born >= self.max_stale + self.ttl:
                    self._drop(key)  # too old even for stale serving
                    self._c["expired"] += 1
                    e = None
                if e is not None and now - e.born < self.ttl:
                    self._entries.move_to_end(key)
                    self._c["hits"] += 1
                    return copy.deepcopy(e.value), HIT, now - e.born
                if e is not None and now < e.retry_at:  # a refresh failed a moment ago: keep serving the old value, do not hammer the database
                    self._entries.move_to_end(key)
                    self._c["stale"] += 1
                    return copy.deepcopy(e.value), STALE, now - e.born
                flight = self._flights.get(key)
                leader = flight is None
                if leader:
                    flight = self._flights[key] = _Flight()
                else:
                    self._c["coalesced"] += 1
            if leader:
                return self._refresh(key, compute, flight)
            flight.done.wait()
            if flight.error is None:
                with self._lock:
                    self._c["hits"] += 1
                return copy.deepcopy(flight.value), HIT, 0.0
            # the leader failed: serve the old value if it is young enough, else the same exception
            with self._lock:
                now = self.clock()
                e = self._entries.get(key)
                if e is not None and now - e.born < self.ttl + self.max_stale and not isinstance(flight.error, self.no_stale):
                    self._c["stale"] += 1
                    return copy.deepcopy(e.value), STALE, now - e.born
            raise flight.error

    def stats(self) -> dict:
        with self._lock:
            c = dict(self._c)
            c.update(entries=len(self._entries), bytes=self._bytes, enabled=self.enabled, ttl=self.ttl, max_entries=self.max_entries, max_bytes=self.max_bytes)
            looked = c["hits"] + c["misses"] + c["stale"]
            c["hit_ratio"] = c["hits"] / looked if looked else 0.0
            return c

    # ---- internals ----
    def _refresh(self, key, compute, flight: _Flight):
        try:
            value = compute()
        except BaseException as exc:  # noqa: BLE001 - always release the followers, then decide between stale and raising
            flight.error = exc
            with self._lock:
                self._c["errors"] += 1
                self._flights.pop(key, None)
                now = self.clock()
                e = self._entries.get(key)
                if e is not None and now - e.born < self.ttl + self.max_stale and isinstance(exc, Exception) and not isinstance(exc, self.no_stale):
                    e.retry_at = now + self.retry
                    self._c["stale"] += 1
                    flight.done.set()
                    log.warning("graph cache refresh failed for %s; serving the entry from %.0fs ago", key[0], now - e.born, exc_info=True)
                    return copy.deepcopy(e.value), STALE, now - e.born
            flight.done.set()
            raise
        stored = copy.deepcopy(value)
        size = estimate_size(stored)
        with self._lock:
            self._flights.pop(key, None)
            self._c["misses"] += 1
            self._c["refreshes"] += 1
            self._drop(key)  # a success replaces the old entry: its age and its failure marker are gone
            if size > self.max_bytes:
                self._c["oversize"] += 1
            else:
                self._entries[key] = _Entry(stored, self.clock(), size)
                self._bytes += size
                while len(self._entries) > self.max_entries or self._bytes > self.max_bytes:
                    old, _ = next(iter(self._entries.items()))
                    self._drop(old)
                    self._c["evictions"] += 1
        flight.value = value
        flight.done.set()
        return copy.deepcopy(value), MISS, 0.0

    def _drop(self, key) -> None:  # caller holds the lock
        e = self._entries.pop(key, None)
        if e is not None:
            self._bytes -= e.size


graph_cache = TTLCache.from_env()
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=lambda: graph_cache.reset_after_fork())

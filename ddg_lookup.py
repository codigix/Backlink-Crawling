"""DuckDuckGo (ddgs) candidate lookup for a business: URLs of other sites' pages that may link to it. A result is only a candidate;
a backlink exists only once the crawler finds the hyperlink.

Continuous discovery: every (query variation, backend) has its own saved page cursor (discovery_cursors). A run continues from it and reads
until `target` URLs NEW to the business were found. ddgs page offsets differ per engine (DuckDuckGo 10+(p-2)*15, Bing (p-1)*10, Yahoo (p-1)*7+1,
Brave p-1), so a cursor is pinned to one backend and a backend never inherits another one's offset."""
import hashlib
import os
import random
import time

from ddgs import DDGS
from ddgs.exceptions import DDGSException

import crawler_core as core


BACKENDS = [b.strip() for b in os.environ.get("DDGS_BACKENDS", "duckduckgo,brave,bing,yahoo").split(",") if b.strip()]  # first usable one serves a query
MAX_PAGES_PER_QUERY = int(os.environ.get("DDGS_MAX_PAGES_PER_QUERY", 5))  # pages one run reads per query and backend (10 results each)
DUP_PAGES_LIMIT = int(os.environ.get("DDGS_DUP_PAGES_LIMIT", 3))  # consecutive pages without a new URL: the query is exhausted on that backend
REDISCOVERY_SECONDS = int(float(os.environ.get("DDGS_REDISCOVERY_DAYS", 7)) * 86400)  # an exhausted query is read again from page 1 after this long
EMPTY_CONFIRM = int(os.environ.get("DDGS_EMPTY_CONFIRM_RUNS", 2))  # a run that STARTS on an empty page may be a blocked client: exhausted only after this many such runs
SEARCH_BUDGET = int(os.environ.get("DDGS_SEARCH_TIMEOUT_SECONDS", 180))  # wall clock for one whole search: it holds a global search slot


def queries(domain: str, name: str | None = None) -> list[str]:
    q = [f'"{domain}" -site:{domain}', f'"www.{domain}" -site:{domain}', f'"https://{domain}" -site:{domain}']
    if name and name.strip().lower() != domain:
        q += [f'"{name.strip()}" -site:{domain}', f'"{name.strip()}" "{domain}" -site:{domain}']
    return q


def qhash(query: str) -> str:
    return hashlib.sha1(query.encode()).hexdigest()[:16]


class Run:
    """What one search read and learned. The caller stores it in the SAME transaction as the URLs, so a cursor never moves past a page whose URLs
    were not stored. cursors: {(query_hash, backend): row} from discovery_cursors; known: normalized URLs this target already has.
    persist=False is the stateless legacy mode (backend "auto", stops at the first page without news)."""

    def __init__(self, cursors: dict | None = None, known: set[str] | None = None, persist: bool = True):
        self.persist, self.cursors, self.known = persist, cursors or {}, known or set()
        self.hits: list[tuple[str, str, bool]] = []  # (url, query_hash, new for this target)
        self.updates: dict[tuple[str, str], dict] = {}  # cursor rows to upsert
        self.waiting_until: str | None = None  # every cursor exhausted and none due: the earliest rediscovery time

    def cursor(self, qh: str, backend: str, query: str) -> dict:
        row = self.updates.get((qh, backend)) or self.cursors.get((qh, backend))
        return dict(row) if row else {"query": query, "next_page": 1, "status": "active", "dup_streak": 0, "pages_done": 0, "urls_found": 0,
                                      "exhausted_at": None, "rediscover_after": None, "last_ok_at": None, "empty_streak": 0}


def _is_empty(e: Exception) -> bool:
    """ddgs signals "no result" by RAISING DDGSException("No results found.") (all engines empty, none failed); any other text is an engine failure."""
    return type(e) is DDGSException and str(e).strip().lower().startswith("no results found")


def _page(query: str, page: int, attempts: int, end: float = float("inf"), backend: str = "auto") -> list[dict]:
    """One result page. Every attempt uses a fresh client, after an exponential pause with jitter (some engines refuse with 429/403 or answer
    "No results" for a query another one serves). Returns [] when every attempt came back empty; raises the last error when it was a failure
    (rate limit, timeout, transport): a failure is never an empty page."""
    for n in range(attempts):
        try:
            with DDGS(timeout=10) as ddgs:
                return ddgs.text(query, region="in-en", safesearch="moderate", max_results=10, page=page, backend=backend)
        except Exception as e:  # noqa: BLE001
            last = n == attempts - 1 or time.time() > end
            if last and _is_empty(e):
                return []
            if last:
                raise
            time.sleep(2 ** (n + 1) + random.random())
    return []


def _exhaust(cur: dict) -> None:
    cur.update(status="exhausted", exhausted_at=core.now(), rediscover_after=core.later(seconds=REDISCOVERY_SECONDS))


def _usable(cur: dict) -> bool:
    """Active, or exhausted long enough ago to be read again from the start (controlled rediscovery)."""
    if cur["status"] != "exhausted":
        return True
    if cur["rediscover_after"] and cur["rediscover_after"] <= core.now():
        cur.update(status="active", next_page=1, dup_streak=0, exhausted_at=None, rediscover_after=None)
        return True
    return False


def _read(run: Run, q: str, qh: str, backend: str, cur: dict, pages: int, own: str, found: dict, seen: set, target: int, end: float) -> bool:
    """Read up to `pages` pages of one query on one backend, from its cursor; True if a page had results. A failing page raises and leaves the cursor ON that page."""
    got = False
    kw = {"backend": backend} if run.persist else {}
    for _ in range(pages):
        if time.time() > end:
            raise TimeoutError("search budget exhausted")
        page = cur["next_page"]
        results = _page(q, page, 4 if page == 1 else 2, end, **kw)
        hrefs = [r["href"] for r in results or [] if r.get("href")]
        if not hrefs:
            if page > 1 and run.persist:
                if got:  # this run already read a page from this engine: it is answering, so the empty page is the real end of the results
                    _exhaust(cur)
                else:  # the run STARTED here: a blocked or limited client looks the same, so it takes repeated runs to call it the end
                    cur["empty_streak"] = cur.get("empty_streak", 0) + 1
                    if cur["empty_streak"] >= EMPTY_CONFIRM:
                        _exhaust(cur)
                run.updates[(qh, backend)] = dict(cur)
            return got  # an empty page 1 may be a transient "No results" of one engine: never "exhausted"
        got = True
        new = 0
        for h in hrefs:
            norm = core.normalize_url(h)
            if not norm or core.registrable_domain(core.hostname(h)) == own:
                continue
            fresh = norm not in run.known and norm not in seen
            seen.add(norm)
            run.hits.append((h, qh, fresh))  # a known URL still records WHICH query found it again
            if fresh:
                found[h] = None
                new += 1
        cur["dup_streak"] = 0 if new else cur["dup_streak"] + 1
        cur.update(empty_streak=0, next_page=page + 1, pages_done=cur["pages_done"] + 1, urls_found=cur["urls_found"] + new, last_ok_at=core.now())
        if cur["dup_streak"] >= (DUP_PAGES_LIMIT if run.persist else 1):
            if run.persist:
                _exhaust(cur)
            run.updates[(qh, backend)] = dict(cur)
            return got
        run.updates[(qh, backend)] = dict(cur)
        if len(found) >= target:
            return got
        time.sleep(1 + random.random())
    return got


def search_urls(domain: str, target: int = 50, name: str | None = None, pages: int | None = None, budget: float | None = None,
                run: Run | None = None) -> list[str]:
    """Result URLs new to the business (excluding its own pages), `target` or more (the last page is kept whole), over several query variations.
    With a Run each (query, backend) continues from its saved page; without one it is the stateless search of old. Raises when NOTHING was found
    and a request failed (a provider failure is never "no backlinks"); an empty answer with no error returns []."""
    domain = domain.strip().lower()
    own = core.registrable_domain(domain)
    run = run or Run(persist=False)
    pages = pages or MAX_PAGES_PER_QUERY
    found: dict[str, None] = {}
    seen: set[str] = set()
    err: Exception | None = None
    end = time.time() + (SEARCH_BUDGET if budget is None else budget)
    backends = BACKENDS if run.persist else ["auto"]
    qs = queries(domain, name)
    dead: set[str] = set()
    for q in qs:
        if len(found) >= target:
            break
        qh = qhash(q)
        for b in backends:
            if b in dead:  # it already failed in this run without answering: do not spend the budget on its timeouts again
                continue
            cur = run.cursor(qh, b, q)  # a fallback backend continues from ITS OWN saved page (page 1 when it has none), never from another engine's offset
            if not _usable(cur):
                continue
            if time.time() > end:
                err = err or TimeoutError(f"search budget exhausted ({SEARCH_BUDGET if budget is None else budget:.0f}s)")
                break
            try:
                got = _read(run, q, qh, b, cur, pages, own, found, seen, target, end)
            except Exception as e:  # noqa: BLE001 - remembered; the next backend or query may use another engine
                err = e
                if (qh, b) in run.updates:  # it read pages before failing: keep that progress, do not start over elsewhere
                    break
                dead.add(b)
                continue
            if len(found) >= target or (got and run.updates.get((qh, b), cur)["status"] != "exhausted"):
                break  # served; an empty or exhausted backend hands the query to the next one
    if run.persist:  # every (query, backend) cursor exhausted and none due yet: nothing to read until the earliest rediscovery time
        rows = [run.cursor(qhash(q), b, q) for q in qs for b in backends]
        if all(r["status"] == "exhausted" and r["rediscover_after"] > core.now() for r in rows):
            run.waiting_until = min(r["rediscover_after"] for r in rows)
    if not found and err and not run.updates:  # pages that were read (even with nothing new) are progress to keep; no page at all is a failure
        raise err
    return list(found) if run.persist else list(found)[:target]

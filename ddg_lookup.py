"""DuckDuckGo (ddgs) candidate lookup for a business: URLs of other sites' pages that may link to it. A result is only a candidate;
a backlink exists only once the crawler finds the hyperlink.

Continuous discovery: every (query variation, backend) has its own saved page cursor (discovery_cursors). A run continues from it and reads
until `target` URLs NEW to the business were found. ddgs page offsets differ per engine (DuckDuckGo 10+(p-2)*15, Bing (p-1)*10, Yahoo (p-1)*7+1,
Brave p-1), so a cursor is pinned to one backend and a backend never inherits another one's offset."""
import hashlib
import logging
import os
import random
import re
import time
from collections import deque
from urllib.parse import parse_qs, quote_plus, urlparse

import httpx
from ddgs import DDGS
from ddgs.exceptions import DDGSException

import crawler_core as core

log = logging.getLogger("crawler.search")


def _backends_from_env() -> list[str]:
    b = [x.strip() for x in os.environ.get("DDGS_BACKENDS", "duckduckgo,brave,bing,yahoo").split(",") if x.strip()]  # first usable one serves a query
    if os.environ.get("SERPER_API_KEY") and "serper" not in b:
        b.append("serper")  # last resort: paid credits, used only when the free engines answered nothing
    if os.environ.get("DOGPILE", "1") != "0" and "dogpile" not in b:
        b.insert(0, "dogpile")  # primary engine (headless browser); DOGPILE=0 turns it off
    return b


BACKENDS = _backends_from_env()
SERPER_DAILY_LIMIT = int(os.environ.get("SERPER_DAILY_LIMIT", 1000))  # serper.dev credits are paid: one per page request; 0 = no limit
_serper_day: list = ["", 0]  # [UTC day, requests that day] of this process
MAX_PAGES_PER_QUERY = int(os.environ.get("DDGS_MAX_PAGES_PER_QUERY", 5))  # pages one run reads per query and backend (10 results each)
DUP_PAGES_LIMIT = int(os.environ.get("DDGS_DUP_PAGES_LIMIT", 3))  # consecutive pages without a new URL: the query is exhausted on that backend
REDISCOVERY_SECONDS = int(float(os.environ.get("DDGS_REDISCOVERY_DAYS", 3)) * 86400)  # an exhausted query is read again from page 1 after this long (businesses.rediscovery_days overrides it per target)
EMPTY_CONFIRM = int(os.environ.get("DDGS_EMPTY_CONFIRM_RUNS", 2))  # a run that STARTS on an empty page may be a blocked client: exhausted only after this many such runs
HEAD_RECHECK_SECONDS = int(float(os.environ.get("DDGS_HEAD_RECHECK_HOURS", 24)) * 3600)  # a cursor deep in the results re-reads pages 1..HEAD_PAGES this often: new pages rank near the top
HEAD_PAGES = 2
HEAD_JITTER = 0.25  # a stamped head re-scan is due up to 25% early, so targets that were scanned together drift apart
HOURLY_CAP = int(os.environ.get("DDGS_MAX_REQUESTS_PER_HOUR", 1000))  # page requests per hour of this process, all targets; 0 = no cap
HEAD_CAP_SHARE = 0.5  # head re-scans and rediscovery are optional work: they stop at this share of the cap and leave the rest to the normal reads
_sent: deque = deque()  # epoch of every page request of the last hour
BACKEND_COOLDOWN = int(os.environ.get("DDGS_BACKEND_COOLDOWN_SECONDS", 600))  # a backend that answered nothing in BACKEND_STRIKES searches in a row is skipped this long
BACKEND_STRIKES = 2
_health: dict[str, list] = {}  # backend -> [strikes in a row, skipped until (epoch)]; per process: a search holds the one slot, so a dead engine's timeouts must not be paid again by every search
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

    def __init__(self, cursors: dict | None = None, known: set[str] | None = None, persist: bool = True, rediscovery_seconds: int | None = None):
        self.persist, self.cursors, self.known = persist, cursors or {}, known or set()
        self.rediscovery_seconds = rediscovery_seconds or REDISCOVERY_SECONDS
        self.hits: list[tuple[str, str, bool]] = []  # (url, query_hash, new for this target)
        self.updates: dict[tuple[str, str], dict] = {}  # cursor rows to upsert
        self.waiting_until: str | None = None  # every cursor exhausted and none due: the earliest rediscovery time

    def cursor(self, qh: str, backend: str, query: str) -> dict:
        row = self.updates.get((qh, backend)) or self.cursors.get((qh, backend))
        return dict(row) if row else {"query": query, "next_page": 1, "status": "active", "dup_streak": 0, "pages_done": 0, "urls_found": 0,
                                      "exhausted_at": None, "rediscover_after": None, "last_ok_at": None, "empty_streak": 0, "head_checked_at": None}


def _is_empty(e: Exception) -> bool:
    """ddgs signals "no result" by RAISING DDGSException("No results found.") (all engines empty, none failed); any other text is an engine failure."""
    return type(e) is DDGSException and str(e).strip().lower().startswith("no results found")


class SerperStop(RuntimeError):
    """Retrying cannot help: the daily limit, or a refused key / out of credits."""


class SerperCreditsExhausted(SerperStop):
    """Account out of credits (HTTP 400 'Not enough credits' or HTTP 402)."""


class SerperAuthError(SerperStop):
    """Invalid or refused API key (HTTP 401, 403)."""


class SerperRateLimit(RuntimeError):
    """Temporary rate limit (HTTP 429)."""


def _serper_post(payload: dict) -> dict:
    key = os.environ.get("SERPER_API_KEY", "")
    r = httpx.post("https://google.serper.dev/search", json=payload, headers={"X-API-KEY": key}, timeout=10)
    if r.status_code == 429:
        raise SerperRateLimit("serper rate limited (HTTP 429)")
    if r.status_code == 402:
        raise SerperCreditsExhausted("serper credits exhausted (HTTP 402)")
    if r.status_code in (401, 403):
        raise SerperAuthError(f"serper refused the request (HTTP {r.status_code})")
    if r.status_code == 400:
        msg = ""
        try:
            msg = r.text.lower()
        except Exception:
            pass
        if "credit" in msg or "not enough" in msg:
            raise SerperCreditsExhausted("serper credits exhausted (HTTP 400)")
        raise SerperStop(f"serper refused the request (HTTP 400)")
    r.raise_for_status()  # the message names the URL and status, never the key
    return r.json()


def _serper(query: str, page: int) -> list[dict]:
    """One serper.dev (Google) result page as [{"href": ...}]. [] = Google has no more results; anything else wrong raises (a failure is never an empty page)."""
    if not os.environ.get("SERPER_API_KEY"):
        raise SerperStop("SERPER_API_KEY not configured")
    day = core.now()[:10]
    if _serper_day[0] != day:
        _serper_day[:] = [day, 0]
    if SERPER_DAILY_LIMIT and _serper_day[1] >= SERPER_DAILY_LIMIT:
        raise SerperStop(f"serper daily limit of {SERPER_DAILY_LIMIT} requests reached")
    _serper_day[1] += 1
    q = query if os.environ.get("SERPER_QUOTES") == "1" else query.replace('"', "")  # free serper accounts refuse quoted queries; SERPER_QUOTES=1 on a paid plan
    try:
        data = _serper_post({"q": q, "page": page, "num": 10, "gl": "in"})
    except (SerperCreditsExhausted, SerperAuthError) as e:
        _health["serper"] = [BACKEND_STRIKES, time.time() + 86400]
        log.warning("search: serper is unavailable (%s); putting in cooldown for 24h", e)
        raise
    return [{"href": x["link"], "title": x.get("title", ""), "body": x.get("snippet", "")} for x in data.get("organic", []) if x.get("link")]


def _dogpile_url(href: str) -> str:
    """Dogpile may wrap a result in its own redirect: pull the real URL out of it."""
    p = urlparse(href)
    if "dogpile.com" in p.netloc:
        for k in ("ru", "url", "u"):
            if k in (qs := parse_qs(p.query)):
                return qs[k][0]
    return href


def _dogpile(query: str, page: int) -> list[dict]:
    """One dogpile.com result page as [{"href": ...}], read with a headless browser (one per call: the search slot is global, so no sharing).
    [] = the page held no result link; a browser or navigation error raises."""
    from playwright.sync_api import sync_playwright  # imported here: the module must load where playwright is not installed

    query = re.sub(r"\s*-site:\S+", "", query)  # dogpile answers nothing to -site:; the target's own pages are dropped later by _collect

    out, seen, started = [], set(), time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                                      locale="en-US", viewport={"width": 1366, "height": 850})
            pg = ctx.new_page()
            pg.goto(f"https://www.dogpile.com/serp?q={quote_plus(query)}&page={page}", wait_until="domcontentloaded", timeout=60000)
            # Wait until the results have loaded: /serp redirects to /search and first shows "Loading search results"; reading then looked "empty".
            body, deadline = "", time.time() + 30
            while time.time() < deadline:
                try:
                    body = pg.inner_text("body")
                except Exception:  # noqa: BLE001 - the redirect replaced the page mid-read
                    body = ""
                if body and "loading search results" not in body.lower():
                    break
                pg.wait_for_timeout(500)
            else:
                raise RuntimeError("dogpile: results did not finish loading")  # a failure, never an empty (= exhausted) page
            if "temporarily unavailable" in body.lower():  # dogpile's throttle page: a failure, never an empty (= exhausted) page
                raise RuntimeError("dogpile: search results temporarily unavailable (throttled)")
            # one browser round trip for all links: per-link calls (~100 per page) crawled while the engine's crawler held the CPU
            for href, text in pg.eval_on_selector_all("a[href^='http']", "els => els.map(a => [a.getAttribute('href') || '', a.innerText || ''])"):
                real = _dogpile_url(href)
                host = urlparse(real).netloc
                text = text.strip()
                if not host or "dogpile.com" in host or len(text) < 15 or real in seen:  # menu links and dogpile's own links are short or internal
                    continue
                seen.add(real)
                out.append({"href": real, "title": text.replace("\n", " ")[:150], "body": ""})
            if not out:
                log.warning("dogpile: no result links on %s (%s): %s", pg.url, pg.title(), " | ".join(body.split("\n"))[:300])
        finally:
            browser.close()
    if time.time() - started > 15:
        log.warning("dogpile: page %d took %.0fs for %s", page, time.time() - started, query)
    return out


def _page(query: str, page: int, attempts: int, end: float = float("inf"), backend: str = "auto") -> list[dict]:
    """One result page. Every attempt uses a fresh client, after an exponential pause with jitter (some engines refuse with 429/403 or answer
    "No results" for a query another one serves). Returns [] when every attempt came back empty; raises the last error when it was a failure
    (rate limit, timeout, transport): a failure is never an empty page."""
    for n in range(attempts):
        try:
            if backend == "serper":
                return _serper(query, page)
            if backend == "dogpile":
                return _dogpile(query, page)
            with DDGS(timeout=10) as ddgs:
                return ddgs.text(query, region="in-en", safesearch="moderate", max_results=10, page=page, backend=backend)
        except Exception as e:  # noqa: BLE001
            last = n == attempts - 1 or time.time() > end or isinstance(e, SerperStop)
            if last and _is_empty(e):
                return []
            if last:
                raise
            time.sleep(2 ** (n + 1) + random.random())
    return []


def _cooling(backends: list[str]) -> set[str]:
    now = time.time()
    cool = {b for b in backends if _health.get(b, [0, 0])[1] > now}
    return set() if len(cool) == len(backends) else cool  # all of them cooling: try anyway, a search that tries nothing finds nothing


def _note(backend: str, answered: bool) -> None:
    if answered:
        _health.pop(backend, None)
        return
    h = _health.setdefault(backend, [0, 0.0])
    h[0] += 1
    if h[0] >= BACKEND_STRIKES:
        h[1] = time.time() + BACKEND_COOLDOWN


def _room(share: float = 1.0) -> bool:
    """Is there room for one more page request under the hourly cap (`share` of it for optional work)?"""
    cut = time.time() - 3600
    while _sent and _sent[0] < cut:
        _sent.popleft()
    return HOURLY_CAP <= 0 or len(_sent) < HOURLY_CAP * share


def _exhaust(cur: dict, seconds: int = REDISCOVERY_SECONDS) -> None:
    cur.update(status="exhausted", exhausted_at=core.now(), rediscover_after=core.later(seconds=seconds))


def _usable(cur: dict) -> bool:
    """Active, or exhausted long enough ago to be read again from the start (controlled rediscovery)."""
    if cur["status"] != "exhausted":
        return True
    if cur["rediscover_after"] and cur["rediscover_after"] <= core.now():
        cur.update(status="active", next_page=1, dup_streak=0, exhausted_at=None, rediscover_after=None)
        return True
    return False


# Adult (18+) sites never become crawl candidates: some engines (Bing, Yahoo) answer a quoted domain with porn pages that never link to a business.
ADULT_WORDS = ("porn", "xxx", "xnxx", "xvideos", "xhamster", "hentai")
ADULT_DOMAINS = {"redtube.com", "tube8.com", "spankbang.com", "beeg.com", "motherless.com", "chaturbate.com", "stripchat.com", "onlyfans.com",
                 "brazzers.com", "livejasmin.com", "bongacams.com", "cam4.com"}


def is_adult(url: str) -> bool:
    reg = core.registrable_domain(core.hostname(url))
    return reg in ADULT_DOMAINS or any(w in reg for w in ADULT_WORDS)


def simplify_query(query: str) -> str:
    """Safely strip problematic operators like -site:... for search engines that do not support them reliably."""
    return re.sub(r"\s*-site:\S+", "", query).strip()


def _collect(run: Run, hrefs: list[str], qh: str, own: str, found: dict, seen: set) -> int:
    """Record one page's URLs (a known URL still records WHICH query found it again); returns how many are new to the target."""
    new = 0
    for h in hrefs:
        norm = core.normalize_url(h)
        if not norm or core.registrable_domain(core.hostname(h)) == own or is_adult(h):
            continue
        fresh = norm not in run.known and norm not in seen
        seen.add(norm)
        run.hits.append((h, qh, fresh))
        if fresh:
            found[h] = None
            new += 1
    return new


def _head(run: Run, q: str, qh: str, backend: str, cur: dict, own: str, found: dict, seen: set, end: float) -> int:
    """Re-read the first pages of a query whose cursor is deep or exhausted: results that appeared since come in at the top. Only head_checked_at
    changes on the cursor here. Returns the number of URLs new to the target, or -1 when the scan did not complete (failed or empty page, no budget):
    nothing is stamped then and it is retried next run."""
    new = 0
    for page in range(1, HEAD_PAGES + 1):
        if time.time() > end or not _room(HEAD_CAP_SHARE):
            return -1
        _sent.append(time.time())
        try:
            hrefs = [r["href"] for r in _page(q, page, 2, end, backend=backend) or [] if r.get("href")]
        except Exception:  # noqa: BLE001
            return -1
        if not hrefs:
            return -1
        new += _collect(run, hrefs, qh, own, found, seen)
        time.sleep(1 + random.random())
    cur["head_checked_at"] = core.later(seconds=-random.uniform(0, HEAD_JITTER * HEAD_RECHECK_SECONDS))
    return new


def _read(run: Run, q: str, qh: str, backend: str, cur: dict, pages: int, own: str, found: dict, seen: set, target: int, end: float) -> bool:
    """Read up to `pages` pages of one query on one backend, from its cursor; True if a page had results. A failing page raises and leaves the cursor ON that page."""
    got = False
    kw = {"backend": backend} if run.persist else {}
    for _ in range(pages):
        if time.time() > end:
            raise TimeoutError("search budget exhausted")
        if not _room():
            return got  # hourly cap reached: the cursor stays where it is and the next run continues from it
        page = cur["next_page"]
        _sent.append(time.time())
        results = _page(q, page, 4 if page == 1 else 2, end, **kw)
        hrefs = [r["href"] for r in results or [] if r.get("href")]
        if not hrefs and page == 1:
            sq = simplify_query(q)
            if sq != q:
                simplified_results = _page(sq, page, 2, end, **kw)
                hrefs = [r["href"] for r in simplified_results or [] if r.get("href")]
                if hrefs:
                    log.info("search: %s succeeded with simplified query %r (original: %r)", backend, sq, q)
                    results = simplified_results
            if not hrefs:
                unquoted = sq.strip('"').strip()
                if unquoted != sq and unquoted != q:
                    unquoted_results = _page(unquoted, page, 2, end, **kw)
                    hrefs = [r["href"] for r in unquoted_results or [] if r.get("href")]
                    if hrefs:
                        log.info("search: %s succeeded with unquoted query %r (original: %r)", backend, unquoted, q)
                        results = unquoted_results
        if not hrefs:
            if page > 1 and run.persist:  # an empty page may be a flaky engine: the end of the results needs EMPTY_CONFIRM runs in a row to say so
                cur["empty_streak"] = cur.get("empty_streak", 0) + 1
                if cur["empty_streak"] >= EMPTY_CONFIRM:
                    _exhaust(cur, run.rediscovery_seconds)
                run.updates[(qh, backend)] = dict(cur)
            elif page == 1:
                log.warning("search: %s returned no results for %s; trying the next engine", backend, q)
            return got  # an empty page 1 may be a transient "No results" of one engine: never "exhausted"
        got = True
        if page == 1 and run.persist:
            cur["head_checked_at"] = core.now()
        new = _collect(run, hrefs, qh, own, found, seen)
        cur["dup_streak"] = 0 if new else cur["dup_streak"] + 1
        cur.update(empty_streak=0, next_page=page + 1, pages_done=cur["pages_done"] + 1, urls_found=cur["urls_found"] + new, last_ok_at=core.now())
        if cur["dup_streak"] >= (DUP_PAGES_LIMIT if run.persist else 1):
            if run.persist:
                _exhaust(cur, run.rediscovery_seconds)
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
    dead: set[str] = _cooling(backends) if run.persist else set()
    for q in qs:
        if len(found) >= target:
            break
        qh = qhash(q)
        for b in backends:
            if b in dead:  # it already failed in this run without answering: do not spend the budget on its timeouts again
                continue
            cur = run.cursor(qh, b, q)  # a fallback backend continues from ITS OWN saved page (page 1 when it has none), never from another engine's offset
            usable = _usable(cur)
            if time.time() > end:
                err = err or TimeoutError(f"search budget exhausted ({SEARCH_BUDGET if budget is None else budget:.0f}s)")
                break
            if run.persist and (not usable or (cur["status"] == "active" and cur["next_page"] > HEAD_PAGES)) and (cur.get("head_checked_at") or "") <= core.later(seconds=-HEAD_RECHECK_SECONDS):
                n = _head(run, q, qh, b, cur, own, found, seen, end)
                if not usable and n >= 0:  # an exhausted query that has news is active again, continuing behind the pages just read
                    if n:
                        cur.update(status="active", next_page=HEAD_PAGES + 1, dup_streak=0, empty_streak=0, exhausted_at=None, rediscover_after=None)
                        usable = True
                    run.updates[(qh, b)] = dict(cur)
            if not usable:
                continue
            try:
                got = _read(run, q, qh, b, cur, pages, own, found, seen, target, end)
            except Exception as e:  # noqa: BLE001 - remembered; the next backend or query may use another engine
                log.warning("search: %s failed for %s: %r", b, q, e)
                err = e
                if run.persist and (qh, b) not in run.updates:
                    _note(b, False)
                if (qh, b) in run.updates:  # it read pages before failing: keep that progress, do not start over elsewhere
                    break
                dead.add(b)
                continue
            if run.persist and b not in dead:
                if got or cur.get("pages_done", 0) == 0:  # empty on a cursor that never read anything here: a blocked engine; an exhausted cursor's empty page is not
                    _note(b, got)
            if len(found) >= target or (got and run.updates.get((qh, b), cur)["status"] != "exhausted"):
                break  # served; an empty or exhausted backend hands the query to the next one
    if run.persist:  # every (query, backend) cursor exhausted and none due yet: nothing to read until the earliest rediscovery time
        rows = [run.cursor(qhash(q), b, q) for q in qs for b in backends]
        if all(r["status"] == "exhausted" and r["rediscover_after"] > core.now() for r in rows):
            run.waiting_until = min(r["rediscover_after"] for r in rows)
    if not found and err and not run.updates:  # pages that were read (even with nothing new) are progress to keep; no page at all is a failure
        if isinstance(err, (SerperCreditsExhausted, SerperAuthError)) and len(backends) > 1:
            log.warning("search: discovery finished with no results; serper was exhausted/refused")
            return []
        raise err
    return list(found) if run.persist else list(found)[:target]

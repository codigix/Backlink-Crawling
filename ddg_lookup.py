"""DuckDuckGo (ddgs) candidate lookup for a business: URLs of other sites' pages that may link to it. A result is only a candidate;
a backlink exists only once the crawler finds the hyperlink."""
import os
import random
import time

from ddgs import DDGS

import crawler_core as core


SEARCH_BUDGET = int(os.environ.get("DDGS_SEARCH_TIMEOUT_SECONDS", 180))  # wall clock for one whole search: it holds a global search slot


def queries(domain: str, name: str | None = None) -> list[str]:
    q = [f'"{domain}" -site:{domain}', f'"www.{domain}" -site:{domain}', f'"https://{domain}" -site:{domain}']
    if name and name.strip().lower() != domain:
        q += [f'"{name.strip()}" -site:{domain}', f'"{name.strip()}" "{domain}" -site:{domain}']
    return q


def _page(query: str, page: int, attempts: int, end: float = float("inf")) -> list[dict]:
    """One result page. backend="auto" picks a random engine per client and some refuse (429/403) or answer "No results" for a query another
    one serves, so every attempt uses a fresh client, after an exponential pause with jitter. Raises the last error when all attempts fail."""
    for n in range(attempts):
        try:
            with DDGS(timeout=10) as ddgs:
                return ddgs.text(query, region="in-en", safesearch="moderate", max_results=10, page=page, backend="auto")
        except Exception:
            if n == attempts - 1 or time.time() > end:
                raise
            time.sleep(2 ** (n + 1) + random.random())
    return []


def search_urls(domain: str, target: int = 50, name: str | None = None, pages: int = 5, budget: float | None = None) -> list[str]:
    """Up to `target` unique result URLs for the business, excluding its own pages, over several query variations. Raises when NOTHING was
    found and a request failed (a provider failure is never "no backlinks"); an empty answer with no error returns []."""
    domain = domain.strip().lower()
    own = core.registrable_domain(domain)
    found: dict[str, None] = {}
    err: Exception | None = None
    end = time.time() + (SEARCH_BUDGET if budget is None else budget)
    for q in queries(domain, name):
        for page in range(1, pages + 1):
            if time.time() > end:  # out of time: keep what was found; with nothing it is a failure (retried), not "no backlinks"
                err = err or TimeoutError(f"search budget exhausted ({SEARCH_BUDGET if budget is None else budget:.0f}s)")
                break
            try:
                results = _page(q, page, 4 if page == 1 else 2, end)
            except Exception as e:  # noqa: BLE001 - remembered; the next query may use another engine
                err = e
                break
            hrefs = [r["href"] for r in results or [] if r.get("href")]
            if not hrefs or all(h in found for h in hrefs):
                break
            found.update(dict.fromkeys(h for h in hrefs if h not in found and core.registrable_domain(core.hostname(h)) != own))
            if len(found) >= target:
                return list(found)[:target]
            time.sleep(1 + random.random())
    if not found and err:
        raise err
    return list(found)[:target]

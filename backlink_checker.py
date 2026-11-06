"""Crawl every page of the given source sites and find backlinks to the target domain.

Run: python backlink_checker.py
Results are written to backlink_results.json next to this file.
Pages where BeautifulSoup finds no backlink are re-checked in a real browser (Playwright),
so links added by JavaScript are detected too.
"""

import asyncio
import json
import re
import uuid
from datetime import timedelta
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urljoin, urlparse

from crawlee import ConcurrencySettings, Request
from bs4 import BeautifulSoup
from crawlee.crawlers import (
    BeautifulSoupCrawler, BeautifulSoupCrawlingContext, PlaywrightCrawler, PlaywrightCrawlingContext,
)
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

CONFIG = {
    "target_domain": "codigix.co",
    "source_urls": [
        "https://in.linkedin.com/company/codigix-infotech",
        "https://www.bookmark4you.com/",
        "https://www.activesearchresults.com/",
        "https://www.freewebsubmission.com/",
        "https://telegra.ph/",
        "https://www.highrankdirectory.com/",
        "https://www.submissionwebdirectory.com/",
        "https://www.marketinginternetdirectory.com/",
        "https://www.247webdirectory.com/",
        "https://morayamultispecialitycharitablehospital.com/",
    ],
    "max_pages": None,  # None = crawl every page of each source site
    "concurrency": 5,
}

# Files (PDFs, images, archives) have no links to check and break the HTML parser.
NON_HTML = re.compile(r".*\.(pdf|jpe?g|png|gif|webp|svg|zip|rar|docx?|xlsx?|pptx?|mp4|mp3)(\?|$)", re.IGNORECASE)

# Huge platforms (millions of pages, block crawlers): only the exact URL given is checked, e.g. your profile page.
SINGLE_PAGE_DOMAINS = (
    "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com",
    "pinterest.com", "tiktok.com", "reddit.com", "quora.com", "medium.com", "telegra.ph",
)

RESULTS_FILE = Path(__file__).parent / "backlink_results.json"


def is_on_domain(url: str, domain: str) -> bool:
    """True if url's host is domain or a subdomain of it (not just containing the text)."""
    host = (urlparse(url).hostname or "").lower()
    return host == domain or host.endswith("." + domain)


def find_backlinks(soup, page_url: str, target_domain: str) -> list[dict]:
    """All <a> links on the page that point to target_domain, with anchor text and rel."""
    if is_on_domain(page_url, target_domain):
        return []  # internal links on the target's own site are not backlinks
    found = []
    for a in soup.find_all("a", href=True):
        href = urljoin(page_url, a["href"].strip())
        if urlparse(href).scheme in ("http", "https") and is_on_domain(href, target_domain):
            rel = a.get("rel") or []
            found.append(
                {
                    "href": href,
                    "anchor_text": a.get_text(" ", strip=True)[:200],
                    "rel": " ".join(rel) or None,
                    "dofollow": not {"nofollow", "ugc", "sponsored"} & {r.lower() for r in rel},
                }
            )
    return found


async def check_backlinks(
    target_domain: str,
    source_urls: list[str],
    max_pages: int | None = None,
    concurrency: int = 5,
    on_result: Callable[[dict], None] | None = None,
) -> list[dict]:
    """Crawl every page of each source site and report links to target_domain.

    on_result is called with each page's result as soon as it is crawled.
    """
    target_domain = target_domain.strip().lower()
    results: list[dict] = []

    def record(result: dict) -> None:
        results.append(result)
        if on_result:
            on_result(result)

    # Own in-memory queue per run; the shared default queue makes repeated/parallel runs dedupe each other's URLs.
    async def fresh_queue() -> RequestQueue:
        return await RequestQueue.open(alias=uuid.uuid4().hex, storage_client=MemoryStorageClient())

    # Pages with no backlink in the static HTML, keyed by URL; re-checked with JavaScript rendering below.
    no_static_backlink: dict[str, dict] = {}

    crawler = BeautifulSoupCrawler(
        request_manager=await fresh_queue(),
        max_requests_per_crawl=max_pages,
        concurrency_settings=ConcurrencySettings(desired_concurrency=concurrency),
        request_handler_timeout=timedelta(seconds=30),
        max_request_retries=3,
    )

    @crawler.router.default_handler
    async def handler(context: BeautifulSoupCrawlingContext) -> None:
        site = context.request.user_data["source_site"]
        page_url = context.request.loaded_url or context.request.url
        backlinks = find_backlinks(context.soup, page_url, target_domain)

        context.log.info(f"{page_url} -> backlinks: {len(backlinks)}")
        result = {
            "source_site": site,
            "source_url": page_url,
            "backlink_found": bool(backlinks),
            "matching_links": backlinks,
            "error": None,
        }
        if backlinks:
            record(result)
        else:
            no_static_backlink[page_url] = result
        # Follow every internal link so every page of the source site is checked.
        if any(is_on_domain(site, d) for d in SINGLE_PAGE_DOMAINS):
            return
        await context.enqueue_links(strategy="same-domain", exclude=[NON_HTML], user_data={"source_site": site})

    @crawler.failed_request_handler
    async def failed_handler(context: BeautifulSoupCrawlingContext, error: Exception) -> None:
        context.log.warning(f"{context.request.url} -> failed: {error}")
        record(
            {
                "source_site": context.request.user_data.get("source_site"),
                "source_url": context.request.url,
                "backlink_found": None,
                "matching_links": [],
                "error": repr(error),
            }
        )

    await crawler.run([Request.from_url(url, user_data={"source_site": url}) for url in source_urls])

    if no_static_backlink:
        browser = PlaywrightCrawler(
            request_manager=await fresh_queue(),
            concurrency_settings=ConcurrencySettings(desired_concurrency=min(concurrency, 3)),
            request_handler_timeout=timedelta(seconds=45),
            max_request_retries=1,
        )

        @browser.router.default_handler
        async def js_handler(context: PlaywrightCrawlingContext) -> None:
            try:  # let scripts finish adding links; busy pages never go idle, so cap the wait
                await context.page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            url = context.request.user_data["static_url"]
            html = await context.page.content()
            backlinks = find_backlinks(BeautifulSoup(html, "html.parser"), url, target_domain)
            context.log.info(f"{url} -> backlinks after JavaScript: {len(backlinks)}")
            result = no_static_backlink.pop(url)
            record({**result, "backlink_found": bool(backlinks), "matching_links": backlinks})

        # No internal link following here: only pages the static crawl already reached are re-checked.
        await browser.run([Request.from_url(u, user_data={"static_url": u}) for u in no_static_backlink])
        # Pages the browser could not load keep their static "no backlink" result.

    for result in no_static_backlink.values():
        record(result)

    # A site whose start page never finished (max_pages hit while it was waiting or retrying) must not vanish silently.
    checked_sites = {r["source_site"] for r in results}
    for url in dict.fromkeys(source_urls):
        if url not in checked_sites:
            record({"source_site": url, "source_url": url, "backlink_found": None, "matching_links": [],
                    "error": "not checked: crawl stopped (max_pages reached) before this page loaded"})
    return results


async def main() -> None:
    results = await check_backlinks(
        CONFIG["target_domain"], CONFIG["source_urls"], CONFIG["max_pages"], CONFIG["concurrency"]
    )
    RESULTS_FILE.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Saved {len(results)} results to {RESULTS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())

from playwright.sync_api import sync_playwright
from urllib.parse import urlparse, parse_qs, quote_plus
import json
import time


def clean_url(href: str) -> str:
    """If Dogpile wraps the link in a redirect, try to pull out the real URL."""
    p = urlparse(href)
    if "dogpile.com" in p.netloc:
        qs = parse_qs(p.query)
        for key in ("ru", "url", "u"):
            if key in qs:
                return qs[key][0]
    return href


def dogpile_search(query: str, max_results: int = 10, headless: bool = False, max_pages: int = 10):
    results = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1366, "height": 850},
        )
        page = context.new_page()

        for page_no in range(1, max_pages + 1):
            url = f"https://www.dogpile.com/serp?q={quote_plus(query)}&page={page_no}"
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Give the page time to pass any bot check and load results
            page.wait_for_timeout(5000)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            before = len(results)
            for a in page.query_selector_all("a[href^='http']"):
                href = a.get_attribute("href") or ""
                text = (a.inner_text() or "").strip().replace("\n", " ")

                real = clean_url(href)
                host = urlparse(real).netloc

                # skip dogpile's own links, empty text, and short menu-like links
                if not host or "dogpile.com" in host:
                    continue
                if len(text) < 15:
                    continue
                if real in seen:
                    continue

                seen.add(real)
                results.append({"title": text[:150], "url": real})
                if len(results) >= max_results:
                    break

            print(f"page {page_no}: +{len(results) - before} (total {len(results)})")
            # stop when we have enough, or the page gave nothing new (end of results)
            if len(results) >= max_results or len(results) == before:
                break

        # Save debug files if nothing was found, so we can fix the selectors
        if not results:
            page.screenshot(path="dogpile_debug.png", full_page=True)
            with open("dogpile_debug.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("No results found. Saved dogpile_debug.png and dogpile_debug.html")

        browser.close()

    return results


if __name__ == "__main__":
    query = "borderlinegenius.com"
    data = dogpile_search(query, max_results=50, headless=False)

    print(f"Found {len(data)} results\n")
    for i, r in enumerate(data, 1):
        print(f"{i}. {r['title']}")
        print(f"   {r['url']}\n")

    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
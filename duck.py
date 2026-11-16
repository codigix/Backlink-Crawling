
# from ddgs import DDGS
# import time

# def search_domain(domain, target=50, per_page=10):

#     ddgs = DDGS(timeout=30)
#     all_results = {}
#     page = 1

#     while len(all_results) < target:

#         print(f"\nSearching page {page}...")

#         try:
#             results = ddgs.text(
#                 query=domain,
#                 max_results=per_page,
#                 page=page,
#                 region="in-en",
#                 safesearch="off",
#                 backend="auto"
#             )

#             if not results:
#                 print("No more results available.")
#                 break

#             new_count = 0

#             for item in results:
#                 url = item.get("href")

#                 if url and url not in all_results:
#                     all_results[url] = {
#                         "title": item.get("title", ""),
#                         "url": url,
#                         "snippet": item.get("body", "")
#                     }
#                     new_count += 1

#                 if len(all_results) >= target:
#                     break

#             print(
#                 f"Page {page}: {new_count} new URLs | "
#                 f"Total: {len(all_results)}"
#             )

#             if new_count == 0:
#                 print("No new unique results.")
#                 break

#             page += 1
#             time.sleep(2)

#         except Exception as e:
#             print(f"Search failed on page {page}: {e}")
#             break

#     print("\n========== FINAL RESULTS ==========")

#     for i, item in enumerate(all_results.values(), 1):
#         print(f"\n{i}. {item['title']}")
#         print(f"URL: {item['url']}")
#         print(f"Snippet: {item['snippet']}")

#     print(f"\nTotal unique URLs: {len(all_results)}")

#     return list(all_results.values())


# if __name__ == "__main__":
#     domain = input("Enter domain: ").strip()
#     search_domain(domain, target=50)










import os
import json
import time
import random
import logging
from datetime import datetime, timezone
from urllib.parse import (
    urlsplit, urlunsplit, parse_qsl, urlencode
)

import requests


# ==========================================
# CONFIGURATION
# ==========================================

TARGET_RESULTS = 100
MAX_PAGES_PER_QUERY = 20
RESULTS_PER_PAGE = 20
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
MAX_EMPTY_PAGES = 3

OUTPUT_DIR = "search_results"

INSTANCE_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "SEARXNG_URLS",
        "http://localhost:8080"
    ).split(",")
    if url.strip()
]

TRACKING_PARAMS = {
    "fbclid", "gclid", "ref", "source"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("searxng")


# ==========================================
# URL NORMALIZATION
# ==========================================

def normalize_url(url):

    try:
        parts = urlsplit(url.strip())

        if parts.scheme.lower() not in ("http", "https"):
            return None

        if not parts.hostname:
            return None

        scheme = parts.scheme.lower()
        hostname = parts.hostname.lower()

        port = parts.port

        if port and not (
            (scheme == "http" and port == 80)
            or (scheme == "https" and port == 443)
        ):
            hostname = f"{hostname}:{port}"

        query_params = []

        for key, value in parse_qsl(
            parts.query,
            keep_blank_values=True
        ):
            key_lower = key.lower()

            if (
                key_lower.startswith("utm_")
                or key_lower in TRACKING_PARAMS
            ):
                continue

            query_params.append((key, value))

        query = urlencode(query_params, doseq=True)

        path = parts.path.rstrip("/")

        return urlunsplit((
            scheme,
            hostname,
            path,
            query,
            ""
        ))

    except (ValueError, TypeError):
        return None


# ==========================================
# STORAGE
# ==========================================

def get_output_file(domain):

    safe_name = (
        domain.replace("://", "_")
        .replace("/", "_")
        .replace(":", "_")
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    return os.path.join(
        OUTPUT_DIR,
        f"{safe_name}.json"
    )


def load_existing_results(output_file):

    collected = {}

    if not os.path.exists(output_file):
        return collected

    try:
        with open(
            output_file,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        for item in data.get("results", []):

            url = normalize_url(item.get("url", ""))

            if not url:
                continue

            collected[url] = {
                "title": item.get("title", ""),
                "url": url,
                "snippet": item.get("snippet", ""),
                "engine": item.get("engine", ""),
                "engines": item.get("engines", [])
            }

        logger.info(
            "Loaded %s existing URLs from storage",
            len(collected)
        )

    except (OSError, json.JSONDecodeError) as exc:

        logger.error("Could not load previous results: %s", exc)

    return collected


def save_results(
    domain,
    collected,
    output_file,
    pages_fetched,
    errors
):

    output = {
        "domain": domain,
        "provider": "SearXNG",
        "requested_results": TARGET_RESULTS,
        "total_results": len(collected),
        "pages_fetched": pages_fetched,
        "completed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": (
            "completed"
            if len(collected) >= TARGET_RESULTS
            else "partial"
        ),
        "errors": errors,
        "results": list(collected.values())
    }

    temp_file = output_file + ".tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            output,
            file,
            indent=4,
            ensure_ascii=False
        )

    # Atomic replacement prevents partially written JSON.
    os.replace(temp_file, output_file)

    return output


# ==========================================
# SEARXNG CLIENT
# ==========================================

class SearXNGSearch:

    def __init__(self, instances=None):

        self.instances = instances or INSTANCE_URLS

        if not self.instances:
            raise ValueError(
                "No SearXNG instances configured"
            )

        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json"
        })

    # ======================================
    # REQUEST WITH RETRIES
    # ======================================

    def request_page(self, query, page):

        errors = []

        start_index = (page - 1) % len(self.instances)

        ordered_instances = (
            self.instances[start_index:]
            + self.instances[:start_index]
        )

        for instance in ordered_instances:

            endpoint = f"{instance}/search"

            params = {
                "q": query,
                "pageno": page,
                "format": "json",
                "language": "en",
                "safesearch": 0
            }

            for attempt in range(1, MAX_RETRIES + 1):

                try:

                    response = self.session.get(
                        endpoint,
                        params=params,
                        timeout=REQUEST_TIMEOUT
                    )

                    if response.status_code in (403, 404):
                        errors.append(
                            f"{instance}: HTTP {response.status_code}"
                        )
                        break

                    if response.status_code == 429:

                        retry_after = response.headers.get(
                            "Retry-After"
                        )

                        try:
                            delay = min(
                                max(float(retry_after), 1),
                                30
                            ) if retry_after else 2 ** attempt

                        except ValueError:
                            delay = 2 ** attempt

                        if attempt < MAX_RETRIES:
                            time.sleep(delay)
                            continue

                        errors.append(
                            f"{instance}: HTTP 429"
                        )
                        break

                    response.raise_for_status()

                    data = response.json()

                    if (
                        not isinstance(data, dict)
                        or not isinstance(
                            data.get("results"), list
                        )
                    ):
                        raise ValueError(
                            "Invalid SearXNG JSON response"
                        )

                    return data

                except (
                    requests.exceptions.RequestException,
                    ValueError
                ) as exc:

                    logger.warning(
                        "%s | Attempt %s/%s | %s",
                        instance,
                        attempt,
                        MAX_RETRIES,
                        exc
                    )

                    if attempt < MAX_RETRIES:

                        time.sleep(
                            min(2 ** attempt, 15)
                            + random.uniform(0, 0.5)
                        )

                    else:
                        errors.append(
                            f"{instance}: {str(exc)}"
                        )

        raise RuntimeError(
            f"All instances failed on page {page}: "
            + " | ".join(errors)
        )

    # ======================================
    # AUTOMATIC QUERY GENERATION
    # ======================================

    def generate_queries(self, domain):

        domain = domain.lower().strip()

        return list(dict.fromkeys([
            domain,
            f'"{domain}"',
            f'"www.{domain}"'
        ]))

    # ======================================
    # SEARCH + PAGINATION
    # ======================================

    def search_domain(self, domain):

        domain = domain.strip().lower()

        if not domain:
            raise ValueError("Domain cannot be empty")

        output_file = get_output_file(domain)

        # IMPORTANT: Load previous URLs.
        collected = load_existing_results(output_file)

        errors = []
        total_pages = 0

        logger.info("=" * 55)
        logger.info("SEARCHING DOMAIN: %s", domain)
        logger.info(
            "Previously collected: %s",
            len(collected)
        )
        logger.info("=" * 55)

        if len(collected) >= TARGET_RESULTS:

            logger.info(
                "Target already reached. Keeping saved results."
            )

            return save_results(
                domain,
                collected,
                output_file,
                total_pages,
                errors
            )

        queries = self.generate_queries(domain)

        for query_index, query in enumerate(queries, 1):

            if len(collected) >= TARGET_RESULTS:
                break

            logger.info(
                "\nQuery %s/%s: %s",
                query_index,
                len(queries),
                query
            )

            empty_pages = 0

            for page in range(
                1,
                MAX_PAGES_PER_QUERY + 1
            ):

                if len(collected) >= TARGET_RESULTS:
                    break

                logger.info(
                    "Query %s | Page %s | URLs %s/%s",
                    query_index,
                    page,
                    len(collected),
                    TARGET_RESULTS
                )

                try:

                    data = self.request_page(
                        query,
                        page
                    )

                    total_pages += 1

                    results = data.get("results", [])

                    if not results:
                        logger.info(
                            "No results on this page."
                        )
                        break

                    new_count = 0

                    for item in results:

                        if not isinstance(item, dict):
                            continue

                        clean_url = normalize_url(
                            item.get("url", "")
                        )

                        if not clean_url:
                            continue

                        if clean_url in collected:
                            continue

                        collected[clean_url] = {
                            "title": item.get("title", ""),
                            "url": clean_url,
                            "snippet": item.get("content", ""),
                            "engine": item.get("engine", ""),
                            "engines": item.get("engines", [])
                        }

                        new_count += 1

                        if len(collected) >= TARGET_RESULTS:
                            break

                    logger.info(
                        "New URLs: %s | Total: %s/%s",
                        new_count,
                        len(collected),
                        TARGET_RESULTS
                    )

                    if new_count == 0:
                        empty_pages += 1
                    else:
                        empty_pages = 0

                    # Save after every successful page.
                    save_results(
                        domain,
                        collected,
                        output_file,
                        total_pages,
                        errors
                    )

                    if empty_pages >= MAX_EMPTY_PAGES:

                        logger.info(
                            "No new URLs in 3 consecutive pages. "
                            "Moving to next query."
                        )
                        break

                    time.sleep(
                        random.uniform(1.0, 2.0)
                    )

                except Exception as exc:

                    logger.error(
                        "Page failed: %s",
                        exc
                    )

                    errors.append({
                        "query": query,
                        "page": page,
                        "error": str(exc)
                    })

                    # Continue with the next query.
                    break

        # ==================================
        # FINAL SAVE
        # ==================================

        output = save_results(
            domain,
            collected,
            output_file,
            total_pages,
            errors
        )

        logger.info("\n========== FINAL SUMMARY ==========")
        logger.info("Domain: %s", domain)
        logger.info(
            "Total unique URLs: %s/%s",
            len(collected),
            TARGET_RESULTS
        )
        logger.info("Status: %s", output["status"])
        logger.info("Saved: %s", output_file)

        return output


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":

    domain = input("Enter domain: ").strip()

    searcher = SearXNGSearch()

    result = searcher.search_domain(domain)

    print("\n========== FINAL SUMMARY ==========")

    print(json.dumps({
        "domain": result["domain"],
        "total_results": result["total_results"],
        "requested_results": result["requested_results"],
        "status": result["status"],
        "errors": len(result["errors"])
    }, indent=4))

"""Historical OpenPageScore referring domains for the read-only backlink gap route."""
import logging
import os
import threading
import time
from collections import deque

import httpx
from httpx import get as http_get

from competitors import _reg
from read_cache import TTLCache

URL = "https://openpagescore.com/api/v1/linkgap"
LIMIT = 1000
CURSOR = "openpagescore:"
log = logging.getLogger("crawler.openpagescore")
ttl = float(os.getenv("OPENPAGESCORE_CACHE_TTL_SECONDS", "86400"))
cache = TTLCache(enabled=True, ttl=ttl, max_ttl=ttl, max_stale=0)
_requests = deque()
_lock = threading.Lock()


def _after_fork():
    global _lock
    cache.reset_after_fork()
    _lock = threading.Lock()
    _requests.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def _domain(value):
    try:
        return _reg(value) if isinstance(value, str) else None
    except ValueError:
        return None


def _fetch(owner, batch):
    # Coalesced cache misses share a per-process budget. Never queue an API worker waiting for quota.
    with _lock:
        now = time.monotonic()
        while _requests and _requests[0] <= now - 60:
            _requests.popleft()
        if len(_requests) >= 30:
            raise ValueError("local quota")
        _requests.append(now)
    response = http_get(URL, params={"domain": owner, "competitor": ",".join(batch), "min": 1, "limit": LIMIT}, timeout=4)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not isinstance(data.get("domains"), list):
        raise ValueError("invalid response")
    if ("domain" in data and _domain(data["domain"]) != owner) or (
            "competitor" in data and (len(batch) != 1 or _domain(data["competitor"]) != batch[0])):
        raise ValueError("unexpected comparison")
    rows = data["domains"]
    warnings = set()
    skipped = data.get("skipped", [])
    if not isinstance(skipped, list):
        raise ValueError("invalid skipped")
    covered = set(batch) - {_domain(d) for d in skipped}
    if "competitors" in data:
        if not isinstance(data["competitors"], list):
            raise ValueError("invalid competitors")
        covered.intersection_update(_domain(d) for d in data["competitors"])
    if covered != set(batch):
        warnings.add("missing competitor coverage")
    total = data.get("total", len(rows))
    if type(total) is not int or total < 0:
        raise ValueError("invalid total")
    if total > min(len(rows), LIMIT) or len(rows) > LIMIT:
        warnings.add("truncated results")
    if not rows:
        warnings.add("empty result")
    records = []
    normalized = 0
    for index, row in enumerate(rows[:LIMIT], 1):
        domain = _domain(row.get("domain")) if isinstance(row, dict) else None
        if not domain:
            warnings.add("invalid referring domain")
            log.debug("OpenPageScore excluded provider row=%d reason=invalid_domain", index)
            continue
        normalized += 1
        links = row.get("links_to", list(batch) if len(batch) == 1 else [])
        targets = covered.intersection(_domain(d) for d in links) if isinstance(links, list) else set()
        if not targets:
            warnings.add("missing competitor relationship")
            log.debug("OpenPageScore excluded domain=%s row=%d reason=missing_competitor_relationship", domain, index)
            continue
        score = row.get("openpage_score")
        records.append((domain, sorted(targets), score if type(score) in (int, float) and 0 <= score <= 100 else None))
    log.info("OpenPageScore fetch owner=%s competitors=%s reported=%d received=%d examined=%d normalized=%d mapped=%d",
             owner, ",".join(batch), total, len(rows), min(len(rows), LIMIT), normalized, len(records))
    return records, sorted(warnings)


def candidates(owner, competitors):
    selected = sorted(set(competitors))
    exclude = {owner, *selected}
    by_domain = {}
    warnings = set()
    mapped = self_links = duplicates = 0
    for i in range(0, len(selected), 10):
        batch = tuple(selected[i:i + 10])
        try:
            (rows, issues), status, _ = cache.get((URL, owner, batch, 1, LIMIT), lambda: _fetch(owner, batch))
            log.info("OpenPageScore batch owner=%s competitors=%s cache=%s mapped=%d", owner, ",".join(batch), status, len(rows))
        except (httpx.HTTPError, ValueError, RecursionError) as exc:
            reason = f"HTTP {exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
            issues = [f"request unavailable ({reason})"]
            rows = []
        for issue in issues:
            warnings.add(f"OpenPageScore: {issue}")
            log.warning("OpenPageScore link gap coverage", extra={"reason": issue, "competitor_count": len(batch)})
        mapped += len(rows)
        for domain, targets, score in rows:
            if domain in exclude:
                self_links += 1
                log.debug("OpenPageScore excluded domain=%s reason=business_or_selected_competitor", domain)
                continue
            if domain in by_domain:
                duplicates += 1
                log.debug("OpenPageScore excluded domain=%s reason=duplicate_provider_row", domain)
                by_domain[domain]["competitor_targets"].update(targets)
                continue
            by_domain[domain] = {"referring_domain": domain, "competitor_targets": set(targets), "classification": "competitor_only",
                                 "links_to_own_business": False, "source_pages": [], "source": "openpagescore",
                                 "verification_status": "historical_unverified"}
            if score is not None:
                by_domain[domain]["authority_score"] = score
    for row in by_domain.values():
        row["competitor_targets"] = sorted(row["competitor_targets"])
    log.info("OpenPageScore candidates owner=%s mapped=%d self_links=%d duplicates=%d unique=%d",
             owner, mapped, self_links, duplicates, len(by_domain))
    return by_domain, sorted(warnings)


def append_page(result, candidates, cursor):
    """PostgreSQL already sliced every list. Fill only unused opportunity slots."""
    summary, page = result["summary"], result["pagination"]
    database_count = summary["gap_domains"]
    external = list(candidates.values())
    total = database_count + len(external)
    summary.update(postgres_gap_domains=database_count, gap_domains=total, openpagescore_unique_domains=len(external),
                   total_unique_opportunities=total)
    info = page["lists"]["opportunities"]
    info["total"] = total
    if cursor and cursor.startswith(CURSOR):
        # The token indexes only the deterministic external suffix.
        start = int(cursor[len(CURSOR):])
    else:
        start = max(0, page["offset"] - database_count)
    rows = result["opportunities"]
    end = start + max(0, page["limit"] - len(rows))
    added = external[start:end]
    rows.extend(added)
    log.info("OpenPageScore page owner=%s postgres=%d eligible_external=%d combined=%d offset=%d limit=%d returned=%d returned_external=%d",
             result["business_domain"], database_count, len(external), total, page["offset"], page["limit"], len(rows), len(added))
    if external and end < len(external):
        info.update(has_more=True, next_after=CURSOR + str(end) if added else rows[-1]["referring_domain"])
    page["has_more"] = any(v["has_more"] for v in page["lists"].values())
    page["next_offset"] = page["offset"] + page["limit"] if page["has_more"] else None
    return result

"""Live DDGS backend qualification. Run it where the real ddgs dependencies and internet access work (the deployment server):

  python tests/live_ddgs_probe.py [--query '"example.org" -site:example.org'] [--pages 4] [--backends duckduckgo,brave,bing,yahoo] [--json out.json]

No database, no business data: a neutral public query, one request every ~2 s. For every backend it checks and prints
  pages      results per page and how pages overlap (overlap is reported, never required to be zero)
  repeat     the same page asked twice with fresh clients: how many URLs are shared
  empty      a nonsense query: must come back as the "No results found." signal (empty), not as a failure
  failure    an unreachable proxy: must raise a failure that _page does NOT treat as empty
  cursor     ddg_lookup.search_urls with a Run on this backend only: the second run must continue at the saved page, and a failure must leave the cursor
verdict per backend: WORKS | BLOCKED (every request empty, e.g. a bot challenge / HTTP 202) | FAILING (errors).
A BLOCKED or FAILING backend is a finding about the network, not about the cursor logic; the script exits 0 either way and says so.
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import ddg_lookup  # noqa: E402
from ddgs import DDGS  # noqa: E402


def fetch(q: str, page: int, backend: str, proxy: str | None = None) -> tuple[str, list[str], str]:
    """(kind, urls, detail) with kind in ok | empty | failure, classified exactly as ddg_lookup._page does."""
    try:
        with DDGS(timeout=10, proxy=proxy) as d:
            r = d.text(q, region="in-en", safesearch="moderate", max_results=10, page=page, backend=backend)
        return "ok", [x["href"] for x in r if x.get("href")], ""
    except Exception as e:  # noqa: BLE001
        return ("empty" if ddg_lookup._is_empty(e) else "failure"), [], f"{type(e).__name__}: {str(e)[:120]}"


def probe(backend: str, q: str, pages: int) -> dict:
    out: dict = {"backend": backend, "pages": [], "notes": []}
    seen: list[list[str]] = []
    for p in range(1, pages + 1):
        time.sleep(2)
        kind, urls, detail = fetch(q, p, backend)
        out["pages"].append({"page": p, "kind": kind, "count": len(urls), "detail": detail})
        seen.append(urls)
    flat = [u for pg in seen for u in pg]
    out["unique"], out["total"] = len(set(flat)), len(flat)
    out["overlap_between_pages"] = {f"{i + 1}&{j + 1}": len(set(seen[i]) & set(seen[j])) for i in range(len(seen)) for j in range(i + 1, len(seen)) if seen[i] and seen[j]}
    time.sleep(2)
    again = fetch(q, 1, backend)
    out["repeat_page1_shared"] = len(set(again[1]) & set(seen[0])) if seen and seen[0] else None
    time.sleep(2)
    k, _, d = fetch("zq9xv7kw3j8h2m5 qlpwoeiruty1234 nonexistentterm", 1, backend)
    out["empty_query"] = {"kind": k, "detail": d}
    k, _, d = fetch(q, 1, backend, proxy="http://127.0.0.1:9")
    out["unreachable_proxy"] = {"kind": k, "detail": d}
    # cursor persistence on this backend only
    saved_b, saved_pages = ddg_lookup.BACKENDS, ddg_lookup.MAX_PAGES_PER_QUERY
    ddg_lookup.BACKENDS, ddg_lookup.MAX_PAGES_PER_QUERY = [backend], 2
    try:
        dom = "example.org"
        r1 = ddg_lookup.Run()
        u1 = ddg_lookup.search_urls(dom, 15, run=r1, budget=40)
        cur = dict(r1.updates)
        r2 = ddg_lookup.Run(cursors=cur, known={ddg_lookup.core.normalize_url(h) for h, _, _ in r1.hits})
        u2 = ddg_lookup.search_urls(dom, 15, run=r2, budget=40)
        first = {k: v["next_page"] for k, v in cur.items()}  # keyed (query_hash, backend)
        second = {k: v["next_page"] for k, v in r2.updates.items()}
        out["cursor"] = {"run1_urls": len(u1), "run2_urls": len(u2), "next_page_after_run1": {f"{k[0]}/{k[1]}": v for k, v in first.items()}, "next_page_after_run2": {f"{k[0]}/{k[1]}": v for k, v in second.items()},
                         "run2_continued": all(second.get(k, 0) > v for k, v in first.items()) if first else None, "run2_repeated_run1": len(set(u1) & set(u2))}
    except Exception as e:  # noqa: BLE001 - a failed search is a result here, not a crash of the probe
        out["cursor"] = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
    finally:
        ddg_lookup.BACKENDS, ddg_lookup.MAX_PAGES_PER_QUERY = saved_b, saved_pages
    kinds = [x["kind"] for x in out["pages"]]
    out["verdict"] = "WORKS" if "ok" in kinds else ("BLOCKED" if set(kinds) == {"empty"} else "FAILING")
    if out["unreachable_proxy"]["kind"] != "failure":
        out["notes"].append("a transport failure was NOT classified as a failure: it would look like an empty page")
    if out["empty_query"]["kind"] == "ok":
        out["notes"].append("nonsense query returned results (unusual)")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", default='"example.org" -site:example.org')
    ap.add_argument("--pages", type=int, default=4)
    ap.add_argument("--backends", default="duckduckgo,brave,bing,yahoo")
    ap.add_argument("--json")
    a = ap.parse_args()
    report = [probe(b.strip(), a.query, a.pages) for b in a.backends.split(",") if b.strip()]
    for r in report:
        print(f"\n== {r['backend']}: {r['verdict']}")
        print("  pages:", [(p["page"], p["kind"], p["count"]) for p in r["pages"]], f"unique {r['unique']}/{r['total']}", "overlap", r["overlap_between_pages"])
        print("  repeat page1 shared:", r["repeat_page1_shared"], "| empty query:", r["empty_query"]["kind"], "| unreachable proxy:", r["unreachable_proxy"]["kind"], r["unreachable_proxy"]["detail"][:60])
        print("  cursor:", r["cursor"])
        for n in r["notes"]:
            print("  NOTE:", n)
        for p in r["pages"]:
            if p["detail"]:
                print("  detail:", p["page"], p["detail"])
                break
    if a.json:
        Path(a.json).write_text(json.dumps(report, indent=2))
    print("\nverdicts:", {r["backend"]: r["verdict"] for r in report})
    if all(r["verdict"] != "WORKS" for r in report):
        print("NO backend answered: this is a network/provider limitation of the machine running the probe (blocked IP, proxy or dependency), not a result about pagination.")


if __name__ == "__main__":
    main()

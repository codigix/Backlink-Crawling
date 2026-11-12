"""Check which pages of the given source sites link to a target domain. Thin CLI over the main engine: same crawler (static HTML
first, Playwright for JavaScript-driven pages), same SSRF guard, robots.txt rules and PostgreSQL storage as everything else.

  python backlink_checker.py codigix.co https://www.bookmark4you.com/ https://www.activesearchresults.com/
  python backlink_checker.py codigix.co            # uses the default source list below

Results are in PostgreSQL (GET /backlinks/<target>); this script also prints them. No JSON side file.
"""

import asyncio
import sys

import crawler_core as core
import crawler_engine as engine

DEFAULT_SOURCES = [
    "https://www.bookmark4you.com/", "https://www.activesearchresults.com/", "https://www.freewebsubmission.com/",
    "https://www.highrankdirectory.com/", "https://www.submissionwebdirectory.com/", "https://www.marketinginternetdirectory.com/",
    "https://www.247webdirectory.com/",
]


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    target, sources = core.clean_target(argv[0]), argv[1:] or DEFAULT_SOURCES
    with core.connect() as c:
        core.migrate(c, recover=False)  # never reset another process's in-flight work
    cfg = engine.Config(target_domain=target, max_depth=3, max_pages=500, concurrency=5)
    asyncio.run(engine.run_crawl(cfg, sources))
    with core.connect() as c:
        rows = c.execute("SELECT b.source_url, b.destination_url, b.anchor_text, b.rel_attributes, b.link_status FROM backlinks b"
                         " JOIN target_domains t ON t.id=b.target_domain_id WHERE t.domain=? ORDER BY b.id", (target,)).fetchall()
    for r in rows:
        print(f"{r['link_status']:<18} {r['source_url']}  ->  {r['destination_url']}  [{r['anchor_text']}] rel={r['rel_attributes']}")
    print(f"{len(rows)} backlink(s) to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

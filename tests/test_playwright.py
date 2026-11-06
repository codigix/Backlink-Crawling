"""JS-rendered links are found when use_playwright=True (skipped if no browser is installed)."""
import asyncio

import crawler_core as core
import crawler_engine as engine
from test_crawler import SITES, cfg, env, page, port, rows  # noqa: F401 - reuse the local test server


def test_playwright_sees_js_links(env):
    SITES["a.localhost"] = {"/": (200, "text/html", "<html><body><script>document.body.innerHTML='<a href=\"https://codigix.co/js\">JS</a>'</script></body></html>")}
    asyncio.run(engine.run_crawl(cfg(target_domain="codigix.co", use_playwright=True, max_depth=0), [env("a")]))
    assert [r["destination_url"] for r in rows("SELECT destination_url FROM backlinks")] == ["https://codigix.co/js"]

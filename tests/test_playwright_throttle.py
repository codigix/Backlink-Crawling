"""Playwright obeys the same per-host politeness as the static fetcher, and the browser cannot reach private addresses."""
import asyncio
import time

import pytest

import crawler_core as core
import crawler_engine as engine
import egress_proxy
from test_crawler import Handler, SITES, cfg, env, page, port, rows  # noqa: F401

DELAY = 1.0


@pytest.fixture()
def stamps(monkeypatch):
    """Arrival time of every page request the test server sees (log_message runs once per request)."""
    seen: list[tuple[str, float]] = []
    monkeypatch.setattr(Handler, "log_message", lambda self, *a: seen.append((self.path, time.monotonic())))
    return seen


def gaps(stamps, prefix="/p"):
    t = sorted(t for p, t in stamps if p.startswith(prefix))
    return [b - a for a, b in zip(t, t[1:])]


def test_playwright_same_host_requests_respect_domain_delay(env, stamps):
    SITES["a.localhost"] = {"/": page(*[f'<a href="/p{i}">x</a>' for i in range(4)])}
    SITES["a.localhost"].update({f"/p{i}": page("<p>page</p>") for i in range(4)})
    asyncio.run(engine.run_crawl(cfg(use_playwright=True, concurrency=4, domain_delay=DELAY, max_depth=1), [env("a")]))
    g = gaps(stamps)
    assert len(g) == 3, stamps  # all four pages were fetched
    assert min(g) >= DELAY * 0.9, g  # four parallel browser pages still reach one host one at a time


def test_js_fallback_render_pass_is_throttled_too(env, stamps):
    shell = lambda i: (200, "text/html", f'<html><body><div id="app"></div><script>document.getElementById("app").innerHTML="<a href=\\"https://x.example.org/{i}\\">j</a>"</script></body></html>')  # noqa: E731
    SITES["a.localhost"] = {"/": page(*[f'<a href="/p{i}">x</a>' for i in range(3)]), **{f"/p{i}": shell(i) for i in range(3)}}
    asyncio.run(engine.run_crawl(cfg(concurrency=4, domain_delay=DELAY, max_depth=1), [env("a")]))
    assert len(rows("SELECT 1 FROM outgoing_links WHERE destination_url LIKE '%x.example.org%'")) == 3  # rendered links found
    g = gaps(stamps)  # static pass (3) + render pass (3) against one host
    assert len(g) >= 5 and min(g) >= DELAY * 0.9, g


def test_browser_cannot_reach_a_rebound_private_address(env, monkeypatch):
    """Name validated as public, then 'rebinds' to a private IP: the browser's connection goes through the proxy and is refused."""
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    monkeypatch.setattr(core, "ip_allowed", lambda a: a == "93.184.216.34")  # only this one counts as public

    async def fake(host):
        return ["10.255.255.1"]  # what the attacker's DNS answers at connect time
    monkeypatch.setattr(egress_proxy, "resolve", fake)
    monkeypatch.setattr(core, "blocked_reason", lambda url: None)  # the name-level pre-check was fooled (that is the attack)

    async def go():
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            b = await p.chromium.launch(proxy={"server": egress_proxy.ensure()}, args=["--proxy-bypass-list=<-loopback>"])
            pg = await b.new_page()
            try:
                r = await pg.goto("http://rebind.test/", timeout=8000)
                return r.status if r else None
            except Exception as e:  # noqa: BLE001
                return str(e)
            finally:
                await b.close()
    out = asyncio.run(go())
    assert out != 200 and "hello" not in str(out)


def test_playwright_redirect_chain_still_completes(env, stamps):
    # limitation (documented): Chromium follows redirects internally, so a hop is not separately delayed; each page load still is.
    SITES["a.localhost"] = {"/": page('<a href="/p0">x</a>'), "/p0": (302, "", "/q0"), "/q0": page('<a href="https://x.example.org/r">r</a>')}
    asyncio.run(engine.run_crawl(cfg(use_playwright=True, domain_delay=DELAY, max_depth=1), [env("a")]))
    assert rows("SELECT 1 FROM outgoing_links WHERE destination_url='https://x.example.org/r'")

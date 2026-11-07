"""End-to-end verification: seed allocation scenarios, all five workers on a local site, SSRF redirect, monitoring == database."""

import asyncio
import time

import autonomous
import crawler_core as core
import crawler_engine as engine
import engine_stats
from test_autonomous import acfg, use_source
from test_crawler import SITES, a, env, page, port, rows  # noqa: F401 - fixtures


def populate(seeds, seed_urls, normals=20, normal_urls=20):
    with core.connect() as c:
        for d, n, prio in [(f"seed{i}.localhost", seed_urls, 0) for i in range(seeds)] + [(f"n{i}.localhost", normal_urls, 3) for i in range(normals)]:
            for i in range(n):
                core.save_url(c, f"http://{d}/{i}", None, "t", 1, True)
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain) VALUES (?)", (d,))
            c.execute("UPDATE discovered_domains SET discovery_status='admitted', priority=? WHERE domain=?", (prio, d))


def split(got):
    s = [u for u, _ in got if "//seed" in u]
    return len(s), len(got) - len(s)


def test_seed_allocation_scenarios(env):
    cfg = autonomous.AutoConfig(claim_batch=100, per_batch=10, seed_batch=10, domain_page_budget=1000)
    populate(1, 20)
    got = autonomous.claim_urls("w1", cfg)
    assert split(got) == (10, 90) and len(set(got)) == 100  # reserved 10 for the seed, 90 for normal domains, no duplicates
    assert autonomous.claim_urls("w2", cfg) and not set(autonomous.claim_urls("w3", cfg)) & set(got)  # a second claimer never overlaps
    assert rows("SELECT COUNT(*) n FROM (SELECT url FROM discovered_urls WHERE lease_owner IS NOT NULL GROUP BY url HAVING COUNT(*)>1)")[0]["n"] == 0


def test_unused_seed_allocation_goes_to_normal_domains(env):
    cfg = autonomous.AutoConfig(claim_batch=100, per_batch=10, seed_batch=10, domain_page_budget=1000)
    populate(1, 5)
    assert split(autonomous.claim_urls("w", cfg)) == (5, 95)


def test_two_seeds_share_the_allocation_within_the_batch(env):
    cfg = autonomous.AutoConfig(claim_batch=100, per_batch=10, seed_batch=10, domain_page_budget=1000)
    populate(2, 20)
    s, n = split(autonomous.claim_urls("w", cfg))
    assert s == 10 and s + n == 100
    cfg2 = autonomous.AutoConfig(claim_batch=8, per_batch=10, seed_batch=10, domain_page_budget=1000)
    assert split(autonomous.claim_urls("x", cfg2)) == (8, 0)  # the total batch still caps the reservation


def test_normal_domains_keep_progressing_over_repeated_rounds(env):
    cfg = autonomous.AutoConfig(claim_batch=100, per_batch=10, seed_batch=10, domain_page_budget=1000)
    populate(1, 20)
    seen = set()
    for r in range(3):
        for u, _ in autonomous.claim_urls(f"w{r}", cfg):
            seen.add(u.split("/")[2])
    assert {f"n{i}.localhost" for i in range(20)} <= seen  # every normal domain got URLs within three rounds


def test_lease_longer_than_expiry_is_not_renewed_known_gap(env):
    """Documents current behaviour: there is no lease renewal. A lease that outlives lease_seconds is returned to the
    frontier by release(), even though a slow round may still be crawling it."""
    cfg = autonomous.AutoConfig(claim_batch=5, domain_page_budget=100)
    populate(0, 0, normals=1, normal_urls=5)
    assert len(autonomous.claim_urls("slow", cfg)) == 5
    with core.connect() as c:
        c.execute("UPDATE discovered_urls SET lease_expires='2000-01-01 00:00:00'")
    assert autonomous.release() == 5  # reclaimed while 'slow' might still be working: duplicate-crawl risk


# ---- all five workers together on a controlled site ----
def test_five_workers_end_to_end(env, monkeypatch):
    u = env
    SITES["a.localhost"] = {
        "/": page(a("/about"), a("/private/x"), a("/boom"), a("https://codigix.co/", "Codigix", "nofollow"), a(u("b"))),
        "/about": page(a("/services"), a("https://codigix.co/about", "About Codigix")),
        "/services": page("services"), "/boom": (500, "text/plain", "err"),
        "/private/x": page(a("https://leak.example/")),
        "/robots.txt": (200, "text/plain", "User-agent: *\nDisallow: /private/\n")}
    SITES["b.localhost"] = {"/": page(a("https://codigix.co/b", "From B"))}
    with core.connect() as c:
        core.add_target(c, "codigix.co")
    use_source(monkeypatch, lambda: [(u("a"), 1, 0)])
    cfg = acfg(max_admitted=2, heartbeat_every=0.2, monitor_every=1)
    eng = autonomous.Engine(cfg)

    async def go():
        t = asyncio.create_task(eng.run())
        end = time.time() + 120
        while time.time() < end:
            await asyncio.sleep(2)
            if rows("SELECT 1 FROM discovered_urls WHERE url LIKE '%/services'  AND crawl_status='completed'") and \
                    rows("SELECT 1 FROM crawl_history WHERE source_url LIKE '%b.localhost%'"):
                break
        alive = {n: not tk.done() for n, tk in eng.tasks.items()}
        with core.connect() as c:  # make a page due and let the verifier recrawl it
            c.execute("UPDATE discovered_urls SET next_crawl_at='2000-01-01 00:00:00' WHERE url LIKE '%/services'")
        recrawled = await eng.verifier()
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        return alive, recrawled
    alive, recrawled = asyncio.run(go())
    assert set(alive) == {"discovery", "scheduler", "crawler", "indexer", "verifier"} and all(alive.values())
    assert recrawled == 1
    by = {r["url"].rsplit("/", 1)[-1] or "root": r for r in rows("SELECT * FROM discovered_urls WHERE domain='a.localhost'")}
    assert by["services"]["crawl_status"] == "completed" and by["x"]["crawl_status"] == "skipped" and by["x"]["skip_reason"] == "robots_txt"
    assert by["boom"]["crawl_status"] in ("failed", "retry_pending")
    assert not rows("SELECT * FROM crawl_history WHERE source_url LIKE '%/private/%'")
    assert {r["source_url"].split(":")[0] for r in rows("SELECT * FROM backlinks")} == {"http"}
    assert len(rows("SELECT * FROM backlinks WHERE link_status='verified'")) == 3  # /, /about on a; / on b: backlinks were stored
    assert rows("SELECT COUNT(*) n FROM discovered_urls GROUP BY normalized_url HAVING COUNT(*)>1") == []
    w = {r["name"]: r for r in rows("SELECT * FROM worker_status")}
    assert set(w) == set(alive) and all(r["last_heartbeat_at"] for r in w.values())
    assert rows("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('crawling','recrawling')")[0]["n"] == 0


def test_monitoring_counters_match_database(env):
    with core.connect() as c:
        for i in range(3):
            core.save_url(c, f"http://m.localhost/{i}", None, "t", 1, True, "completed" if i else "pending")
        c.execute("UPDATE discovered_urls SET next_crawl_at='2000-01-01 00:00:00' WHERE crawl_status='completed'")
    m = engine_stats.metrics()
    assert m["urls"] == {"completed": 2, "pending": 1} and m["recrawl_backlog"] == 2 and m["domains"] == {"discovered": 1}
    assert m["urls"] == engine_stats.counts()["urls_by_status"]


def test_ssrf_guard_rejects_unsafe_urls(env, monkeypatch):
    """Unit check of the guard only. The engine applies it to redirects (crawler_engine.handle) but the local test server
    cannot issue redirects, so redirect-to-private-IP is not covered end to end."""
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    for bad in ("http://127.0.0.1/", "http://169.254.169.254/", "http://10.0.0.1/", "http://[::1]/", "http://localhost/",
                "http://user:pw@example.com/", "ftp://example.com/"):
        assert not core.is_safe_url(bad), bad

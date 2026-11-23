"""Phase 2: business_jobs.claim() picks in SQL and business_jobs.expand() works set-based. Each is compared, on randomized graphs, with a VERBATIM copy of
the code it replaced (`legacy_*` below), so "same URLs claimed, same URLs added, same depths" is checked rather than assumed. Real PostgreSQL, isolated
schema per test (the `env` fixture); no network, no crawl."""

import random
from types import SimpleNamespace

import pytest

import autonomous
import business_jobs as bj
import crawler_core as core
from test_crawler import env, port, rows  # noqa: F401 - fixtures

# ------------------------------------------------------------------ the replaced code, verbatim ------------------------------------------------------------------


def legacy_pick(conn, owner, cfg) -> dict:
    """The selection half of the old claim(): every candidate row loaded, sorted and filtered in Python. -> {url id: depth} in pick order."""
    full = [r[0] for r in conn.execute(
        "SELECT ju.job_id FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE u.crawl_status='crawling' AND u.lease_expires>?"
        " AND u.lease_owner<>? GROUP BY ju.job_id HAVING COUNT(DISTINCT u.lease_owner)>=?", (core.now(), owner, cfg.max_jobs_per_business))]
    cands = conn.execute(
        "SELECT u.id, u.domain, ju.job_id, ju.depth, j.priority, ROW_NUMBER() OVER (PARTITION BY ju.job_id ORDER BY ju.depth, u.id) rn"
        " FROM job_urls ju JOIN business_jobs j ON j.id=ju.job_id AND j.status='crawling' JOIN discovered_urls u ON u.id=ju.url_id"
        " WHERE (u.crawl_status='pending' OR (u.crawl_status='retry_pending' AND u.next_crawl_at<=?)) AND ju.job_id<>ALL(?::bigint[])",
        (core.now(), full)).fetchall()
    picked: dict[int, int] = {}
    per_domain: dict[str, int] = {}
    for r in sorted(cands, key=lambda r: (r["priority"], r["rn"], r["job_id"])):
        if len(picked) >= cfg.claim_batch:
            break
        if r["id"] in picked or per_domain.get(r["domain"], 0) >= cfg.per_batch:
            continue
        picked[r["id"]] = r["depth"]
        per_domain[r["domain"]] = per_domain.get(r["domain"], 0) + 1
    return picked


def legacy_expand(conn, job: dict) -> int:
    ready = conn.execute(
        "SELECT ju.url_id, ju.depth, u.url, u.domain, u.crawl_status FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id"
        " WHERE ju.job_id=? AND ju.expanded=0 AND u.crawl_status IN ('completed','failed','skipped') ORDER BY ju.depth, ju.url_id LIMIT 200", (job["id"],)).fetchall()
    for r in ready:
        room = job["max_pages"] - conn.execute("SELECT COUNT(*) FROM job_urls WHERE job_id=?", (job["id"],)).fetchone()[0]
        if r["crawl_status"] == "completed" and r["depth"] < job["max_depth"] and room > 0:
            norms = {n for n in (core.normalize_url(x["destination_url"]) for x in conn.execute(
                "SELECT destination_url FROM outgoing_links WHERE source_url=? AND link_type='html' AND is_active=1", (r["url"],)).fetchall()) if n}
            if norms:
                new = [x["id"] for x in conn.execute(
                    "SELECT id FROM discovered_urls d WHERE normalized_url = ANY(?::text[]) AND domain<>? AND (?::boolean OR domain=?) AND NOT EXISTS"
                    " (SELECT 1 FROM job_urls j WHERE j.job_id=? AND j.url_id=d.id) ORDER BY id LIMIT ?",
                    (sorted(norms), job["target_domain"], bj.FOLLOW_EXTERNAL_DOMAINS, r["domain"], job["id"], room)).fetchall()]
                if new:
                    conn.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, unnest(?::bigint[]), ? ON CONFLICT DO NOTHING", (job["id"], new, r["depth"] + 1))
                    conn.execute("UPDATE discovered_urls SET crawl_status='pending', skip_reason=NULL, crawl_depth=LEAST(crawl_depth, ?) WHERE id = ANY(?::bigint[])"
                                 " AND crawl_status='skipped' AND skip_reason='max_depth'", (r["depth"] + 1, new))
        conn.execute("UPDATE job_urls SET expanded=1 WHERE job_id=? AND url_id=?", (job["id"], r["url_id"]))
    return len(ready)


# ------------------------------------------------------------------ claim: same picks ------------------------------------------------------------------

def make_claim_world(seed: int) -> None:
    """Jobs of every priority, URLs on few domains (so per-domain caps bite), URLs wanted by several jobs at different depths, retry_pending URLs
    due and not due, URLs of other statuses, and jobs already served by other workers."""
    rnd = random.Random(seed)
    jobs, n_urls, doms = rnd.randint(1, 6), rnd.randint(5, 120), rnd.randint(1, 8)
    with core.connect() as c:
        for j in range(jobs):
            c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES (?,?,?,3,100)",
                      (f"biz{j}.example", rnd.choice([0, 0, 1, 2, 3]), rnd.choice(["crawling"] * 5 + ["done", "pending"])))
        for i in range(n_urls):
            st = rnd.choice(["pending"] * 6 + ["retry_pending"] * 3 + ["completed", "failed", "skipped", "crawling"])
            lease = (f"other{rnd.randint(0, 3)}", core.later(seconds=600)) if st == "crawling" else (None, None)
            nxt = core.later(seconds=rnd.choice([-3600, 3600])) if st == "retry_pending" else None
            c.execute("INSERT INTO discovered_urls (url, normalized_url, domain, hostname, crawl_status, next_crawl_at, lease_owner, lease_expires)"
                      " VALUES (?,?,?,?,?,?,?,?)", (f"http://d{i % doms}.example/p{i}", f"http://d{i % doms}.example/p{i}", f"d{i % doms}.example", f"d{i % doms}.example", st, nxt, *lease))
        ids = [r[0] for r in c.execute("SELECT id FROM discovered_urls ORDER BY id")]
        job_ids = [r[0] for r in c.execute("SELECT id FROM business_jobs ORDER BY id")]
        for uid in ids:
            for jid in rnd.sample(job_ids, rnd.choice([1, 1, 1, 2, min(3, len(job_ids))]) if len(job_ids) > 1 else 1):
                c.execute("INSERT INTO job_urls (job_id, url_id, depth) VALUES (?,?,?)", (jid, uid, rnd.randint(0, 3)))


@pytest.mark.parametrize("seed", range(40))
def test_sql_pick_equals_the_python_loop(env, seed):
    make_claim_world(seed)
    for claim_batch in (1, 3, 7, 500):
        for per_batch in (1, 2, 5, 25):
            for max_jobs in (1, 2, 9):
                cfg = SimpleNamespace(claim_batch=claim_batch, per_batch=per_batch, max_jobs_per_business=max_jobs)
                with core.connect() as c:
                    old = legacy_pick(c, "me", cfg)
                    new = {r["id"]: r["depth"] for r in bj._pick(c, "me", cfg)}
                    assert list(new.items()) == list(old.items()), (seed, claim_batch, per_batch, max_jobs)  # same URLs, same depths, same order


def test_domain_caps_make_the_pick_reach_past_claim_batch_in_one_job(env):
    """The reason _pick must not pre-limit by rank: 10 URLs of one domain come first in the job, per_batch=2 skips 8 of them, the others follow."""
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES ('biz.example', 0, 'crawling', 3, 100)")
        jid = c.execute("SELECT id FROM business_jobs").fetchone()[0]
        for i in range(10):
            core.save_url(c, f"http://big.example/p{i}", None, "t", 0, False)
        for i in range(3):
            core.save_url(c, f"http://other{i}.example/p", None, "t", 0, False)
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls", (jid,))
        cfg = SimpleNamespace(claim_batch=4, per_batch=2, max_jobs_per_business=9)
        got = [r["id"] for r in bj._pick(c, "me", cfg)]
        assert got == list(legacy_pick(c, "me", cfg)) and len(got) == 4
        assert [r[0] for r in c.execute("SELECT domain FROM discovered_urls WHERE id=ANY(?)", (got,))].count("big.example") == 2  # capped, yet 4 URLs claimed


def test_claiming_to_exhaustion_leases_every_pending_url_exactly_once(env):
    make_claim_world(7)
    with core.connect() as c:
        c.execute("DELETE FROM discovered_urls WHERE crawl_status='crawling'")  # no foreign leases: every pending URL of a crawling job is claimable
        want = {r[0] for r in c.execute("SELECT DISTINCT u.id FROM discovered_urls u JOIN job_urls ju ON ju.url_id=u.id JOIN business_jobs j ON j.id=ju.job_id"
                                        " WHERE j.status='crawling' AND u.crawl_status='pending'")}
        c.execute("UPDATE discovered_urls SET crawl_status='completed' WHERE crawl_status='retry_pending'")
    cfg = autonomous.AutoConfig(sources=("test",), claim_batch=4, per_batch=3, max_crawl_workers=50, business_reserved_workers=1, max_jobs_per_business=50)
    seen: list[int] = []
    for n in range(200):
        with core.connect() as c:
            work = bj.claim(c, f"w{n}", cfg)
        if not work:
            break
        seen += [r["id"] for r in rows("SELECT id FROM discovered_urls WHERE url=ANY(?)", [u for u, _ in work])]
    assert len(seen) == len(set(seen)), "a URL was leased twice"
    assert set(seen) == want, "a pending URL was never claimed"


def test_claim_uses_a_constant_number_of_statements(env):
    """Round trips per claim: independent of how many candidates exist."""
    make_claim_world(3)
    counts = []
    for extra in (0, 400):
        with core.connect() as c:
            for i in range(extra):
                core.save_url(c, f"http://bulk{i % 9}.example/p{i}", None, "t", 0, False)
            c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT (SELECT MIN(id) FROM business_jobs WHERE status='crawling'), id, 1 FROM discovered_urls"
                      " WHERE domain LIKE 'bulk%' ON CONFLICT DO NOTHING")
        n = []
        orig = core.Cursor.execute
        core.Cursor.execute = lambda self, q, p=None, **kw: (n.append(1), orig(self, q, p, **kw))[1]
        try:
            cfg = autonomous.AutoConfig(sources=("test",), claim_batch=5, per_batch=5, max_crawl_workers=50, business_reserved_workers=1, max_jobs_per_business=50)
            with core.connect() as c:
                bj.claim(c, "m", cfg)
        finally:
            core.Cursor.execute = orig
        counts.append(len(n))
    assert counts[0] == counts[1] <= 4, counts  # _owners + _pick + UPDATE


# ------------------------------------------------------------------ expand: same job_urls, same URL states ------------------------------------------------------------------

def make_expand_world(seed: int) -> dict:
    rnd = random.Random(seed)
    pages, doms = rnd.randint(8, 70), rnd.randint(1, 5)
    max_depth, max_pages = rnd.randint(1, 4), rnd.choice([3, 8, 25, 500])
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES ('target.example', 0, 'crawling', ?, ?)", (max_depth, max_pages))
        job = dict(c.execute("SELECT * FROM business_jobs").fetchone())
        for i in range(pages + 5):  # the last 5 live on the target's own domain: never added
            d = "target.example" if i >= pages else f"d{i % doms}.example"
            st = rnd.choice(["completed"] * 5 + ["pending", "failed", "skipped", "skipped"])
            reason = "max_depth" if st == "skipped" and rnd.random() < 0.7 else ("robots" if st == "skipped" else None)
            c.execute("INSERT INTO discovered_urls (url, normalized_url, domain, hostname, crawl_status, skip_reason, crawl_depth) VALUES (?,?,?,?,?,?,?)",
                      (f"http://{d}/p{i}", f"http://{d}/p{i}", d, d, st, reason, rnd.randint(0, 5)))
        ids = [r[0] for r in c.execute("SELECT id FROM discovered_urls ORDER BY id")]
        urls = [r[0] for r in c.execute("SELECT url FROM discovered_urls ORDER BY id")]
        for _ in range(pages * 4):  # links: html active (counted), inactive, non-html, odd spellings that normalize to the same URL
            dest = rnd.choice(urls)
            if rnd.random() < 0.15:
                dest = dest.replace("http://", "HTTP://").replace(".example/", ".EXAMPLE:80/")
            c.execute("INSERT OR IGNORE INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, link_type, is_active) VALUES (?,?,?,?,?,?)",
                      (rnd.choice(urls), dest, "x", f"a{rnd.randint(0, 3)}", rnd.choice(["html"] * 5 + ["img", "canonical"]), rnd.choice([1, 1, 1, 0])))
        for uid in rnd.sample(ids, min(len(ids), rnd.randint(1, 12))):  # the job's starting URLs (depth 0, some deeper), some already expanded
            c.execute("INSERT INTO job_urls (job_id, url_id, depth, expanded) VALUES (?,?,?,?)", (job["id"], uid, rnd.randint(0, 2), int(rnd.random() < 0.1)))
    return job


def snapshot(c) -> tuple:
    return (c.execute("SELECT job_id, url_id, depth, expanded FROM job_urls ORDER BY job_id, url_id").fetchall(),
            c.execute("SELECT id, crawl_status, skip_reason, crawl_depth FROM discovered_urls ORDER BY id").fetchall())


@pytest.mark.parametrize("follow", [False, True])
@pytest.mark.parametrize("seed", range(30))
def test_expand_adds_the_same_urls_at_the_same_depths_as_the_per_page_version(env, monkeypatch, seed, follow):
    monkeypatch.setattr(bj, "FOLLOW_EXTERNAL_DOMAINS", follow)
    job = make_expand_world(seed)
    with core.connect() as c:  # the old version runs in a transaction that is rolled back, so both versions start from identical data
        steps = 0
        while legacy_expand(c, job) and steps < 50:
            steps += 1
        old = snapshot(c)
        c.rollback()
    with core.connect() as c:
        steps = 0
        while bj.expand(c, job) and steps < 50:
            steps += 1
        new = snapshot(c)
    assert new == old, (seed, follow)


def test_expand_statement_count_does_not_grow_with_the_pages(env):
    """~6 statements per page before; now a handful per call whatever the batch size."""
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES ('target.example', 0, 'crawling', 3, 5000)")
        job = dict(c.execute("SELECT * FROM business_jobs").fetchone())
        for i in range(300):
            core.save_url(c, f"http://d{i % 7}.example/p{i}", None, "t", 0, False)
            c.execute("UPDATE discovered_urls SET crawl_status='completed' WHERE url=?", (f"http://d{i % 7}.example/p{i}",))
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, link_type)"
                  " SELECT a.url, b.url, b.domain, 'x', 'html' FROM discovered_urls a JOIN discovered_urls b ON b.id=a.id+7")
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls WHERE id<=150", (job["id"],))
    n = []
    orig = core.Cursor.execute
    core.Cursor.execute = lambda self, q, p=None, **kw: (n.append(1), orig(self, q, p, **kw))[1]
    try:
        with core.connect() as c:
            assert bj.expand(c, job) == 150
    finally:
        core.Cursor.execute = orig
    assert len(n) <= 9, len(n)  # 150 pages: the old version needed ~900 statements
    assert rows("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", job["id"])[0]["n"] > 150  # and it did expand


def test_expand_keeps_depth_budget_and_marks_every_ready_page_expanded(env):
    with core.connect() as c:
        c.execute("INSERT INTO business_jobs (target_domain, priority, status, max_depth, max_pages) VALUES ('t.example', 0, 'crawling', 1, 3)")
        job = dict(c.execute("SELECT * FROM business_jobs").fetchone())
        core.save_url(c, "http://s.example/a", None, "t", 0, False)
        c.execute("UPDATE discovered_urls SET crawl_status='completed'")
        for i in range(5):
            core.save_url(c, f"http://s.example/n{i}", None, "t", 0, False)
            c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, link_type) VALUES ('http://s.example/a', ?, 's.example', 'x', 'html')",
                      (f"http://s.example/n{i}",))
        c.execute("INSERT INTO job_urls (job_id, url_id, depth) SELECT ?, id, 0 FROM discovered_urls WHERE url='http://s.example/a'", (job["id"],))
        assert bj.expand(c, job) == 1
        got = c.execute("SELECT u.url, ju.depth, ju.expanded FROM job_urls ju JOIN discovered_urls u ON u.id=ju.url_id WHERE ju.job_id=? ORDER BY ju.url_id", (job["id"],)).fetchall()
    assert [(r["url"], r["depth"], r["expanded"]) for r in got] == [("http://s.example/a", 0, 1), ("http://s.example/n0", 1, 0), ("http://s.example/n1", 1, 0)]  # budget 3, depth+1
    with core.connect() as c:
        assert bj.expand(c, job) == 0  # nothing ready (the new URLs are pending): no further work

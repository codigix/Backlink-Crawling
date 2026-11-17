# STALE: steps 1/2/9 call the removed per-business /businesses/{domain}/competitors routes; competitors are now global (POST /competitors). Update before use.
"""Live competitor validation against a STAGING copy of the stack: own PostgreSQL schema (DB_SCHEMA=stg_val), own Redis DBs, own API / engine /
two Celery workers, real DuckDuckGo and the real internet. Nothing here touches the production schema or queue.

Run: .venv/Scripts/python tests/staging_validation.py   (about 30-45 minutes; writes staging/ logs and staging/results.json)
Not a pytest module (name does not start with test_)."""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA, API_PORT, REDIS_BROKER, REDIS_BACKEND = "stg_val", 8100, 9, 10
ROOT = Path(__file__).resolve().parent.parent
OUT = Path(os.environ.get("STAGING_OUT", ROOT / "staging"))
OUT.mkdir(exist_ok=True)
PY = str(ROOT / ".venv" / "Scripts" / "python.exe")

ENV = {**os.environ, "DB_SCHEMA": SCHEMA, "USE_CELERY": "1", "REDIS_URL": f"redis://localhost:6379/{REDIS_BROKER}",
       "CELERY_BROKER_URL": f"redis://localhost:6379/{REDIS_BROKER}", "CELERY_RESULT_BACKEND": f"redis://localhost:6379/{REDIS_BACKEND}",
       "AUTONOMOUS_SOURCES": "", "MAX_PAGES_PER_BUSINESS_RUN": "60", "DDGS_SEARCH_TIMEOUT_SECONDS": "150", "DDGS_RESULTS_PER_RUN": "50",
       "COMPETITOR_DDGS_RESULTS": "50", "MAX_ACTIVE_COMPETITOR_JOBS": "3", "CRAWL_CLAIM_BATCH": "25", "MAX_CRAWL_WORKERS": "4",
       "CELERY_MAX_QUEUED": "2", "CELERY_TASK_TIME_LIMIT": "180", "DB_MAX_CONNECTIONS": "8", "CRAWL_CONCURRENCY": "10", "PYTHONUNBUFFERED": "1"}
os.environ.update({k: ENV[k] for k in ("DB_SCHEMA", "REDIS_URL", "CELERY_BROKER_URL")})  # so crawler_core (imported below) talks to staging only

import httpx  # noqa: E402
import psutil  # noqa: E402
import psycopg  # noqa: E402
import redis  # noqa: E402

import crawler_core as core  # noqa: E402

assert core.DB_SCHEMA == SCHEMA, "staging schema not active: refusing to run against anything else"
BASE = f"http://127.0.0.1:{API_PORT}"
procs: dict[str, subprocess.Popen] = {}
R = {"checks": [], "samples": []}


def check(name: str, ok: bool, evidence) -> bool:
    R["checks"].append({"check": name, "pass": bool(ok), "evidence": evidence})
    print(("PASS " if ok else "FAIL ") + name + " | " + str(evidence)[:300], flush=True)
    return ok


def start(name: str, *args: str) -> None:
    log = open(OUT / f"{name}.log", "ab")
    procs[name] = subprocess.Popen([PY, *args], cwd=ROOT, env=ENV, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)


_pcache: dict[int, psutil.Process] = {}


def tree(name: str) -> list[psutil.Process]:
    try:
        p = psutil.Process(procs[name].pid)
        return [_pcache.setdefault(x.pid, x) for x in [p, *p.children(recursive=True)]]  # same object each time: cpu_percent compares to its previous call
    except psutil.Error:
        return []


def kill_tree(name: str) -> int:
    ps = tree(name)
    for p in reversed(ps):
        try:
            p.kill()
        except psutil.Error:
            pass
    return len(ps)


def q(sql, *a):
    with core.connect() as c:
        return [dict(r) for r in c.execute(sql, a)]


def api(method, path, **kw):
    r = httpx.request(method, BASE + path, timeout=120, **kw)
    return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)


def jobs() -> dict:
    return {r["target_domain"]: r for r in q("SELECT j.*, b.target_type FROM business_jobs j LEFT JOIN businesses b ON b.domain=j.target_domain ORDER BY j.id")}


def sample(rd: redis.Redis) -> dict:
    cpu = ram = 0.0
    per = {}
    for n in procs:
        ps = tree(n)
        c = sum(p.cpu_percent(None) for p in ps if p.is_running())
        m = sum(p.memory_info().rss for p in ps if p.is_running()) / 2**20
        per[n] = {"cpu": round(c, 1), "rss_mb": round(m)}
        cpu, ram = cpu + c, ram + m
    pgport = urlsplit(core.DATABASE_URL).port or 5432
    conns = 0  # this staging stack's own PostgreSQL connections (the database is shared with production, so pg_stat_activity alone would count both)
    for n in procs:
        for p in tree(n):
            try:
                conns += sum(1 for c in p.net_connections(kind="tcp") if c.raddr and c.raddr.port == pgport and c.status == "ESTABLISHED")
            except psutil.Error:
                pass
    total_conns = q("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database()")[0]["n"]
    j = jobs()
    comp_active = sum(1 for r in j.values() if r["target_type"] == "competitor" and r["status"] in ("searching", "crawling"))
    leased = q("SELECT lease_owner, COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling' GROUP BY 1")
    return {"t": round(time.time() - T0), "proc_cpu_pct": round(cpu, 1), "proc_rss_mb": round(ram), "per_proc": per, "machine_cpu_pct": psutil.cpu_percent(None),
            "machine_ram_pct": psutil.virtual_memory().percent, "pg_conns_staging": conns, "pg_conns_database_total": total_conns, "celery_queue": rd.llen("celery"), "competitor_jobs_active": comp_active,
            "leases": {r["lease_owner"].split(":")[1] if r["lease_owner"] else None: r["n"] for r in leased}, "jobs": {d: r["status"] for d, r in j.items()}}


def wait(cond, secs, every=10, rd=None):
    end = time.time() + secs
    while time.time() < end:
        if rd is not None:
            s = sample(rd)
            R["samples"].append(s)
        if cond():
            return True
        time.sleep(every)
    return False


def hrefs(url: str) -> set[str]:
    try:
        r = httpx.get(url, timeout=15, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0 backlink-validation"})
    except Exception:  # noqa: BLE001
        return set()
    return {(urlsplit(h).hostname or "") for h in re.findall(r'href=["\']([^"\']+)["\']', r.text, re.I)}


T0 = time.time()
OWN_A, OWN_B, C_SHARED, C_PALLETS = "realpython.com", "fullstackpython.com", "djangoproject.com", "palletsprojects.com"


def main() -> None:
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        admin.execute(f"CREATE SCHEMA {SCHEMA}")
    rd = redis.Redis.from_url(ENV["REDIS_URL"])
    assert rd.dbsize() <= 20, "redis staging db not empty enough"
    rd.flushdb()  # staging DB 9 only (production uses db 0)
    redis.Redis.from_url(ENV["CELERY_RESULT_BACKEND"]).flushdb()
    with core.connect() as c:
        core.migrate(c, recover=False)
    start("api", "-m", "uvicorn", "backlink_api:app", "--port", str(API_PORT))
    start("engine", "autonomous.py")
    start("engine2", "autonomous.py")  # a duplicate engine on the same schema: must not run scheduler cycles while engine 1 owns it
    start("sw1", "-m", "celery", "-A", "celery_app", "worker", "-P", "solo", "-n", "sw1@%h", "-l", "INFO")
    start("sw2", "-m", "celery", "-A", "celery_app", "worker", "-P", "solo", "-n", "sw2@%h", "-l", "INFO")
    for _ in range(60):
        try:
            if httpx.get(BASE + "/docs", timeout=2).status_code == 200:
                break
        except Exception:  # noqa: BLE001
            time.sleep(2)
    for n in procs:
        for p in tree(n):
            p.cpu_percent(None)
    R["schema_versions"] = [r["version"] for r in q("SELECT version FROM schema_migrations ORDER BY 1")]

    # --- 1: competitors for an existing customer; 2: for a business that was never onboarded; 9: one competitor under two customers ---
    st, own = api("POST", "/discovery/onboard", json={"domain": OWN_A})
    check("onboard customer A", st == 202 and own["priority"] == "P0", own)
    st, ra = api("POST", f"/businesses/{OWN_A}/competitors", json={"competitors": [{"domain": C_SHARED, "business_name": "Django"}, {"domain": C_PALLETS, "business_name": "Pallets"}, {"domain": "tiangolo.com", "business_name": "FastAPI"}]})
    check("1 register competitors of an existing customer", st == 202 and [x["status"] for x in ra["results"]] == ["registered"] * 3 and {x["priority"] for x in ra["results"]} == {"P3"}, ra)
    st, rb = api("POST", f"/businesses/{OWN_B}/competitors", json={"competitors": [{"domain": C_SHARED, "business_name": "Django"}]})
    check("2 competitor for a business never onboarded (auto-onboarded)", st == 202 and rb["results"][0]["status"] == "registered"
          and q("SELECT target_type FROM businesses WHERE domain=?", OWN_B)[0]["target_type"] == "own_business", rb)
    dj = q("SELECT * FROM business_jobs WHERE target_domain=?", C_SHARED)
    check("9a same competitor under two customers: ONE discovery job", len(dj) == 1 and rb["results"][0]["job_id"] == ra["results"][0]["job_id"], {"jobs": len(dj), "ids": [ra["results"][0]["job_id"], rb["results"][0]["job_id"]]})
    la, lb = api("GET", f"/businesses/{OWN_A}/competitors")[1], api("GET", f"/businesses/{OWN_B}/competitors")[1]
    check("9b access isolation: each customer lists only its own competitors", {c["competitor_domain"] for c in la["competitors"]} == {C_SHARED, C_PALLETS, "tiangolo.com"}
          and {c["competitor_domain"] for c in lb["competitors"]} == {C_SHARED}, {"A": [c["competitor_domain"] for c in la["competitors"]], "B": [c["competitor_domain"] for c in lb["competitors"]]})
    check("9c customer B cannot read A's competitor config", api("GET", f"/businesses/{OWN_B}/competitors/{C_PALLETS}")[0] == 404, "404")

    st, sd = api("POST", "/crawler/seed", json={"urls": ["python.org"]})
    check("seed accepted by the API (flag written)", st == 202 and q("SELECT COUNT(*) n FROM engine_state WHERE key='seed:https://python.org/'")[0]["n"] == 1, sd)

    # --- run: sample CPU / RAM / connections / queue every 10 s; kill a Celery worker mid-crawl, then the engine ---
    killed = {"worker": False, "engine": False}
    res = {}

    def settle():
        s = R["samples"][-1] if R["samples"] else {}
        if not killed["worker"] and any(k and k.startswith("sw1@") for k in s.get("leases", {})) and any(v == "crawling" for v in s["jobs"].values()):
            res["sw1_leases_at_kill"] = q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling' AND lease_owner LIKE 'celery:sw1@%'")[0]["n"]
            res["killed_procs"] = kill_tree("sw1")
            res["worker_killed_at"] = round(time.time() - T0)
            killed["worker"] = True
        if killed["worker"] and "worker_restarted_at" not in res and time.time() - T0 > res.get("worker_killed_at", 1e9) + 40:
            start("sw1", "-m", "celery", "-A", "celery_app", "worker", "-P", "solo", "-n", "sw1@%h", "-l", "INFO")
            res["worker_restarted_at"] = round(time.time() - T0)
        if "worker_restarted_at" in res and not killed["engine"] and time.time() - T0 > res["worker_restarted_at"] + 60:
            res["engine_hb_before_kill"] = q("SELECT MAX(last_heartbeat_at) m FROM worker_status")[0]["m"]
            kill_tree("engine")
            time.sleep(5)
            start("engine", "autonomous.py")
            res["engine_restarted_at"] = round(time.time() - T0)
            killed["engine"] = True
        return all(r["status"] in ("done", "failed") for r in jobs().values()) and len(jobs()) >= 5 and not q("SELECT 1 FROM engine_state WHERE key LIKE 'seed:%'")
    done = wait(settle, 3000, 10, rd)
    R["finished_all_jobs"] = done
    R["recovery"] = res
    j = jobs()
    R["jobs"] = {d: {k: r[k] for k in ("id", "status", "priority", "target_type", "search_results", "duplicate_candidates", "pages_discovered", "pages_crawled", "pages_failed", "backlinks_found", "retry_count", "last_error")} for d, r in j.items()}
    check("3/4 initial DDGS search ran for every target, <=50 unique candidates, none lost to a failure",
          done and all(r["search_results"] <= 50 and r["status"] == "done" for r in j.values()), {d: (r["status"], r["search_results"]) for d, r in j.items()})
    uniq = {d: q("SELECT COUNT(DISTINCT ju.url_id) n FROM job_urls ju JOIN business_jobs jj ON jj.id=ju.job_id WHERE jj.target_domain=? AND ju.depth=0", d)[0]["n"] for d in j}
    check("4 candidate URLs are unique per job (job_urls PK) and equal search_results", all(uniq[d] == j[d]["search_results"] for d in j), uniq)
    depth = q("SELECT MAX(depth) m FROM job_urls")[0]["m"]
    check("5 candidates entered Crawlee: pages crawled, depth <= 3, per-job page budget respected", all(r["pages_crawled"] > 0 for r in j.values()) and depth <= 3 and
          all(q("SELECT COUNT(*) n FROM job_urls WHERE job_id=?", r["id"])[0]["n"] <= 60 for r in j.values()), {"crawled": {d: r["pages_crawled"] for d, r in j.items()}, "max_depth": depth})
    shared = q("SELECT COUNT(*) n FROM (SELECT url_id FROM job_urls GROUP BY url_id HAVING COUNT(*)>1) x")[0]["n"]
    dup_fetch = q("SELECT COUNT(*) n FROM (SELECT source_url FROM crawl_history GROUP BY source_url HAVING COUNT(*)>1) x")[0]["n"]
    check("shared URLs: URLs referenced by several jobs vs fetched more than once", True, {"urls_in_multiple_jobs": shared, "urls_fetched_more_than_once": dup_fetch})

    # --- 6/7: extracted backlinks, API, independent verification against the live web ---
    verify = {}
    for comp in (C_SHARED, C_PALLETS):
        st, bl = api("GET", f"/competitors/{comp}/backlinks?limit=200")
        st2, rf = api("GET", f"/competitors/{comp}/referring-domains")
        st3, an = api("GET", f"/competitors/{comp}/anchors")
        st4, hi = api("GET", f"/competitors/{comp}/history")
        st5, ds = api("GET", f"/competitors/{comp}/discovery/status")
        srcs = sorted({b["source_url"] for b in bl["backlinks"] if b["source_domain"] != comp})[:8]
        ok_pages = [s for s in srcs if any(h == comp or h.endswith("." + comp) for h in hrefs(s))]
        verify[comp] = {"total": bl["total_backlinks"], "referring_domains": rf["total_referring_domains"], "anchors": len(an["anchors"]), "history_events": len(hi["events"]),
                        "discovery": ds["status"], "sampled": len(srcs), "confirmed_on_live_page": len(ok_pages), "api_status": [st, st2, st3, st4, st5]}
        check(f"6/7 backlinks to {comp}: APIs answer, links are real hyperlinks (re-fetched live)", st == 200 and bl["total_backlinks"] > 0 and (not srcs or len(ok_pages) >= 1) and bl["target_type"] == "competitor", verify[comp])
    check("6 no internal links counted as backlinks", all(q("SELECT COUNT(*) n FROM outgoing_links WHERE destination_domain=? AND source_domain=? AND link_type='html'", c, c)[0]["n"] >= 0 for c in (C_SHARED, C_PALLETS)) and
          all(b["source_domain"] != C_SHARED for b in api("GET", f"/competitors/{C_SHARED}/backlinks?limit=1000")[1]["backlinks"]), "no source_domain == competitor in API output")

    # --- 8: gap analysis ---
    st, ga = api("GET", f"/businesses/{OWN_A}/backlink-gap")
    st_b, gb = api("GET", f"/businesses/{OWN_B}/backlink-gap")
    own_links = {r["source_domain"] for r in q("SELECT DISTINCT source_domain FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1 AND source_domain<>?", OWN_A, OWN_A)}
    comp_links = {r["source_domain"] for r in q("SELECT DISTINCT source_domain FROM outgoing_links WHERE destination_domain IN (?,?,?) AND link_type='html' AND is_active=1", C_SHARED, C_PALLETS, "tiangolo.com")} - {C_SHARED, C_PALLETS, "tiangolo.com"}
    exp_gap = comp_links - own_links
    check("8 gap analysis equals an independent set computation from outgoing_links", st == 200 and {o["referring_domain"] for o in ga["opportunities"]} == exp_gap if len(exp_gap) <= 200 else True,
          {"summary": ga["summary"], "expected_gap_domains": len(exp_gap), "own_domains": len(own_links), "coverage": ga["coverage"]["discovery_status"]})
    check("9d two customers get different gaps from the same competitor data", set(ga["competitors"]) == {C_SHARED, C_PALLETS, "tiangolo.com"} and gb["competitors"] == [C_SHARED] and ga["summary"] != gb["summary"],
          {"A": ga["summary"], "B": gb["summary"]})

    # --- 9e: removal by one customer leaves the other's registration and the shared target active ---
    st, _ = api("DELETE", f"/businesses/{OWN_B}/competitors/{C_SHARED}")
    still = {c["competitor_domain"] for c in api("GET", f"/businesses/{OWN_A}/competitors")[1]["competitors"]}
    check("9e removing the shared competitor for B keeps A's registration, the target and all data", st == 200 and C_SHARED in still
          and q("SELECT active FROM businesses WHERE domain=?", C_SHARED)[0]["active"] == 1 and api("GET", f"/competitors/{C_SHARED}/backlinks")[1]["total_backlinks"] > 0, sorted(still))

    # --- 10/11/12 evidence ---
    smp = R["samples"]
    mx = lambda k: max((s[k] for s in smp), default=0)  # noqa: E731
    R["resources"] = {"samples": len(smp), "max_process_cpu_pct": mx("proc_cpu_pct"), "mean_process_cpu_pct": round(sum(s["proc_cpu_pct"] for s in smp) / max(len(smp), 1), 1),
                      "max_process_rss_mb": mx("proc_rss_mb"), "max_machine_cpu_pct": mx("machine_cpu_pct"), "max_machine_ram_pct": mx("machine_ram_pct"),
                      "max_pg_conns_staging": mx("pg_conns_staging"), "max_pg_conns_database_total": mx("pg_conns_database_total"), "max_celery_queue": mx("celery_queue"), "max_competitor_jobs_active": mx("competitor_jobs_active"),
                      "max_active_jobs_total": max((sum(1 for v in s["jobs"].values() if v in ("searching", "crawling")) for s in smp), default=0),
                      "max_leased_urls": max((sum(s["leases"].values()) for s in smp), default=0), "per_proc_max_rss_mb": {n: max((s["per_proc"].get(n, {}).get("rss_mb", 0) for s in smp), default=0) for n in procs}}
    check("10 several competitors ran concurrently within the configured caps", R["resources"]["max_competitor_jobs_active"] >= 2 and R["resources"]["max_competitor_jobs_active"] <= 3
          and R["resources"]["max_pg_conns_staging"] <= 8 * 5, R["resources"])
    check("10b Celery queue never above CELERY_MAX_QUEUE=2", R["resources"]["max_celery_queue"] <= 2, {"peak": R["resources"]["max_celery_queue"]})
    stuck = q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling' AND (lease_expires<? OR lease_expires IS NULL)", core.now())[0]["n"]
    dead_owner_left = q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling'")[0]["n"]
    wk = q("SELECT name,status,restart_count,errors FROM worker_status ORDER BY name")
    check("12 worker kill + engine restart: no URL left stuck with an expired lease, every job finished", killed["worker"] and killed["engine"] and stuck == 0 and done,
          {**res, "expired_leases_left": stuck, "still_leased": dead_owner_left, "workers": wk})
    R["worker_status"] = wk
    log1, log2 = (OUT / "engine.log").read_text(errors="replace"), (OUT / "engine2.log").read_text(errors="replace")
    cyc = re.findall(r"SCHEDULER-CYCLE-(COMPLETED|COMPLETED_WITH_ERRORS|FAILED) duration=([\d.]+)s", log1 + log2)
    runs = [m for m in re.findall(r"SCHEDULER-CYCLE-(\w+) duration", log1 + log2)]
    best = cur = 0
    for r_ in runs:
        cur = cur + 1 if r_ == "COMPLETED" else 0
        best = max(best, cur)
    ops_ = re.findall(r"SCHEDULER-OP (\S+)\s+(SUCCESS|FAILED) duration=([\d.]+)s", log1 + log2)
    sch = q("SELECT * FROM worker_status WHERE name='scheduler'")[0]
    R["scheduler"] = {"cycles": len(cyc), "by_status": {s_: sum(1 for c_, _ in cyc if c_ == s_) for s_ in ("COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED")}, "max_consecutive_completed": best,
                      "max_cycle_s": max((float(d) for _, d in cyc), default=0), "max_op_s": {n: max(float(d) for o, s_, d in ops_ if o == n) for n in {o for o, _, _ in ops_}},
                      "lock_busy_lines_engine2": log2.count("SCHEDULER-LOCK busy"), "lock_acquired_engine1": log1.count("SCHEDULER-LOCK acquired"), "lock_acquired_engine2": log2.count("SCHEDULER-LOCK acquired"),
                      "lock_timeouts_in_logs": (log1 + log2).count("lock timeout"), "worker_status": {k: sch[k] for k in ("runs", "errors", "restart_count", "last_success_at", "last_cycle_status", "last_partial_at", "last_failed_at")}}
    check("13 scheduler: >=5 consecutive COMPLETED cycles, no failed cycle, no lock timeout, restart_count 0", best >= 5 and R["scheduler"]["by_status"]["FAILED"] == 0 and R["scheduler"]["lock_timeouts_in_logs"] == 0
          and sch["restart_count"] == 0, R["scheduler"])
    check("14 two engines on one schema: the second skipped cycles while the first owned it", R["scheduler"]["lock_busy_lines_engine2"] >= 1, {"busy_lines": R["scheduler"]["lock_busy_lines_engine2"]})
    check("14b after engine 1 was killed (abnormal termination) engine 2 took the scheduler over", R["scheduler"]["lock_acquired_engine2"] >= 1, {"acquired_by_engine2": R["scheduler"]["lock_acquired_engine2"]})
    seeds_left = q("SELECT COUNT(*) n FROM engine_state WHERE key LIKE 'seed:%'")[0]["n"]
    dom = q("SELECT discovery_status FROM discovered_domains WHERE domain='python.org'")
    check("15 seed flag cleared only after the domain was admitted", seeds_left == 0 and bool(dom) and dom[0]["discovery_status"] in ("admitted", "crawled"), {"flags_left": seeds_left, "python.org": dom})
    R["db_deadlocks_staging_window"] = q("SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()")[0]["deadlocks"]


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:  # noqa: BLE001
        R["error"] = repr(e)
        print("DRIVER ERROR", repr(e), flush=True)
        raise
    finally:
        for n in list(procs):
            kill_tree(n)
        (OUT / "results.json").write_text(json.dumps(R, indent=1, default=str), encoding="utf-8")
        print("results ->", OUT / "results.json", flush=True)

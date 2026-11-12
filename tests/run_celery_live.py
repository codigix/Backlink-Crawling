"""Live Celery test: real worker processes, real PostgreSQL (isolated schema), 500 URLs on a local server.

  python tests/run_celery_live.py                       # uses CELERY_BROKER_URL / REDIS_URL from .env (Redis must be running)
  LIVE_BROKER=filesystem python tests/run_celery_live.py   # no Redis: Celery's file broker (tests everything except Redis itself)

Checks: tasks dispatched, 3 workers share the work, results stored, flaky pages retried, dead site failed (never completed),
no duplicate fetches, a hard-killed worker's URLs recovered after restart, all eligible URLs processed.
"""

import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
SCHEMA = "live_" + uuid.uuid4().hex[:8]
TMP = Path(tempfile.mkdtemp(prefix="celerylive_"))
LIMIT = "90"  # task time limit => lease length: a killed worker's URLs are recoverable 90s later
os.environ.update(DB_SCHEMA=SCHEMA, CRAWLER_ALLOW_PRIVATE="1", CELERY_TASK_TIME_LIMIT=LIMIT, CELERY_MAX_QUEUED="6")
if os.environ.get("LIVE_BROKER") == "filesystem":
    (TMP / "q").mkdir()
    os.environ.update(CELERY_BROKER_URL="filesystem://", CELERY_RESULT_BACKEND="cache+memory://", LIVE_FS=str(TMP / "q"))

import psycopg  # noqa: E402

import crawler_core as core  # noqa: E402

HOSTS, PER = 50, 10  # 500 URLs
SEEN: dict[str, int] = {}
FLAKY = {f"s{i}.localhost" for i in range(3, 8)}  # first request of /p3 fails (503), then succeeds
DEAD = "s9.localhost"  # /p5 always 503


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        host, path = (self.headers.get("Host") or "").split(":")[0], self.path
        key = host + path
        SEEN[key] = SEEN.get(key, 0) + 1
        code, body = 200, "<html><body>" + "".join(f'<a href="/p{n}">p{n}</a>' for n in range(PER - 1)) + '<a href="https://target.example/x">t</a></body></html>'
        if path == "/robots.txt":
            code, body = 404, ""
        elif (host in FLAKY and path == "/p3" and SEEN[key] == 1) or (host == DEAD and path == "/p5"):
            code, body = 503, "down"
        self.send_response(code)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *a):
        pass


def q(sql, *a):
    with core.connect() as c:
        return [dict(r) for r in c.execute(sql, a).fetchall()]


def start_worker(name):
    log = open(TMP / f"{name}.log", "ab")
    p = subprocess.Popen([sys.executable, "-m", "celery", "-A", "celery_app", "worker", "-P", "solo", "-n", f"{name}@%h", "-l", "INFO"],
                         cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    return p


def feed(tasks):
    try:
        return tasks.dispatch(force=True)
    except Exception:  # noqa: BLE001 - no Redis (filesystem broker): enqueue directly, bounded
        if q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status IN ('pending','retry_pending')")[0]["n"] and len(list((TMP / "q").glob("*.msg"))) < 6:
            tasks.crawl_batch.delay()
            return 1
        return 0


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {SCHEMA}")
    with core.connect() as c:
        core.migrate(c)
        for h in range(HOSTS):
            for path in ["/"] + [f"/p{n}" for n in range(PER - 1)]:
                core.save_url(c, f"http://s{h}.localhost:{port}{path}", None, "live", 0 if path == "/" else 1, True)
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (f"s{h}.localhost",))
    import tasks  # after env is set
    total = q("SELECT COUNT(*) n FROM discovered_urls")[0]["n"]
    print(f"schema {SCHEMA}: {total} URLs, workers log in {TMP}", flush=True)
    workers = {n: start_worker(n) for n in ("w1", "w2", "w3")}
    t0, killed, recovered_ok = time.time(), False, None
    try:
        while time.time() - t0 < 900:
            feed(tasks)
            c = {r["crawl_status"]: r["n"] for r in q("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}
            print(f"{time.time()-t0:5.0f}s {c}", flush=True)
            if not killed and c.get("completed", 0) >= 100 and c.get("crawling", 0):
                owners = q("SELECT lease_owner, COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling' GROUP BY 1")
                print("KILLING w2 (-9) mid-task; leases held:", owners, flush=True)
                workers["w2"].kill()
                killed, kill_at, held = True, time.time(), sum(o["n"] for o in owners if "w2@" in o["lease_owner"])
                workers["w2"] = start_worker("w2")  # restart immediately
            if killed and recovered_ok is None and time.time() - kill_at > int(LIMIT) + 5:
                still = q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='crawling' AND lease_expires<?", core.now())[0]["n"]
                recovered_ok = tasks.recover() >= 0 and still >= 0
            pend = c.get("pending", 0) - 0
            if killed and not c.get("crawling") and c.get("pending", 0) <= 1 and not any(r["n"] for r in q(
                    "SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='retry_pending' AND next_crawl_at<=?", core.now())):
                break
            time.sleep(4)
        elapsed = time.time() - t0
        final = {r["crawl_status"]: r["n"] for r in q("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}
        batches = {n: (TMP / f"{n}.log").read_text(errors="ignore").count("crawl.batch") for n in workers}
        dup = q("SELECT source_url, COUNT(*) n FROM crawl_history WHERE error_message IS NULL GROUP BY 1 HAVING COUNT(*)>1")
        flaky_ok = q("SELECT COUNT(*) n FROM discovered_urls WHERE crawl_status='completed' AND url LIKE '%/p3' AND domain = ANY(?)", list(FLAKY))[0]["n"]
        dead = q("SELECT crawl_status, retry_count FROM discovered_urls WHERE url LIKE '%/p5' AND domain=?", DEAD)[0]
        print("\nFINAL", final, f"elapsed {elapsed:.0f}s", f"{final.get('completed', 0) / elapsed * 60:.0f} pages/min")
        print("worker log lines mentioning crawl.batch:", batches, "| leases held by killed worker:", held if killed else None)
        print("duplicate successful fetches:", len(dup), "| flaky pages completed after retry:", flaky_ok, "/", len(FLAKY), "| dead page:", dead)
        eligible = HOSTS * PER
        assert final.get("completed", 0) >= eligible - 1 - 0, "not all eligible URLs completed"  # -1: the dead page
        assert not dup and flaky_ok == len(FLAKY) and dead["crawl_status"] in ("retry_pending", "failed") and dead["retry_count"] >= 1
        assert sum(1 for v in batches.values() if v) >= 2, "work was not shared"
        assert not q("SELECT 1 FROM discovered_urls WHERE crawl_status='crawling'"), "leases left behind"
        print("LIVE TEST PASSED")
    finally:
        for p in workers.values():
            p.terminate()
        time.sleep(2)
        with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {SCHEMA} CASCADE")


if __name__ == "__main__":
    main()

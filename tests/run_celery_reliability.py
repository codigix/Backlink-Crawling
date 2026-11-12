"""Celery reliability scenarios with REAL worker processes, real Redis (spare db 5) and real PostgreSQL (isolated schema).

  .venv/Scripts/python tests/run_celery_reliability.py [s1 s2 s3 s4 ...]     (default: all, ~6 minutes)

s1  worker unavailable during dispatch (no worker; Redis itself down) -> nothing lost, finished once a worker appears
s2  worker restart between batches                                        -> every URL completed exactly once
s3  graceful shutdown (Ctrl-Break) while a task is mid-flight             -> worker exits, no lease left behind
s4  hard kill (TerminateProcess) mid-task                                 -> leases survive, are NOT stolen while valid, are recovered after
                                                                             expiry, the unacked message is redelivered, no duplicate records
Every scenario also checks: no URL claimed by two workers, no URL recorded as fetched twice.
"""

import os
import socket
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
SCHEMA = "rel_" + uuid.uuid4().hex[:8]
TMP = Path(tempfile.mkdtemp(prefix="celeryrel_"))
LIMIT = 65  # CELERY_TASK_TIME_LIMIT (smallest allowed: soft limit = LIMIT-60): lease length 65s, Redis visibility timeout 130s
SLOW = 8    # seconds a "slow" page takes to answer: holds a task in flight
BROKER = "redis://localhost:6379/5"  # spare db, never the production queue in db 0
os.environ.update(DB_SCHEMA=SCHEMA, CRAWLER_ALLOW_PRIVATE="1", CELERY_TASK_TIME_LIMIT=str(LIMIT), CELERY_MAX_QUEUED="4",
                  CELERY_BROKER_URL=BROKER, CELERY_RESULT_BACKEND=BROKER, REDIS_URL=BROKER, USE_CELERY="1")

import psycopg  # noqa: E402
import redis  # noqa: E402

import crawler_core as core  # noqa: E402

R = redis.Redis.from_url(BROKER)
SEEN: dict[str, int] = {}
WORKERS: list = []


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        SEEN[host + self.path] = SEEN.get(host + self.path, 0) + 1
        if self.path == "/robots.txt":
            self.send_response(404)
            self.end_headers()
            return
        if host.startswith("slow"):
            time.sleep(SLOW)
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body><a href='https://target.example/x'>t</a></body></html>")

    def log_message(self, *a):
        pass


def q(sql, *a):
    with core.connect() as c:
        return [dict(r) for r in c.execute(sql, a).fetchall()]


def counts():
    return {r["crawl_status"]: r["n"] for r in q("SELECT crawl_status, COUNT(*) n FROM discovered_urls GROUP BY 1")}


def wait(cond, timeout, what):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return time.time() - t0
        time.sleep(1)
    raise AssertionError(f"timeout waiting for {what}; counts={counts()} queue={R.llen('celery')} unacked={R.hlen('unacked')}")


def reset():
    with core.connect() as c:
        for t in ("discovered_urls", "discovered_domains", "crawl_history", "outgoing_links"):
            c.execute(f"TRUNCATE {t} RESTART IDENTITY CASCADE")
    for k in R.keys():
        R.delete(k)
    SEEN.clear()


def seed(port, prefix: str, n_hosts: int, per: int):
    with core.connect() as c:
        for h in range(n_hosts):
            for p in range(per):
                core.save_url(c, f"http://{prefix}{h}.localhost:{port}/p{p}", None, "rel", 0, True)
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (f"{prefix}{h}.localhost",))


def start_worker(name):
    log = open(TMP / f"{name}.log", "ab")
    p = subprocess.Popen([sys.executable, "-m", "celery", "-A", "celery_app", "worker", "-P", "solo", "-n", f"{name}@%h", "-l", "INFO"],
                         cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    WORKERS.append(p)
    return p


def graceful(p) -> int:
    """Warm shutdown through Celery's own remote-control command (Windows delivers no catchable signal to a console child)."""
    import celery_app
    name = next(a.split("@")[0] for a in p.args if "@%h" in a)
    for _ in range(12):  # a broadcast sent while the worker is busy with a task is dropped, so repeat until it exits
        celery_app.app.control.shutdown(destination=[f"{name}@{socket.gethostname()}"])
        try:
            return p.wait(10)
        except subprocess.TimeoutExpired:
            continue
    raise AssertionError(f"worker {name} ignored shutdown for 120s")


def no_duplicates(total: int, allow_refetch=()):
    dup = q("SELECT source_url, COUNT(*) n FROM crawl_history WHERE error_message IS NULL GROUP BY 1 HAVING COUNT(*)>1")
    assert not dup, f"URLs recorded as fetched twice: {dup[:3]}"
    refetched = sorted(k for k, v in SEEN.items() if v > 1 and k not in allow_refetch and not k.endswith("robots.txt"))
    assert not refetched, f"URLs fetched from the site more than once: {refetched[:5]}"
    assert counts().get("completed") == total, counts()
    assert not q("SELECT 1 FROM discovered_urls WHERE lease_owner IS NOT NULL"), "lease left behind"


def s1(tasks, port):
    seed(port, "a", 2, 10)
    assert tasks.dispatch(force=True) > 0
    assert R.llen("celery") > 0 and counts() == {"pending": 20}, "tasks queued, URLs untouched while no worker exists"
    live, tasks._redis = tasks._redis, redis.Redis(port=1, socket_connect_timeout=1)  # Redis down at dispatch time
    try:
        tasks.dispatch(force=True)
        raise AssertionError("dispatch should surface a broker outage")
    except redis.RedisError:
        pass
    finally:
        tasks._redis = live
    assert counts() == {"pending": 20}, "a failed dispatch loses nothing: PostgreSQL still holds every URL"
    w = start_worker("s1w")
    wait(lambda: counts().get("completed") == 20, 120, "20 completed after the worker appeared")
    graceful(w)
    no_duplicates(20)


def s2(tasks, port):
    seed(port, "b", 4, 10)
    tasks.dispatch(force=True)
    w1 = start_worker("s2w1")
    wait(lambda: counts().get("completed", 0) >= 5, 120, "first completions")
    code = graceful(w1)
    assert not q("SELECT 1 FROM discovered_urls WHERE crawl_status='crawling'"), "graceful stop left leases"
    w2 = start_worker("s2w2")  # restart
    tasks.dispatch(force=True)
    wait(lambda: counts().get("completed") == 40, 150, "all 40 after restart")
    graceful(w2)
    no_duplicates(40)
    return f"w1 exit code {code}"


def s3(tasks, port):
    seed(port, "slow", 1, 6)
    tasks.dispatch(force=True)
    w = start_worker("s3w")
    wait(lambda: counts().get("crawling", 0) > 0, 90, "task mid-flight")
    t0 = time.time()
    code = graceful(w)
    took = time.time() - t0
    assert not q("SELECT 1 FROM discovered_urls WHERE crawl_status='crawling'"), "graceful shutdown left a lease"
    done_before = counts().get("completed", 0)
    w2 = start_worker("s3w2")
    tasks.dispatch(force=True)
    wait(lambda: counts().get("completed") == 6, 120, "remaining after graceful stop")
    graceful(w2)
    no_duplicates(6, allow_refetch=[k for k in SEEN if k.startswith("slow")] if done_before < 6 else ())
    return f"exit code {code} after {took:.0f}s; {done_before}/6 finished before exit, rest re-claimed"


def s4(tasks, port):
    seed(port, "slow", 1, 6)
    tasks.dispatch(force=True)
    w = start_worker("s4w")
    wait(lambda: counts().get("crawling", 0) > 0, 90, "task mid-flight")
    held = q("SELECT url, lease_owner FROM discovered_urls WHERE crawl_status='crawling'")
    w.kill()
    w.wait()
    t_kill = time.time()
    assert R.hlen("unacked") >= 1, "acks_late: the dead worker's message must still be unacked in Redis"
    assert counts().get("crawling") == len(held) and all(h["lease_owner"].startswith("celery:") for h in held)
    w2 = start_worker("s4w2")  # a new worker must not steal live leases
    tasks.dispatch(force=True)
    time.sleep(15)
    stolen = tasks.recover.apply().result
    assert counts().get("crawling") == len(held) and stolen == 0, f"valid leases must not be taken over: held={len(held)} counts={counts()} recover freed={stolen}"
    first_hits = [k for k in SEEN if k.startswith("slow")]
    wait(lambda: time.time() - t_kill > LIMIT + 3, 120, "lease expiry")
    freed = tasks.recover.apply().result
    assert freed == len(held), f"expired leases recovered: {freed} of {len(held)}"
    tasks.dispatch(force=True)
    wait(lambda: counts().get("completed") == 6, 150, "all 6 completed after recovery")
    wait(lambda: R.hlen("unacked") == 0 and R.llen("celery") == 0, LIMIT * 2 + 240, "redelivered message consumed")
    graceful(w2)
    no_duplicates(6, allow_refetch=first_hits)
    return f"{len(held)} leases survived the kill, recovered after {LIMIT}s, message redelivered+consumed ~{time.time() - t_kill:.0f}s after kill"


def main():
    want = sys.argv[1:] or ["s1", "s2", "s3", "s4"]
    srv = ThreadingHTTPServer(("0.0.0.0", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {SCHEMA}")
    with core.connect() as c:
        core.migrate(c)
    import tasks
    print(f"schema {SCHEMA}, worker logs in {TMP}", flush=True)
    results = {}
    try:
        for name in want:
            reset()
            t0 = time.time()
            try:
                note = globals()[name](tasks, srv.server_address[1])
                results[name] = f"PASS ({time.time() - t0:.0f}s) {note or ''}"
            except Exception as e:  # noqa: BLE001
                results[name] = f"FAIL: {e!r}"
            print(name, results[name], flush=True)
    finally:
        for p in WORKERS:
            if p.poll() is None:
                p.kill()
        time.sleep(1)
        with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
        for k in R.keys():
            R.delete(k)
    print("\nSUMMARY")
    for k, v in results.items():
        print(" ", k, v)
    sys.exit(any(v.startswith("FAIL") for v in results.values()))


if __name__ == "__main__":
    main()

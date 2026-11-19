"""Scheduling simulation on a VIRTUAL clock: the real schedule_due / claim / reschedule code against a scratch PostgreSQL schema, no waiting.

  DATABASE_URL=postgresql://postgres:postgres@localhost:5432/<scratch> python tests/sim_fairness.py [--days 7] [--search-seconds 120] [--policy hierarchical|target]

It reports, per target type and per parent business: discovery turns, and the delay between "due" and "search started". One search slot
(DDGS_SEARCH_CONCURRENCY=1) and the business/competitor intervals of business_jobs (DDGS_INTERVAL_HOURS / COMPETITOR_DDGS_INTERVAL_HOURS).
The aged-competitor claim is switched off (it compares real created_at): what is measured is the rotation itself. Refuses the production tunnel port.
"""

import argparse
import random
import statistics
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import asyncio  # noqa: E402

import business_jobs as bj  # noqa: E402
import competitors  # noqa: E402
import crawler_core as core  # noqa: E402

FMT = "%Y-%m-%d %H:%M:%S"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--search-seconds", type=float, default=120, help="how long one DuckDuckGo search holds the slot (virtual)")
    ap.add_argument("--policy", default="hierarchical")
    ap.add_argument("--owners", default="big:10,mid:2,solo:0", help="business:competitors,...")
    a = ap.parse_args()
    if ":5433" in core.DATABASE_URL:
        sys.exit("refusing: port 5433 is the production tunnel; point DATABASE_URL at the local scratch database")
    schema = "sim_" + uuid.uuid4().hex[:8]
    with psycopg.connect(core.DATABASE_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        try:
            run(schema, a)
        finally:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


def run(schema: str, a) -> None:
    core.DB_SCHEMA = schema
    random.seed(1)
    clock = [datetime.now(timezone.utc).replace(microsecond=0)]
    start = clock[0]
    core.now = lambda: clock[0].strftime(FMT)
    core.later = lambda **kw: (clock[0] + timedelta(**kw)).strftime(FMT)
    bj.FAIR_POLICY, bj.COMPETITOR_STARVATION_SECONDS, bj.DDG_CONCURRENCY, bj.DDG_COOLDOWN = a.policy, 0, 1, 0
    bj.ddg_lookup.search_urls = lambda d, n, **_: []  # the search itself is not under test; how long it holds the slot is (--search-seconds)
    with core.connect() as c:
        core.migrate(c, recover=False)
    owners = {f"{k}.sim": int(v) for k, v in (x.split(":") for x in a.owners.split(","))}
    with core.connect() as c:
        for b in owners:
            bj.create_job(c, b)
        for b, n in owners.items():
            for i in range(n):
                competitors.register(c, b, f"{b.split('.')[0]}-c{i}.sim")
    parent = {b: b for b in owners} | {f"{b.split('.')[0]}-c{i}.sim": b for b, n in owners.items() for i in range(n)}
    kind = {t: ("business" if t in owners else "competitor") for t in parent}
    turns: dict[str, int] = dict.fromkeys(parent, 0)
    delays: dict[str, list[float]] = {"business": [], "competitor": []}
    end = start + timedelta(days=a.days)
    while clock[0] < end:
        bj.tick()  # schedules due targets, closes finished jobs (which sets the next due time)
        jobs = bj._claim_search(1)
        if not jobs:  # idle: jump to the next moment anything is due (nothing can change before it)
            with core.connect() as c:
                nxt = c.execute("SELECT MIN(next_run_at) FROM businesses WHERE active=1").fetchone()[0]
            clock[0] = max(clock[0] + timedelta(seconds=1), datetime.strptime(nxt, FMT).replace(tzinfo=timezone.utc)) if nxt else end
            continue
        j = jobs[0]
        with core.connect() as c:
            due = c.execute("SELECT next_run_at FROM businesses WHERE domain=?", (j["target_domain"],)).fetchone()[0]
        delays[kind[j["target_domain"]]].append((clock[0] - datetime.strptime(due, FMT).replace(tzinfo=timezone.utc)).total_seconds())
        turns[j["target_domain"]] += 1
        asyncio.run(bj._search_one(j))
        clock[0] += timedelta(seconds=a.search_seconds)
    print(f"policy={a.policy} days={a.days:g} search={a.search_seconds:g}s slot=1 | intervals: business {bj.SEARCH_INTERVAL // 3600}h, competitor {bj.COMPETITOR_INTERVAL // 3600}h")
    for k, d in delays.items():
        if d:
            s = sorted(d)
            print(f"  {k:10s} searches={len(d):4d} delay(due->start) mean={statistics.mean(d) / 60:8.1f}min p95={s[int(len(s) * .95) - 1] / 60:8.1f}min max={s[-1] / 60:8.1f}min")
    for b in owners:
        print(f"  parent {b:10s} turns={sum(n for t, n in turns.items() if parent[t] == b):4d}  (own {turns[b]}, competitors {sum(n for t, n in turns.items() if parent[t] == b and t != b)})")
    served = sum(1 for n in turns.values() if n)
    print(f"  targets served at least once: {served}/{len(turns)}  min turns per target: {min(turns.values())}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""python backlinks_cli.py <domain> [--release cc-main-2026-aug-sep-oct] [--out file.txt]"""
import argparse
from cc_lookup import DATA, query

ap = argparse.ArgumentParser()
ap.add_argument("domain")
ap.add_argument("--release", default="cc-main-2026-aug-sep-oct")
ap.add_argument("--out")
a = ap.parse_args()

# shortcut: reloads the graph (~15 s) on every call, fine for one-off lookups
found = query((DATA / f"{a.release}-domain").as_posix(), a.domain.lower().removeprefix("www."))
if found is None:
    print(f"{a.domain}: not in the {a.release} graph")
else:
    print("\n".join(sorted(found)) + f"\n\n{a.domain}: {len(found)} referring domains")
    if a.out:
        open(a.out, "w").write("\n".join(sorted(found)))

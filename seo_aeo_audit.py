"""SEO (pyseoanalyzer) + AEO/GEO (sage-audit) + structured data (extruct) in one JSON report."""
import json
import sys
import types

sys.modules.setdefault("resource", types.ModuleType("resource"))  # sage-audit imports it; Unix-only, only used for memory eval

from dataclasses import asdict

import socket

_getaddrinfo = socket.getaddrinfo


def _ipv4_only(host, port, family=0, *args, **kwargs):
    return _getaddrinfo(host, port, socket.AF_INET if family == 0 else family, *args, **kwargs)


# shortcut: force IPv4 for every tool (unreachable IPv6 causes 10s timeouts); remove if your IPv6 works.
# Installed before geo_optimizer is imported so its DNS-pinning wrapper chains onto this.
socket.getaddrinfo = _ipv4_only

import extruct
import geo_optimizer
import ppc_checks
import pyseoanalyzer
import requests
import sage_audit


def normalize_url(url: str) -> str:
    return url if url.startswith(("http://", "https://")) else f"https://{url}"


def run(url: str) -> dict:
    url = normalize_url(url)
    report = {"url": url}
    steps = {
        # follow_links=False: audit this page only, not the whole site
        "ppcblogpro_style": lambda: ppc_checks.analyze(
            requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).text, url
        ),
        "seo_pyseoanalyzer": lambda: pyseoanalyzer.analyze(url, follow_links=False, analyze_headings=True),
        "sage_audit": lambda: sage_audit.audit_url(url).model_dump(mode="json"),
        "geo_optimizer": lambda: asdict(geo_optimizer.audit(url)),
        "structured_data_extruct": lambda: extruct.extract(
            requests.get(url, timeout=20).text, base_url=url
        ),
    }
    for name, fn in steps.items():
        try:
            report[name] = fn()
        except Exception as e:  # one tool failing must not lose the others' results
            report[name] = {"error": f"{type(e).__name__}: {e}"}
    return report


def summary(r: dict) -> str:
    lines = [f"URL: {r['url']}"]
    c = r["ppcblogpro_style"]
    if "error" in c:
        lines.append(f"PPCBlogPro-style checks: ERROR {c['error']}")
    else:
        lines.append(f"== PPCBlogPro-style 48 checks: {c['overall']}/100 ==")
        for p in ("seo", "aeo", "geo"):
            lines.append(f"  {p.upper()}: {c[p]['score']}/100 ({c[p]['passed']} passed)")
            lines += [f"    - FIX {k['name']} +{k['weight']}: {k['detail']}" for k in c[p]["checks"] if k["status"] == "FIX"]
        lines.append("")
    s = r["sage_audit"]
    if "error" in s:
        lines.append(f"sage-audit: ERROR {s['error']}")
    else:
        lines.append(f"sage-audit overall: {s['overall_score']}/100 ({s['grade']})")
        for p in ("seo", "aeo", "geo"):
            lines.append(f"  {p.upper()}: {s[p]['score']}/100 ({s[p]['grade']})")
            for f in s[p]["findings"]:
                if f["status"] != "pass":
                    lines.append(f"    - [{f['status']}] {f['title']}: {f['details']}")
    g = r["geo_optimizer"]
    if g.get("error"):
        lines.append(f"geo-optimizer: ERROR {g['error']}")
    else:
        lines.append(f"geo-optimizer GEO: {g['score']}/100 ({g['band']})")
        lines += [f"    - {rec}" for rec in g.get("recommendations", [])]
    p = r["seo_pyseoanalyzer"]
    if "error" in p:
        lines.append(f"pyseoanalyzer: ERROR {p['error']}")
    else:
        for pg in p["pages"]:
            lines.append(f"pyseoanalyzer: title={pg['title']!r} words={pg['word_count']}")
            noisy = [w for w in pg["warnings"] if w.startswith(("Anchor missing title tag", "Image missing alt tag"))]
            lines += [f"    - {w}" for w in pg["warnings"] if w not in noisy]
            lines.append(f"    - {len(noisy)} anchor/image warnings (full list in JSON)")
    x = r["structured_data_extruct"]
    lines.append("extruct: " + (x["error"] if "error" in x else ", ".join(f"{k}={len(v)}" for k, v in x.items())))
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: python seo_aeo_audit.py <url> [out.json]")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Windows cp1252 can't print some finding text
    report = run(sys.argv[1])
    out = sys.argv[2] if len(sys.argv) > 2 else "seo_aeo_report.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(summary(report))
    print(f"\nSaved full report to {out}")


if __name__ == "__main__":
    main()

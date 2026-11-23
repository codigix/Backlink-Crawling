"""The backlink gap as it was before Phase 12, verbatim (it loaded every link set into Python). Kept ONLY as the oracle of tests/test_phase12.py."""

import business_jobs as bj
from competitors import _reg, own_business

LINKS = ("SELECT source_url, source_domain, destination_url, anchor_text, rel_attributes, is_nofollow, is_sponsored, is_ugc, first_seen, last_seen"
         " FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1 AND source_domain<>? ORDER BY id")


def gap_reference(conn, owner: str, only: str | None = None, limit: int = 200) -> dict:
    owner = _reg(owner)
    if not own_business(conn, owner):
        raise LookupError(f"{owner} is not an onboarded business")
    comps = [r["competitor_domain"] for r in conn.execute(  # only this business's own competitors (never itself)
        "SELECT competitor_domain FROM business_competitors WHERE business_domain=? AND active=1 AND competitor_domain<>?"
        " GROUP BY competitor_domain ORDER BY MIN(id)", (owner, owner)).fetchall()]
    if only:
        only = _reg(only)
        if not conn.execute("SELECT 1 FROM business_competitors WHERE business_domain=? AND competitor_domain=?", (owner, only)).fetchone():
            raise LookupError(f"{only} is not a competitor of {owner}")
        comps = [only]
    own_rows = conn.execute(LINKS, (owner, owner)).fetchall()
    own_domains = {r["source_domain"] for r in own_rows}
    own_pages = {r["source_url"] for r in own_rows}
    cdom: dict[str, dict[str, set]] = {}  # source domain -> competitor -> pages
    pages: dict[str, dict] = {}  # source page -> info
    for c in comps:
        for r in conn.execute(LINKS, (c, c)).fetchall():
            cdom.setdefault(r["source_domain"], {}).setdefault(c, set()).add(r["source_url"])
            p = pages.setdefault(r["source_url"], {"source_url": r["source_url"], "referring_domain": r["source_domain"], "competitor_targets": set(), "links": []})
            p["competitor_targets"].add(c)
            p["links"].append({"competitor": c, "destination_url": r["destination_url"], "anchor_text": r["anchor_text"], "rel": r["rel_attributes"],
                               "nofollow": bool(r["is_nofollow"]), "sponsored": bool(r["is_sponsored"]), "ugc": bool(r["is_ugc"]), "last_seen": r["last_seen"]})
    gap_domains = sorted(set(cdom) - own_domains)
    shared = sorted(set(cdom) & own_domains)
    own_only = sorted(own_domains - set(cdom))
    page_gap = [{**p, "competitor_targets": sorted(p["competitor_targets"]), "domain_links_to_own_business": p["referring_domain"] in own_domains}
                for u, p in sorted(pages.items()) if u not in own_pages]
    states = {t: bj.api_state(bj.latest(conn, t)) for t in [owner, *comps]}
    warnings = []
    if not states[owner].startswith("completed"):
        warnings.append(f"discovery for {owner} is '{states[owner]}': its backlinks may be incomplete, so gap domains may be overstated")
    warnings += [f"discovery for competitor {c} is '{states[c]}': its backlinks may be incomplete" for c in comps if not states[c].startswith("completed")]
    return {"business_domain": owner, "competitors": comps,
            "summary": {"own_referring_domains": len(own_domains), "competitor_referring_domains": len(cdom), "gap_domains": len(gap_domains),
                        "shared_domains": len(shared), "own_only_domains": len(own_only), "gap_pages": len(page_gap)},
            "coverage": {"basis": "discovered index only (pages crawled so far), not the whole internet", "discovery_status": states, "warnings": warnings},
            "opportunities": [{"referring_domain": d, "classification": "competitor_only", "competitor_targets": sorted(cdom[d]), "links_to_own_business": False,
                               "source_pages": sorted(u for ps in cdom[d].values() for u in ps)[:50]} for d in gap_domains[:limit]],
            "shared_domains": shared[:limit], "own_only_domains": own_only[:limit], "source_page_gap": page_gap[:limit]}

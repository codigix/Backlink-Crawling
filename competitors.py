"""Competitors: a competitor is a monitored TARGET like any business, so it reuses everything business_jobs does (DDGS search, job_urls,
Crawlee claims, the shared outgoing_links graph). This module only adds the owner relation (business_competitors), target
classification (businesses.target_type) and the backlink gap, which is computed from the graph. Nothing here crawls or searches.
"""

import business_jobs as bj
import crawler_core as core

LINKS = ("SELECT source_url, source_domain, destination_url, anchor_text, rel_attributes, is_nofollow, is_sponsored, is_ugc, first_seen, last_seen"
         " FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1 AND source_domain<>? ORDER BY id")


GLOBAL = "*"  # owner of a competitor registered once for every business (POST /competitors)


def _o(owner: str) -> str:
    return owner if owner == GLOBAL else _reg(owner)


def _reg(domain: str) -> str:
    return core.registrable_domain(core.clean_target(domain))  # ValueError for anything that is not a hostname


def own_business(conn, domain: str) -> dict | None:
    r = conn.execute("SELECT * FROM businesses WHERE domain=? AND target_type='own_business'", (_reg(domain),)).fetchone()
    return dict(r) if r else None


def known_backlinks(conn, domain: str) -> int:
    """Links already in the shared graph: available the moment a competitor is registered, before anything new is crawled."""
    return conn.execute("SELECT COUNT(*) FROM outgoing_links WHERE destination_domain=? AND link_type='html' AND is_active=1 AND source_domain<>?",
                        (domain, domain)).fetchone()[0]


def _sync_target(conn, comp: str) -> None:
    """A competitor target is monitored while at least one owner has it active; own businesses are never touched."""
    conn.execute("UPDATE businesses SET active=CASE WHEN EXISTS (SELECT 1 FROM business_competitors WHERE competitor_domain=? AND active=1) THEN 1 ELSE 0 END"
                 " WHERE domain=? AND target_type='competitor'", (comp, comp))


def register(conn, owner: str, domain: str, name: str | None = None) -> dict:
    """Register one competitor of `owner` (an onboarded business) and queue its first discovery job. Raises LookupError (owner unknown)
    or ValueError (invalid domain, the owner itself). A repeat registration returns status 'duplicate' and changes nothing."""
    owner, comp = _o(owner), _reg(domain)
    if comp == owner:
        raise ValueError("a business cannot be its own competitor")
    if owner != GLOBAL and not own_business(conn, owner):
        raise LookupError(f"{owner} is not an onboarded business")  # the API onboards it first; direct callers must
    if not conn.execute("INSERT INTO business_competitors (business_domain, competitor_domain, competitor_name) VALUES (?,?,?) ON CONFLICT DO NOTHING RETURNING id",
                        (owner, comp, name)).fetchone():
        return {"domain": comp, "status": "duplicate", "known_backlinks": known_backlinks(conn, comp)}
    job, existing = None, conn.execute("SELECT target_type FROM businesses WHERE domain=?", (comp,)).fetchone()
    if existing and existing[0] == "own_business":
        note = "already monitored as a customer business"  # never turned into a competitor; it keeps its own schedule
    elif not bj.COMPETITOR_ENABLED:
        note = "disabled (COMPETITOR_DISCOVERY_ENABLED=false)"
    else:
        job, _ = bj.create_job(conn, comp, name=name, target_type="competitor")
        note = "queued"
    _sync_target(conn, comp)
    return {"domain": comp, "status": "registered", "job_id": job and job["id"], "priority": job and f"P{job['priority']}",
            "target_type": conn.execute("SELECT target_type FROM businesses WHERE domain=?", (comp,)).fetchone()[0] if (job or existing) else None,
            "known_backlinks": known_backlinks(conn, comp), "discovery": note}


def detail(conn, owner: str, comp: str) -> dict | None:
    owner, comp = _o(owner), _reg(comp)
    r = conn.execute("SELECT * FROM business_competitors WHERE business_domain=? AND competitor_domain=?", (owner, comp)).fetchone()
    if not r:
        return None
    job, sched = bj.latest(conn, comp), conn.execute("SELECT target_type, next_run_at, last_run_at, active FROM businesses WHERE domain=?", (comp,)).fetchone()
    return {"competitor_domain": comp, "competitor_name": r["competitor_name"], "active": bool(r["active"]), "registered_at": r["created_at"],
            "target_type": sched["target_type"] if sched else "competitor", "discovery_status": bj.api_state(job), "job": job,
            "schedule": dict(sched) if sched else None, "known_backlinks": known_backlinks(conn, comp)}


def listing(conn, owner: str) -> list[dict]:
    owner = _o(owner)
    return [detail(conn, owner, r["competitor_domain"]) for r in conn.execute(
        "SELECT competitor_domain FROM business_competitors WHERE business_domain=? ORDER BY id", (owner,)).fetchall()]


def set_active(conn, owner: str, comp: str, active: bool) -> bool:
    owner, comp = _o(owner), _reg(comp)
    n = conn.execute("UPDATE business_competitors SET active=?, updated_at=? WHERE business_domain=? AND competitor_domain=?",
                     (int(active), core.now(), owner, comp)).rowcount
    _sync_target(conn, comp)
    return bool(n)


def remove(conn, owner: str, comp: str) -> bool:
    """Drops the relation only: crawled pages, outgoing_links, jobs and history stay (they are shared graph data)."""
    owner, comp = _o(owner), _reg(comp)
    n = conn.execute("DELETE FROM business_competitors WHERE business_domain=? AND competitor_domain=?", (owner, comp)).rowcount
    _sync_target(conn, comp)
    return bool(n)


def is_competitor(conn, domain: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM business_competitors WHERE competitor_domain=?", (_reg(domain),)).fetchone())


def gap(conn, owner: str, only: str | None = None, limit: int = 200) -> dict:
    """Backlink gap of `owner` against its active competitors (or just `only`), over the DISCOVERED index.
    Referring-domain level: registrable source domains classified competitor_only / shared / own_only (many pages of one domain = one domain).
    Source-page level: individual pages that link a competitor but not the owner. Pure SQL reads of the shared graph; no crawling."""
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

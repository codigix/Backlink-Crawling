"""Competitors: a competitor is a monitored TARGET like any business, so it reuses everything business_jobs does (DDGS search, job_urls,
Crawlee claims, the shared outgoing_links graph). This module only adds the owner relation (business_competitors), target
classification (businesses.target_type) and the backlink gap, which is computed from the graph. Nothing here crawls or searches.
"""

import business_jobs as bj
import crawler_core as core

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
    """A competitor target is monitored while at least one owner has it active; own businesses are never touched. Its search turns are charged to
    one owner (the lowest-sorting active one), so a competitor shared by many businesses is still ONE search stream."""
    conn.execute("UPDATE businesses SET active=CASE WHEN EXISTS (SELECT 1 FROM business_competitors WHERE competitor_domain=? AND active=1) THEN 1 ELSE 0 END,"
                 " parent_domain=COALESCE((SELECT MIN(business_domain) FROM business_competitors WHERE competitor_domain=? AND active=1), parent_domain)"
                 " WHERE domain=? AND target_type='competitor'", (comp, comp, comp))


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
        job, created = bj.create_job(conn, comp, name=name, target_type="competitor")
        note = "queued" if created else "already monitored: runs on its 24h schedule"  # shared with another business; no extra scan
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


def listing(conn, owner: str, limit: int | None = None, offset: int = 0, counts: bool = True) -> list[dict]:
    """Competitors of `owner` in registration order (id is unique); limit/offset are applied in SQL so only the requested page is built.
    limit=None = all. counts=False skips the known-backlink aggregation (the one expensive query) and reports `known_backlinks: null`. The per-competitor facts (job, schedule, known backlinks) are read for the whole page in ONE query each (was ~4 per competitor)."""
    owner = _o(owner)
    rel = conn.execute("SELECT * FROM business_competitors WHERE business_domain=? ORDER BY id LIMIT ? OFFSET ?", (owner, limit, offset)).fetchall()
    doms = [r["competitor_domain"] for r in rel]
    if counts:
        _no_jit(conn)  # only the count is JIT-prone
    jobs = bj.latest_many(conn, doms)
    sched = {r["domain"]: r for r in conn.execute("SELECT domain, target_type, next_run_at, last_run_at, active FROM businesses WHERE domain=ANY(?::text[])", (doms,))}
    known = {} if not counts else {r[0]: r[1] for r in conn.execute(
        "SELECT destination_domain, COUNT(*) FROM outgoing_links WHERE destination_domain=ANY(?::text[]) AND link_type='html' AND is_active=1"
        " AND source_domain<>destination_domain GROUP BY destination_domain", (doms,)).fetchall()}
    return [{"competitor_domain": r["competitor_domain"], "competitor_name": r["competitor_name"], "active": bool(r["active"]), "registered_at": r["created_at"],
             "target_type": sched[r["competitor_domain"]]["target_type"] if r["competitor_domain"] in sched else "competitor",
             "discovery_status": bj.api_state(jobs.get(r["competitor_domain"])), "job": jobs.get(r["competitor_domain"]),
             "schedule": {k: v for k, v in sched[r["competitor_domain"]].items() if k != "domain"} if r["competitor_domain"] in sched else None,
             "known_backlinks": known.get(r["competitor_domain"], 0) if counts else None} for r in rel]


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


def _no_jit(conn) -> None:
    """Transaction-local: PostgreSQL ends a SET LOCAL at COMMIT/ROLLBACK, so the pooled connection goes back with the server's default and no other request
    sees it. These statements are priced by the planner at millions of rows (it cannot see that ~98% of a competitor's links are internal) although
    they return a few thousand, which makes it JIT-compile them: ~1.5 s of compilation for ~0.2 s of work. Outside a transaction block (autocommit)
    PostgreSQL only warns and the setting simply does not apply."""
    conn.execute("SET LOCAL jit = off")


def is_competitor(conn, domain: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM business_competitors WHERE competitor_domain=?", (_reg(domain),)).fetchone())


_ACTIVE = "l.link_type='html' AND l.is_active=1"  # the one definition of an eligible link: html, active; "not from the target's own domain" is added per query
# source_domain<>destination_domain is source_domain<>comps.c (the join makes them equal) written so a partial index on exactly that predicate is provable
_COMP_LINK = f"FROM outgoing_links l JOIN unnest(?::text[]) AS comps(c) ON l.destination_domain=comps.c WHERE {_ACTIVE} AND l.source_domain<>l.destination_domain"
# Sites the competitor built or owns are not opportunities (nobody can place a backlink there for us). They show up as: (1) a source domain linking the
# same competitor from SITEWIDE_MIN+ pages (footer/credit link, client site, PBN); (2) the SAME link (destination + anchor) repeated on TEMPLATE_MIN+ pages
# of one domain = a theme footer "Design by ..."; (3) a link on the source site's homepage (footer credit of a one-page client site).
# shortcut: judged from link counts and URL shape only (no HTML is stored), so a genuine publisher that repeats one anchor on 2 pages or links from its
# homepage is dropped too: raise the limits or add an allowlist if that bites.
SITEWIDE_MIN = 10
TEMPLATE_MIN = 2
_HOME = "^https*://[^/#]+/*$"  # no '?': the driver would read it as a placeholder
_LISTS = (("opportunities", "g", "ic AND NOT io"), ("shared_domains", "s", "ic AND io"), ("own_only_domains", "o", "io AND NOT ic"))  # response key, tag, filter on j


def gap(conn, owner: str, only: str | None = None, limit: int = 200, offset: int = 0, after: dict | None = None) -> dict:
    """Backlink gap of `owner` against its active competitors (or just `only`), over the DISCOVERED index.
    Referring-domain level: registrable source domains classified competitor_only / shared / own_only (many pages of one domain = one domain).
    Source-page level: individual pages that link a competitor but not the owner. Pure SQL reads of the shared graph; no crawling.
    The comparison and the paging run in PostgreSQL: every list is ordered by its unique key (domain / source_url, code-point order) and cut with
    LIMIT, so Python only receives the requested page (+1 row to know whether another page exists), never the link sets.
    `after` = {list name: last key of that list's previous page}: keyset paging per list; `offset` instead makes every list skip that many entries.
    The summary counts always describe the whole comparison."""
    owner, after = _reg(owner), after or {}
    _no_jit(conn)
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
    # ---- ONE scan of the competitors' links (cl) and ONE of the owner's (ol) feed both comparisons: the referring domains (counts + one page of each
    # classification) and the source-page gap (pages that link a competitor but no eligible page of the owner = an anti-join against ol, not one index
    # probe into outgoing_links per competitor page). Everything comes back in one statement / one round trip. ----
    parts, args = [], [comps, owner, owner]
    nulls = "NULL::bigint AS a, NULL::bigint AS b, NULL::bigint AS g, NULL::bigint AS s, NULL::bigint AS o, NULL::bigint AS p"
    for key, tag, flt in _LISTS:
        cur = after.get(key)
        parts.append(f"(SELECT '{tag}' AS k, d, {nulls} FROM j WHERE {flt}" + (' AND d COLLATE "C" > ?' if cur is not None else "") + ' ORDER BY d COLLATE "C" OFFSET ? LIMIT ?)')
        args += ([cur] if cur is not None else []) + [offset, limit + 1]
    pcur = after.get("source_page_gap")
    parts.append(f"(SELECT 'u' AS k, u AS d, {nulls} FROM gp WHERE TRUE" + (' AND u COLLATE "C" > ?' if pcur is not None else "") + ' ORDER BY u COLLATE "C" OFFSET ? LIMIT ?)')
    args += ([pcur] if pcur is not None else []) + [offset, limit + 1]
    rows = conn.execute(
        f"WITH cl0 AS MATERIALIZED (SELECT l.source_domain AS d, l.source_url AS u, l.destination_domain AS c, l.destination_url AS t, COALESCE(l.anchor_text,'') AS a {_COMP_LINK}),"
        f" cl1 AS MATERIALIZED (SELECT d, u, c FROM cl0 WHERE u !~ '{_HOME}' AND (d, c, t, a) NOT IN (SELECT d, c, t, a FROM cl0 GROUP BY d, c, t, a HAVING COUNT(DISTINCT u) >= {TEMPLATE_MIN})),"
        f" cl AS MATERIALIZED (SELECT DISTINCT d, u FROM cl1 WHERE (d, c) NOT IN (SELECT d, c FROM cl0 GROUP BY d, c HAVING COUNT(DISTINCT u) >= {SITEWIDE_MIN})),"
        " ol AS MATERIALIZED (SELECT DISTINCT l.source_domain AS d, l.source_url AS u FROM outgoing_links l WHERE l.destination_domain=? AND " + _ACTIVE + " AND l.source_domain<>? AND l.source_domain<>l.destination_domain),"
        " j AS MATERIALIZED (SELECT COALESCE(cd.d, od.d) AS d, cd.d IS NOT NULL AS ic, od.d IS NOT NULL AS io FROM (SELECT DISTINCT d FROM cl) cd FULL JOIN (SELECT DISTINCT d FROM ol) od ON cd.d=od.d),"
        " gp AS MATERIALIZED (SELECT DISTINCT u FROM cl WHERE NOT EXISTS (SELECT 1 FROM ol WHERE ol.u=cl.u))"
        " SELECT 'n' AS k, NULL::text AS d, COUNT(*) FILTER (WHERE io) AS a, COUNT(*) FILTER (WHERE ic) AS b, COUNT(*) FILTER (WHERE ic AND NOT io) AS g,"
        " COUNT(*) FILTER (WHERE ic AND io) AS s, COUNT(*) FILTER (WHERE io AND NOT ic) AS o, (SELECT COUNT(*) FROM gp) AS p FROM j UNION ALL " + " UNION ALL ".join(parts), args).fetchall()
    cnt = next(r for r in rows if r["k"] == "n")
    domains = {key: [r["d"] for r in rows if r["k"] == tag] for key, tag, _ in _LISTS}
    gap_pages = cnt["p"]
    urls = [r["d"] for r in rows if r["k"] == "u"]
    more = {key: len(domains[key]) > limit for key, _, _ in _LISTS} | {"source_page_gap": len(urls) > limit}
    urls, domains = urls[:limit], {k: v[:limit] for k, v in domains.items()}
    # ---- details of the (at most `limit`) entries that are returned ----
    detail: dict[str, dict] = {}
    if domains["opportunities"]:
        for r in conn.execute(
                "WITH pairs AS (SELECT DISTINCT l.source_domain AS d, l.destination_domain AS c, l.source_url AS u FROM outgoing_links l"
                " WHERE l.source_domain=ANY(?::text[]) AND l.destination_domain=ANY(?::text[]) AND " + _ACTIVE + " AND l.source_domain<>l.destination_domain)"
                ' SELECT d, array_agg(DISTINCT c) AS targets, (array_agg(u ORDER BY u COLLATE "C"))[1:50] AS pages FROM pairs GROUP BY d', (domains["opportunities"], comps)):
            detail[r["d"]] = r
    pages: dict[str, dict] = {}
    own_domains: set = set()
    if urls:
        for r in conn.execute(  # competitor order, then id: the order the links of a page have always been listed in
                "SELECT l.source_url, l.source_domain, comps.c, l.destination_url, l.anchor_text, l.rel_attributes, l.is_nofollow, l.is_sponsored, l.is_ugc, l.last_seen"
                " FROM outgoing_links l JOIN unnest(?::text[]) WITH ORDINALITY AS comps(c, i) ON l.destination_domain=comps.c"
                " WHERE l.source_url=ANY(?::text[]) AND " + _ACTIVE + " AND l.source_domain<>comps.c ORDER BY comps.i, l.id", (comps, urls)):
            p = pages.setdefault(r["source_url"], {"source_url": r["source_url"], "referring_domain": r["source_domain"], "competitor_targets": set(), "links": []})
            p["competitor_targets"].add(r["c"])
            p["links"].append({"competitor": r["c"], "destination_url": r["destination_url"], "anchor_text": r["anchor_text"], "rel": r["rel_attributes"],
                               "nofollow": bool(r["is_nofollow"]), "sponsored": bool(r["is_sponsored"]), "ugc": bool(r["is_ugc"]), "last_seen": r["last_seen"]})
        own_domains = {r["source_domain"] for r in conn.execute(
            "SELECT DISTINCT l.source_domain FROM outgoing_links l WHERE l.destination_domain=? AND " + _ACTIVE + " AND l.source_domain<>? AND l.source_domain=ANY(?::text[])",
            (owner, owner, [p["referring_domain"] for p in pages.values()]))}
    page_gap = [{**pages[u], "competitor_targets": sorted(pages[u]["competitor_targets"]), "domain_links_to_own_business": pages[u]["referring_domain"] in own_domains}
                for u in urls]
    jobs = bj.latest_many(conn, [owner, *comps])  # one query, not one per competitor
    states = {t: bj.api_state(jobs.get(t)) for t in [owner, *comps]}
    warnings = []
    if not states[owner].startswith("completed"):
        warnings.append(f"discovery for {owner} is '{states[owner]}': its backlinks may be incomplete, so gap domains may be overstated")
    warnings += [f"discovery for competitor {c} is '{states[c]}': its backlinks may be incomplete" for c in comps if not states[c].startswith("completed")]
    totals = {"opportunities": cnt["g"], "shared_domains": cnt["s"], "own_only_domains": cnt["o"], "source_page_gap": gap_pages}
    last = {**{k: domains[k][-1:] for k, _, _ in _LISTS}, "source_page_gap": urls[-1:]}
    return {"business_domain": owner, "competitors": comps,
            "summary": {"own_referring_domains": cnt["a"], "competitor_referring_domains": cnt["b"], "gap_domains": cnt["g"],
                        "shared_domains": cnt["s"], "own_only_domains": cnt["o"], "gap_pages": gap_pages},
            "coverage": {"basis": "discovered index only (pages crawled so far), not the whole internet", "discovery_status": states, "warnings": warnings},
            "opportunities": [{"referring_domain": d, "classification": "competitor_only", "competitor_targets": sorted(detail[d]["targets"]), "links_to_own_business": False,
                               "source_pages": detail[d]["pages"]} for d in domains["opportunities"]],
            "shared_domains": domains["shared_domains"], "own_only_domains": domains["own_only_domains"], "source_page_gap": page_gap,
            "pagination": {"limit": limit, "offset": offset, "has_more": any(more.values()), "next_offset": offset + limit if any(more.values()) else None,
                           "lists": {k: {"total": totals[k], "has_more": more[k], "next_after": last[k][0] if more[k] else None} for k in totals}}}

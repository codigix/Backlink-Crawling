"""Deterministic link-graph fixture for the API read paths (set-based SQL, so 1M+ rows load in seconds).

`load(c, scale)` fills the CURRENT schema (the caller picks it via DB_SCHEMA / the `env` fixture). scale=1 is the small regression
dataset; the benchmark uses a large one. Edge cases are deliberate: NULL source_domain, removed / suspected-removed links,
self-links, img links, a subdomain target, a look-alike domain, ties on first_seen / detected_at, failed source pages."""

TARGET, SUB, EVIL = "codigix.co", "blog.codigix.co", "evilcodigix.co"
_TS = "to_char(timestamp '2026-01-01 00:00:00' + (({n}) || ' minutes')::interval, 'YYYY-MM-DD HH24:MI:SS')"


def ts(n: str) -> str:
    return _TS.format(n=n)


def load(c, scale: float = 1.0, urls: int = 4000, links: int = 400, noise: int = 6000, history: int = 600) -> dict:
    urls, links, noise, history = (max(10, int(x * scale)) for x in (urls, links, noise, history))
    doms = max(5, urls // 130)
    # frontier: every URL belongs to one of `doms` domains; all crawl_status values the API distinguishes
    c.execute(f"""INSERT INTO discovered_urls (url, normalized_url, domain, hostname, crawl_status, discovery_method, crawl_depth)
        SELECT 'https://s'||(g%{doms})||'.example/p'||g, 'https://s'||(g%{doms})||'.example/p'||g, 's'||(g%{doms})||'.example', 's'||(g%{doms})||'.example',
               (ARRAY['pending','completed','completed','failed','retry_pending','crawling'])[1+g%6],
               CASE WHEN g%11=0 THEN NULL ELSE (ARRAY['sitemap','link','seed'])[1+g%3] END, g%4
        FROM generate_series(1,{urls}) g""")
    c.execute(f"""INSERT INTO discovered_domains (domain, discovery_status, priority)
        SELECT 's'||g||'.example', (ARRAY['discovered','admitted','crawled','unreachable','skipped'])[1+g%5], g%6 FROM generate_series(0,{doms - 1}) g""")
    # backlinks into TARGET: g%8=0 -> subdomain page; g%20=0 img; g%10=0 removed; g%17=0 suspected/lost; g%97=0 NULL source_domain; g%53=0 self-link
    c.execute(f"""INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, link_type, first_seen, last_seen,
                                               is_active, lost_at, source_domain, rel_attributes)
        SELECT CASE WHEN g%53=0 THEN 'https://{TARGET}/self'||g ELSE 'https://s'||(g%{doms})||'.example/p'||g END,
               CASE WHEN g%8=0 THEN 'https://{SUB}/x'||g ELSE 'https://{TARGET}/page'||(g%50) END,
               '{TARGET}', (ARRAY['Codigix','click here','','home','Codigix'])[1+g%5],
               CASE WHEN g%20=0 THEN 'img' ELSE 'html' END, {ts('g/3')}, {ts('g/3 + 5')},
               CASE WHEN g%10=0 THEN 0 ELSE 1 END, CASE WHEN g%17=0 THEN {ts('g')} END,
               CASE WHEN g%97=0 THEN NULL WHEN g%53=0 THEN '{TARGET}' ELSE 's'||(g%{max(3, doms // 2)})||'.example' END,
               CASE WHEN g%7=0 THEN 'nofollow' END
        FROM generate_series(1,{links}) g""")
    c.execute(f"""INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, source_domain, first_seen)
        SELECT 'https://s'||(g%{doms})||'.example/q'||g, 'https://{EVIL}/e'||g, '{EVIL}', 'evil', 's'||(g%{doms})||'.example', {ts('g')}
        FROM generate_series(1,{max(5, links // 40)}) g""")
    c.execute(f"""INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, source_domain, first_seen, is_active)
        SELECT 'https://s'||(g%{doms})||'.example/n'||g, 'https://d'||(g%{max(10, noise // 5)})||'.example/'||g, 'd'||(g%{max(10, noise // 5)})||'.example',
               'a'||(g%9), 's'||(g%{doms})||'.example', {ts('g')}, CASE WHEN g%4=0 THEN 0 ELSE 1 END
        FROM generate_series(1,{noise}) g""")
    # history: events for TARGET pages from foreign sources (kept by the Python host filter), some from the target's own host (dropped), ties on detected_at
    c.execute(f"""INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)
        SELECT CASE WHEN g%9=0 THEN 'https://{SUB}/own'||g ELSE 'https://s'||(g%{doms})||'.example/p'||g END,
               CASE WHEN g%6=0 THEN 'https://{SUB}/x'||g ELSE 'https://{TARGET}/page'||(g%50) END, '{TARGET}', (ARRAY['Codigix','click here','','home'])[1+g%4],
               (ARRAY['suspected_removed','lost','reappeared','lost'])[1+g%4], {ts('g/4')}
        FROM generate_series(1,{history}) g""")
    c.execute("INSERT INTO target_domains (domain) VALUES (?) ON CONFLICT DO NOTHING".replace("?", f"'{TARGET}'"))
    c.execute(f"""INSERT INTO backlinks (target_domain_id, source_url, destination_url, anchor_text)
        SELECT (SELECT id FROM target_domains WHERE domain='{TARGET}'), 'https://s'||g||'.example/b', 'https://{TARGET}/', 'x' FROM generate_series(1,{max(5, history // 20)}) g""")
    c.execute(f"""INSERT INTO backlink_history (backlink_id, event_type, detected_at, details)
        SELECT b.id, (ARRAY['verified','lost','changed'])[1+(b.id%3)::int], {ts('b.id/2 + g')}, 'd'||g FROM backlinks b, generate_series(1,3) g""")
    return {"urls": urls, "links": links, "noise": noise, "history": history, "domains": doms}

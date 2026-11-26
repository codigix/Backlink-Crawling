"""Phase 3: deterministic classification of ONE extracted link into editorial / sitewide / profile / directory / client_credit / own_property / other / unknown.

Pure functions over evidence the crawler already collected (Phase 1 placement, Phase 2 page content and link context); no HTML parsing, no network, no database,
no model call. Classification is per link, never per page: one page can hold a footer credit, a menu link and a contextual article link, each classified on its own.

Decision order (first match wins, but each step needs its own evidence, no single weak signal decides):
  1 the link must point at the monitored target          -> otherwise `other`
  2 internal link / explicitly configured ownership      -> own_property
  3 development / design credit                          -> client_credit
  4 global placement (footer, header, navigation, aside) -> sitewide      (a logo in a confirmed client header/footer, or explicit credit wording, supports a credit)
  5 template repetition across the source domain's pages -> sitewide
  6 profile / directory page evidence                    -> profile / directory
  7 contextual editorial evidence                        -> editorial
  8 understood but not editorial (button, logo, bare list entry, tag) -> other ; insufficient or conflicting evidence -> unknown

Relationships (which domains we own, which client sites we built) are NOT inferred from names. They come from `Target` objects (see load_relationships); without them
a domain is never owned and never a confirmed client.
"""

import functools
import json
import re
import threading
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlsplit

import crawler_core as core

LINK_CLASSES = ("editorial", "sitewide", "profile", "directory", "client_credit", "own_property", "other", "unknown")
SITEWIDE_POSITIONS = ("header", "nav", "footer", "aside")


@dataclass(frozen=True)
class Thresholds:
    """Every number the rules use. Confidence is built from named evidence (see _editorial), not hard-coded per class."""
    editorial_candidate_min: float = 0.70  # an editorial result below this confidence is not an editorial candidate
    min_context_words: int = 8             # words around the link (outside the anchor) needed to call the context meaningful
    strong_context_words: int = 15
    template_min_observed: int = 3         # pages of the source domain seen before repetition can say anything
    template_min_pages: int = 3            # pages carrying the same destination
    template_min_share: float = 0.6        # ... and at least this share of the observed pages
    profile_min_score: float = 3.5
    directory_min_score: float = 3.0
    conflict_margin: float = 1.0           # profile vs directory scores closer than this: unknown
    max_links_per_sentence: int = 3        # more links than this sharing one sentence / list line is a link list, not a contextual mention
    max_confidence: float = 0.98


DEFAULT = Thresholds()


@dataclass(frozen=True)
class Target:
    """A monitored business and what we CONFIRMED about its relationships. Nothing here is guessed from names."""
    domain: str
    name: str | None = None
    brands: tuple = ()
    owned_domains: frozenset = frozenset()   # registrable domains / hosts we control
    owned_accounts: tuple = ()               # accounts on shared platforms we control: "tumblr.com/ourblog" or "ourblog.tumblr.com"
    client_domains: frozenset = frozenset()  # sites we built (a credit on them is a development credit)

    @staticmethod
    def from_dict(domain: str, d: dict | None = None) -> "Target":
        d = d or {}
        return Target(domain=core.clean_target(domain), name=d.get("name"), brands=tuple(d.get("brands", ())),
                      owned_domains=frozenset(core.clean_target(x) for x in d.get("owned_domains", ())),
                      owned_accounts=tuple(x.lower().strip().strip("/") for x in d.get("owned_accounts", ())),
                      client_domains=frozenset(core.clean_target(x) for x in d.get("client_domains", ())))


def load_relationships(path: str) -> dict[str, Target]:
    """JSON file {"targets": {"codigix.co": {"name": ..., "brands": [...], "owned_domains": [...], "owned_accounts": [...], "client_domains": [...]}}} -> {domain: Target}.
    There is no relationship table in the database yet (Phase 4), so this file is the only source of confirmed ownership."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return {core.clean_target(d): Target.from_dict(d, v) for d, v in raw.get("targets", {}).items()}


@dataclass(frozen=True)
class Repetition:
    """How often this destination shows up across the source domain's OTHER pages (from a RepetitionTracker). pages_seen = pages of the domain observed so far."""
    pages_seen: int = 0
    pages_with_link: int = 0
    pages_same_context: int = 0   # pages where the link also has the same surrounding sentence (a template repeats its words; an article does not)
    positions: tuple = ()         # positions the link was seen in, e.g. ("footer",)


NO_REPETITION = Repetition()


def dest_key(url: str) -> str:
    """One key for http/https, www, trailing slash, fragment, tracking parameters and index files: the 'same destination' across URL variations."""
    p = urlsplit(url.strip())
    host = core.idna(p.hostname or "").removeprefix("www.")
    path = re.sub(r"/(index|default)\.(html?|php|aspx?)$", "/", p.path or "/").rstrip("/") or "/"
    q = urlencode(sorted((k, v) for k, v in parse_qsl(p.query) if not (k.lower().startswith("utm_") or k.lower() in ("fbclid", "gclid", "ref", "mc_cid", "mc_eid"))))
    return host + path + ("?" + q if q else "")



@functools.lru_cache(maxsize=65536)
def _host(url: str) -> str:
    return core.hostname(url)


@functools.lru_cache(maxsize=65536)
def _reg(host: str) -> str:
    return core.registrable_domain(host)


@functools.lru_cache(maxsize=65536)
def _matches(host: str, domain: str) -> bool:
    return core.host_matches(host, domain)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def _ctx_key(link_context: dict) -> str:
    s = _norm(link_context.get("sentence") or "") or _norm((link_context.get("text_before") or "")[-60:])
    return s[:120]


class RepetitionTracker:
    """Explicit per-crawl store of what each source domain's pages link to (only destinations in `watch`, so it stays small). Owned by one crawl / one PageClassifier,
    never global. observe() is idempotent per page (sets), so a deadlock retry of process_page cannot double count. Evidence is order dependent: pages seen
    later in a crawl see more of the domain; reclassify when the crawl is done to use all of it."""

    def __init__(self, watch_domains=()):
        self._lock = threading.Lock()
        self._watch = tuple(watch_domains)
        self._d: dict = {}

    def observe(self, source_domain: str, page_url: str, links) -> None:
        with self._lock:
            st = self._d.setdefault(_repetition_group(source_domain, page_url), {"pages": {}, "dest": {}})
            observed = {(dest_key(dest), _ctx_key(extra.get("link_context") or {}), extra.get("position"))
                        for dest, _, _, kind, extra in links
                        if kind == "html" and any(_matches(_host(dest), w) for w in self._watch)}
            previous = st["pages"].get(page_url)
            if previous == observed:
                return
            for key, ctx, pos in previous or ():
                entry = st["dest"][key]
                entry["pages"].discard(page_url)
                for group, value in (("ctx", ctx), ("pos", pos)):
                    pages = entry[group].get(value)
                    if pages:
                        pages.discard(page_url)
                        if not pages:
                            del entry[group][value]
            for key in {item[0] for item in previous or ()}:
                if not st["dest"][key]["pages"]:
                    del st["dest"][key]
            st["pages"][page_url] = observed
            for key, ctx, pos in observed:
                entry = st["dest"].setdefault(key, {"pages": set(), "ctx": {}, "pos": {}})
                entry["pages"].add(page_url)
                entry["ctx"].setdefault(ctx, set()).add(page_url)
                entry["pos"].setdefault(pos, set()).add(page_url)

    def add_watch(self, domains) -> None:
        with self._lock:
            self._watch = tuple(sorted({*self._watch, *domains}))

    def context_for(self, source_domain: str, dest: str, link_context: dict | None) -> Repetition:
        with self._lock:
            st = self._d.get(source_domain)
            if not st:
                return NO_REPETITION
            e = st["dest"].get(dest_key(dest))
            if not e:
                return Repetition(pages_seen=len(st["pages"]))
            return Repetition(len(st["pages"]), len(e["pages"]), len(e["ctx"].get(_ctx_key(link_context or {}), ())), tuple(sorted(str(p) for p in e["pos"])))


# ---------------------------------------------------------------- vocabularies
SHARED_PLATFORMS = frozenset({"tumblr.com", "wordpress.com", "blogspot.com", "blogger.com", "medium.com", "weebly.com", "wixsite.com", "wix.com", "github.io", "substack.com",
                              "livejournal.com", "typepad.com", "squarespace.com", "webflow.io", "netlify.app", "vercel.app", "pages.dev", "hubspotpagebuilder.com", "sites.google.com"})
PROFILE_PLATFORMS = frozenset({"crunchbase.com", "linkedin.com", "bebee.com", "theorg.com", "peerlist.io", "clutch.co", "goodfirms.co", "g2.com", "trustpilot.com", "glassdoor.com",
                               "wellfound.com", "angel.co", "zoominfo.com", "owler.com", "patsnap.com", "f6s.com", "behance.net", "dribbble.com", "about.me"})
DIRECTORY_PLATFORMS = frozenset({"infobel.com", "yellowpages.com", "justdial.com", "indiamart.com", "sulekha.com", "yelp.com", "foursquare.com", "manta.com", "hotfrog.com",
                                 "cylex.com", "brownbook.net", "tupalo.com", "curlie.org", "dmoz-odp.org", "local.yahoo.com", "bizhwy.com"})
_CREDIT = re.compile(
    r"\b(?:designed|developed|built|created|crafted|made|powered|hosted|maintained|engineered|coded|programmed)(?:\s+and\s+(?:designed|developed|hosted|maintained|built))?\s+by\b"
    r"|\b(?:web\s?site|web|site)\s+by\b|\b(?:web\s?site|web|site)\s+credits?\b"
    r"|\b(?:web\s?site|web|site)\s+(?:design|designing|development|developer|designer)s?\s*(?:by\b|-|–|—|\|)"  # no ":" -- "Web Development: Django, Flask" is a label, not a credit
    r"|\b(?:design|development|developer|designer)s?\s*by\b"
    r"|\bcredits?\s*:", re.I)
_COPYRIGHT = re.compile(r"©|&copy;|\(c\)|\bcopyright\b|\ball rights reserved\b", re.I)
_GENERIC_ANCHORS = frozenset({"here", "click here", "read more", "more", "link", "this link", "website", "visit website", "visit", "source", "learn more", "this", "site",
                              "homepage", "home", "official website", "official site", "view", "view more", "go", "link here"})
_URL_ANCHOR = re.compile(r"^(?:https?://)?(?:www\.)?[\w.-]+\.[a-z]{2,}(?:/\S*)?$", re.I)
_LABEL_ANCHORS = frozenset({"website", "visit website", "visit site", "official website", "official site", "homepage", "web site", "site web", "visit our website", "www"})
_CONTEXT_BLOCKS = ("paragraph", "blockquote", "definition", "preformatted")


def _words(s: str) -> int:
    return len((s or "").split())


def _brands(target: Target) -> tuple:
    reg = _reg(target.domain)
    return tuple(sorted({_norm(x) for x in (*target.brands, target.name or "", reg.split(".")[0], reg) if x and len(_norm(x)) >= 3}))


def _mentions(text: str, brands: tuple) -> bool:
    t = " " + _norm(text) + " "
    return any(" " + b + " " in t for b in brands)


def _host_in(host: str, domains) -> bool:
    return any(_matches(host, d) for d in domains)


@functools.lru_cache(maxsize=4096)
def describe_domain(src_host: str, target: Target) -> dict:
    """Facts about the source domain that never change between its links (cached per host and target). `client` / `owned` come ONLY from the configured relationships;
    `brand_similar` is a hint, never an ownership claim."""
    reg = _reg(src_host)
    owned = [d for d in target.owned_domains if d not in SHARED_PLATFORMS]  # a bare shared platform (tumblr.com) is never "owned"; only accounts on it are
    brands = _brands(target)
    return {"host": src_host, "registrable": reg, "shared_platform": reg in SHARED_PLATFORMS, "profile_platform": reg in PROFILE_PLATFORMS,
            "directory_platform": reg in DIRECTORY_PLATFORMS, "owned_domain": _host_in(src_host, owned), "client": _host_in(src_host, target.client_domains),
            "brand_similar": any(b.replace(" ", "") in src_host.replace("-", "").replace(".", "") for b in brands if len(b.replace(" ", "")) >= 5)}


def _account_slug(url: str, host: str) -> str:
    """The account part of a URL on a shared blogging platform: the subdomain (name.tumblr.com) or the first path segment (tumblr.com/name)."""
    reg = _reg(host)
    sub = host.removesuffix("." + reg).removeprefix("www.") if host != reg else ""
    seg = (urlsplit(url).path or "/").strip("/").split("/")[0]
    return (sub if sub and sub != "www" else seg).lower()


def _repetition_group(domain: str, url: str) -> str:
    """Shared publishers have separate templates even when they share a registrable domain."""
    host = _host(url).removeprefix("www.")
    if domain in SHARED_PLATFORMS:
        return host if host != domain else domain + "/" + _account_slug(url, host)
    if host == "sites.google.com":
        return host + "/" + "/".join(urlsplit(url).path.strip("/").split("/")[:2])
    return domain


def _owned_account(url: str, target: Target) -> str | None:
    p = urlsplit(url)
    host, path = core.idna(p.hostname or "").removeprefix("www."), (p.path or "/").lower().rstrip("/")
    for acct in target.owned_accounts:
        h, _, rest = acct.partition("/")
        h = h.removeprefix("www.")
        if rest and host == h and (path == "/" + rest or path.startswith("/" + rest + "/")):
            return acct
        if not rest and host == h and h not in SHARED_PLATFORMS:
            return acct
    return None


# ---------------------------------------------------------------- link / page views
@dataclass(frozen=True)
class _Link:
    dest: str
    anchor: str
    rel: str
    ltype: str
    position: str | None
    local_region: str | None
    img_alt: str | None
    nofollow: bool
    sponsored: bool
    ugc: bool
    ctx: dict
    parent_text: str


def _link(link) -> _Link:
    if isinstance(link, dict):
        dest, anchor, rel, ltype, extra = link["dest"], link.get("anchor", ""), link.get("rel"), link.get("link_type", "html"), link.get("extra", link)
    else:
        dest, anchor, rel, ltype, extra = link
    return _Link(dest, anchor or "", rel or "", ltype, extra.get("position"), extra.get("local_region"), extra.get("img_alt"), bool(extra.get("nofollow")),
                 bool(extra.get("sponsored")), bool(extra.get("ugc")), extra.get("link_context") or {}, extra.get("context") or "")


def page_stats(links, source_domain: str) -> dict:
    """Facts about one page's whole link set, computed ONCE per page (not per link): used to recognise listing-style pages and repeated links inside the page."""
    ext, doms, bare, by_dest, per_sentence = 0, set(), 0, {}, {}
    for dest, _a, _r, ltype, extra in links:
        if ltype != "html":
            continue
        d = _reg(_host(dest))
        by_dest.setdefault(dest_key(dest), []).append(extra.get("position"))
        s_key = _norm((extra.get("link_context") or {}).get("sentence") or "")
        if s_key:
            per_sentence[s_key] = per_sentence.get(s_key, 0) + 1
        if d == source_domain:
            continue
        ext += 1
        doms.add(d)
        lc = extra.get("link_context") or {}
        if extra.get("position") in ("content", "unknown", None) and lc.get("context_quality") != "sufficient":
            bare += 1
    return {"external_links": ext, "external_domains": len(doms), "bare_share": (bare / ext) if ext else 0.0, "listing_like": len(doms) >= 8 and ext and bare / ext >= 0.6,
            "positions_by_dest": {k: tuple(v) for k, v in by_dest.items()}, "per_sentence": per_sentence}


def _anchor_kind(l: _Link, brands: tuple) -> str:
    a = l.anchor.strip()
    if not a:
        return "image" if (l.img_alt or l.ctx.get("link_kind") == "image") else "empty"
    if _URL_ANCHOR.match(a):
        return "url"
    n = _norm(a)
    if n in _GENERIC_ANCHORS:
        return "generic"
    return "brand" if _mentions(a, brands) else "descriptive"


def _credit_phrase(l: _Link) -> str | None:
    """Design / development credit wording: in the anchor itself, in the sentence, or in the text just before the link ("Website Designed by <link>")."""
    match = _CREDIT.search(l.anchor)
    if match:
        return f"{match.group()!r} in anchor"
    before = (l.ctx.get("text_before") or "").rstrip()
    if not before and l.anchor and l.parent_text.endswith(l.anchor):
        prefix = l.parent_text[:-len(l.anchor)].strip()
        if _CREDIT.fullmatch(prefix):
            return f"{prefix!r} in the direct parent before the anchor"
    for match in _CREDIT.finditer(before):
        if not before[match.end():].strip(" :|-\u2013\u2014"):
            return f"{match.group()!r} immediately before the link"
    return None


def _result(cls: str, conf: float, reason: str, evidence: list, l: _Link, th: Thresholds, qualifiers=()) -> dict:
    conf = round(max(0.0, min(conf, th.max_confidence)), 2)
    q = list(qualifiers) + [k for k, v in (("nofollow", l.nofollow), ("sponsored", l.sponsored), ("ugc", l.ugc)) if v and k not in qualifiers]
    return {"link_class": cls, "confidence": conf, "reason": reason, "evidence": evidence, "is_editorial_candidate": cls == "editorial" and conf >= th.editorial_candidate_min,
            "attributes": {"nofollow": l.nofollow, "sponsored": l.sponsored, "ugc": l.ugc, "rel": l.rel or None}, "qualifiers": q, "destination": l.dest, "anchor": l.anchor,
            "position": l.position}


def _page_evidence(page_content: dict | None) -> dict:
    pc = page_content or {}
    sig = pc.get("page_type_signals") or {}
    te = sig.get("type_evidence") or {}
    q = (pc.get("extraction_quality") or {}).get("status")
    mc = pc.get("main_content") or {}
    return {"quality": q, "te": te, "schema": set(sig.get("schema_types") or ()), "meta": list(sig.get("metadata_types") or ()), "url": list(sig.get("url_patterns") or ()),
            "markers": list(sig.get("content_markers") or ()), "author": bool(sig.get("has_author")), "date": bool(sig.get("has_published_date")),
            "article_structure": bool(sig.get("has_article_structure")), "words": mc.get("word_count", 0), "title": pc.get("title") or "", "h1": " ".join((pc.get("headings") or {}).get("h1", [])[:2])}


def _profile_score(pe: dict, l: _Link, dctx: dict, brands: tuple) -> tuple[float, list, bool]:
    score, ev, structural = 0.0, [], False
    schema = {s.lower() for s in pe["schema"]}
    if "profilepage" in schema:
        score, structural = score + 2, True
        ev.append("schema: ProfilePage")
    elif "person" in schema:
        score, structural = score + 1, True
        ev.append("schema: Person")
    if any(m.startswith("og:type=profile") for m in pe["meta"]):
        score, structural = score + 2, True
        ev.append("meta: og:type=profile")
    if "url:profile" in pe["te"].get("profile", []):
        score, structural = score + 1, True
        ev.append("url: profile pattern")
    if _mentions(pe["title"] + " " + pe["h1"], brands):
        score, structural = score + 1.5, True
        ev.append("page title/h1 is about the monitored business")
    if dctx["profile_platform"]:
        score += 1
        ev.append(f"source is a known profile platform ({dctx['registrable']})")
    if _anchor_kind(l, brands) == "url" or _norm(l.anchor) in _LABEL_ANCHORS:
        score += 1
        ev.append("anchor is the bare website address or a 'Website' label")
    if l.ctx.get("context_quality") != "sufficient":
        score += 0.5
        ev.append("no sentence around the link (label-like)")
    return score, ev, structural


def _directory_score(pe: dict, l: _Link, dctx: dict, stats: dict | None, brands: tuple) -> tuple[float, list, bool]:
    score, ev, structural = 0.0, [], False
    d_ev = pe["te"].get("directory", [])
    schema_hits = [e for e in d_ev if e.startswith("schema:")]
    if schema_hits:
        score, structural = score + min(2.0, 1.0 * len(schema_hits)), True
        ev.append("schema: " + ", ".join(e[7:] for e in schema_hits))
    if any(e.startswith("marker:listing") for e in d_ev):
        score, structural = score + 1.5, True
        ev.append("repeated listing blocks / addresses on the page")
    if any(e.startswith("url:directory") for e in d_ev):
        score, structural = score + 1.5, True
        ev.append("url: directory/listing pattern")
    if dctx["directory_platform"]:
        score += 1
        ev.append(f"source is a known directory ({dctx['registrable']})")
    if stats and stats.get("listing_like"):
        score, structural = score + 1.5, True
        ev.append(f"page is a list of {stats['external_domains']} external sites with no sentences around them")
    if _norm(l.anchor) in _LABEL_ANCHORS or l.ctx.get("context_quality") != "sufficient":
        score += 0.5
        ev.append("label-like link without a sentence")
    return score, ev, structural


def _editorial(l: _Link, pe: dict, brands: tuple, th: Thresholds, rep: Repetition) -> dict | None:
    """Evidence for / against an editorial link. Returns None when the context is not meaningful. Confidence is the sum of named evidence, capped."""
    c = l.ctx
    sentence_words = max(0, _words(c.get("sentence")) - _words(l.anchor))
    ctx_words = sentence_words
    kind = c.get("block_kind")
    if c.get("link_kind") in ("button", "image") or c.get("context_quality") != "sufficient" or ctx_words < th.min_context_words:
        return None
    ev, score = [f"placement: {l.position}", f"block_kind: {kind}", "context_quality: sufficient", f"surrounding_words: {ctx_words}"], 0.55
    if kind in _CONTEXT_BLOCKS:
        score += 0.12
    elif kind == "list_item":
        score -= 0.02
        ev.append("list entry with its own sentence (resource-style)")
    elif kind in ("table_row", "caption", "heading"):
        score -= 0.12
    score += 0.08 if ctx_words >= th.strong_context_words else 0.04
    if c.get("section_heading"):
        score += 0.02
        ev.append(f"section_heading: {c['section_heading'][:60]}")
    if pe["quality"] == "complete":
        score += 0.08
        ev.append("page extraction: complete")
    elif pe["quality"] == "fallback":
        score -= 0.10
        ev.append("page extraction: fallback (content region not recognised)")
    structure = [s for s in sorted(pe["schema"]) if s.lower() in ("article", "blogposting", "newsarticle", "techarticle", "scholarlyarticle", "discussionforumposting", "qapage")]
    if structure or pe["article_structure"] or any(m == "og:type=article" for m in pe["meta"]):
        score += 0.04
        ev.append("page_type_signal: " + (", ".join(structure) or "article structure") + " (supporting only)")
    if pe["author"] or pe["date"]:
        score += 0.03
        ev.append("page has author / publication date markup")
    if _mentions(c.get("sentence", "") + " " + l.anchor, brands):
        score += 0.05
        ev.append("the target is named in the sentence")
    ak = _anchor_kind(l, brands)
    if ak == "generic":
        score -= 0.05
        ev.append("generic anchor text")
    if l.sponsored:
        score -= 0.10
        ev.append("rel: sponsored (qualified link, attribute preserved)")
    groups = {"article" if k in ("blog", "news", "article") else k for k, v in pe["te"].items() if v and not (k == "homepage" and all(x.startswith("url:") for x in v))}
    if len(groups) >= 2:  # several page types supported at once (Organization + BlogPosting + ItemList ...): the link is still judged on its own, with less certainty
        score -= 0.06 * (len(groups) - 1)
        ev.append("conflicting page-type evidence: " + ", ".join(sorted(groups)) + " (confidence reduced, no type forced)")
    ev.append("global_template_link: false" + (f" ({rep.pages_with_link}/{rep.pages_seen} pages carry it, with different context)" if rep.pages_with_link > 1 else ""))
    return {"score": score, "evidence": ev}


def classify_link(target: Target, source_page, link, page_content: dict | None, domain_context: dict | None = None, repetition_context: Repetition | None = None, *,
                  page_stats: dict | None = None, thresholds: Thresholds | None = None) -> dict:
    """Classify one link. `source_page` is a URL or a dict with `url`; `link` is an extract_links tuple (dest, anchor, rel, type, extra) or an equivalent dict;
    `page_content` is the Phase 2 evidence dict (None is allowed: then nothing about the page can be assumed). Returns the result dict documented in the module header."""
    th = thresholds or DEFAULT
    l = _link(link)
    src_url = source_page if isinstance(source_page, str) else (source_page.get("url") or (page_content or {}).get("url") or "")
    return _classify(target, src_url, l, _page_evidence(page_content), domain_context, repetition_context, page_stats, th)


def _classify(target, src_url, l, pe, domain_context, repetition_context, page_stats, th):
    if l.ltype != "html":
        return _result("other", 0.99, f"{l.ltype} link, not an anchor link", [f"link_type: {l.ltype}"], l, th)
    dhost = _host(l.dest)
    if not _matches(dhost, target.domain):
        return _result("other", 0.99, "destination is not the monitored target", [f"destination_host: {dhost}"], l, th)
    src_host = _host(src_url)
    dctx = domain_context or describe_domain(src_host, target)
    rep = repetition_context or NO_REPETITION
    brands = _brands(target)
    ev = [f"placement: {l.position}"]
    # 2. internal / owned
    if _matches(src_host, target.domain) or (_reg(src_host) == _reg(target.domain) and not _host_in(src_host, SHARED_PLATFORMS)):
        return _result("own_property", 0.99, "internal link: the source page is on the monitored domain", ev + ["source domain == target domain"], l, th)
    if dctx.get("owned_domain"):
        return _result("own_property", 0.97, "source domain is configured as owned by the target", ev + ["ownership: confirmed (configured owned domain)"], l, th)
    acct = _owned_account(src_url, target)
    if acct:
        return _result("own_property", 0.95, f"self-published account on a shared platform ({acct})", ev + [f"ownership: confirmed (configured account {acct})"] +
                       (["platform: shared, only this account is owned"] if dctx.get("shared_platform") else []), l, th)
    # 3. development / design credit
    phrase = _credit_phrase(l)
    client = bool(dctx.get("client"))
    glob = l.position in SITEWIDE_POSITIONS
    if phrase and glob:
        conf = 0.88 + (0.08 if client else 0) + (0.02 if rep.pages_with_link >= th.template_min_pages else 0)
        return _result("client_credit", conf, f"development/design credit in the global {l.position}", ev + [f"credit wording: {phrase}", "client_relationship: " + ("confirmed" if client else "unconfirmed (wording + global placement)")], l, th)
    if client and l.position in ("footer", "header") and (l.img_alt or l.ctx.get("link_kind") == "image"):
        return _result("client_credit", 0.9 if l.position == "footer" else 0.8, f"link in the global {l.position} of a confirmed client site (site we built)", ev + ["client_relationship: confirmed", f"anchor_kind: {_anchor_kind(l, brands)}"], l, th)
    if phrase and l.position in ("unknown", None) and _COPYRIGHT.search(((l.ctx.get("text_before") or "") + " " + (l.ctx.get("sentence") or ""))):
        return _result("client_credit", 0.8, "credit wording next to a copyright notice (placement unknown)", ev + [f"credit wording: {phrase}", "copyright notice in the same line"], l, th)
    # 4. global placement
    if glob:
        base = {"footer": 0.92, "header": 0.90, "nav": 0.92, "aside": 0.82}[l.position]
        extra = []
        if rep.pages_with_link >= th.template_min_pages and rep.pages_seen:
            base += 0.04
            extra.append(f"repeated on {rep.pages_with_link}/{rep.pages_seen} pages of the source domain")
        return _result("sitewide", base, f"link in the global {l.position}" + (" (sidebar widget)" if l.position == "aside" else ""), ev + [f"global_template_link: true ({l.position})"] + extra, l, th)
    # 5. template repetition (content / unknown placement)
    if (rep.pages_seen >= th.template_min_observed and rep.pages_with_link >= th.template_min_pages and rep.pages_with_link / rep.pages_seen >= th.template_min_share
            and rep.pages_same_context >= th.template_min_pages and rep.pages_same_context / rep.pages_seen >= th.template_min_share):
        return _result("sitewide", 0.70 + 0.12 * (rep.pages_with_link / rep.pages_seen), "same link with the same surrounding text on most pages of the source domain (template)",
                       ev + [f"repeated on {rep.pages_with_link}/{rep.pages_seen} pages", f"same context on {rep.pages_same_context} pages", "global_template_link: true (repetition)"], l, th)
    # one of many links in the same sentence / list line ("AI: PyTorch, TensorFlow, ..."): a link list, not a contextual mention
    sk = _norm(l.ctx.get("sentence") or "")
    if page_stats and sk and page_stats.get("per_sentence", {}).get(sk, 0) > th.max_links_per_sentence and l.position == "content":
        return _result("other", 0.70, f"one of {page_stats['per_sentence'][sk]} links in the same sentence/list line (link list), not a contextual mention",
                       ev + [f"links_in_same_sentence: {page_stats['per_sentence'][sk]}", f"block_kind: {l.ctx.get('block_kind')}"], l, th)
    # credit-style wording inside content is ambiguous
    if phrase and l.position == "content":
        return _result("unknown", 0.40, "credit-style wording inside content: could be a development credit or a genuine mention", ev + [f"credit wording: {phrase}", "client_relationship: " + ("confirmed" if client else "unconfirmed")], l, th)
    # a source (or, on a shared platform, an account) that merely looks like the brand is not owned: never counted as independent, never assumed owned
    slug = _account_slug(src_url, src_host) if dctx.get("shared_platform") else ""
    if dctx.get("brand_similar") or (slug and any(b.replace(" ", "") in slug.replace("-", "").replace("_", "") for b in brands if len(b.replace(" ", "")) >= 5)):
        return _result("unknown", 0.40, "source " + ("account" if slug else "domain") + " resembles the brand but ownership is not confirmed", ev + ["brand_similar_source: true", "ownership: unconfirmed",
                       "to confirm: add it to Target.owned_domains / owned_accounts"], l, th)
    # 6. profile / directory
    ps, pev, pstruct = _profile_score(pe, l, dctx, brands)
    ds, dev, dstruct = _directory_score(pe, l, dctx, page_stats, brands)
    is_p, is_d = ps >= th.profile_min_score and pstruct, ds >= th.directory_min_score and dstruct
    if is_p and is_d and (abs(ps - ds) < th.conflict_margin or ("profilepage" in {x.lower() for x in pe["schema"]} and any(x.startswith("marker:listing") for x in pe["te"].get("directory", [])))):
        return _result("unknown", 0.35, "conflicting profile and directory evidence", ev + ["profile: " + "; ".join(pev), "directory: " + "; ".join(dev)], l, th)
    if is_p and (not is_d or ps > ds):
        return _result("profile", min(0.95, 0.5 + 0.08 * ps), "company/person profile page that lists the business", ev + pev, l, th)
    if is_d:
        return _result("directory", min(0.95, 0.5 + 0.08 * ds), "business directory / listing page", ev + dev, l, th)
    # 7. editorial / other / unknown
    if pe["quality"] in ("insufficient", "thin", None) or l.position not in ("content",):
        why = ("source page content could not be extracted reliably" if pe["quality"] in ("insufficient", None) else "source page is too thin to judge"
               if pe["quality"] == "thin" else "link placement could not be identified")
        return _result("unknown", 0.25 if pe["quality"] in ("insufficient", None) else 0.35, why, ev + [f"page_extraction: {pe['quality']}", f"context_quality: {l.ctx.get('context_quality')}"], l, th)
    e = _editorial(l, pe, brands, th, rep)
    if e is None:
        ak = _anchor_kind(l, brands)
        kind = l.ctx.get("block_kind")
        if kind not in ("list_item", "heading", "table_row") and ak not in ("image", "empty", "url") and l.ctx.get("link_kind") not in ("button", "image"):
            return _result("unknown", 0.35, "insufficient context around this link", ev + [f"block_kind: {kind}"], l, th)
        return _result("other", 0.65, "content link without a sentence around it (" + (kind or "no block") + f", {ak} anchor): not an editorial mention",
                       ev + [f"block_kind: {kind}", f"context_quality: {l.ctx.get('context_quality')}", f"link_kind: {l.ctx.get('link_kind')}", f"anchor_kind: {ak}"], l, th)
    score, ev2 = e["score"], e["evidence"]
    if (is_p or is_d) or (ps >= 2.5 and pstruct) or (ds >= 2.0 and dstruct):
        score -= 0.10
        ev2.append("weaker profile/directory evidence on the page (confidence reduced)")
    if client:
        ev2.append("client_relationship: confirmed")
        if re.search(r"\b(worked with|hired|contracted|commissioned)\b", l.ctx.get("sentence", ""), re.I):
            return _result("unknown", 0.45, "client describes its engagement with the target", ev2, l, th)
    if pe["te"].get("directory") and any(x.startswith(("schema:", "url:")) for x in pe["te"]["directory"]) and pe["te"].get("blog") and not pe["article_structure"]:
        return _result("unknown", 0.40, "conflicting page-type evidence (directory and blog signals)", ev2, l, th)
    reason = "contextual link in meaningful content" + (" (forum / user-generated)" if l.ugc else "")
    return _result("editorial", score, reason, ev2, l, th)


def classify_page(targets, source_url: str, links, page_content: dict | None, *, relationships: dict | None = None, tracker: RepetitionTracker | None = None,
                  thresholds: Thresholds | None = None, only_targets: bool = True) -> list[tuple[int, dict]]:
    """[(index into links, classification)] for the links of one page that point at a monitored target. `targets` are Target objects or domain strings. Page-level work
    (page statistics, repetition lookups) is done once per page, then each link is classified on its own."""
    ts = [t if isinstance(t, Target) else (relationships or {}).get(core.clean_target(t)) or Target(core.clean_target(t)) for t in targets]
    src_host = _host(source_url)
    src_dom = _reg(src_host)
    stats = page_stats(links, src_dom)
    out = []
    for i, link in enumerate(links):
        dest, _a, _r, ltype, extra = link
        if ltype != "html":
            continue
        dh = _host(dest)
        for t in ts:
            if _matches(dh, t.domain):
                rep = tracker.context_for(_repetition_group(src_dom, source_url), dest, extra.get("link_context")) if tracker is not None else None
                out.append((i, classify_link(t, source_url, link, page_content, None, rep, page_stats=stats, thresholds=thresholds)))
                break
    return out


class PageClassifier:
    """The crawler-side wrapper: one instance per crawl (set as `cfg.classifier`; process_page calls classify_page_facts). Holds the relationships, one RepetitionTracker and
    the results, all explicitly owned (no module globals), so two crawls or two monitored targets never mix. Results are in memory only (Phase 4 persists)."""

    def __init__(self, relationships: dict | None = None, thresholds: Thresholds | None = None, keep_results: bool = True, on_page=None):
        self.relationships = relationships or {}
        self.thresholds = thresholds or DEFAULT
        self.tracker = RepetitionTracker()
        self.keep_results = keep_results
        self.on_page = on_page  # optional callable(source_url, [(index, classification)]) instead of / besides keeping results
        self.results: dict[str, list] = {}
        self._lock = threading.RLock()
        self._pages = {}
        self._finalized = False

    def classify_page_facts(self, source_url: str, facts, target_domains) -> list:
        """Return provisional results. One instance covers one crawl with a fixed monitored target set."""
        links = facts[0]
        targets = [self.relationships.get(d) or Target(d) for d in target_domains]
        domain = _reg(_host(source_url))
        stats = page_stats(links, domain)
        evidence = _page_evidence(getattr(facts, "content", None))
        entries = []
        for index, link in enumerate(links):
            if link[3] != "html":
                continue
            target = next((t for t in targets if _matches(_host(link[0]), t.domain)), None)
            if target:
                view = _link(link)
                sentence = _norm(view.ctx.get("sentence") or "")
                # Keep no DOM, article body or evidence for unrelated destinations.
                summary = {"listing_like": stats["listing_like"], "external_domains": stats["external_domains"],
                           "per_sentence": {sentence: stats["per_sentence"].get(sentence, 0)}}
                entries.append((index, target, view, summary))
        with self._lock:
            if self._finalized:
                raise RuntimeError("this crawl has been finalized")
            self.tracker.add_watch(t.domain for t in targets)
            self.tracker.observe(domain, source_url, links)
            self._pages[source_url] = (_repetition_group(domain, source_url), evidence, entries)
            return self._publish(source_url, provisional=True)

    def _publish(self, url, provisional):
        domain, evidence, entries = self._pages[url]
        results = []
        for index, target, link, summary in entries:
            repetition = self.tracker.context_for(domain, link.dest, link.ctx)
            result = _classify(target, url, link, evidence, None, repetition, summary, self.thresholds)
            result["provisional"] = provisional
            results.append((index, result))
        if self.keep_results:
            self.results[url] = results
        if self.on_page is not None:
            self.on_page(url, results)
        return results

    def finalize(self):
        """Publish one final replacement per page after the last observation, including to streaming consumers."""
        with self._lock:
            if not self._finalized:
                for url in sorted(self._pages):
                    self._publish(url, provisional=False)
                self._pages.clear()
                self._finalized = True
            return self.results

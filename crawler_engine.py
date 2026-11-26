"""Forward crawler: crawls approved source domains, stores every URL and link, admits new domains, recrawls.

Crawlee's RequestQueue is a throwaway per-round work list. Everything permanent (what was found, what is
crawled, when to recrawl) lives in PostgreSQL via crawler_core, so a restart loses nothing.
"""

import asyncio
import functools
import hashlib
import json
import logging
import os
import re
import warnings
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import timedelta
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag, XMLParsedAsHTMLWarning
from crawlee import ConcurrencySettings, Request, SkippedReason
from crawlee.crawlers import (
    BeautifulSoupCrawler, BeautifulSoupCrawlingContext, PlaywrightCrawler, PlaywrightCrawlingContext,
)
from crawlee._utils.robots import RobotsTxtFile
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser  # shortcut: private crawlee module (pinned in requirements.txt); re-check when upgrading
from crawlee.errors import PersistentRateLimitError, RequestThrottledError
from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.http_clients import ImpitHttpClient
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue
from yarl import URL

import crawler_core as core
import egress_proxy

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)  # XML pages are skipped by content type anyway

# Dead or forbidden pages: retrying will not help and we must not push past access controls.
PERMANENT_HTTP = [400, 401, 403, 404, 405, 406, 410, 451]
ACCESS_RESTRICTED = (401, 403, 451)  # the site refuses us; recorded as such, retried only after the permanent-failure interval, never worked around


def is_challenge(soup) -> bool:
    """A bot-challenge interstitial served with a success status (Cloudflare's "Just a moment..."): its HTML says nothing about the page we wanted."""
    t = soup.title.get_text(" ", strip=True).lower() if soup is not None and soup.title else ""
    return t.startswith("just a moment") or t.startswith("attention required! | cloudflare")
log = logging.getLogger("crawler.discovery")
vlog = logging.getLogger("crawler.verification")
xlog = logging.getLogger("crawler.extraction")

# Hosts with millions of pages that block crawlers: store their URLs but never crawl inside them.
NO_CRAWL_DOMAINS = {"linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com",
                    "pinterest.com", "tiktok.com", "reddit.com", "quora.com", "medium.com", "google.com"}


@dataclass
class Config:
    target_domain: str | None = None
    max_pages: int = 10_000
    max_depth: int = 5
    concurrency: int = 10
    use_playwright: bool = False
    enable_sitemap_discovery: bool = True
    enable_external_domain_discovery: bool = False
    max_domains: int = 20  # cap on admitted source domains per job (controlled expansion)
    admit_batch: int = 5  # domains admitted per expansion step
    timeout: int = 30  # seconds per request
    max_retries: int = 3
    domain_delay: float = 0.2  # min seconds between requests to one domain (politeness)
    recrawl_days: int = 7
    max_response_bytes: int = 2_000_000
    max_redirects: int = 5
    js_fallback: bool = True  # re-fetch JavaScript-looking pages in a browser (never every page)
    rate_limit_base: float = 2.0  # first backoff after an HTTP 429, doubling up to 60s
    rate_limit_stall: float = 300.0  # a host rate limited this long ends the round
    max_js_pages: int = 20  # per round: browsers are expensive


HTTP = {"started": 0, "done": 0, "total": 0.0, "retries": 0}  # fetch counters for the diagnostics log (single event loop, no lock needed)
_T0: dict[str, float] = {}


def _http_done(url: str, failed: bool = False) -> None:
    t = _T0.pop(url, None)
    if t is not None:
        HTTP["done"] += 1
        HTTP["total"] += time.monotonic() - t
        HTTP["retries"] += failed


_PL: dict = {}
WRITERS = 8  # background database writers per crawl round (each holds one connection while storing a page)
PERSIST_QUEUE_SECONDS = 300  # extra handler time for waiting in line behind pages of the same domain


def _persist_lock(domain: str) -> asyncio.Lock:
    """In-process queue per source domain (the cross-process guard is the advisory lock inside process_page). Waiting here holds
    no thread and no database connection. Different domains never wait for each other."""
    loop = asyncio.get_running_loop()
    if _PL.get("loop") is not loop:
        _PL.clear()
        _PL.update(loop=loop, locks={})
    return _PL["locks"].setdefault(domain, asyncio.Lock())


def _job_add(job_id: str | None, col: str, n: int = 1) -> None:
    if job_id and col in ("pages_crawled", "pages_failed", "domains_admitted"):
        with core.connect() as c:
            c.execute(f"UPDATE crawl_jobs SET {col}={col}+? WHERE job_id=?", (n, job_id))


# ---------- link placement ----------
# Where on the page a link sits: nav / header / footer / aside (GLOBAL site regions), content, or unknown. Evidence per ancestor, top-down:
#  * strong: class / id tokens that name a site-level region (site-footer, primary-nav, sidebar, elementor-location-footer, ...). Matched on whole
#    tokens, with exclusions for local words (entry-footer, card-header, page-header, post-navigation), so "header" alone never counts.
#  * weak: the semantic tag (<nav> <header> <footer> <aside>) or ARIA role. A weak region INSIDE an <article> (or a <main> that is not the page
#    wrapper) is article-local (article header / table of contents / article footer): it is content, not a site region. Strong evidence is never local.
#    Directly in a <main> (no <article> around it) a weak header / nav / footer is ambiguous (page title or sloppy site chrome): unknown.
# Nested regions resolve by precedence footer > aside > nav > header (a footer inside a <header> is a footer; a menu inside a header is navigation).
# One element whose own signals contradict each other (<header class="site-footer">) gives "unknown" rather than a guess; so does no evidence at all. <body>/<html> are never evidence: themes put
# "ehf-footer" and the like on <body>. State is computed once per element and shared by all links below it (no per-link ancestor walk).
_TAG_KIND = {"nav": "nav", "header": "header", "footer": "footer", "aside": "aside"}
_ROLE_KIND = {"navigation": "nav", "banner": "header", "contentinfo": "footer", "complementary": "aside"}
_NOT_STRUCT = {"entry", "post", "posts", "article", "card", "modal", "dialog", "comment", "comments", "popup", "tab", "tabs", "panel", "product", "item"}
_HEADER_QUAL = {"site", "global", "main", "top", "website", "primary", "sticky", "wrapper", "container", "location", "desktop", "mobile", "responsive", "fixed"}
_NAV_QUAL = {"site", "primary", "main", "global", "top", "menu", "mobile", "desktop", "sticky", "secondary", "bar"}
_NAV_LOCAL = {"toc", "contents", "breadcrumb", "breadcrumbs", "pagination", "pager", "paging", "tab", "tabs", "step", "steps", "post", "posts", "article", "entry", "comment", "comments"}
_CONTENT_PAIRS = ({"main", "content"}, {"article", "content"}, {"post", "content"}, {"entry", "content"}, {"blog", "content"}, {"article", "body"}, {"post", "body"}, {"entry", "body"})
_PROSE = {"p", "blockquote", "figcaption"}
_SPLIT = re.compile(r"[^a-z0-9]+")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


@functools.lru_cache(maxsize=8192)
def _declares_local(item: str) -> bool:
    """class / id such as entry-header, post-footer, card-header: the template says this header / footer belongs to one post or card."""
    t = set(_SPLIT.split(_CAMEL.sub(" ", item).lower()))
    return bool(t & {"entry", "post", "article", "card"}) and bool(t & {"header", "footer"})


@functools.lru_cache(maxsize=8192)
def _item_kinds(item: str) -> tuple[set, bool, bool, bool]:
    """(strong region kinds, is a content container, is a local navigation such as a table of contents, bare nav token) for one class or id value.
    A qualified nav name (site-nav, primary-navigation, main-menu-nav) is strong; a bare "nav" / "navbar" is only as strong as a <nav> tag (see _signal)."""
    t = set(_SPLIT.split(_CAMEL.sub(" ", item).lower())) - {""}
    kinds = set()
    words = _SPLIT.split(_CAMEL.sub(" ", item).lower())
    after_content = any(a == "content" and b == "footer" for a, b in zip(words, words[1:]))  # "content-footer" ends a page's content; "footer-content" is the site footer
    if ("footer" in t or "copyright" in t or "colophon" in t) and not t & (_NOT_STRUCT | {"pre", "prefooter"}) and not after_content:  # "pre-footer" is a call-to-action band above the footer, not the footer
        kinds.add("footer")
    if ("masthead" in t or ("header" in t and t & _HEADER_QUAL)) and not t & (_NOT_STRUCT | {"page", "section", "table"}):
        kinds.add("header")
    local_nav = bool(t & {"toc", "breadcrumb", "breadcrumbs", "pagination", "pager", "paging"}) or {"table", "contents"} <= t
    bare = False
    if ("navbar" in t or "nav" in t or ("navigation" in t and t & _NAV_QUAL)) and not t & _NAV_LOCAL:
        if t & _NAV_QUAL or ("navbar" not in t and "nav" not in t):
            kinds.add("nav")
        else:
            bare = True
    if "sidebar" in t or "sidebars" in t or {"side", "bar"} <= t or {"widget", "area"} <= t:
        kinds.add("aside")
    return kinds, any(p <= t for p in _CONTENT_PAIRS) or "maincontent" in t, local_nav, bare


def _signal(el) -> tuple[frozenset, str | None, str | None, bool, bool, bool]:
    """(strong kinds, weak kind from tag/role, container 'article'|'main'|None, local-navigation marker, contradiction, declared local) of ONE element."""
    strong, container, local_nav, bare_nav, declared = set(), None, False, False, False
    role = (el.get("role") or "").strip().lower()
    cls = el.get("class")
    for item in [*(cls if isinstance(cls, list) else cls.split() if cls else ()), el.get("id") or ""]:
        k, is_content, ln, bare = _item_kinds(item)
        bare_nav = bare_nav or bare
        declared = declared or _declares_local(item)
        strong |= k
        container = "article" if is_content else container
        local_nav = local_nav or ln
    if local_nav:  # "navigation pagination", "toc": the item named it a local navigation, so no sibling class can make it a site menu
        strong.discard("nav")
    weak = _TAG_KIND.get(el.name) or _ROLE_KIND.get(role) or ("nav" if bare_nav else None)
    etype = (el.get("data-elementor-type") or "").lower()  # Elementor states what a template is: site chrome or the page/post body
    if etype in ("header", "footer"):
        strong.add(etype)
    elif etype in ("wp-page", "wp-post", "single", "single-post", "single-page", "archive"):
        container = container or "article"
    if weak == "nav":  # a table of contents / breadcrumb / pager is local to its page even when it is marked up as <nav>
        label = (el.get("aria-label") or "").lower()
        if local_nav or "table of contents" in label or "breadcrumb" in label or "pagination" in label:
            weak, local_nav = None, True
    if el.name == "article" or role == "article":
        container = "article"
    elif el.name == "main" or role == "main":  # <main id="main-content"> is the main landmark, not an article
        container = "main"
    clash = bool(weak and strong and weak not in strong and weak != "nav" and not (weak == "header" and "nav" in strong))
    return frozenset(strong), weak, container, local_nav, clash, declared and weak in ("header", "footer")


def _is_page_wrapper(el) -> bool:
    """A <main> (or content-class element) that directly holds site-footer evidence (a <footer>, role=contentinfo, a footer class / id, an Elementor footer
    template) is the whole page, not the article: it gives no locality. Footer evidence inside an <article> / <section> / <aside> / <nav> belongs to that
    block (a showcase or card footer), not to the site."""
    for d in el.find_all(True):
        a = d.attrs
        if d.name == "footer" or a.get("role") == "contentinfo" or str(a.get("data-elementor-type", "")).lower() == "footer" or (
                ("class" in a or "id" in a) and any("footer" in _item_kinds(i)[0] for i in [*(a.get("class") or ()), a.get("id") or ""])):
            for p in d.parents:
                if p is el:
                    return True
                if p.name in ("article", "section", "aside", "nav") or p.get("role") == "article":
                    break
    return False


def _resolve(kinds: frozenset) -> str | None:
    return next((k for k in ("footer", "aside", "nav", "header") if k in kinds), None)


_SEMANTIC = {"nav", "header", "footer", "aside", "main", "article"}
_ROOT_STATE = (frozenset(), frozenset(), frozenset(), None, False, False)


def _state(el, memo: dict) -> tuple:
    """(strong kinds, weak global kinds, local kinds, container, inside prose, contradiction) for everything below `el`, inherited top-down and
    memoized per element. Iterative (a pathological page can be thousands of elements deep) and elements without tag/class/id/role evidence just
    share their parent's state."""
    chain = []
    while el is not None and id(el) not in memo and el.name not in (None, "[document]"):
        chain.append(el)
        el = el.parent
    ps = memo[id(el)] if el is not None and id(el) in memo else _ROOT_STATE
    for el in reversed(chain):
        name = el.name
        if name in ("html", "body"):
            memo[id(el)] = ps
            continue
        gs, gw, loc, cont, prose, clash = ps
        if name in _SEMANTIC or "class" in el.attrs or "id" in el.attrs or "role" in el.attrs or "data-elementor-type" in el.attrs or name == "section":
            strong, weak, ctn, local_nav, bad, declared = _signal(el)
            clash = clash or (bad and cont != "article")
            if "!nav" in loc:  # below a pager / table of contents: nothing in it is a site menu ("nav-links" inside "navigation pagination")
                strong, weak = strong - {"nav"}, None if weak == "nav" else weak
            gs |= strong
            if weak:
                if cont == "article" or declared:
                    loc |= {weak}
                elif weak != "aside" and (cont == "main" or "§" in loc):  # directly in <main> / a <section>: a page title or block header, or site chrome in a sloppy template
                    loc |= {"?" + weak}
                else:
                    gw |= {weak}
            if local_nav:
                loc |= {"nav", "!nav"}
            if name == "section":
                loc |= {"§"}
            if ctn and (name == "article" or el.get("role") == "article" or el.get("data-elementor-type") or not _is_page_wrapper(el)):
                cont = "article" if "article" in (ctn, cont) else "main"
        prose = prose or name in _PROSE
        ps = memo[id(el)] = (gs, gw, loc, cont, prose, clash)
    return ps


def _position(a, memo: dict | None = None) -> str:
    """nav | header | footer | aside | content | unknown. See the block comment above; `memo` is shared by all links of one page."""
    placement, _ = _placement(a, {} if memo is None else memo)
    return placement


def _placement(a, memo: dict) -> tuple[str, str | None]:
    gs, gw, loc, cont, prose, clash = _state(a, memo)
    if clash:
        return "unknown", None
    strong, weak = _resolve(gs), _resolve(gw)
    if strong:
        return (weak if strong == "nav" and weak in ("footer", "aside") else strong), None
    if weak:
        return weak, None
    if any(k[0] == "?" for k in loc):  # ambiguous block header / footer / nav: not claimed as content, not as a site region
        return "unknown", None
    if cont or prose:  # article-local header / footer / nav stay content, remembered for later phases
        local = next((k for k in ("nav", "footer", "header", "aside") if k in loc), None)
        return "content", f"article_{local}" if local else None
    return "unknown", None


# ---------- Phase 2: page content, page-type evidence and link context (evidence only) ----------
# Nothing here decides whether a link is an editorial backlink; Phase 3 reads this evidence. Everything is computed from the one BeautifulSoup tree that
# extract_links already walks: ONE pass over the tags (_index) feeds the links, the headings, the text blocks and the page-type signals, the Phase 1
# placement memo is shared (a text block is main content when Phase 1 places it in "content"), and every text is bounded.
LINK_CONTEXT_CHARS = int(os.environ.get("LINK_CONTEXT_CHARS", 300))        # text kept on each side of a link
BLOCK_SCAN_CHARS = int(os.environ.get("LINK_BLOCK_SCAN_CHARS", 6000))      # longest block (paragraph / row / parent) read for a link's context
PAGE_TEXT_CHARS = int(os.environ.get("PAGE_CONTENT_MAX_CHARS", 20000))     # main-content text kept per page (counts are for the full text, up to the scan cap)
PAGE_SCAN_CHARS = int(os.environ.get("PAGE_CONTENT_SCAN_CHARS", 250000))
MIN_COMPLETE_WORDS = 50

_HEADINGS = frozenset(("h1", "h2", "h3", "h4", "h5", "h6"))
_TEXT_BLOCKS = frozenset(("p", "li", "blockquote", "pre", "figcaption", "dd", "dt"))
_LINK_BLOCKS = _TEXT_BLOCKS | _HEADINGS | {"td", "th", "caption"}
_BREAKS = _HEADINGS | {"br", "div", "p", "li", "ul", "ol", "dl", "tr", "td", "th", "table", "section", "article", "blockquote", "hr", "figure", "figcaption",
                       "pre", "dd", "dt", "header", "footer", "nav", "aside", "form", "main"}
_SKIP_TEXT = frozenset(("script", "style", "noscript", "template", "svg", "iframe", "head", "title"))
_BLOCK_KIND = {"p": "paragraph", "li": "list_item", "dd": "definition", "dt": "definition", "td": "table_row", "th": "table_row", "tr": "table_row", "blockquote": "blockquote",
               "figcaption": "caption", "caption": "caption", "pre": "preformatted"}
_WS = re.compile(r"\s+")
_SENT = re.compile(r"(?<=[.!?…])[\"')\]]*\s+(?=[A-Z0-9\"“(\[])")
_BOILER_TOKENS = {"cookie", "cookies", "consent", "gdpr", "ccpa", "popup", "modal", "lightbox", "advert", "advertisement", "adsbygoogle", "adslot", "adunit", "ads", "ad"}
_LINK_VOCAB = {"card", "listing", "directory", "profile", "comment", "comments", "reply", "resource", "resources", "reference", "references", "footnotes", "widget",
               "author", "bio", "review", "testimonial", "partner", "partners", "sponsor", "client", "clients", "related", "share", "social"}
_RESOURCE_HEADING = re.compile(r"\b(resources?|references?|further reading|useful links|related (links|reading)|sources|bibliography|recommended)\b", re.I)
_URL_PATTERNS = tuple((k, re.compile(v, re.I)) for k, v in (
    ("blog", r"/(blog|blogs|post|posts)(/|$)"), ("news", r"/(news|press|press-releases?)(/|$)"), ("article", r"/(articles?|stories|story)(/|$)"),
    ("resource", r"/(resources?|guides?|learn|library|whitepapers?|case-stud(y|ies))(/|$)"), ("documentation", r"/(docs?|documentation|manual|reference|api)(/|$)"),
    ("profile", r"/(profiles?|users?|members?|people|author|authors|company|organization|organisations?|in)/[^/]+"),
    ("directory", r"/(directory|directories|listings?|business(es)?|companies|yellow-?pages|places?)(/|$)"),
    ("forum", r"/(forums?|threads?|topics?|discussions?|community|questions?|viewtopic)(/|$)"), ("dated_path", r"/(19|20)\d{2}/\d{1,2}(/|$)"),
    ("category_or_tag", r"/(category|categories|tag|tags)/"), ("product", r"/(products?|shop|store|collections?|item)(/|$)")))
_SCHEMA_GROUPS = {"blog": {"blogposting", "blog", "liveblogposting"},
                  "news": {"newsarticle", "reportagenewsarticle", "analysisnewsarticle", "opinionnewsarticle", "newsmediaorganization"},
                  "article": {"article", "techarticle", "scholarlyarticle", "socialmediaposting", "report"}, "resource": {"howto", "course", "learningresource", "dataset"},
                  "profile": {"profilepage", "person", "organization", "corporation", "ngo", "onlinebusiness"},
                  "directory": {"itemlist", "collectionpage", "localbusiness", "store", "restaurant", "hospital", "medicalbusiness", "professionalservice", "realestateagent",
                                "lodgingbusiness"},
                  "forum": {"discussionforumposting", "qapage"}, "homepage": {"website"},
                  "ecommerce": {"product", "offer", "aggregateoffer", "productgroup"}}


@functools.lru_cache(maxsize=8192)
def _toks(item: str) -> frozenset:
    return frozenset(_SPLIT.split(_CAMEL.sub(" ", item).lower())) - {""}


@functools.lru_cache(maxsize=8192)
def _class_marks(items: tuple) -> tuple:
    """Page-type markers named by one element's class / id values (whole tokens)."""
    t = frozenset().union(*(_toks(i) for i in items)) if items else frozenset()
    if t & {"fa", "fas", "far", "fab", "icon", "icons", "social", "badge"}:  # an icon or a social button named after the thing ("fa-comments", "social-link-threads")
        return ()
    out = []
    if t & {"comments", "commentlist", "respond"} or {"comment", "list"} <= t:
        out.append("comments_section")
    if t & {"thread", "threads", "forum", "bbp", "phpbb", "vbulletin", "xenforo", "discourse"}:
        out.append("forum_structure")
    if t & {"listing", "listings"} or {"business", "card"} <= t or {"company", "card"} <= t or {"directory", "item"} <= t:
        out.append("listing_block")
    if t & {"byline", "author", "authors"}:
        out.append("author_markup")
    if t & {"published", "pubdate"} or {"entry", "date"} <= t or {"post", "date"} <= t:
        out.append("date_markup")
    if t & {"cart", "woocommerce"} or {"product", "price"} <= t:
        out.append("shop_markup")
    return tuple(out)


@functools.lru_cache(maxsize=8192)
def _link_marks(items: tuple) -> tuple:
    return tuple(sorted(frozenset().union(*(_toks(i) for i in items)) & _LINK_VOCAB)) if items else ()


@functools.lru_cache(maxsize=4096)
def _is_boiler_item(item: str) -> bool:
    return bool(_toks(item) & _BOILER_TOKENS)


@functools.lru_cache(maxsize=4096)
def _is_button_item(item: str) -> bool:
    return bool(_toks(item) & {"btn", "button", "cta"})


class _Index:
    """Every element Phase 2 needs, collected in ONE pass over the tree (document order)."""
    __slots__ = ("ah", "anchors", "headings", "blocks", "metas", "links", "ld", "imgs", "times", "itemtypes", "marks", "tel", "addresses", "has_body")

    def __init__(self) -> None:
        self.ah, self.anchors, self.headings, self.blocks, self.metas, self.links, self.ld = [], [], [], [], [], [], []
        self.imgs, self.times, self.itemtypes, self.marks = [], [], [], {}
        self.tel = self.addresses = 0
        self.has_body = False


def _index(soup) -> _Index:
    ix = _Index()
    marks = ix.marks
    for el in soup.find_all(True):
        n, at = el.name, el.attrs
        if n == "a":
            href = at.get("href")
            if href is not None:
                ix.anchors.append(el)
                ix.ah.append(el)
                if isinstance(href, str) and href[:4].lower() == "tel:":
                    ix.tel += 1
        elif n in _HEADINGS:
            ix.headings.append(el)
            ix.ah.append(el)
        elif n in _TEXT_BLOCKS:
            ix.blocks.append(el)
        elif n == "meta":
            ix.metas.append(el)
        elif n == "link":
            ix.links.append(el)
        elif n == "script":
            if (at.get("type") or "").lower() == "application/ld+json":
                ix.ld.append(el)
        elif n == "img":
            ix.imgs.append(el)
        elif n == "time":
            ix.times.append(el)
        elif n == "address":
            ix.addresses += 1
        elif n == "body":
            ix.has_body = True
        if "itemtype" in at:
            ix.itemtypes.append(at["itemtype"])
        if "class" in at or "id" in at:
            cls = at.get("class")
            for m in _class_marks((*(cls if isinstance(cls, list) else cls.split() if cls else ()), at.get("id") or "")):
                marks[m] = marks.get(m, 0) + 1
    return ix


def _hidden(el, bmemo: dict) -> bool:
    """True inside hidden UI, cookie / consent banners, pop-ups, ads, <noscript>, <template>, <svg>, <iframe>. Inherited top-down and memoized per element."""
    chain = []
    while el is not None and id(el) not in bmemo and el.name not in (None, "[document]"):
        chain.append(el)
        el = el.parent
    flag = bmemo.get(id(el), False) if el is not None else False
    for e in reversed(chain):
        if not flag:
            at = e.attrs
            style = at.get("style")
            if (e.name in ("noscript", "template", "svg", "iframe") or "hidden" in at or at.get("aria-hidden") == "true" or at.get("role") in ("dialog", "alertdialog")
                    or (isinstance(style, str) and ("display:none" in style.replace(" ", "").lower() or "visibility:hidden" in style.replace(" ", "").lower()))
                    or (("class" in at or "id" in at) and any(_is_boiler_item(i) for i in [*(at.get("class") or ()), at.get("id") or ""]))):
                flag = True
        bmemo[id(e)] = flag
    return flag


def _build(block, cap: int = BLOCK_SCAN_CHARS) -> tuple[str, dict, bool]:
    """(text, {id(anchor): (start, end)}, truncated) of one block: inline text keeps its natural spacing, block-level boundaries become a space, scripts and
    styles are skipped. The offsets of the links inside the block are recorded while the text is built, so every link of the block shares this one pass."""
    parts: list[str] = []
    size = 0
    spans: dict = {}
    space = True
    cut = False

    def add(s: str) -> None:
        nonlocal size, space, cut
        s = _WS.sub(" ", s)
        if space and s[:1] == " ":
            s = s[1:]
        if s:
            parts.append(s)
            size += len(s)
            space = s[-1] == " "
            cut = size > cap

    def walk(node, depth: int) -> None:
        if depth > 60:
            return
        for ch in node.children:
            if cut:
                return
            if isinstance(ch, Tag):
                nm = ch.name
                if nm in _SKIP_TEXT:
                    continue
                if nm in _BREAKS:
                    add(" ")
                if nm == "a" and ch.has_attr("href"):
                    start = size
                    walk(ch, depth + 1)
                    spans[id(ch)] = (start, size)
                else:
                    walk(ch, depth + 1)
                if nm in _BREAKS and nm not in ("br", "hr"):
                    add(" ")
            elif type(ch) is NavigableString:
                add(str(ch))

    walk(block, 0)
    return "".join(parts).rstrip(), spans, cut


def _sentences(text: str) -> list[tuple[int, int]]:
    bounds, last = [], 0
    for m in _SENT.finditer(text):
        bounds.append((last, m.start() + 1))
        last = m.end()
    bounds.append((last, len(text)))
    return bounds


def _clip(s: str, n: int, keep_end: bool = False) -> str:
    """At most n characters, without a cut-off word at the open end."""
    s = s.strip()
    if len(s) <= n:
        return s
    if keep_end:
        s = s[-n:]
        return s.split(" ", 1)[1] if " " in s else s
    s = s[:n]
    return s.rsplit(" ", 1)[0] if " " in s else s


def _enclosing_block(a):
    for i, p in enumerate(a.parents):
        if i > 7 or p.name in ("body", "html", "[document]"):
            return None
        if p.name in _LINK_BLOCKS:
            return p.parent if p.name in ("td", "th") and p.parent is not None and p.parent.name == "tr" else p
    return None


def _link_context(a, anchor_text: str, img, heading, cache: dict) -> dict:
    """Evidence about the text around one link. Empty strings, never invented text, when the block holds nothing but the link."""
    parent = a.parent
    block = _enclosing_block(a)
    if block is None and parent is not None and parent.name not in ("body", "html", "[document]"):
        block = parent  # text sitting straight in a <div> / <span> / <section>
    bt = block.name if block is not None else None
    cls = a.get("class")
    item_cls = cls if isinstance(cls, list) else cls.split() if cls else []
    pcls = parent.get("class") if parent is not None else None
    ctx = {"parent_tag": parent.name if parent is not None else None,
           "parent_class": (" ".join(pcls) if isinstance(pcls, list) else pcls or "")[:80] or None,
           "parent_id": ((parent.get("id") or "")[:40] or None) if parent is not None else None,
           "block_tag": bt, "block_kind": (_BLOCK_KIND.get(bt) or ("heading" if bt in _HEADINGS else "other")) if bt else None,
           "link_kind": "image" if img is not None and not anchor_text else "button" if a.get("role") == "button" or any(_is_button_item(i) for i in item_cls) else "text",
           "section_heading": heading[1] if heading else None, "heading_level": heading[0] if heading else None,
           "text_before": "", "text_after": "", "sentence": "", "prev_sentence": "", "next_sentence": "", "surrounding_text": "", "lead_in": "",
           "container_markers": [], "context_quality": "insufficient"}
    marks: set = set()
    for i, p in enumerate(a.parents):
        if i > 5:
            break
        c = p.get("class")
        marks.update(_link_marks((*(c if isinstance(c, list) else c.split() if c else ()), p.get("id") or "")))
    ctx["container_markers"] = sorted(marks)
    if block is None:
        return ctx
    got = cache.get(id(block))
    if got is None:
        text, spans, _ = _build(block)
        got = cache[id(block)] = (text, spans, _sentences(text) if text else [])
    text, spans, sents = got
    span = spans.get(id(a))
    if span is None or not text:
        return ctx
    s, e = span
    before, after = text[:s], text[e:]
    if len(_WS.sub(" ", before + after).split()) < 3:  # the block is the link and (almost) nothing else
        if bt in ("li", "dd", "dt") and block.parent is not None:  # a list entry: the sentence that introduces the list is the useful context
            lead = block.parent.find_previous_sibling()
            while lead is not None and lead.name not in _TEXT_BLOCKS | _HEADINGS | {"div", "span"}:
                lead = lead.find_previous_sibling()
            if lead is not None:
                ctx["lead_in"] = _clip(lead.get_text(" ", strip=True), LINK_CONTEXT_CHARS)
        return ctx
    i = next((k for k, (x, y) in enumerate(sents) if x <= s < y), len(sents) - 1)
    b4, af = _clip(before, LINK_CONTEXT_CHARS, keep_end=True), _clip(after, LINK_CONTEXT_CHARS)
    ctx.update(text_before=b4, text_after=af, sentence=_clip(text[sents[i][0]:sents[i][1]], 2 * LINK_CONTEXT_CHARS),
               prev_sentence=_clip(text[sents[i - 1][0]:sents[i - 1][1]], LINK_CONTEXT_CHARS) if i > 0 else "",
               next_sentence=_clip(text[sents[i + 1][0]:sents[i + 1][1]], LINK_CONTEXT_CHARS) if i + 1 < len(sents) else "",
               surrounding_text=" ".join(x for x in (b4, anchor_text[:200], af) if x), context_quality="sufficient")
    return ctx


def _meta(ix: _Index) -> dict:
    m: dict = {}
    for el in ix.metas:
        at = el.attrs
        key = (at.get("name") or at.get("property") or at.get("http-equiv") or "").strip().lower()
        val = at.get("content")
        if key and isinstance(val, str) and val.strip() and key not in m:
            m[key] = val.strip()
    return m


def _walk_ld(node, found: dict, budget: list, depth: int = 0) -> None:
    if depth > 6 or budget[0] <= 0:
        return
    budget[0] -= 1
    if isinstance(node, list):
        for x in node:
            _walk_ld(x, found, budget, depth + 1)
    elif isinstance(node, dict):
        t = node.get("@type")
        for x in ([t] if isinstance(t, str) else t if isinstance(t, list) else []):
            if isinstance(x, str):
                name = x.rsplit("/", 1)[-1]
                found["types"][name] = found["types"].get(name, 0) + 1
        if node.get("datePublished") or node.get("dateCreated"):
            found["date"] = True
        if node.get("author") or node.get("creator"):
            found["author"] = True
        for k, v in node.items():
            if k != "@context" and isinstance(v, (dict, list)):
                _walk_ld(v, found, budget, depth + 1)


def _page_type_signals(base: str, ix: _Index, meta: dict, headings: list, has_article: bool) -> dict:
    """Evidence for what kind of page this is. Several types can be supported at once (an article page with Organization markup, a homepage that lists
    businesses); no type is decided here, `type_evidence` only groups the signals under the type they would support."""
    found = {"types": {}, "date": False, "author": False}
    for s in ix.ld:
        raw = s.string if s.string is not None else s.get_text()
        if raw and len(raw) <= 200_000:
            try:
                _walk_ld(json.loads(raw), found, [2000])
            except (ValueError, RecursionError):
                pass
    schema_types = dict(found["types"])
    for it in ix.itemtypes:  # microdata: itemtype="https://schema.org/BlogPosting"
        for part in (it if isinstance(it, str) else " ".join(it)).split():
            t = part.rsplit("/", 1)[-1]
            if t:
                schema_types[t] = schema_types.get(t, 0) + 1
    metadata_types = [f"og:type={meta['og:type']}"] if meta.get("og:type") else []
    metadata_types += [k for k in ("article:published_time", "article:author", "article:section", "profile:first_name", "og:profile:username") if k in meta]
    path = urlsplit(base).path or "/"
    url_patterns = [name for name, rx in _URL_PATTERNS if rx.search(path)]
    if path == "/":
        url_patterns.insert(0, "root")
    marks = ix.marks
    markers = sorted(k for k in marks if k not in ("author_markup", "date_markup"))
    heads = " ".join(t for _, t in headings).lower()
    if _RESOURCE_HEADING.search(heads):
        markers.append("resource_heading")
    if re.search(r"\b(comments?|replies|reply|responses?)\b", heads) and "comments_section" not in markers:
        markers.append("comments_heading")
    if ix.tel >= 3:
        markers.append(f"tel_links={ix.tel}")
    if ix.addresses >= 3:
        markers.append(f"address_elements={ix.addresses}")
    if marks.get("listing_block", 0) >= 3:
        markers.append(f"repeated_listing_blocks={marks['listing_block']}")
    has_author = bool(found["author"] or "author" in meta or "article:author" in meta or marks.get("author_markup")
                      or any("author" in (el.get("rel") or []) for el in ix.links + ix.anchors))
    has_date = bool(found["date"] or "article:published_time" in meta or ix.times or marks.get("date_markup"))
    low = {k.lower(): (k, v) for k, v in schema_types.items()}
    ev: dict = {k: [] for k in ("blog", "news", "article", "resource", "documentation", "profile", "directory", "forum", "homepage", "ecommerce")}
    for group, names in _SCHEMA_GROUPS.items():
        for n in names:
            if n in low:
                ev[group].append(f"schema:{low[n][0]}" + (f"x{low[n][1]}" if low[n][1] > 1 and group == "directory" else ""))
    og = (meta.get("og:type") or "").lower()
    if og == "article":
        ev["article"].append("og:type=article")
    elif og.startswith("profile"):
        ev["profile"].append(f"og:type={og}")
    elif og == "website":
        ev["homepage"].append("og:type=website")
    elif og.startswith("product"):
        ev["ecommerce"].append(f"og:type={og}")
    for name in url_patterns:
        if name == "category_or_tag":  # a taxonomy archive says nothing about the page type; it stays visible in url_patterns only
            continue
        ev[{"root": "homepage", "dated_path": "article", "product": "ecommerce"}.get(name, name)].append(f"url:{name}")
    if "comments_section" in markers or "comments_heading" in markers:
        ev["blog"].append("marker:comments")
    if "forum_structure" in markers:
        ev["forum"].append("marker:forum_structure")
    if marks.get("listing_block", 0) >= 3 or ix.addresses >= 3:  # phone links alone are on every business homepage, so they are reported but not counted here
        ev["directory"].append("marker:listing_blocks_or_contact_details")
    if "resource_heading" in markers:
        ev["resource"].append("marker:resource_heading")
    if "shop_markup" in markers:
        ev["ecommerce"].append("marker:shop_markup")
    if has_author and has_date and has_article:
        ev["article"].append("structure:author+date+article")
    return {"schema_types": sorted(schema_types), "metadata_types": metadata_types, "url_patterns": url_patterns, "content_markers": markers,
            "has_author": has_author, "has_published_date": has_date, "has_article_structure": has_article, "type_evidence": {k: v for k, v in ev.items() if v}}


def page_content(soup, base: str, ix: "_Index | None" = None, memo: dict | None = None, status: int | None = None) -> dict:
    """Page-level evidence: metadata, headings, main-content text and statistics, page-type signals and an extraction-quality verdict. Reuses the
    Phase 1 placement memo: a text block is main content when Phase 1 places it in "content"; boilerplate regions, hidden UI, cookie banners and ads are left out."""
    ix = ix if ix is not None else _index(soup)
    memo = memo if memo is not None else {}
    bmemo: dict = {}
    meta = _meta(ix)
    title = soup.title.get_text(strip=True)[:300] if soup.title else None
    canonical = next((core.resolve(base, el["href"]) for el in ix.links if "canonical" in [r.lower() for r in (el.get("rel") or [])] and el.get("href")), None)
    html_el = soup.find("html")
    lang = ((html_el.get("lang") or html_el.get("xml:lang")) if html_el is not None else None) or meta.get("content-language") or meta.get("og:locale")
    desc = meta.get("description", "")[:500] or None
    og = {"title": meta["og:title"][:300] if meta.get("og:title") and meta["og:title"] != title else None,
          "description": meta["og:description"][:500] if meta.get("og:description") and meta["og:description"][:500] != desc else None, "type": meta.get("og:type")}
    # headings in order; a heading inside a site region (menu, footer, sidebar widget) or hidden UI is not an article heading
    outline, by_level = [], {"h1": [], "h2": [], "h3": [], "other": []}
    for h in ix.headings:
        if _placement(h, memo)[0] in ("content", "unknown") and not _hidden(h, bmemo):
            txt = _WS.sub(" ", h.get_text(" ", strip=True))[:200]
            if txt and len(outline) < 60:
                outline.append({"level": int(h.name[1]), "text": txt})
                bucket = by_level.get(h.name, by_level["other"])
                if len(bucket) < 30:
                    bucket.append(txt)
    # main content: the text blocks Phase 1 places in content (innermost block only, no link-only lines, no repeated boilerplate)
    nested = set()
    for b in ix.blocks:  # a block that holds another text block is skipped: its inner blocks carry the text
        for i, p in enumerate(b.parents):
            if i > 5:
                break
            if p.name in _TEXT_BLOCKS:
                nested.add(id(p))
                break
    chosen, seen, kinds = [], set(), {}
    stats = {"words": 0, "chars": 0, "kept": 0}

    def take(text: str, spans: dict, container) -> bool:
        w = len(text.split())
        if len(text) < 15 or w < 3 or text in seen:
            return False
        if sum(e - s for s, e in spans.values()) > 0.8 * len(text) and w < 25:
            return False  # a line of links (related posts, share buttons), not prose
        seen.add(text)
        stats["words"] += w
        stats["chars"] += len(text)
        if stats["kept"] < PAGE_TEXT_CHARS:
            chosen.append(text[:PAGE_TEXT_CHARS - stats["kept"]])
            stats["kept"] += len(text) + 1
        kinds[container] = kinds.get(container, 0) + 1
        return True

    lists, paragraphs, blocks_n, fallback = set(), 0, 0, False
    for b in ix.blocks:
        if stats["chars"] > PAGE_SCAN_CHARS or id(b) in nested:
            continue
        if _placement(b, memo)[0] != "content" or _hidden(b, bmemo):
            continue
        text, spans, _ = _build(b)
        if take(text, spans, _state(b, memo)[3]):
            blocks_n += 1
            paragraphs += b.name == "p"
            if b.name == "li" and b.parent is not None:
                lists.add(id(b.parent))
    if stats["words"] < MIN_COMPLETE_WORDS:  # conservative fallback for div-soup pages: long loose text in regions Phase 1 could not place
        for el in soup.find_all(("div", "span", "td", "section")):
            if stats["chars"] > PAGE_SCAN_CHARS:
                break
            own = _WS.sub(" ", " ".join(s for s in el.find_all(string=True, recursive=False) if type(s) is NavigableString)).strip()
            if len(own) >= 80 and _placement(el, memo)[0] == "unknown" and not _hidden(el, bmemo) and take(own, {}, "fallback"):
                blocks_n += 1
                fallback = True
    words, chars = stats["words"], stats["chars"]
    images = sum(1 for im in ix.imgs if im.get("width") != "1" and im.get("height") != "1" and _placement(im, memo)[0] == "content" and not _hidden(im, bmemo))
    if words >= MIN_COMPLETE_WORDS:
        quality, reason = ("fallback", "loose text outside any recognised content region") if fallback else ("complete", None)
    elif words > 0:
        quality, reason = "thin", f"only {words} words of main content"
    elif not ix.has_body and not ix.anchors and not ix.blocks:
        quality, reason = "insufficient", "empty_document"
    else:
        quality, reason = "insufficient", "js_shell_suspected" if needs_js(soup) else "no_text_in_content_regions"
    has_article = bool(by_level["h1"] or by_level["h2"]) and any(
        _state(b, memo)[3] == "article" and id(b) not in nested and _placement(b, memo)[0] == "content" for b in ix.blocks[:300])
    return {"url": base, "http_status": status, "title": title, "meta_description": desc, "canonical_url": canonical, "language": (lang or "").strip()[:20] or None,
            "meta_robots": (meta.get("robots") or meta.get("googlebot") or "")[:100] or None, "og": og, "headings": {**by_level, "outline": outline},
            "main_content": {"text": "\n".join(chosen), "word_count": words, "character_count": chars, "paragraph_count": paragraphs, "heading_count": len(outline),
                             "list_count": len(lists), "image_count": images, "block_count": blocks_n, "container": max(kinds, key=kinds.get) if kinds else None,
                             "truncated": chars > PAGE_TEXT_CHARS, "has_main_content": quality in ("complete", "fallback")},
            "page_type_signals": _page_type_signals(base, ix, meta, [(o["level"], o["text"]) for o in outline], has_article),
            "extraction_quality": {"status": quality, "reason": reason}}


class PageFacts(tuple):
    """What page_facts returns: still the (links, content_hash, title) triple every caller unpacks, plus `.content`, the page-level evidence of page_content()."""
    content: dict

    def __new__(cls, links, digest, title, content):
        self = super().__new__(cls, (links, digest, title))
        self.content = content
        return self


EVIDENCE_SINK = None  # optional callable(page_url, content, links) that process_page calls with the Phase 2 evidence (Phase 3 plugs in here; nothing is persisted yet)


def extract_links(soup, base: str, ix: "_Index | None" = None, memo: dict | None = None) -> list[tuple[str, str, str | None, str, dict]]:
    """(destination, anchor_text, rel, link_type, extra) for every <a href> and the canonical URL. Query strings kept.
    extra: nofollow/sponsored/ugc flags, img_alt (image links), position (nav/header/footer/aside/content/unknown), local_region
    (article_header / article_footer / article_nav / article_aside when a content link sits in an article-local region; not persisted), context (the
    direct parent's first 300 characters, unchanged) and link_context (Phase 2: the text around the link, see _link_context; not persisted).
     ix / memo let page_facts share its one tree pass and the placement memo with the page-level extraction."""
    ix = ix if ix is not None else _index(soup)
    memo = {} if memo is None else memo
    out, cache, heading_in = [], {}, {}
    for el in ix.ah:  # anchors and headings in document order: a link's section heading is the latest heading of the same page region above it
        if el.name != "a":
            txt = _WS.sub(" ", el.get_text(" ", strip=True))[:200]
            if txt:
                heading_in[_placement(el, memo)[0]] = (int(el.name[1]), txt)
            continue
        a = el
        dest = core.resolve(base, a["href"])
        if dest:
            tokens = [t.lower() for t in (a.get("rel") or [])]
            img = a.find("img")
            position, local = _placement(a, memo)
            anchor = a.get_text(" ", strip=True)[:200]
            extra = {"nofollow": "nofollow" in tokens, "sponsored": "sponsored" in tokens, "ugc": "ugc" in tokens,
                     "img_alt": (img.get("alt") or "").strip()[:200] if img else None, "position": position, "local_region": local,
                     "context": (a.parent.get_text(" ", strip=True) if a.parent else "")[:300],
                     "link_context": _link_context(a, anchor, img, heading_in.get(position), cache)}
            out.append((dest, anchor, " ".join(tokens) or None, "html", extra))
    for ln in ix.links:
        if ln.get("href") is not None and "canonical" in [r.lower() for r in (ln.get("rel") or [])]:
            dest = core.resolve(base, ln["href"])
            if dest:
                out.append((dest, "", None, "canonical", {}))
    return out


def link_event(conn, source: str, dest: str, ddom: str, anchor: str, kind: str) -> None:
    """Append-only record of a link changing state (suspected_removed / lost / reappeared). Never updated or deleted, so repeated
    removal and reappearance all stay visible; outgoing_links only holds the current state."""
    conn.execute("INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type) VALUES (?,?,?,?,?)",
                 (source, dest, ddom, anchor, kind))


def page_facts(soup, base: str) -> "PageFacts":
    """The CPU-bound DOM work for one page (links, content hash, title, and the Phase 2 page evidence). Run it in a thread BEFORE the database transaction, so
    parsing never happens while the domain advisory lock is held. Unpacks as (links, content_hash, title) exactly as before; .content is page_content()."""
    ix, memo = _index(soup), {}
    return PageFacts(extract_links(soup, base, ix, memo), hashlib.sha256(str(soup).encode()).hexdigest(),
                     soup.title.get_text(strip=True)[:300] if soup.title else None, page_content(soup, base, ix, memo))


FENCE: dict[str, int] = {}  # normalized_url -> lease_gen this process got when it claimed the URL (set by autonomous.claim_urls / business_jobs.claim)


def stale_lease(conn, norm: str | None) -> bool:
    """Fencing: True if another worker claimed this URL after we did (its lease expired and was recovered), so our result is
    outdated and must not overwrite the newer one. URLs never claimed through a lease (manual / tests) are not fenced."""
    gen = FENCE.get(norm or "")
    if gen is None:
        return False
    r = conn.execute("SELECT lease_gen FROM discovered_urls WHERE normalized_url=?", (norm,)).fetchone()
    return r is None or r["lease_gen"] != gen


def process_page(conn, key_url: str, base_url: str, depth: int, soup, status: int | None, ctype: str | None,
                 cfg: Config, approved: set[str], follow: bool, started: str, facts: tuple | None = None) -> list[tuple[str, int]]:
    """Persist one successfully fetched page in ONE transaction (all of it or none of it). Returns internal (url, depth) to crawl next.

    Concurrency: pages of the same source domain are serialized by a per-domain advisory lock (they share hot rows: the domain row,
    sibling URLs). Pages of different domains run in parallel; their writes are made in one global order (links sorted by registrable
    domain, then URL; one upsert per domain, also sorted), which is what prevents lock cycles. A rare remaining deadlock is detected by
    PostgreSQL and the whole transaction is retried by the caller (core.retry_locked), so nothing is half-written."""
    src_dom = core.registrable_domain(core.hostname(key_url))
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(?))", ("page:" + src_dom,))
    if stale_lease(conn, core.normalize_url(key_url)):
        log.warning("STALE LEASE: %s was re-claimed by another worker; result dropped", key_url)
        return []
    facts = facts or page_facts(soup, base_url)
    links, digest, title = facts
    if EVIDENCE_SINK is not None:  # Phase 2 evidence for the next phase; the facts object is shared across deadlock retries, so this is idempotent
        content = getattr(facts, "content", None)
        if content is not None:
            content["http_status"] = status
        try:  # the sink runs inside the page transaction under the domain lock: a faulty sink must never roll the page back
            EVIDENCE_SINK(key_url, content, links)
        except Exception:  # noqa: BLE001
            log.exception("EVIDENCE_SINK failed for %s", key_url)
    links = list(links)  # a deadlock retry re-enters here; do not append the redirect twice
    if base_url != key_url:  # redirect
        links.append((base_url, "", None, "redirect", {}))
    prepared = {}
    for dest, anchor, rel, ltype, ex in links:  # one row per (destination, anchor); the order below is the global lock order
        prepared.setdefault((dest, anchor), (core.registrable_domain(core.hostname(dest)), core.normalize_url(dest) or dest, rel, ltype, ex))
    ordered = sorted(prepared.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0][1]))
    targets = core.active_targets(conn)
    classifier = getattr(cfg, "classifier", None)  # Phase 3: an optional per-crawl PageClassifier set on the Config (not a dataclass field, so asdict(cfg) never sees it)
    if classifier is not None:  # classifies, in memory only, the links of this page that point at a monitored target; a failure never aborts the page transaction
        try:
            classifier.classify_page_facts(base_url, facts, [d for _, d in targets])
        except Exception:  # noqa: BLE001
            log.exception("link classification failed for %s", key_url)
    # Set-based writes: a page is ~12 statements however many links it has (it was ~2 per link, and every statement is a network round trip).
    # Rows are still written in the same global (domain, URL) order, which is what keeps concurrent pages from deadlocking.
    items, urls = [], {}  # urls: normalized -> discovered_urls row (first occurrence wins, as the sequential code did)
    for (dest, anchor), (ddom, ndest, rel, ltype, ex) in ordered:
        internal = ddom == src_dom
        ok = core.crawlable(dest) and ddom not in NO_CRAWL_DOMAINS
        too_deep = internal and depth + 1 > cfg.max_depth  # claim_urls never leases these, so "pending" would be forever
        norm = core.normalize_url(dest)
        if norm and norm not in urls:
            host = core.hostname(norm)
            urls[norm] = (dest, norm, core.registrable_domain(host), host, depth + 1 if internal else 0, int(internal),
                          "pending" if ok and not too_deep else "skipped", "max_depth" if ok and too_deep else None)
        items.append((dest, anchor, ddom, ndest, norm, rel, ltype, ex, internal, ok))
    new_norms: set[str] = set()
    if urls:
        rows = sorted(urls.values(), key=lambda r: (r[2], r[1]))
        c = [list(x) for x in zip(*rows)]
        new_norms = {r["normalized_url"] for r in conn.execute(
            "INSERT INTO discovered_urls (url, normalized_url, domain, hostname, discovered_from, discovery_method, crawl_depth,"
            " is_internal, is_external, crawl_status, skip_reason)"
            " SELECT u, n, d, h, ?, 'forward_crawl', dp, i, 1-i, s, r FROM unnest(?::text[], ?::text[], ?::text[], ?::text[], ?::int[],"
            " ?::int[], ?::text[], ?::text[]) AS t(u, n, d, h, dp, i, s, r) ORDER BY d, n ON CONFLICT DO NOTHING RETURNING normalized_url",
            (key_url, *c)).fetchall()}
        old = [r for r in rows if r[1] not in new_norms]
        if old:  # a URL skipped only for max_depth comes back as pending when found again within reach; robots / security / not_html skips stay
            c = [list(x) for x in zip(*old)]
            react = "(u.crawl_status='skipped' AND u.skip_reason='max_depth' AND d.s='pending' AND d.dp<=u.crawl_depth)"
            conn.execute(
                "WITH d AS (SELECT * FROM unnest(?::text[], ?::int[], ?::text[]) AS t(n, dp, s)),"
                " k AS (SELECT u.id FROM discovered_urls u JOIN d ON u.normalized_url=d.n ORDER BY u.domain, u.normalized_url FOR UPDATE OF u)"
                f" UPDATE discovered_urls u SET last_seen=?, crawl_status=CASE WHEN {react} THEN 'pending' ELSE u.crawl_status END,"
                f" skip_reason=CASE WHEN {react} THEN NULL ELSE u.skip_reason END, crawl_depth=LEAST(u.crawl_depth, d.dp)"
                " FROM d, k WHERE u.normalized_url=d.n AND u.id=k.id",
                (c[1], c[4], c[6], core.now()))
    seen_out: set[tuple[str, str]] = set()
    found_backlinks: set[tuple[int, str]] = set()
    domains: dict[str, int] = {src_dom: 0}
    to_queue, canonical, out = [], None, []
    follow_norms: list[str] = []
    for dest, anchor, ddom, ndest, norm, rel, ltype, ex, internal, ok in items:
        is_new = norm in new_norms
        new_norms.discard(norm)  # a second spelling of the same URL is not new again
        domains[ddom] = domains.get(ddom, 0) + int(is_new)
        out.append((dest, ddom, anchor, rel, ltype, int(ex.get("nofollow", 0)), int(ex.get("sponsored", 0)), int(ex.get("ugc", 0)),
                    ex.get("img_alt"), ex.get("position"), ex.get("context")))
        seen_out.add((dest, anchor))
        if ltype == "canonical":
            canonical = dest
        if ltype == "html":
            host = core.hostname(dest)
            for tid, tdom in targets:
                if (tid, dest) not in found_backlinks and core.host_matches(host, tdom) \
                        and not core.host_matches(core.hostname(key_url), tdom):
                    core.record_backlink(conn, tid, key_url, dest, anchor, rel)
                    found_backlinks.add((tid, dest))
                    vlog.info("backlink verified %s -> %s", key_url, dest)
        if follow and internal and ok and ltype != "redirect" and depth + 1 <= cfg.max_depth and ddom in approved and norm:
            follow_norms.append(norm)
    new_links = 0
    if out:  # one row per (destination, anchor); new ones are inserted, existing ones refreshed and checked for "reappeared"
        c = [list(x) for x in zip(*out)]
        fresh = {(r["destination_url"], r["anchor_text"]) for r in conn.execute(
            "INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, rel_attributes, link_type,"
            " is_nofollow, is_sponsored, is_ugc, img_alt, position, context, source_domain)"
            " SELECT ?, d, dd, a, r, lt, nf, sp, ug, ia, p, cx, ? FROM unnest(?::text[], ?::text[], ?::text[], ?::text[], ?::text[],"
            " ?::int[], ?::int[], ?::int[], ?::text[], ?::text[], ?::text[]) AS t(d, dd, a, r, lt, nf, sp, ug, ia, p, cx)"
            " ON CONFLICT DO NOTHING RETURNING destination_url, anchor_text", (key_url, src_dom, *c)).fetchall()}
        new_links = len(fresh)
        old = [r for r in out if (r[0], r[2]) not in fresh]
        if old:
            c = [list(x) for x in zip(*old)]
            was = conn.execute(
                "WITH d AS (SELECT * FROM unnest(?::text[], ?::text[], ?::text[], ?::int[], ?::int[], ?::int[], ?::text[],"
                " ?::text[], ?::text[]) AS t(dest, anchor, rel, nf, sp, ug, ia, p, cx)),"
                " o AS (SELECT l.id, l.is_active, l.lost_at, d.* FROM outgoing_links l JOIN d ON l.destination_url=d.dest AND"
                " l.anchor_text=d.anchor WHERE l.source_url=? ORDER BY l.id FOR UPDATE OF l)"
                " UPDATE outgoing_links l SET last_seen=?, is_active=1, lost_at=NULL, rel_attributes=o.rel, is_nofollow=o.nf,"
                " is_sponsored=o.sp, is_ugc=o.ug, img_alt=o.ia, position=o.p, context=o.cx FROM o WHERE l.id=o.id"
                " RETURNING o.is_active AS was_active, o.lost_at AS was_lost, o.dest, o.anchor",
                (c[0], c[2], c[3], c[5], c[6], c[7], c[8], c[9], c[10], key_url, core.now())).fetchall()
            meta = {(r[0], r[2]): (r[1], r[4]) for r in old}  # (destination, anchor) -> (destination_domain, link_type)
            conn.executemany("INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type) VALUES (?,?,?,?,?)",
                             [(key_url, w["dest"], meta[(w["dest"], w["anchor"])][0], w["anchor"], "reappeared")
                              for w in was if meta[(w["dest"], w["anchor"])][1] == "html" and (not w["was_active"] or w["was_lost"])])
    if follow_norms:  # claim the internal URLs this page found so the caller can crawl them next (one statement; was one per link)
        to_queue = [(r["url"], r["crawl_depth"]) for r in conn.execute(
            "UPDATE discovered_urls SET crawl_status='crawling' WHERE normalized_url = ANY(?::text[]) AND crawl_status='pending'"
            " RETURNING url, crawl_depth", (follow_norms,)).fetchall()]
    conn.execute(  # one upsert per domain, in sorted order
        "INSERT INTO discovered_domains (domain, discovery_source, pages_discovered, last_seen)"
        " SELECT d, ?, n, ? FROM unnest(?::text[], ?::int[]) AS t(d, n) ORDER BY d ON CONFLICT (domain) DO UPDATE SET"
        " last_seen=EXCLUDED.last_seen, pages_discovered=discovered_domains.pages_discovered+EXCLUDED.pages_discovered",
        (key_url, core.now(), sorted(domains), [domains[d] for d in sorted(domains)]))
    # Same two-miss rule as backlinks, so the graph and the backlinks table never disagree: first successful miss sets lost_at
    # (suspected, still active); a second one deactivates. Seeing the link again clears lost_at (above). Rows are never deleted.
    miss = [r for r in conn.execute("SELECT id, destination_url, destination_domain, anchor_text, link_type, lost_at FROM outgoing_links"
                                    " WHERE source_url=? AND is_active=1 ORDER BY id FOR UPDATE", (key_url,)).fetchall()
            if (r["destination_url"], r["anchor_text"]) not in seen_out]
    if miss:
        conn.executemany("UPDATE outgoing_links SET is_active=?, lost_at=? WHERE id=?", [(int(not r["lost_at"]), core.now(), r["id"]) for r in miss])
        conn.executemany("INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type) VALUES (?,?,?,?,?)",
                         [(key_url, r["destination_url"], r["destination_domain"], r["anchor_text"], "lost" if r["lost_at"] else "suspected_removed")
                          for r in miss if r["link_type"] == "html"])
    suspected = False  # one miss is only a suspicion (soft blocks, layout changes); a second consecutive miss confirms
    for b in conn.execute("SELECT id, target_domain_id, destination_url, link_status FROM backlinks WHERE source_url=? AND"
                          " is_active=1 AND link_status IN ('verified','fetch_failed','suspected_removed') ORDER BY id FOR UPDATE", (key_url,)).fetchall():
        if (b["target_domain_id"], b["destination_url"]) in found_backlinks:
            continue
        if b["link_status"] == "suspected_removed":
            conn.execute("UPDATE backlinks SET is_active=0, link_status='removed', last_verified=? WHERE id=?", (core.now(), b["id"]))
            core.event(conn, b["id"], "confirmed_removed", "absent on two consecutive successful fetches")
            vlog.info("backlink removed %s -> %s", key_url, b["destination_url"])
        else:
            conn.execute("UPDATE backlinks SET link_status='suspected_removed', last_verified=? WHERE id=?", (core.now(), b["id"]))
            core.event(conn, b["id"], "suspected_removed", "absent after successful fetch; recheck scheduled")
            vlog.info("backlink suspected removed %s -> %s", key_url, b["destination_url"])
            suspected = True
    conn.execute(
        "UPDATE discovered_urls SET crawl_status='completed', http_status=?, content_type=?, last_crawled=?, crawl_error=NULL,"
        " retry_count=0, next_crawl_at=?, canonical_url=?, last_successful_crawl=?, content_hash=?, title=?, lease_owner=NULL,"
        " lease_expires=NULL WHERE normalized_url=?",
        (status, ctype, core.now(), core.later(days=1 if suspected else cfg.recrawl_days), canonical, core.now(),
         digest, title, core.normalize_url(key_url)))
    conn.execute("UPDATE discovered_domains SET last_crawled=? WHERE domain=?", (core.now(), src_dom))
    conn.execute("INSERT INTO crawl_history (source_url, crawl_started, crawl_completed, http_status, links_found,"
                 " new_links_found) VALUES (?,?,?,?,?,?)", (key_url, started, core.now(), status, len(links), new_links))
    return to_queue


def mark_failed(conn, url: str, error: str, status: int | None, cfg: Config, started: str, permanent: bool = False) -> None:
    """A failed fetch changes nothing about links or backlinks; it only schedules a retry with backoff."""
    norm = core.normalize_url(url)
    stale = stale_lease(conn, norm)
    FENCE.pop(norm or "", None)  # this claim's outcome is recorded either way
    if stale:
        log.warning("STALE LEASE: failure of %s ignored, another worker owns it now", url)
        return
    row = conn.execute("SELECT retry_count FROM discovered_urls WHERE normalized_url=?", (norm,)).fetchone()
    tries = (row["retry_count"] if row else 0) + 1
    if permanent:
        state, nxt = "failed", core.later(days=30)
    elif tries >= cfg.max_retries:
        state, nxt = "failed", core.later(days=1)
    else:
        state, nxt = "retry_pending", core.later(minutes=5 * 2 ** tries)
    conn.execute("UPDATE discovered_urls SET crawl_status=?, retry_count=?, next_crawl_at=?, crawl_error=?, http_status=?,"
                 " lease_owner=NULL, lease_expires=NULL WHERE normalized_url=?", (state, tries, nxt, error[:500], status, norm))
    for b in conn.execute("UPDATE backlinks SET link_status='fetch_failed' WHERE source_url=? AND link_status='verified' RETURNING id", (url,)).fetchall():
        core.event(conn, b["id"], "fetch_failed", error[:200])  # not removed: one failed fetch proves nothing about the link
    conn.execute("INSERT INTO crawl_history (source_url, crawl_started, crawl_completed, http_status, error_message)"
                 " VALUES (?,?,?,?,?)", (url, started, core.now(), status, error[:500]))


def concurrent_robots(crawler, timeout: float = 10) -> None:
    """Crawlee fetches robots.txt files one at a time under a single crawler-wide lock, so one slow or dead host stalls every
    other fetch (a round of 100 sites sat idle for minutes). This replaces that lookup with one fetch per site, run in parallel,
    each capped at `timeout`. An unreachable robots.txt counts as 'no rules', same as Crawlee's own behaviour.
    shortcut: overrides a private Crawlee method (crawlee is pinned in requirements.txt); re-check it when upgrading."""
    cache: dict[str, asyncio.Future] = {}

    async def get(url: str):
        p = urlsplit(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in cache:
            cache[origin] = asyncio.ensure_future(asyncio.wait_for(RobotsTxtFile.find(url, crawler._http_client), timeout))
        try:
            return await asyncio.shield(cache[origin])
        except Exception:  # noqa: BLE001 - timeout / network error: crawl as if there were no robots.txt
            return None

    crawler._get_robots_txt_file_for_url = get


class ResponseTooLarge(ValueError):
    """The body exceeded Config.max_response_bytes while it was being downloaded (the download is cut off, not just flagged)."""


class _Buffered:
    """A fully read, size-checked response with the surface that Crawlee's redirect loop and adapter use."""

    def __init__(self, response, body: bytes) -> None:
        self._r, self.content, self.is_closed, self.is_stream_consumed = response, body, True, True

    @property
    def url(self) -> str:
        return core.test_unroute(str(self._r.url))

    def __getattr__(self, name):
        return getattr(self._r, name)


class _Guarded:
    """Wraps impit's AsyncClient. Crawlee follows redirects hop by hop, calling this for the first request AND for every hop, so
    each destination is validated right before it is sent. Bodies are streamed with a hard byte cap."""

    def __init__(self, inner, max_bytes: int) -> None:
        self._inner, self._max = inner, max_bytes

    async def request(self, *, url: str, headers=None, stream: bool = False, **kw):
        why = await asyncio.to_thread(core.blocked_reason, url)
        if why:
            log.warning("BLOCKED %s | %s", url, why)
            raise core.UnsafeURL(f"blocked before fetch ({why}): {url}")
        real, extra = core.test_route(url)
        resp = await self._inner.request(url=real, headers={**(headers or {}), **extra} or None, stream=True, **kw)
        if stream:
            return resp
        try:
            if int(resp.headers.get("content-length") or 0) > self._max:
                raise ResponseTooLarge(f"Content-Length over {self._max} bytes")
            chunks, n = [], 0
            async for chunk in resp.aiter_bytes():
                n += len(chunk)
                if n > self._max:
                    raise ResponseTooLarge(f"body over {self._max} bytes")
                chunks.append(chunk)
        finally:
            resp.close()
        return _Buffered(resp, b"".join(chunks))


class SafeImpitClient(ImpitHttpClient):
    """ImpitHttpClient whose every outgoing request (pages, redirect hops, robots.txt) goes through _Guarded.
    shortcut: overrides the private _get_client / relies on Crawlee's hop-by-hop redirect loop (crawlee is pinned in requirements.txt); re-check on upgrade."""

    def __init__(self, *, max_response_bytes: int, **kw) -> None:
        super().__init__(**kw)
        self._max_bytes, self._guards = max_response_bytes, {}

    def _get_client(self, proxy_url):
        inner = super()._get_client(egress_proxy.ensure())  # every connection is re-validated and pinned by the egress proxy
        return self._guards.setdefault(id(inner), _Guarded(inner, self._max_bytes))

    async def crawl(self, request, **kw):
        try:
            return await super().crawl(request, **kw)
        except (core.UnsafeURL, ResponseTooLarge):
            request.no_retry = True  # the same URL will be refused / too large again
            raise


def needs_js(soup) -> bool:
    """Cheap static heuristic: a near-empty page shell (few links, JS app root / noscript notice / script-heavy with little text)."""
    if soup.body is None or len(soup.find_all("a", href=True)) >= 3:
        return False
    text = len(soup.body.get_text(" ", strip=True))
    shell = soup.find(id=re.compile(r"^(root|app|__next|__nuxt|svelte)$")) is not None
    noscript = any("javascript" in n.get_text().lower() for n in soup.find_all("noscript"))
    return noscript or (shell and text < 300) or (len(soup.find_all("script")) >= 3 and text < 200)


class FallbackSoupParser(BeautifulSoupParser):
    """crawlee's lxml parse with ONE narrow safety net. bs4's lxml builder assumes an attribute name that starts with '{' contains '}'
    (Clark notation, `namespace, attr = tag[1:].split("}", 1)`); markup with such an attribute name ends in `ValueError: not enough values to
    unpack` for the whole page, which then burned its retries and failed. Only that exact error is caught and the SAME bytes are re-parsed with the
    lenient html.parser, so the page and its links are kept; every other error (and every other parser failure) is raised unchanged."""

    def __init__(self, parser="lxml"):
        super().__init__(parser)
        self._lenient = BeautifulSoupParser("html.parser")

    def _parse_body(self, body, content_type):
        try:
            return super()._parse_body(body, content_type)
        except ValueError as e:
            if not str(e).startswith("not enough values to unpack"):
                raise
            log.warning("lxml/bs4 rejected the markup (%s); re-parsed with html.parser", e)
            return self._lenient._parse_body(body, content_type)


async def crawl_round(cfg: Config, job_id: str | None, work: list[tuple[str, int]], approved: set[str], follow: bool,
                      limit: int) -> None:
    """Run one Crawlee crawl over `work` [(url, depth)]; internal links found are queued dynamically if `follow`.
    Static HTML first; pages that look JavaScript-rendered are re-fetched in a browser (Playwright) in a second pass of the
    same round and persisted through the same process_page."""
    if not work or limit <= 0:
        return
    slots: dict[str, float] = {}
    slot_lock = asyncio.Lock()
    started: dict[str, str] = {}
    deferred: dict[str, tuple] = {}  # url -> (soup, base, status, ctype, depth, t0): static fetch awaiting its rendered version
    done_render: set[str] = set()
    finished: set[str] = set()  # URLs whose outcome is recorded (persisted, failed or skipped)

    def req(url: str, depth: int) -> Request:
        return Request.from_url(url, unique_key=core.normalize_url(url) or url, user_data={"depth": depth})

    def valid_reqs(items) -> list[Request]:
        """Requests crawlee accepts. Junk URLs already stored (e.g. "a.com|b.com") are marked skipped, so one bad URL never fails the batch."""
        ok, bad = [], []
        for u, d in items:
            try:
                r = req(u, d)
                URL(u).host  # crawlee's per-host throttler does exactly this in add_requests; a bad host (e.g. soft hyphens) raised ValueError for the WHOLE batch
                ok.append(r)
            except Exception:  # noqa: BLE001 - pydantic ValidationError, yarl ValueError and friends
                bad.append(u)
        if bad:
            with core.connect() as c:
                c.executemany("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='invalid_url', lease_owner=NULL, lease_expires=NULL"
                              " WHERE url=? AND crawl_status IN ('pending','crawling','recrawling')", [(u,) for u in bad])
            finished.update(bad)
            log.warning("skipped %d invalid URL(s), e.g. %s", len(bad), bad[0][:120])
        return ok

    async def slot_wait(url: str) -> None:  # one shared per-domain schedule for static fetches, browser navigations and redirect hops
        dom = core.registrable_domain(core.hostname(url))
        async with slot_lock:
            wait = slots.get(dom, 0) - time.monotonic()
            slots[dom] = max(time.monotonic(), slots.get(dom, 0)) + cfg.domain_delay
        if wait > 0:
            await asyncio.sleep(wait)

    async def polite(context) -> None:  # SSRF check before the request, then per-domain rate limit
        why = await asyncio.to_thread(core.blocked_reason, context.request.url)
        if why:
            context.request.no_retry = True
            raise core.UnsafeURL(f"blocked before fetch ({why}): {context.request.url}")
        await slot_wait(context.request.url)
        started[context.request.url] = core.now()
        if context.request.url not in _T0:
            HTTP["started"] += 1
        _T0[context.request.url] = time.monotonic()

    # follow=False rounds (autonomous, celery, recrawl) need nothing back from the write, so finished pages go to a bounded queue and
    # writer tasks persist them while the crawler keeps fetching. follow=True needs the returned URLs, so it stays inline.
    writes: asyncio.Queue | None = None if follow else asyncio.Queue(maxsize=cfg.concurrency * 4)  # bounded: soups held in memory

    async def writer() -> None:
        while True:
            item = await writes.get()
            try:
                await persist_page(*item)
            except Exception as e:  # noqa: BLE001 - same outcome the crawler handler's failure path gave before
                log.warning("persist FAILED %s | %r", item[0], e)
                def fail():
                    with core.connect() as c:
                        mark_failed(c, item[0], repr(e), None, cfg, item[6])
                await asyncio.to_thread(fail)
                finished.add(item[0])
                _job_add(job_id, "pages_failed")
            finally:
                writes.task_done()

    async def persist_page(url, base, depth, soup, status, ctype, t0, facts=None):
        def persist():
            def once():
                with core.connect() as c:  # one transaction per page; a deadlock victim is rolled back whole and retried
                    return process_page(c, url, base, depth, soup, status, ctype, cfg, approved, follow, t0, facts)
            return core.retry_locked(once)
        try:
            async with _persist_lock(core.registrable_domain(core.hostname(url))):  # waiting here holds no thread or connection
                nxt = await asyncio.to_thread(persist)
        finally:
            FENCE.pop(core.normalize_url(url) or "", None)
        finished.add(url)
        _job_add(job_id, "pages_crawled")
        def last():
            with core.connect() as c:
                return c.execute("SELECT links_found, new_links_found FROM crawl_history WHERE source_url=? ORDER BY id DESC LIMIT 1", (url,)).fetchone()
        n = await asyncio.to_thread(last)
        xlog.info("crawled %s | status %s | links %s (new %s) | queued %d", url, status, n["links_found"], n["new_links_found"], len(nxt))
        return nxt

    async def handle(context, soup, status, ctype, defer: bool = False) -> None:
        url, base = context.request.url, context.request.loaded_url or context.request.url
        _http_done(url)  # response received (body parsing and database work after this are not HTTP time)
        t0 = started.get(url, core.now())
        restricted = status in ACCESS_RESTRICTED or (status is not None and status < 400 and is_challenge(soup))
        if (status is not None and status >= 400) or restricted:  # ignored by Crawlee so it does not retry; recorded as a permanent failure
            def fail():  # an access restriction (HTTP 401/403/451 or a challenge page) is never bypassed, and a challenge page is never a "completed" crawl: no link of it was seen
                with core.connect() as c:
                    mark_failed(c, url, f"access_restricted: HTTP {status}" if restricted else f"HTTP {status}", status, cfg, t0, permanent=True)
            await asyncio.to_thread(fail)
            finished.add(url)
            _job_add(job_id, "pages_failed")
            log.info("skipped %s | HTTP %s", url, status)
            return
        if base != url and not await asyncio.to_thread(core.is_safe_url, base):
            raise core.UnsafeURL(f"redirect to unsafe URL blocked: {base}")
        if ctype and "html" not in ctype.lower():
            with core.connect() as c:
                c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='not_html', content_type=?, http_status=?,"
                          " lease_owner=NULL, lease_expires=NULL WHERE normalized_url=?", (ctype, status, core.normalize_url(url)))
            finished.add(url)
            return
        depth = context.request.user_data["depth"]
        if defer and cfg.js_fallback and len(deferred) < cfg.max_js_pages and await asyncio.to_thread(needs_js, soup):
            deferred[url] = (soup, base, status, ctype, depth, t0)  # persisted after the rendered pass (or, if that fails, as fetched)
            return
        facts = await asyncio.to_thread(page_facts, soup, base)  # DOM traversal off the event loop
        if writes is not None:
            await writes.put((url, base, depth, soup, status, ctype, t0, facts))  # returns at once unless the queue is full
            return
        nxt = await persist_page(url, base, depth, soup, status, ctype, t0, facts)
        if nxt:
            await context.add_requests(valid_reqs(nxt))

    async def failed(context, error: Exception) -> None:
        log.warning("FAILED %s | %s", context.request.url, error)
        _http_done(context.request.url, failed=True)
        permanent = isinstance(error, (core.UnsafeURL, ResponseTooLarge))  # blocked / oversized: retrying cannot help
        def fail():
            with core.connect() as c:
                mark_failed(c, context.request.url, f"{type(error).__name__}: {error}" if permanent else repr(error),
                            getattr(error, "status_code", None) or (429 if isinstance(error, RequestThrottledError) else None), cfg, started.get(context.request.url, core.now()), permanent=permanent)
        await asyncio.to_thread(fail)
        finished.add(context.request.url)
        _job_add(job_id, "pages_failed")

    def common(**extra) -> dict:
        return dict(
            max_requests_per_crawl=limit, max_request_retries=cfg.max_retries,
            # fresh per crawler: the default ./storage is shared by every process and purged on start; only temporary run statistics live here
            storage_client=MemoryStorageClient(),
            concurrency_settings=ConcurrencySettings(desired_concurrency=cfg.concurrency, max_concurrency=cfg.concurrency),
            request_handler_timeout=timedelta(seconds=cfg.timeout * 2 + PERSIST_QUEUE_SECONDS), respect_robots_txt_file=True,
            ignore_http_error_status_codes=PERMANENT_HTTP, http_client=SafeImpitClient(
                max_response_bytes=cfg.max_response_bytes, timeout=cfg.timeout, browser=None, max_redirects=cfg.max_redirects,
                headers={"User-Agent": core.USER_AGENT}), **extra)

    async def run(crawler, items) -> None:
        @crawler.on_skipped_request
        async def skipped(url: str, reason: SkippedReason) -> None:
            if reason != "robots_txt":
                return
            # terminal state: without this the URL stays 'crawling', is released to 'pending' and is claimed again forever
            with core.connect() as c:
                c.execute("UPDATE discovered_urls SET crawl_status='skipped', skip_reason='robots_txt', lease_owner=NULL, lease_expires=NULL"
                          " WHERE normalized_url=? AND crawl_status IN ('crawling','recrawling')", (core.normalize_url(url),))
            finished.add(url)
            log.info("skipped %s | robots.txt", url)

        crawler.pre_navigation_hook(polite)
        concurrent_robots(crawler)
        await asyncio.gather(*(crawler._get_robots_txt_file_for_url(u) for u, _ in items))  # all sites at once, before add_requests checks them one by one
        workers = [asyncio.create_task(writer()) for _ in range(WRITERS)] if writes is not None else []
        interrupted, run_crawler = False, crawler._run_crawler

        async def tracked() -> None:  # crawlee turns Ctrl+C into "pause this round" and swallows it: the process would then never stop
            nonlocal interrupted
            try:
                await run_crawler()
            except asyncio.CancelledError:
                interrupted = True
                raise
        crawler._run_crawler = tracked
        try:
            try:
                await crawler.run(valid_reqs(items))
            except PersistentRateLimitError as e:  # a host answered 429 for the whole stall window: reschedule what never finished
                log.warning("rate limited: %s", e)
                if writes is not None:
                    await writes.join()  # queued pages count as finished, not as unreached
                def resched():
                    with core.connect() as c:
                        for u, _ in items:
                            if u not in finished:
                                mark_failed(c, u, "HTTP 429 (persistent rate limit)", 429, cfg, started.get(u, core.now()))
                await asyncio.to_thread(resched)
            if writes is not None:
                await writes.join()  # callers release leases right after the round, so every queued page must be stored first
            if interrupted:  # pages already fetched are stored above; the caller's finally returns the rest of the leases
                if (t := asyncio.current_task()) is not None and t.cancelling():
                    raise asyncio.CancelledError  # a real cancellation (task time limit) that crawlee swallowed: keep it a cancellation
                raise KeyboardInterrupt
        finally:  # on cancellation the queue is abandoned; leases expire / the callers' release() returns the URLs
            for w in workers:
                w.cancel()

    async def queue(*, alias=None, storage_client=None, configuration=None):  # also the opener for the per-host sub-queues
        # always a fresh alias: the throttler's own per-host aliases are fixed names, and a reused queue remembers URLs as already handled
        return await RequestQueue.open(alias=uuid.uuid4().hex, storage_client=MemoryStorageClient())

    async def queue_for(items):
        """Per-host throttling manager: an HTTP 429 holds that host back with exponential backoff (cfg.rate_limit_base .. 60s) and
        robots.txt Crawl-delay is honoured; other hosts keep going. A host that stays rate limited for cfg.rate_limit_stall seconds
        ends the round (caught below) and its unfinished URLs are rescheduled."""
        hosts = sorted({core.hostname(u) for u, _ in items if core.hostname(u)})
        return ThrottlingRequestManager(await queue(), domains=hosts, request_manager_opener=queue,
                                        base_delay=timedelta(seconds=cfg.rate_limit_base), max_delay=timedelta(seconds=60),
                                        max_domain_stall=timedelta(seconds=cfg.rate_limit_stall))

    async def playwright_crawler(handler_failed, items):
        crawler = PlaywrightCrawler(
            request_manager=await queue(),  # plain queue: the throttling manager never dispatched requests to Playwright (verified)
            browser_new_context_options={"user_agent": core.USER_AGENT},
            # all browser traffic through the egress proxy; <-loopback> stops Chromium from bypassing it for 127.0.0.1 / localhost
            browser_launch_options={"proxy": {"server": egress_proxy.ensure()}, "args": ["--proxy-bypass-list=<-loopback>"]},
            navigation_timeout=timedelta(seconds=cfg.timeout), **common())
        safe: dict[str, bool] = {}

        @crawler.pre_navigation_hook
        async def guard(context: PlaywrightCrawlingContext) -> None:  # every browser request, including redirects and sub-resources
            async def route(r) -> None:
                u = r.request.url
                if not u.startswith(("http://", "https://")):
                    return await r.continue_()
                host = core.hostname(u)
                if host not in safe:
                    safe[host] = await asyncio.to_thread(core.is_safe_url, u)
                if safe[host]:
                    return await r.continue_()
                log.warning("BLOCKED (browser) %s", u)
                await r.abort("blockedbyclient")
            await context.page.route("**/*", route)

        @crawler.router.default_handler
        async def pw(context: PlaywrightCrawlingContext) -> None:
            try:  # let scripts add links; busy pages never go idle, so cap the wait
                await context.page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:  # noqa: BLE001
                pass
            html = await context.page.content()
            resp = context.response
            await handle(context, await asyncio.to_thread(BeautifulSoup, html, "html.parser"), resp.status if resp else None,
                         (resp.headers.get("content-type") if resp else None))
            done_render.add(context.request.url)
        crawler.failed_request_handler(handler_failed)
        return crawler

    if cfg.use_playwright:  # everything rendered (explicit opt-in)
        await run(await playwright_crawler(failed, work), work)
        return

    crawler = BeautifulSoupCrawler(request_manager=await queue_for(work), navigation_timeout=timedelta(seconds=cfg.timeout), **common())
    crawler._parser = FallbackSoupParser()  # shortcut: replaces crawlee's private parser attribute (crawlee is pinned); same lxml parse plus the narrow fallback above

    @crawler.router.default_handler
    async def bs(context: BeautifulSoupCrawlingContext) -> None:
        r = context.http_response
        await handle(context, context.soup, r.status_code, r.headers.get("content-type"), defer=True)

    crawler.failed_request_handler(failed)
    await run(crawler, work)
    if not deferred:
        return
    log.info("rendering %d page(s) that look JavaScript-driven", len(deferred))

    async def render_failed(context, error: Exception) -> None:  # browser could not render it: keep the static result, never lose the page
        log.warning("render failed for %s (%s); using the static HTML", context.request.url, error)
        _http_done(context.request.url, failed=True)
    try:
        items = [(u, v[4]) for u, v in deferred.items()]
        await run(await playwright_crawler(render_failed, items), items)
    except Exception:  # noqa: BLE001 - e.g. browsers not installed: fall back to static for all
        log.exception("Playwright pass failed; persisting static HTML")
    for url, (soup, base, status, ctype, depth, t0) in deferred.items():
        if url not in done_render:
            await persist_page(url, base, depth, soup, status, ctype, t0)


async def _guard(request: httpx.Request) -> None:
    why = await asyncio.to_thread(core.blocked_reason, str(request.url))
    if why:
        log.warning("BLOCKED %s | %s", request.url, why)
        raise httpx.RequestError(f"unsafe URL blocked ({why}): {request.url}")
    real, extra = core.test_route(str(request.url))  # tests only
    if extra:
        request.url = httpx.URL(real)
        request.headers["Host"] = extra["Host"]


async def admit_domain(cfg: Config, job_id: str | None, domain: str, seed_url: str | None = None) -> bool:
    """Approve a domain for crawling: seed its start URL and (optionally) its sitemap URLs."""
    with core.connect() as c:
        row = c.execute("SELECT url FROM discovered_urls WHERE domain=? ORDER BY id LIMIT 1", (domain,)).fetchone()
    origin = "{0.scheme}://{0.netloc}".format(urlsplit(seed_url or (row["url"] if row else f"https://{domain}/")))
    if not await asyncio.to_thread(core.is_safe_url, origin + "/"):
        with core.connect() as c:
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain) VALUES (?)", (domain,))
            c.execute("UPDATE discovered_domains SET discovery_status='blocked' WHERE domain=?", (domain,))
        return False
    sitemap = []
    if cfg.enable_sitemap_discovery:
        async with httpx.AsyncClient(timeout=cfg.timeout, follow_redirects=True, headers={"User-Agent": core.USER_AGENT},
                                     proxy=egress_proxy.ensure(), event_hooks={"request": [_guard]}) as client:  # the hook also runs for every redirect hop
            sitemap = await core.discover_sitemaps(client, origin + "/")
    def save() -> None:
        with core.connect() as c:
            c.execute("INSERT OR IGNORE INTO discovered_domains (domain, discovery_source) VALUES (?, 'seed')", (domain,))
            c.execute("UPDATE discovered_domains SET discovery_status='admitted' WHERE domain=?", (domain,))
            core.save_url(c, seed_url or origin + "/", None, "seed", 0, True)
            for u, mod in sitemap:
                if core.registrable_domain(core.hostname(u)) == domain:
                    core.save_url(c, u, origin + "/sitemap.xml", "sitemap", 1, True, "pending" if core.crawlable(u) else "skipped", mod)
    await asyncio.to_thread(save)  # up to 5000 urls: 2-3s of database work that must not block crawling
    _job_add(job_id, "domains_admitted")
    log.info("admitted %s (%d sitemap urls)", domain, len(sitemap))
    return True


def _pending(cfg: Config, approved: set[str], limit: int) -> list[tuple[str, int]]:
    with core.connect() as c:
        marks = ",".join("?" * len(approved)) or "''"
        rows = c.execute(f"SELECT url, crawl_depth FROM discovered_urls WHERE crawl_status='pending' AND domain IN ({marks})"
                         " AND crawl_depth<=? ORDER BY crawl_depth, id LIMIT ?", (*approved, cfg.max_depth, limit)).fetchall()
        work = [(r["url"], r["crawl_depth"]) for r in rows]
        for u, _ in work:
            c.execute("UPDATE discovered_urls SET crawl_status='crawling' WHERE url=?", (u,))
    return work


def _approved() -> set[str]:
    with core.connect() as c:
        return {r["domain"] for r in c.execute("SELECT domain FROM discovered_domains WHERE discovery_status='admitted'")}


async def run_crawl(cfg: Config, source_urls: list[str], job_id: str | None = None, only: set[str] | None = None) -> None:
    """Seed -> crawl approved domains -> admit new external domains (bounded) -> repeat. Resumes from SQLite.
    `only`: crawl just these domains instead of every admitted one (a focused crawl that leaves the global backlog alone)."""
    with core.connect() as c:
        if cfg.target_domain:
            core.add_target(c, cfg.target_domain)
            core.match_target_from_graph(c, cfg.target_domain)  # results already in the graph show up immediately
    log.info("job %s started | target=%s | %d seed urls | max_pages=%d max_depth=%d", job_id, cfg.target_domain, len(source_urls), cfg.max_pages, cfg.max_depth)
    for u in source_urls:
        if core.normalize_url(u):
            await admit_domain(cfg, job_id, core.registrable_domain(core.hostname(u)), u)
    done = 0
    while done < cfg.max_pages:
        approved = set(only) if only is not None else _approved()
        work = _pending(cfg, approved, cfg.max_pages - done)
        if work:
            with core.connect() as c:
                before = c.execute("SELECT COUNT(*) n FROM crawl_history").fetchone()["n"]
            await crawl_round(cfg, job_id, work, approved, True, cfg.max_pages - done)
            with core.connect() as c:
                done += c.execute("SELECT COUNT(*) n FROM crawl_history").fetchone()["n"] - before
                c.execute("UPDATE discovered_urls SET crawl_status='pending' WHERE crawl_status='crawling'")  # cut off by the page cap
            if done >= cfg.max_pages:
                break
            continue
        if not cfg.enable_external_domain_discovery or len(approved) >= cfg.max_domains:
            break
        with core.connect() as c:
            skip = cfg.target_domain and core.registrable_domain(cfg.target_domain)
            cand = [r["domain"] for r in c.execute(
                "SELECT domain FROM discovered_domains WHERE discovery_status='discovered' AND domain!=? ORDER BY priority, id"
                " LIMIT ?", (skip or "", min(cfg.admit_batch, cfg.max_domains - len(approved))))]
        if not cand:
            break
        for d in cand:
            await admit_domain(cfg, job_id, d)
    finalize = getattr(getattr(cfg, "classifier", None), "finalize", None)
    if finalize is not None:
        try:
            finalize()
        except Exception:
            log.exception("end-of-crawl classification failed")
    log.info("job %s crawl loop finished | pages crawled this run: %d", job_id, done)
    with core.connect() as c:
        c.execute("UPDATE discovered_domains SET discovery_status='crawled' WHERE discovery_status='admitted' AND domain NOT IN"
                  " (SELECT domain FROM discovered_urls WHERE crawl_status IN ('pending','crawling'))")


def due_urls(limit: int = 1000) -> list[tuple[str, int]]:
    """Successfully crawled pages whose recrawl time has come. failed / retry_pending are never recrawl work:
    retry_pending is retried by the forward crawler (claim_urls), failed is terminal."""
    with core.connect() as c:
        rows = c.execute("SELECT url, crawl_depth FROM discovered_urls WHERE crawl_status='completed'"
                         " AND next_crawl_at IS NOT NULL AND next_crawl_at<=? ORDER BY next_crawl_at LIMIT ?",
                         (core.now(), limit)).fetchall()
    return [(r["url"], r["crawl_depth"]) for r in rows]


def claim_recrawl(limit: int, urls: list[str] | None = None, owner: str = "recrawl", lease_seconds: int = 900) -> list[tuple[str, int, str]]:
    """Atomically move due pages (or an explicit manual list, any non-skipped state) to 'recrawling' under a lease.
    Returns (url, depth, previous_status) so unreached pages go back exactly where they were."""
    with core.connect() as c:
        if urls is not None:
            norms = [n for n in map(core.normalize_url, urls) if n]
            rows = c.execute(f"SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE normalized_url IN"
                             f" ({','.join('?' * len(norms)) or 'NULL'}) AND crawl_status IN ('completed','retry_pending','failed')"
                             " FOR UPDATE SKIP LOCKED", norms).fetchall()
        else:
            rows = c.execute("SELECT id, url, crawl_depth, crawl_status FROM discovered_urls WHERE crawl_status='completed'"
                             " AND next_crawl_at IS NOT NULL AND next_crawl_at<=? ORDER BY next_crawl_at LIMIT ? FOR UPDATE SKIP LOCKED",
                             (core.now(), limit)).fetchall()
        c.executemany("UPDATE discovered_urls SET crawl_status='recrawling', lease_owner=?, lease_expires=? WHERE id=?",
                      [(owner, core.later(seconds=lease_seconds), r["id"]) for r in rows])
    return [(r["url"], r["crawl_depth"], r["crawl_status"]) for r in rows]


async def recrawl_due(cfg: Config, job_id: str | None = None, urls: list[str] | None = None, limit: int | None = None,
                      owner: str = "recrawl", lease_seconds: int = 900) -> int:
    """Re-fetch up to `limit` due pages (or the given URLs). Crawlee never revisits completed requests on its own, so each
    recrawl uses a fresh queue fed from SQLite. Success -> completed, failure -> mark_failed (retry_pending/failed);
    pages the round never reached return to their previous status. Newly found internal URLs are stored as pending."""
    claimed = claim_recrawl(limit or cfg.max_pages, urls, owner, lease_seconds)
    try:
        await crawl_round(cfg, job_id, [(u, d) for u, d, _ in claimed], set(), False, max(len(claimed), 1))
    finally:  # also runs on cancellation: nothing stays 'recrawling' because of a shutdown
        with core.connect() as c:
            c.executemany("UPDATE discovered_urls SET crawl_status=?, lease_owner=NULL, lease_expires=NULL WHERE url=? AND crawl_status='recrawling'",
                          [(prev, u) for u, _, prev in claimed])
    return len(claimed)


async def scheduler_loop(cfg: Config, every: int = 3600, stop: asyncio.Event | None = None) -> None:
    """Continuous recrawling: wake up regularly and refetch whatever is due."""
    while not (stop and stop.is_set()):
        try:
            await recrawl_due(cfg)
        except Exception:  # noqa: BLE001 - keep the scheduler alive
            log.exception("recrawl failed")
        await asyncio.sleep(every)


def config_dict(cfg: Config) -> dict:
    return asdict(cfg)

"""Phase 1: where on the page a link sits (outgoing_links.position). Unit tests on HTML fixtures, plus one end-to-end test that the value reaches PostgreSQL.

The four regression fixtures use the ancestor chains of the real pages named in the link-extraction audit (fetched and inspected on 2026-11-25),
trimmed to the structural skeleton: tags, classes, ids and roles are the real ones, page text is shortened.
"""

import asyncio

import pytest
from bs4 import BeautifulSoup

import crawler_engine as engine
from test_crawler import SITES, cfg, env, port, rows  # noqa: F401 - fixtures


def place(html: str, parser: str = "lxml") -> dict[str, tuple[str, str | None]]:
    """{href: (position, local_region)} for every <a href> of the page."""
    out = engine.extract_links(BeautifulSoup(html, parser), "https://src.test/p")
    return {d: (e["position"], e["local_region"]) for d, _, _, t, e in out if t == "html"}


def only(html: str, parser: str = "lxml") -> str:
    got = place(html, parser)
    assert len(got) == 1, got
    return next(iter(got.values()))[0]


X = '<a href="https://example.com/">Example</a>'


# 1-3. semantic global header / navigation / footer
def test_semantic_global_header():
    assert only(f"<body><header>{X}</header><main><article><p>text</p></article></main></body>") == "header"


def test_semantic_global_navigation():
    assert only(f"<body><nav>{X}</nav><main><p>text</p></main></body>") == "nav"


def test_semantic_global_footer():
    assert only(f"<body><main><article><p>text</p></article></main><footer>{X}</footer></body>") == "footer"


# 4. sidebar
def test_semantic_sidebar():
    assert only(f"<body><main><article><p>text</p></article><aside>{X}</aside></main></body>") == "aside"


def test_sidebar_by_class():
    assert only(f'<body><div class="widget-area"><ul><li>{X}</li></ul></div></body>') == "aside"
    assert only(f'<body><div id="sidebar-container">{X}</div></body>') == "aside"


# 5-6. main content / article body
def test_main_content_and_article_body():
    got = place(f'<body><header><nav><a href="/n">n</a></nav></header><main><p>Read <a href="/c">c</a></p></main></body>')
    assert {k.rsplit("/", 1)[1]: v[0] for k, v in got.items()} == {"n": "nav", "c": "content"}
    assert only(f"<body><main><p>Read {X} now</p></main></body>") == "content"
    assert only(f"<body><main><article><p>Read {X} now</p></article></main></body>") == "content"
    assert only(f'<body><div class="entry-content"><p>{X}</p></div></body>') == "content"


# 7-9. article-local header / footer / table of contents are content, and the local region is remembered
def test_article_local_header_footer_and_toc_are_content():
    assert place(f"<main><article><header>{X}</header><p>text</p></article></main>") == {"https://example.com/": ("content", "article_header")}
    assert place(f"<main><article><p>text</p><footer>{X}</footer></article></main>") == {"https://example.com/": ("content", "article_footer")}
    assert place(f'<main><article><nav aria-label="Table of contents">{X}</nav></article></main>') == {"https://example.com/": ("content", "article_nav")}
    assert place(f'<main><article><aside>{X}</aside></article></main>') == {"https://example.com/": ("content", "article_aside")}


def test_toc_and_breadcrumb_outside_an_article_are_not_global_navigation():
    assert only(f'<body><div><nav class="toc">{X}</nav></div></body>') != "nav"
    assert only(f'<body><nav aria-label="Breadcrumb">{X}</nav><p>x</p></body>') != "nav"
    assert only(f'<body><div class="navigation pagination"><div class="nav-links">{X}</div></div></body>') != "nav"


# 10-11. footer by class and by id, with and without semantic tags
def test_footer_identified_by_class():
    assert only(f'<body><div class="website-wrapper"><div class="footer-container"><div class="footer-links">{X}</div></div></div></body>') == "footer"
    assert only(f'<body><div class="site-footer"><p>Designed by Codigix</p>{X}</div></body>') == "footer"
    assert only(f'<body><div class="bottom-footer"><p>Designed by {X}</p></div></body>') == "footer"
    assert only(f'<body><div class="siteFooter">{X}</div></body>') == "footer"  # camelCase


def test_footer_identified_by_id():
    assert only(f'<div id="site-footer"><p>Designed by Codigix</p>{X}</div>') == "footer"


# 12-14. ARIA roles
def test_header_by_aria_role():
    assert only(f'<body><div role="banner">{X}</div><main><p>t</p></main></body>') == "header"


def test_footer_by_aria_role():
    assert only(f'<body><main><p>t</p></main><div role="contentinfo">{X}</div></body>') == "footer"


def test_navigation_by_aria_role():
    assert only(f'<body><div role="navigation"><ul><li>{X}</li></ul></div></body>') == "nav"
    assert only(f'<body><div role="complementary">{X}</div></body>') == "aside"


# 15. nested header and navigation
def test_nested_header_and_navigation_is_navigation():
    assert only(f'<body><header class="site-header"><nav class="primary-nav">{X}</nav></header><main><article><p>x</p></article></main></body>') == "nav"
    assert only(f"<body><header><nav>{X}</nav></header></body>") == "nav"
    assert only(f"<body><footer><nav>{X}</nav></footer></body>") == "footer"  # a menu inside the footer is a footer link
    assert only(f"<body><aside><nav>{X}</nav></aside></body>") == "aside"


# 16. conflicting evidence
def test_conflicting_signals():
    assert only(f'<body><footer class="site-header">{X}</footer></body>') == "unknown"  # one element contradicting itself
    assert only(f"<body><header><footer>{X}</footer></header></body>") == "footer"  # nested regions: the footer inside the header
    assert only(f'<body><div class="entry-footer">{X}</div></body>') == "unknown"  # a local word never makes a site region


def test_local_words_and_bare_words_are_not_regions():
    for cls in ("card-header", "entry-header", "page-header", "header", "menu", "section-header", "post-footer", "modal-header", "header-title"):
        assert only(f'<body><div class="{cls}"><div>{X}</div></div></body>') == "unknown", cls


def test_body_and_html_classes_are_not_evidence():
    assert only(f'<html class="has-footer"><body class="ehf-header ehf-footer site-footer"><main><p>{X}</p></main></body></html>') == "content"


# 17. missing / malformed HTML
def test_malformed_html():
    assert only(f"<footer><p>unclosed {X}", "lxml") == "footer"
    assert only(f"<footer><p>unclosed {X}", "html.parser") == "footer"
    assert only(f"<div><header><nav><a href='https://example.com/'>x</nav></header>", "html.parser") == "nav"
    assert place("<a>no href</a><a href=''>empty</a><p>x</p>") == {}
    assert place("") == {}


# 18. empty anchor with an image
def test_image_link_with_empty_anchor_keeps_its_data():
    out = engine.extract_links(BeautifulSoup('<footer><a href="/logo"><img src="l.png" alt="Logo"></a></footer>', "lxml"), "https://src.test/p")
    dest, anchor, rel, kind, extra = out[0]
    assert (dest, anchor, rel, kind) == ("https://src.test/logo", "", None, "html")
    assert extra["img_alt"] == "Logo" and extra["position"] == "footer"


# 19. several sections of one page
def test_multiple_links_in_different_sections():
    html = ('<body><header class="site-header"><a href="/h">h</a></header><nav><a href="/n">n</a></nav>'
            '<main><article><p><a href="/c">c</a></p></article><aside class="sidebar"><a href="/s">s</a></aside></main>'
            '<div id="footer"><a href="/f">f</a></div></body>')
    got = {k.rsplit("/", 1)[1]: v[0] for k, v in place(html).items()}
    assert got == {"h": "header", "n": "nav", "c": "content", "s": "aside", "f": "footer"}


# 20. no structural evidence
def test_unknown_without_structural_signal():
    assert only(f"<body><div><span>{X}</span></div></body>") == "unknown"
    assert only(f"<body><div><ul><li>{X}</li></ul></div></body>") == "unknown"  # an unmarked menu is not claimed as content
    assert only(f"<body><p>Read {X}</p></body>") == "content"  # a link inside prose, no landmarks (existing behaviour)


# a <main> that wraps the whole page (header and footer inside it) gives no article locality
def test_main_that_wraps_the_page_is_not_a_content_container():
    html = f"<body><main><header>{X}</header><p>t</p><footer><a href='https://f.test/'>f</a></footer></main></body>"
    got = {k: v[0] for k, v in place(html).items()}
    assert got == {"https://example.com/": "header", "https://f.test/": "footer"}


def test_article_inside_a_sidebar_or_footer_keeps_the_global_region():
    assert only(f"<body><aside><article><p>{X}</p></article></aside></body>") == "aside"
    assert only(f"<body><footer><article><p>{X}</p></article></footer></body>") == "footer"


# ---- regression fixtures: the structures reported in the audit (ancestor chains of the live pages) ----
CODIGIX = "https://codigix.co/"


def test_regression_moraya_footer_credit_was_content():
    """Credit sits in div.bottom-footer > div.container > p; the page has an empty <footer> elsewhere, so the old code said 'content'."""
    html = ('<body><div class="theme-layout"><nav><a href="/">Home</a></nav></div><section class="cols"><p>Treatments</p></section><footer></footer>'
            f'<div class="bottom-footer"><div class="container"><p>© Moraya Hospital | Website Designed by <a href="{CODIGIX}">Codigix infotech Pvt Ltd</a></p></div></div></body>')
    assert place(html)[CODIGIX] == ("footer", None)


def test_regression_sanskruti_logo_link_repeated_across_pages():
    """Image link with an empty anchor inside div.elementor-location-footer; the same structure on every page gives the same placement."""
    html = ('<body class="single-post elementor-template-full-width"><a class="skip-link" href="#content">Skip</a>'
            '<div class="elementor elementor-64 elementor-location-header"><div class="elementor-widget-image"><a href="/"><img alt="Logo"></a></div></div>'
            '<div class="elementor elementor-2473"><p>Post</p></div>'
            '<div class="elementor elementor-67 elementor-location-footer"><div class="e-con-inner"><div class="elementor-widget-image">'
            '<a href="http://codigix.co"><img src="c.png" alt="Codigix Infotech"></a></div></div></div></body>')
    results = {p: place(html)["http://codigix.co"] for p in ("page-1", "page-2", "page-3")}
    assert set(results.values()) == {("footer", None)}
    assert place(html)["https://src.test/"][0] == "header"  # the header logo is a header link, not footer


def test_regression_kitchen_canvas_footer_credit_was_header():
    """Credit inside the header's mobile-menu footer (header > ... > div.mobile__footer > div.mobile__copyright > p), and the desktop footer inside a
    <main> that wraps the whole page: both are footer links; the real header logo stays a header link."""
    html = ('<body><div id="page"><main class="main-content"><header class="header -home-2 js-header js-header-sticky"><div class="header__bar js-header-bar">'
            '<div class="header__logo"><a href="/">logo</a></div><div class="menu js-menu"><div class="mobile__footer js-mobile-footer">'
            f'<div class="mobile__copyright"><p>© Kitchen Canvas. Design By <a href="{CODIGIX}">Codigix Infotech</a></p></div></div></div></div></header>'
            f'<div id="content"><footer class="footer -type-1"><div class="footer__bottom"><div class="footer__bottom_text"><a href="{CODIGIX}?x=1">Codigix Infotech</a>'
            "</div></div></footer></div></main></div></body>")
    got = place(html)
    assert got[CODIGIX][0] == "footer" and got[CODIGIX + "?x=1"][0] == "footer"
    assert got["https://src.test/"][0] == "header"


def test_regression_corplegalsolutions_footer_link():
    html = ('<body class="ehf-header ehf-footer"><div id="page" class="hfeed site"><footer id="colophon" role="contentinfo"><div class="footer-width-fixer">'
            '<div class="elementor elementor-49"><div class="e-con-inner"><div class="elementor-widget-image"><a href="http://codigix.co"><img alt=""></a>'
            "</div></div></div></div></footer></div></body>")
    assert place(html)["http://codigix.co"] == ("footer", None)


# ---- extraction contract: nothing else changed ----
def test_extraction_fields_and_canonical_are_preserved():
    html = ('<head><link rel="canonical" href="/canon"></head><body><main><p>See <a href="/rel" rel="NoFollow sponsored UGC">Rel <b>bold</b></a>, '
            '<a href="https://example.com/a?q=1#f">abs</a><a href="mailto:x@y.z">mail</a><a href="javascript:void(0)">js</a><a href="/rel" rel="nofollow">again</a></p></main></body>')
    out = engine.extract_links(BeautifulSoup(html, "lxml"), "https://src.test/dir/p")
    by = [(d, t, k) for d, t, _, k, _ in out]
    assert by == [("https://src.test/rel", "Rel bold", "html"), ("https://example.com/a?q=1#f", "abs", "html"), ("https://src.test/rel", "again", "html"),
                  ("https://src.test/canon", "", "canonical")]  # mailto / javascript dropped, repeated anchors kept, canonical is link_type 'canonical'
    first, canon = out[0], out[-1]
    assert first[2] == "nofollow sponsored ugc" and first[4]["nofollow"] and first[4]["sponsored"] and first[4]["ugc"] and first[4]["position"] == "content"
    assert canon[4] == {}  # page metadata, no placement, never an anchor


def test_placement_is_stable_across_repeated_calls_and_shared_memo():
    soup = BeautifulSoup(f"<body><footer><p>{X}</p><p>{X}</p></footer></body>", "lxml")
    links = soup.find_all("a")
    memo: dict = {}
    assert [engine._position(a, memo) for a in links] == [engine._position(a) for a in links] == ["footer", "footer"]


# ---- persistence: extractor -> page_facts -> process_page -> outgoing_links.position ----
def test_placement_reaches_postgres_unchanged(env):
    u = env
    SITES["a.localhost"] = {"/": (200, "text/html", (
        '<html><head><title>T</title><link rel="canonical" href="/"></head><body>'
        '<header class="site-header"><nav class="primary-nav"><a href="https://t.test/nav">n</a></nav></header>'
        '<main><article><header><a href="https://t.test/ah">ah</a></header><p>Read <a href="https://t.test/body">b</a></p>'
        '<nav aria-label="Table of contents"><a href="https://t.test/toc">toc</a></nav></article><aside class="sidebar"><a href="https://t.test/side">s</a></aside></main>'
        '<div class="bottom-footer"><p><a href="https://t.test/foot">f</a></p></div><div><span><a href="https://t.test/unk">u</a></span></div></body></html>'))}
    asyncio.run(engine.run_crawl(cfg(target_domain="t.test", max_depth=0), [u("a")]))
    got = {r["destination_url"].rsplit("/", 1)[-1]: r["position"] for r in rows("SELECT destination_url, position FROM outgoing_links WHERE link_type='html'")}
    assert got == {"nav": "nav", "ah": "content", "body": "content", "toc": "content", "side": "aside", "foot": "footer", "unk": "unknown"}
    assert [r["position"] for r in rows("SELECT position FROM outgoing_links WHERE link_type='canonical'")] == [None]
    # a re-crawl with a changed layout refreshes the stored placement (process_page's UPDATE path), it does not keep the first value
    SITES["a.localhost"]["/"] = (200, "text/html", '<html><body><div id="site-footer"><a href="https://t.test/nav">n</a></div></body></html>')
    asyncio.run(engine.recrawl_due(cfg(), urls=[u("a")]))
    assert rows("SELECT position FROM outgoing_links WHERE destination_url='https://t.test/nav'")[0]["position"] == "footer"


@pytest.mark.parametrize("n", [1])
def test_page_facts_keeps_position(n):
    links, digest, title = engine.page_facts(BeautifulSoup('<title>x</title><footer><a href="/f">f</a></footer>', "lxml"), "https://src.test/")
    assert links[0][4]["position"] == "footer" and title == "x" and len(digest) == 64


# ---- validation pass: global vs article-local navigation, nested precedence, and the patterns found in the real-page corpus ----
def test_navigation_global_vs_local_matrix():
    nav = lambda h: only(h)  # noqa: E731
    assert nav(f'<body><div class="site-navigation">{X}</div></body>') == "nav"
    assert nav(f'<body><div class="primary-nav">{X}</div></body>') == "nav"
    assert nav(f'<body><ul class="nav">{"<li>" + X + "</li>"}</ul></body>') == "nav"  # bare nav class outside an article: as strong as a <nav> tag
    assert place(f'<main><article><div class="nav">{X}</div></article></main>') == {"https://example.com/": ("content", "article_nav")}  # ... and local inside one
    assert place(f'<main><article><nav class="breadcrumb">{X}</nav></article></main>') == {"https://example.com/": ("content", "article_nav")}
    assert nav(f'<body><div class="site-nav"><p>menu</p>{X}</div><main><article><p>x</p></article></main></body>') == "nav"
    assert nav(f'<body><main><article><div class="site-nav">{X}</div></article></main></body>') == "nav"  # a qualified site-level name is never article-local
    assert nav(f'<body><div class="mobile-nav">{X}</div></body>') == "nav"
    assert nav(f'<body><nav class="mobile-menu" hidden aria-hidden="true">{X}</nav></body>') == "nav"  # placement is structural; hidden menus keep it
    assert nav(f'<body><aside class="sidebar"><nav>{X}</nav></aside></body>') == "aside"
    assert nav(f'<body><footer><nav class="footer-nav">{X}</nav></footer></body>') == "footer"
    assert nav(f'<body><div class="navigation pagination"><div class="nav-links">{X}</div></div></body>') == "unknown"
    assert nav(f'<body><nav aria-label="Table of contents">{X}</nav></body>') == "unknown"


def test_nested_region_precedence():
    assert only(f"<body><header><footer>{X}</footer></header></body>") == "footer"
    assert only(f"<body><footer><header>{X}</header></footer></body>") == "footer"
    assert only(f"<body><aside><header>{X}</header></aside></body>") == "aside"
    assert only(f"<body><footer><aside>{X}</aside></footer></body>") == "footer"
    assert only(f"<body><header><nav><ul><li>{X}</li></ul></nav></header></body>") == "nav"
    assert only(f"<body><main><article><header><nav>{X}</nav></header></article></main></body>") == "content"


def test_header_footer_directly_in_main_or_section_are_ambiguous_not_global():
    assert only(f"<body><main><header><h1>Title</h1>{X}</header><p>t</p></main></body>") == "unknown"
    assert only(f"<body><section><footer>{X}</footer></section></body>") == "unknown"
    assert only(f'<body><main><ul><li class="post"><header class="entry-header">{X}</header></li></ul></main></body>') == "content"  # the template says it is the post's header
    assert only(f"<body><main><section><footer>{X}</footer></section></main></body>") == "unknown"


def test_main_with_a_block_footer_is_still_the_content_container():
    html = f"<body><header><a href='/h'>h</a></header><main><section><footer>b</footer><p><a href='/c'>c</a></p></section></main><footer><a href='/f'>f</a></footer></body>"
    assert {k.rsplit("/", 1)[1]: v[0] for k, v in place(html).items()} == {"h": "header", "c": "content", "f": "footer"}


def test_real_world_class_patterns():
    assert only(f'<body><main><div id="content-footer">{X}</div></main></body>') != "footer"  # "content-footer" closes the content (Guardian)
    assert only(f'<body><div class="footer-content">{X}</div></body>') == "footer"
    assert only(f'<body><div class="dsm-pre-footer">{X}</div></body>') == "unknown"  # a pre-footer call-to-action band is not the footer
    assert only(f'<body><div class="responsive-header">{X}</div></body>') == "header"
    assert only(f'<body><main id="main-content"><nav class="hero-nav">{X}</nav></main></body>') == "unknown"  # <main id="main-content"> is not an article
    assert only(f'<body><div data-elementor-type="header"><a href="/x">x</a></div></body>') == "header"
    assert only(f'<body><div data-elementor-type="footer"><a href="/x">x</a></div></body>') == "footer"
    assert only(f'<body><div data-elementor-type="wp-page"><div class="elementor-widget"><a href="/x">x</a></div></div></body>') == "content"


# ---- local_region: what carries it ----
def test_local_region_is_carried_to_process_page_but_not_persisted(env):
    links, _, _ = engine.page_facts(BeautifulSoup('<main><article><footer><a href="/x">x</a></footer></article></main>', "lxml"), "https://src.test/")
    assert links[0][4]["local_region"] == "article_footer"  # page_facts keeps the whole extra dict
    with engine.core.connect() as c:
        cols = {r["column_name"] for r in c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='outgoing_links'")}
    assert "position" in cols and "local_region" not in cols  # process_page's INSERT / UPDATE list has no column for it: it is dropped there

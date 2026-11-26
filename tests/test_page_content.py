"""Phase 2: page content, page-type evidence and link context (evidence collection only; nothing here classifies a backlink).

Unit tests on HTML fixtures, plus pipeline tests: page_facts -> process_page -> EVIDENCE_SINK through the real crawler (also the Playwright pass).
"""

import asyncio
import json

import pytest
from bs4 import BeautifulSoup

import crawler_engine as engine
from test_crawler import SITES, cfg, env, port, rows  # noqa: F401 - fixtures

URL = "https://src.test/blog/post-1"


def facts(html: str, url: str = URL, parser: str = "lxml") -> engine.PageFacts:
    return engine.page_facts(BeautifulSoup(html, parser), url)


def content(html: str, url: str = URL, parser: str = "lxml") -> dict:
    return facts(html, url, parser).content


def ctx(html: str, href: str, parser: str = "lxml") -> dict:
    """link_context of the (first) link to `href`."""
    out = engine.extract_links(BeautifulSoup(html, parser), "https://src.test/p")
    return next(e["link_context"] for d, _, _, t, e in out if t == "html" and d == href)


PARA = "This is a paragraph with enough words to count as meaningful article content for the extractor. "


# ---------------------------------------------------------------- A. metadata
def test_basic_metadata_and_dedup_of_open_graph():
    c = content('<html lang="en-GB"><head><title> My Page </title><meta name="description" content="About the page"><link rel="canonical" href="/canon/">'
                '<meta name="robots" content="noindex, follow"><meta property="og:title" content="My Page"><meta property="og:description" content="Different text">'
                '<meta property="og:type" content="article"></head><body><p>x</p></body></html>')
    assert (c["title"], c["meta_description"], c["canonical_url"], c["language"], c["meta_robots"]) == ("My Page", "About the page", "https://src.test/canon/", "en-GB", "noindex, follow")
    assert c["og"] == {"title": None, "description": "Different text", "type": "article"}  # the og title equals the title: not stored twice
    assert c["url"] == URL and c["http_status"] is None


def test_missing_metadata_is_none_not_invented():
    c = content("<body><p>x</p></body>")
    assert (c["title"], c["meta_description"], c["canonical_url"], c["language"], c["meta_robots"]) == (None, None, None, None, None)
    assert c["og"] == {"title": None, "description": None, "type": None}


def test_language_fallbacks_and_og_description_dedup():
    assert content('<html><head><meta http-equiv="content-language" content="de"></head><body></body></html>')["language"] == "de"
    assert content('<html><head><meta property="og:locale" content="fr_FR"></head><body></body></html>')["language"] == "fr_FR"
    c = content('<head><meta name="description" content="Same"><meta property="og:description" content="Same"></head><body></body>')
    assert c["meta_description"] == "Same" and c["og"]["description"] is None


def test_title_matches_the_existing_page_facts_title():
    f = facts("<head><title>T " + "x" * 400 + "</title></head><body><p>y</p></body>")
    assert f[2] == f.content["title"] and len(f[2]) == 300


# ---------------------------------------------------------------- B. headings
def test_headings_in_order_with_hierarchy():
    c = content("<body><main><h1>Best Digital Marketing Strategies</h1><h2>SEO Fundamentals</h2><h2>Content Marketing</h2><h3>Creating Useful Blog Content</h3>"
                "<h2>Link Building Strategies</h2></main></body>")
    assert c["headings"]["h1"] == ["Best Digital Marketing Strategies"]
    assert c["headings"]["h2"] == ["SEO Fundamentals", "Content Marketing", "Link Building Strategies"]
    assert c["headings"]["h3"] == ["Creating Useful Blog Content"]
    assert [(h["level"], h["text"]) for h in c["headings"]["outline"]] == [(1, "Best Digital Marketing Strategies"), (2, "SEO Fundamentals"), (2, "Content Marketing"),
                                                                           (3, "Creating Useful Blog Content"), (2, "Link Building Strategies")]


def test_other_heading_levels_and_missing_headings():
    c = content("<main><h4>Four</h4><h6>Six</h6></main>")
    assert c["headings"]["other"] == ["Four", "Six"] and c["headings"]["h1"] == []
    assert content("<main><p>none</p></main>")["headings"]["outline"] == []


def test_navigation_and_boilerplate_headings_are_not_article_headings():
    c = content('<body><header><h1>Site Name</h1></header><nav><h2>Menu</h2></nav><main><article><h1>The Article</h1><h2>Part One</h2></article></main>'
                '<aside class="sidebar"><h3>Recent Posts</h3></aside><footer><h4>Contact us</h4></footer><div class="cookie-banner"><h2>We use cookies</h2></div></body>')
    assert [h["text"] for h in c["headings"]["outline"]] == ["The Article", "Part One"]


# ---------------------------------------------------------------- C. main content
def test_semantic_article_statistics():
    c = content(f"<body><header><nav><a href='/'>Home</a></nav></header><main><article><h1>Title</h1><p>{PARA * 2}</p><p>{PARA} Second.</p><ul><li>A distinct list entry with enough words to count as content here.</li></ul>"
                f"<img src='a.jpg'></article></main><footer><p>Copyright notice that is long enough to be a paragraph on its own.</p></footer></body>")
    m = c["main_content"]
    assert m["paragraph_count"] == 2 and m["list_count"] == 1 and m["image_count"] == 1 and m["heading_count"] == 1 and m["container"] == "article"
    assert m["word_count"] > 40 and m["block_count"] == 3 and not m["truncated"]
    assert "Copyright notice" not in m["text"] and "Home" not in m["text"]
    assert c["extraction_quality"] == {"status": "complete", "reason": None}


def test_main_wordpress_elementor_news_docs_directory_forum_homepage_shapes():
    long = PARA * 4
    wp = f'<body class="single"><div class="site-header"><a href="/">x</a></div><div class="entry-content"><p>{long}</p><p>{PARA}</p></div><div class="widget-area"><p>{long}</p></div></body>'
    el = f'<body><div data-elementor-type="header"><p>{long}</p></div><div data-elementor-type="wp-page"><div class="elementor-widget-text-editor"><p>{long}</p></div></div></body>'
    news = f'<body><main><article><h1>Headline</h1><p class="byline">By A. Writer</p><p>{long}</p></article></main></body>'
    docs = f'<body><nav class="sidebar-nav"><ul><li><a href="/a">Intro to the product docs</a></li></ul></nav><main><h1>API</h1><p>{long}</p><pre>some_call(arg1, arg2, arg3)</pre></main></body>'
    directory = "<body><main>" + "".join(f'<div class="listing"><h3>Company {i}</h3><p>Plumbing services across the city with fast response times {i}.</p></div>' for i in range(5)) + "</main></body>"
    forum = f'<body><main><div class="thread"><div class="post"><p>{long}</p></div><div class="post"><p>{PARA} reply with more words here.</p></div></div></main></body>'
    home = f'<body><header><nav><a href="/a">A</a></nav></header><main><h1>Welcome</h1><p>{long}</p></main><footer><a href="/f">f</a></footer></body>'
    for name, html, container in (("wp", wp, "article"), ("elementor", el, "article"), ("news", news, "article"), ("docs", docs, "main"), ("forum", forum, "main"), ("home", home, "main")):
        c = content(html)
        assert c["extraction_quality"]["status"] == "complete", name
        assert c["main_content"]["container"] == container, (name, c["main_content"])
    assert "widget" not in content(wp)["main_content"]["text"] and content(wp)["main_content"]["text"].count(long.strip()) == 1  # the sidebar copy is not main content
    assert "API" in content(docs)["headings"]["h1"] and "Intro to the product docs" not in content(docs)["main_content"]["text"]
    d = content(directory)
    assert d["main_content"]["block_count"] == 5 and d["page_type_signals"]["type_evidence"]["directory"]
    assert content(el)["main_content"]["text"].startswith("This is a paragraph")  # the Elementor header template is excluded


def test_page_without_semantic_tags_uses_conservative_fallback():
    loose = "Our company has been providing quality services to customers across the region for over twenty years and we are proud of it. " * 4
    c = content(f"<body><div class='a'><div class='b'>{loose}</div></div><div class='c'><span>x</span></div></body>")
    assert c["extraction_quality"]["status"] == "fallback" and c["main_content"]["container"] == "fallback" and c["main_content"]["has_main_content"]
    assert content(f"<body><div><p>{PARA * 4}</p></div></body>")["extraction_quality"]["status"] == "complete"  # prose in a plain div still counts via Phase 1 placement


def test_large_page_text_is_bounded_but_counted():
    body = "".join(f"<p>{PARA} Paragraph number {i} adds distinct words {i * 7919}.</p>" for i in range(600))
    c = content(f"<body><main>{body}</main></body>")
    m = c["main_content"]
    assert m["paragraph_count"] == 600 and m["truncated"] and len(m["text"]) <= engine.PAGE_TEXT_CHARS and m["character_count"] > engine.PAGE_TEXT_CHARS


def test_empty_pages_and_js_shell():
    assert content("")["extraction_quality"] == {"status": "insufficient", "reason": "empty_document"}
    assert content("<html><body></body></html>")["extraction_quality"]["status"] == "insufficient"
    shell = content('<html><body><div id="root"></div><noscript>You need to enable JavaScript to run this app.</noscript></body></html>')
    assert shell["extraction_quality"] == {"status": "insufficient", "reason": "js_shell_suspected"}
    thin = content("<body><main><p>Just a short note with only a few words in it.</p></main></body>")
    assert thin["extraction_quality"]["status"] == "thin" and not thin["main_content"]["has_main_content"]


def test_boilerplate_heavy_page_keeps_only_the_article():
    html = ('<body><script>var junk="script text that must never appear in content";</script><style>.x{color:red}</style>'
            '<div class="cookie-consent"><p>We use cookies to improve your experience on this website, please accept.</p></div>'
            '<div class="advert"><p>Buy our amazing product today with a huge discount for readers.</p></div><div hidden><p>Hidden paragraph that should be skipped by the extractor.</p></div>'
            '<div style="display:none"><p>Display none paragraph that should be skipped by the extractor too.</p></div>'
            f'<nav><ul><li>Home page link text</li></ul></nav><main><article><p>{PARA}</p><p>{PARA}</p><p><a href="/a">Share</a> <a href="/b">Tweet</a> <a href="/c">Pin it</a></p></article></main>'
            '<footer><p>Footer text that is long enough to be a paragraph but belongs to the site footer.</p></footer></body>')
    t = content(html)["main_content"]["text"]
    for bad in ("script text", "color:red", "cookies", "amazing product", "Hidden paragraph", "Display none", "Home page", "Footer text", "Tweet"):
        assert bad not in t, bad
    assert PARA.strip() in t and t.count(PARA.strip()) == 1  # the repeated paragraph is listed once


# ---------------------------------------------------------------- D. link context
def test_context_for_the_spec_example_paragraph():
    html = ("<article><h2>Our Technology Partner</h2><p>We partnered with Codigix Infotech to develop and maintain our digital platform. "
            "Their team helped us improve our website. <a href='https://codigix.co'>Codigix Infotech</a> provided the development support.</p></article>")
    c = ctx(html, "https://codigix.co")
    assert c["section_heading"] == "Our Technology Partner" and c["heading_level"] == 2
    assert (c["parent_tag"], c["block_tag"], c["block_kind"], c["link_kind"]) == ("p", "p", "paragraph", "text")
    assert c["text_before"] == "We partnered with Codigix Infotech to develop and maintain our digital platform. Their team helped us improve our website."
    assert c["text_after"] == "provided the development support."
    assert c["sentence"] == "Codigix Infotech provided the development support." and c["prev_sentence"] == "Their team helped us improve our website."
    assert c["next_sentence"] == "" and c["context_quality"] == "sufficient"
    assert c["surrounding_text"].startswith("We partnered") and "Codigix Infotech provided the development support." in c["surrounding_text"]
    out = engine.extract_links(BeautifulSoup(html, "lxml"), "https://src.test/p")
    assert out[0][4]["position"] == "content" and out[0][4]["context"]  # Phase 1 fields untouched, old 'context' still there


def test_multiple_links_in_one_paragraph_each_get_their_own_context():
    html = ("<main><p>First we use <a href='/one'>Tool One</a> for planning. Then <a href='/two'>Tool Two</a> handles delivery, and finally "
            "<a href='/three'>Tool Three</a> reports results to the whole team every week.</p></main>")
    c1, c2, c3 = (ctx(html, f"https://src.test/{n}") for n in ("one", "two", "three"))
    assert c1["sentence"] == "First we use Tool One for planning." and c1["text_before"] == "First we use"
    assert c2["sentence"].startswith("Then Tool Two handles delivery") and "Tool One" in c2["text_before"]
    assert c3["text_after"].startswith("reports results") and "Tool Two" in c3["text_before"]


def test_list_item_lead_in_and_insufficient_context():
    html = "<main><p>Our partners include:</p><ul><li><a href='https://codigix.co'>Codigix Infotech</a></li></ul></main>"
    c = ctx(html, "https://codigix.co")
    assert (c["block_kind"], c["context_quality"], c["sentence"], c["text_before"], c["text_after"]) == ("list_item", "insufficient", "", "", "")
    assert c["lead_in"] == "Our partners include:"  # the introduction of the list, not invented text
    long_li = "<main><ul><li>Read the case study by <a href='/x'>Codigix</a> about the digital platform migration project</li></ul></main>"
    assert ctx(long_li, "https://src.test/x")["context_quality"] == "sufficient"


def test_table_blockquote_heading_image_button_and_card_links():
    t = ctx("<main><table><tr><td>Codigix Infotech</td><td>Pune software company</td><td><a href='/c'>website</a></td></tr></table></main>", "https://src.test/c")
    assert t["block_kind"] == "table_row" and "Pune software company" in t["text_before"] and t["context_quality"] == "sufficient"
    q = ctx("<main><blockquote>As the report says, <a href='/q'>the study</a> found strong evidence for the effect across all groups.</blockquote></main>", "https://src.test/q")
    assert q["block_kind"] == "blockquote" and q["sentence"].startswith("As the report says")
    h = ctx("<main><h2>Read <a href='/h'>our guide</a> to link building today</h2></main>", "https://src.test/h")
    assert h["block_kind"] == "heading" and h["section_heading"] == "Read our guide to link building today"
    i = ctx("<main><p>Our latest award winning project is shown here <a href='/i'><img src='a.png' alt='Project logo'></a> and described below in detail.</p></main>", "https://src.test/i")
    assert i["link_kind"] == "image" and i["context_quality"] == "sufficient" and "award winning" in i["text_before"]
    b = ctx("<main><p>Ready to start your project? <a class='btn btn-primary' href='/b'>Get started</a> today with a free consultation.</p></main>", "https://src.test/b")
    assert b["link_kind"] == "button"
    card = ctx("<main><div class='card'><h3>Acme Corp</h3><p>Acme builds industrial widgets for factories worldwide.</p><a href='/a'>Visit site</a></div></main>", "https://src.test/a")
    assert "card" in card["container_markers"] and card["section_heading"] == "Acme Corp"


def test_nested_article_sections_use_the_nearest_heading_of_the_same_region():
    html = ("<body><aside class='sidebar'><h3>Sidebar title</h3></aside><main><article><h1>Main</h1><section><h2>First</h2><p>Text one is long enough here to count. <a href='/a'>A</a> end.</p></section>"
            "<section><h2>Second</h2><div><p>Text two is long enough here to count. <a href='/b'>B</a> end.</p></div></section></article></main>"
            "<footer><h4>Company</h4><ul><li><a href='/f'>F</a></li></ul></footer></body>")
    assert ctx(html, "https://src.test/a")["section_heading"] == "First"
    assert ctx(html, "https://src.test/b")["section_heading"] == "Second"
    assert ctx(html, "https://src.test/f")["section_heading"] == "Company"  # footer links get the footer's own heading, not an article heading


def test_long_surrounding_text_is_bounded_and_never_cuts_a_word_open():
    words = " ".join(f"word{i}" for i in range(400))
    c = ctx(f"<main><p>{words} <a href='/x'>anchor</a> {words}.</p></main>", "https://src.test/x")
    assert 0 < len(c["text_before"]) <= engine.LINK_CONTEXT_CHARS and 0 < len(c["text_after"]) <= engine.LINK_CONTEXT_CHARS
    assert c["text_before"].split()[0].startswith("word") and c["text_after"].split()[-1].startswith("word")
    assert len(c["surrounding_text"]) < 3 * engine.LINK_CONTEXT_CHARS + 250


def test_link_directly_in_a_div_and_no_block_at_all():
    c = ctx("<body><div class='x'><a href='/d'>Only link</a></div></body>", "https://src.test/d")
    assert c["context_quality"] == "insufficient" and c["sentence"] == "" and c["block_tag"] == "div"
    c2 = ctx("<body><div>Some loose words around the link in a div <a href='/e'>here</a> and more words after it.</div></body>", "https://src.test/e")
    assert c2["context_quality"] == "sufficient" and "loose words" in c2["text_before"]


def test_link_context_does_not_change_existing_link_fields():
    html = "<main><article><p>Read <a href='/x' rel='nofollow sponsored ugc' class='k'>this <b>bold</b></a> now please.</p></article></main>"
    out = engine.extract_links(BeautifulSoup(html, "lxml"), "https://src.test/p")
    d, anchor, rel, kind, e = out[0]
    assert (d, anchor, rel, kind) == ("https://src.test/x", "this bold", "nofollow sponsored ugc", "html")
    assert e["nofollow"] and e["sponsored"] and e["ugc"] and e["position"] == "content" and e["context"].startswith("Read this bold now") and e["img_alt"] is None


# ---------------------------------------------------------------- E. page-type signals
def ld(obj) -> str:
    return f'<script type="application/ld+json">{json.dumps(obj)}</script>'


def test_blog_posting_schema_author_and_date():
    c = content(f"<head>{ld({'@context': 'https://schema.org', '@type': 'BlogPosting', 'headline': 'x', 'datePublished': '2026-01-01', 'author': {'@type': 'Person', 'name': 'A'}})}</head><body><article><h1>T</h1><p>{PARA}</p></article></body>", "https://site.test/p/1")
    s = c["page_type_signals"]
    assert {"BlogPosting", "Person"} <= set(s["schema_types"]) and s["has_author"] and s["has_published_date"] and s["has_article_structure"]
    assert s["type_evidence"]["blog"] == ["schema:BlogPosting"] and "article" in s["type_evidence"]


def test_news_graph_microdata_and_metadata():
    g = content(f"<head>{ld({'@graph': [{'@type': 'NewsArticle', 'datePublished': 'x'}, {'@type': ['Organization', 'NewsMediaOrganization']}]})}"
                '<meta property="og:type" content="article"><meta property="article:published_time" content="2026-01-01"></head><body></body>')
    s = g["page_type_signals"]
    assert {"NewsArticle", "Organization", "NewsMediaOrganization"} <= set(s["schema_types"]) and "og:type=article" in s["metadata_types"]
    assert any(e.startswith("schema:NewsArticle") for e in s["type_evidence"]["news"]) and s["has_published_date"]
    m = content('<body><div itemscope itemtype="https://schema.org/BlogPosting"><span itemprop="author">A</span></div></body>')
    assert "BlogPosting" in m["page_type_signals"]["schema_types"]


def test_profile_directory_forum_homepage_and_resource_evidence():
    prof = content(f"<head>{ld({'@type': 'ProfilePage'})}{ld({'@type': 'Person'})}<meta property='og:type' content='profile'></head><body></body>", "https://site.test/in/someone")["page_type_signals"]
    assert {"ProfilePage", "Person"} <= set(prof["schema_types"]) and {"schema:ProfilePage", "og:type=profile", "url:profile"} <= set(prof["type_evidence"]["profile"])
    listings = "".join(f'<div class="business-listing"><address>{i} Main St</address><a href="tel:+1555000{i}">call</a></div>' for i in range(4))
    d = content(f"<body><main>{listings}</main></body>", "https://site.test/directory/plumbers")["page_type_signals"]
    assert "url:directory" in d["type_evidence"]["directory"] and "marker:listing_blocks_or_contact_details" in d["type_evidence"]["directory"]
    assert "repeated_listing_blocks=4" in d["content_markers"] and "tel_links=4" in d["content_markers"] and "address_elements=4" in d["content_markers"]
    f = content('<body><main><div class="thread-list"><div class="post"><p>reply</p></div></div></main></body>', "https://site.test/forum/thread/123")["page_type_signals"]
    assert "marker:forum_structure" in f["type_evidence"]["forum"] and "url:forum" in f["type_evidence"]["forum"]
    h = content(f"<head>{ld({'@type': 'WebSite'})}<meta property='og:type' content='website'></head><body></body>", "https://site.test/")["page_type_signals"]
    assert "root" in h["url_patterns"] and set(h["type_evidence"]["homepage"]) == {"schema:WebSite", "og:type=website", "url:root"}
    r = content("<body><main><h1>Guide</h1><h2>Further reading</h2><p>x</p></main></body>", "https://site.test/resources/guide")["page_type_signals"]
    assert "marker:resource_heading" in r["type_evidence"]["resource"] and "url:resource" in r["type_evidence"]["resource"]


def test_conflicting_signals_are_all_reported_without_a_definitive_type():
    c = content(f"<head>{ld({'@type': 'BlogPosting'})}{ld({'@type': 'Organization'})}{ld({'@type': 'ItemList'})}</head><body><main><p>x</p></main></body>", "https://site.test/directory/post")
    s = c["page_type_signals"]
    assert {"blog", "profile", "directory"} <= set(s["type_evidence"]) and "type" not in s and "page_type" not in c


def test_broken_json_ld_and_hostile_values_do_not_break_extraction():
    c = content('<head><script type="application/ld+json">{not json</script><script type="application/ld+json">{"@type": 5, "author": 1}</script></head><body><p>x</p></body>')
    assert c["page_type_signals"]["schema_types"] == [] and c["page_type_signals"]["has_author"]


# ---------------------------------------------------------------- G. pipeline integration
def test_page_facts_is_still_the_same_triple_plus_content():
    f = facts(f"<head><title>T</title></head><body><main><p>{PARA}</p><a href='/x'>x</a></main></body>")
    links, digest, title = f
    assert len(f) == 3 and title == "T" and len(digest) == 64 and links[0][4]["link_context"] is not None and f.content["title"] == "T"
    assert isinstance(f, tuple) and f.content["main_content"]["word_count"] > 10


def test_phase1_placement_and_links_unchanged_by_the_new_index():
    html = ("<body><header class='site-header'><nav class='primary-nav'><a href='/n'>n</a></nav></header><main><article><p>Read <a href='/c'>c</a> text here.</p></article>"
            "<aside class='sidebar'><a href='/s'>s</a></aside></main><div id='site-footer'><a href='/f'>f</a></div><link rel='canonical' href='/canon'></body>")
    out = engine.extract_links(BeautifulSoup(html, "lxml"), "https://src.test/p")
    assert [(d.rsplit("/", 1)[1], e.get("position")) for d, _, _, _, e in out] == [("n", "nav"), ("c", "content"), ("s", "aside"), ("f", "footer"), ("canon", None)]
    assert out[-1][3] == "canonical" and out[-1][4] == {}


def test_evidence_reaches_process_page_and_nothing_is_persisted(env, monkeypatch):
    u = env
    seen = {}
    monkeypatch.setattr(engine, "EVIDENCE_SINK", lambda url, c, links: seen.__setitem__(url, (c, links)))
    SITES["a.localhost"] = {"/": (200, "text/html", (
        '<html lang="en"><head><title>Partner page</title><meta name="description" content="About partners"></head><body><nav><a href="https://t.test/nav">menu</a></nav>'
        f'<main><article><h1>Partners</h1><h2>Our Technology Partner</h2><p>{PARA} We partnered with <a href="https://t.test/p">T Corp</a> to build the platform together.</p></article></main>'
        "<footer><a href='https://t.test/f'>f</a></footer></body></html>"))}
    asyncio.run(engine.run_crawl(cfg(target_domain="t.test", max_depth=0), [u("a")]))
    c, links = seen[u("a")]
    assert c["http_status"] == 200 and c["title"] == "Partner page" and c["language"] == "en" and c["main_content"]["word_count"] > 20
    by = {d.rsplit("/", 1)[1]: e for d, _, _, t, e in links if t == "html"}
    assert by["p"]["link_context"]["section_heading"] == "Our Technology Partner" and "We partnered with" in by["p"]["link_context"]["text_before"]
    assert by["nav"]["position"] == "nav" and by["nav"]["link_context"]["context_quality"] == "insufficient" and by["f"]["position"] == "footer"
    cols = {r["column_name"] for r in rows("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name IN ('outgoing_links','discovered_urls')")}
    assert not cols & {"link_context", "meta_description", "page_content", "page_type_signals", "section_heading"}  # no schema change in Phase 2
    assert rows("SELECT title FROM discovered_urls WHERE url=?", u("a"))[0]["title"] == "Partner page"
    assert rows("SELECT context FROM outgoing_links WHERE destination_url='https://t.test/p'")[0]["context"].startswith("This is a paragraph")  # old 'context' column unchanged


def test_playwright_rendered_html_goes_through_the_same_extraction(env, monkeypatch):
    seen = {}
    monkeypatch.setattr(engine, "EVIDENCE_SINK", lambda url, c, links: seen.__setitem__(url, (c, links)))
    js = ("<html><head><title>Shell</title></head><body><div id='root'></div><script>document.getElementById('root').innerHTML="
          "'<main><article><h1>Rendered Heading</h1><p>" + PARA.replace("'", "") * 4 + " See <a href=\"https://t.test/js\">the JS partner</a> for details.</p></article></main>'</script></body></html>")
    SITES["a.localhost"] = {"/": (200, "text/html", js)}
    asyncio.run(engine.run_crawl(cfg(target_domain="t.test", use_playwright=True, max_depth=0), [env("a")]))
    c, links = seen[env("a")]
    assert c["headings"]["h1"] == ["Rendered Heading"] and c["extraction_quality"]["status"] == "complete"
    lc = next(e["link_context"] for d, _, _, t, e in links if d == "https://t.test/js")
    assert lc["section_heading"] == "Rendered Heading" and "See" in lc["text_before"]


def test_js_shell_fallback_pass_extracts_from_the_rendered_page(env, monkeypatch):
    seen = {}
    monkeypatch.setattr(engine, "EVIDENCE_SINK", lambda url, c, links: seen.setdefault(url, []).append(c))
    shell = ("<html><head><title>App</title></head><body><div id='root'></div><noscript>Please enable JavaScript</noscript><script>document.getElementById('root').innerHTML="
             "'<main><article><h1>Late Content</h1><p>" + PARA.replace("'", "") * 4 + "</p></article></main>'</script></body></html>")
    SITES["a.localhost"] = {"/": (200, "text/html", shell)}
    asyncio.run(engine.run_crawl(cfg(target_domain="t.test", max_depth=0, js_fallback=True), [env("a")]))
    got = seen[env("a")]
    assert len(got) == 1 and got[0]["headings"]["h1"] == ["Late Content"]  # persisted once, from the rendered HTML (no second persist of the static shell)


@pytest.mark.parametrize("html", ["<p>unclosed <a href='/x'>link", "<div><p>a<div>b</p></div>", "<a href='/x'><a href='/y'>nested</a></a><p>tail", "\x00<p>nul</p>"])
def test_malformed_html_never_raises(html):
    f = facts(html, parser="html.parser")
    assert f.content["extraction_quality"]["status"] in ("complete", "thin", "insufficient", "fallback")


def test_weak_signals_are_reported_but_not_mapped_to_a_type():
    s = content("<body><main><p>x</p><a href='tel:1'>a</a><a href='tel:2'>b</a><a href='tel:3'>c</a></main></body>", "https://site.test/category/news-items")["page_type_signals"]
    assert "tel_links=3" in s["content_markers"] and "category_or_tag" in s["url_patterns"]
    assert "directory" not in s["type_evidence"] and all("category_or_tag" not in e for ev in s["type_evidence"].values() for e in ev)

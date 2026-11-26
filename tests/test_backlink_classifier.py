"""Phase 3: the link classification engine. Unit tests run the REAL Phase 1/2 extraction on HTML fixtures (page_facts) and then classify; the structural
fixtures of the audited sites (moraya, sanskruti, kitchen-canvas, corplegalsolutions, growthpixel, brainmine, ruby hospital) use the ancestor structures that were
verified on the live pages in Phases 1-2. No database, no network, except the pipeline tests at the end (scratch schema + local server)."""

import asyncio
import threading

import pytest
from bs4 import BeautifulSoup

import backlink_classifier as bc
import crawler_engine as engine
from test_crawler import SITES, cfg, env, port, rows  # noqa: F401 - fixtures

T = bc.Target("codigix.co")
CODIGIX = "https://codigix.co/"
FILL = ("This paragraph is ordinary article text that exists so the page has enough meaningful words for the extractor to treat it as real content, "
        "describing several unrelated topics at some length. ")


def classify(html, url="https://blog.example.org/post/1", target=T, tracker=None, relationships=None, thresholds=None, only=None):
    """{destination: result} for the links to the target; the full Phase 1/2 pipeline (extract + page evidence) runs first."""
    f = engine.page_facts(BeautifulSoup(html, "lxml"), url)
    res = bc.classify_page([target], url, f[0], f.content, tracker=tracker, thresholds=thresholds)
    return [(f[0][i][0], r) for i, r in res if only is None or only in f[0][i][0]]


def one(html, **kw):
    got = classify(html, **kw)
    assert len(got) == 1, got
    return got[0][1]


def article(inner, extra_head="", extra_body=""):
    return f"<html><head><title>Some Blog Post</title>{extra_head}</head><body><main><article><h1>Marketing notes</h1>{inner}<p>{FILL}{FILL}</p></article></main>{extra_body}</body></html>"


def page(body, head=""):
    return f"<html><head><title>Page</title>{head}</head><body>{body}</body></html>"


def check(r, cls, conf_min=0.0, conf_max=1.0, candidate=None):
    assert r["link_class"] == cls, r
    assert conf_min <= r["confidence"] <= conf_max, r
    assert set(r) >= {"link_class", "confidence", "reason", "evidence", "is_editorial_candidate"} and r["evidence"] and r["reason"]
    if candidate is not None:
        assert r["is_editorial_candidate"] is candidate, r
    return r


# ====================================================================== editorial
def test_editorial_independent_blog_paragraph():
    r = one(article(f"<h2>Choosing a partner</h2><p>When we looked for a healthcare marketing partner in Pune we compared several agencies and finally "
                    f"worked with <a href='{CODIGIX}'>Codigix Infotech</a> because their reporting was clear and honest.</p>"))
    check(r, "editorial", 0.85, 0.95, candidate=True)
    assert "placement: content" in r["evidence"] and "block_kind: paragraph" in r["evidence"] and "context_quality: sufficient" in r["evidence"]
    assert "global_template_link: false" in " ".join(r["evidence"])


def test_editorial_news_reference_with_schema_is_only_supporting():
    ld = '<script type="application/ld+json">{"@type":"NewsArticle","datePublished":"2026-01-01","author":{"@type":"Person","name":"A"}}</script>'
    r = one(article(f"<p>Pune based <a href='{CODIGIX}'>Codigix Infotech</a> announced a new analytics platform for clinics this week, according to the company statement "
                    "released on Tuesday morning.</p>", ld))
    check(r, "editorial", 0.85, candidate=True)
    assert any("NewsArticle" in e and "supporting only" in e for e in r["evidence"])


def test_schema_alone_does_not_make_an_editorial_link():
    ld = '<script type="application/ld+json">{"@type":"BlogPosting","datePublished":"2026-01-01","author":"A"}</script>'
    r = one(page(f"<header class='site-header'><a href='{CODIGIX}'>Codigix</a></header><main><article><h1>T</h1><p>{FILL}{FILL}</p></article></main>", ld))
    check(r, "sitewide")  # BlogPosting page, but this link is in the header


def test_editorial_resource_research_and_forum_links():
    res = one(article(f"<h2>Resources</h2><p>For teams that need local SEO for hospitals and clinics, a useful reference is "
                      f"<a href='{CODIGIX}'>Codigix</a>, which publishes guides on healthcare marketing.</p>"))
    check(res, "editorial", candidate=True)
    study = one(article(f"<p>The study of regional healthcare advertising (see <a href='{CODIGIX}'>Codigix Infotech</a> for the dataset and the methodology notes) found "
                        "that local listings drive most appointment requests among urban patients.</p>"))
    check(study, "editorial", candidate=True)
    forum = one(page(f"<main><div class='thread'><div class='post'><p>I used a few agencies for my clinic website and the best experience I had was with "
                     f"<a href='{CODIGIX}' rel='ugc nofollow'>Codigix</a> because they answered every question quickly and clearly.</p></div>"
                     f"<div class='post'><p>{FILL}{FILL}</p></div></div></main>"), url="https://forum.example.org/thread/55")
    check(forum, "editorial", candidate=True)
    assert forum["attributes"]["ugc"] and forum["attributes"]["nofollow"] and {"ugc", "nofollow"} <= set(forum["qualifiers"]) and "forum" in forum["reason"]


def test_editorial_without_article_tag_and_with_meaningful_sentences():
    r = one(page(f"<div><p>{FILL}{FILL}</p><p>If you need a website for a small clinic, you could start by talking to <a href='{CODIGIX}'>Codigix</a> about "
                 "what they offer for local practices and what it costs per month.</p></div>"))
    check(r, "editorial", 0.7, candidate=True)


def test_nofollow_sponsored_and_ugc_stay_attributes_not_classes():
    base = article("<p>Our long running favourite for clinic websites is <a href='%s' rel='%s'>Codigix</a>, as they have supported our practice for years now.</p>")
    plain = one(base % (CODIGIX, ""))
    nf = one(base % (CODIGIX, "nofollow"))
    sp = one(base % (CODIGIX, "sponsored"))
    check(nf, "editorial", candidate=True)
    assert nf["attributes"]["nofollow"] and nf["qualifiers"] == ["nofollow"] and nf["confidence"] == plain["confidence"]  # rel does not change the class or the confidence
    check(sp, "editorial")
    assert sp["attributes"]["sponsored"] and "sponsored" in sp["qualifiers"] and sp["confidence"] < plain["confidence"] and any("sponsored" in e for e in sp["evidence"])


# ====================================================================== sitewide
@pytest.mark.parametrize("region,html", [
    ("footer", "<footer><a href='%s'>Codigix</a></footer>"), ("header", "<header class='site-header'><a href='%s'>Codigix</a></header>"),
    ("nav", "<nav class='primary-nav'><ul><li><a href='%s'>Codigix</a></li></ul></nav>"), ("aside", "<aside class='sidebar'><a href='%s'>Codigix</a></aside>")])
def test_global_regions_are_sitewide(region, html):
    r = one(page(f"<main><article><h1>T</h1><p>{FILL}{FILL}</p></article></main>" + html % CODIGIX))
    check(r, "sitewide", 0.8, candidate=False)
    assert r["position"] == region and f"global_template_link: true ({region})" in r["evidence"]


def test_footer_without_semantic_tag_and_hidden_navigation():
    check(one(page(f"<main><p>{FILL}{FILL}</p></main><div id='site-footer'><p>Links</p><a href='{CODIGIX}'>Codigix</a></div>")), "sitewide", 0.85)
    check(one(page(f"<nav class='mobile-menu' hidden aria-hidden='true'><a href='{CODIGIX}'>Codigix</a></nav><main><p>{FILL}{FILL}</p></main>")), "sitewide", 0.85)


def test_a_page_with_footer_and_editorial_links_classifies_each_separately():
    html = article(f"<p>Our favourite supplier for clinic websites in Pune is <a href='{CODIGIX}about'>Codigix</a>, and we have used them for several years.</p>",
                   extra_body=f"<footer><a href='{CODIGIX}'>Codigix home</a></footer>")
    got = dict(classify(html))
    check(got[CODIGIX + "about"], "editorial", candidate=True)
    check(got[CODIGIX], "sitewide", candidate=False)


def test_repetition_across_pages_marks_template_links_with_fewer_than_ten_pages():
    tr = bc.RepetitionTracker(["codigix.co"])
    pages = [f"https://shop.example.org/p{i}" for i in range(4)]
    tpl = lambda i: page(f"<main><article><p>{FILL}{FILL} page {i} has its own words here.</p><p class='note'>Recommended partner: <a href='{CODIGIX}'>Codigix</a> for all the support work.</p></article></main>")
    for i, u in enumerate(pages):
        f = engine.page_facts(BeautifulSoup(tpl(i), "lxml"), u)
        tr.observe("example.org", u, f[0])
    f = engine.page_facts(BeautifulSoup(tpl(3), "lxml"), pages[3])
    res = bc.classify_page([T], pages[3], f[0], f.content, tracker=tr)
    r = res[0][1]
    check(r, "sitewide", 0.7, candidate=False)
    assert any("repeated on 4/4 pages" in e for e in r["evidence"]) and "template" in r["reason"]
    # no repetition information: the same link is judged on its own context and stays conservative (it is NOT called sitewide)
    alone = bc.classify_page([T], pages[3], f[0], f.content)[0][1]
    assert alone["link_class"] == "unknown"


def test_same_destination_through_url_variations_counts_as_one():
    keys = {bc.dest_key(u) for u in ("http://codigix.co", "https://codigix.co/", "https://www.codigix.co/?utm_source=x&utm_medium=y", "https://codigix.co/#top", "HTTPS://CODIGIX.CO/index.html")}
    assert keys == {"codigix.co/"}
    assert bc.dest_key("https://codigix.co/a/") == bc.dest_key("http://www.codigix.co/a") != bc.dest_key("https://codigix.co/b")
    tr = bc.RepetitionTracker(["codigix.co"])
    variants = ["http://codigix.co", "https://codigix.co/", "https://www.codigix.co/?utm_source=a", "https://codigix.co/#x"]
    for i, v in enumerate(variants):
        u = f"https://shop.example.org/p{i}"
        f = engine.page_facts(BeautifulSoup(page(f"<main><p>{FILL}{FILL}</p><p>Credit <a href='{v}'>Codigix</a> thanks.</p></main>"), "lxml"), u)
        tr.observe("example.org", u, f[0])
    assert tr.context_for("example.org", "https://codigix.co/", {"sentence": ""}).pages_with_link == 4


def test_observe_is_idempotent_for_a_retried_page():
    tr = bc.RepetitionTracker(["codigix.co"])
    f = engine.page_facts(BeautifulSoup(page(f"<footer><a href='{CODIGIX}'>c</a></footer>"), "lxml"), "https://a.example.org/1")
    for _ in range(3):
        tr.observe("example.org", "https://a.example.org/1", f[0])
    assert tr.context_for("example.org", CODIGIX, {}).pages_with_link == 1 and tr.context_for("example.org", CODIGIX, {}).pages_seen == 1


# ====================================================================== client credit
@pytest.mark.parametrize("wording", ["Designed by", "Website Designed by", "Developed by", "Powered by", "Site by", "Created by", "Web Design by", "Built and hosted by", "Design By"])
def test_credit_wording_in_the_global_footer(wording):
    r = one(page(f"<main><p>{FILL}{FILL}</p></main><footer><p>© 2026 Some Clinic. All rights reserved. {wording} <a href='{CODIGIX}'>Codigix Infotech</a></p></footer>"))
    check(r, "client_credit", 0.85, candidate=False)
    assert any("credit wording" in e for e in r["evidence"]) and "client_relationship: unconfirmed" in " ".join(r["evidence"])


def test_logo_credit_needs_a_confirmed_relationship():
    html = page(f"<main><p>{FILL}{FILL}</p></main><div class='elementor elementor-67 elementor-location-footer'><div class='elementor-widget-image'><a href='{CODIGIX}'><img src='c.png' alt='Codigix Infotech'></a></div></div>")
    unconfirmed = one(html, url="https://sanskruti.example.org/post")
    check(unconfirmed, "sitewide", 0.8)  # a logo in the footer: global, but nothing says it is a development credit
    t = bc.Target("codigix.co", client_domains=frozenset({"example.org"}))
    confirmed = one(html, url="https://sanskruti.example.org/post", target=t)
    check(confirmed, "client_credit", 0.85)
    assert "client_relationship: confirmed" in confirmed["evidence"]


def test_confirmed_relationship_raises_confidence_but_wording_is_still_required_for_content_links():
    t = bc.Target("codigix.co", client_domains=frozenset({"clinic.example.org"}))
    foot = f"<main><p>{FILL}{FILL}</p></main><footer>Website Designed by <a href='{CODIGIX}'>Codigix</a></footer>"
    c1 = one(page(foot), url="https://clinic.example.org/a", target=t)
    c2 = one(page(foot), url="https://other.example.net/a")
    assert c1["link_class"] == c2["link_class"] == "client_credit" and c1["confidence"] > c2["confidence"]
    content = article(f"<p>For this treatment page the clinic worked with <a href='{CODIGIX}'>Codigix</a> on the patient journey and the booking flow last year, which helped.</p>")
    r = one(content, url="https://clinic.example.org/blog/1", target=t)
    check(r, "unknown", 0.0, 0.6, candidate=False)  # a content link on a client site is classified on its own and is ambiguous, not a credit
    assert "client_relationship: confirmed" in r["evidence"]


def test_credit_wording_inside_article_content_is_not_a_footer_credit():
    r = one(article(f"<p>The redesigned clinic website was designed by <a href='{CODIGIX}'>Codigix Infotech</a> over the course of three months, with weekly reviews.</p>"))
    assert r["link_class"] == "unknown" and r["link_class"] != "client_credit" and "credit-style wording inside content" in r["reason"] and not r["is_editorial_candidate"]


def test_generic_web_development_anchor_is_not_a_credit():
    r = one(page(f"<main><p>{FILL}{FILL}</p></main><footer><a href='{CODIGIX}'>Web Development Company in Pune</a></footer>"))
    check(r, "sitewide")  # anchor text alone is not a development credit


# ====================================================================== profile / directory
def test_company_profile_page():
    ld = '<script type="application/ld+json">{"@type":"ProfilePage"}</script><meta property="og:type" content="profile">'
    html = (f"<html><head><title>Codigix Infotech - Company Profile</title>{ld}</head><body><main><h1>Codigix Infotech</h1><p>{FILL}{FILL}</p>"
            f"<p>Website: <a href='{CODIGIX}'>codigix.co</a></p></main></body></html>")
    r = one(html, url="https://www.crunchbase.com/organization/codigix-infotech-private-limited")
    check(r, "profile", 0.75, candidate=False)
    assert any("ProfilePage" in e for e in r["evidence"]) and any("known profile platform" in e for e in r["evidence"])


def test_person_and_author_profile_pages():
    ld = '<script type="application/ld+json">{"@type":"Person","name":"Asha Rao"}</script>'
    html = f"<html><head><title>Asha Rao - Codigix Infotech</title>{ld}</head><body><main><h1>Asha Rao</h1><p>{FILL}{FILL}</p><p>Works at <a href='{CODIGIX}'>Codigix Infotech</a>.</p></main></body></html>"
    check(one(html, url="https://www.example-network.org/in/asha-rao"), "profile", candidate=False)
    check(one(html, url="https://news.example.org/author/asha-rao"), "profile", candidate=False)


def test_business_directory_listing_and_category_pages():
    listing = lambda n: "".join(f"<div class='business-listing'><h3>Company {i}</h3><address>{i} Main Street, Pune</address><p>IT services</p>"
                                f"<a href='{CODIGIX if i == 1 else f'https://c{i}.example.com/'}'>Website</a></div>" for i in range(n))
    ld = '<script type="application/ld+json">{"@type":"ItemList","itemListElement":[]}</script>'
    r = one(f"<html><head><title>IT companies in Pune</title>{ld}</head><body><main><h1>IT companies in Pune</h1>{listing(6)}</main></body></html>", url="https://www.infobel.com/en/india/it/pune")
    check(r, "directory", 0.75, candidate=False)
    cat = one(f"<html><head><title>Category</title></head><body><main>{listing(5)}</main></body></html>", url="https://listings.example.org/directory/it-services")
    check(cat, "directory", candidate=False)


def test_a_page_with_phone_and_address_is_not_a_directory():
    r = one(article(f"<p>Our favourite agency for the clinic site is <a href='{CODIGIX}'>Codigix</a>, based in Pune, and you can reach the whole team at their office.</p>"
                    "<address>1 Main St</address><address>2 Main St</address><address>3 Main St</address><a href='tel:1'>1</a><a href='tel:2'>2</a><a href='tel:3'>3</a>"))
    check(r, "editorial", candidate=True)


def test_repeated_directory_entries_without_markup_use_page_level_listing_evidence():
    items = "".join(f"<li><a href='https://site{i}.example.com/'>Site {i}</a></li>" for i in range(1, 12))
    html = page(f"<main><h1>Links</h1><ul>{items}<li><a href='{CODIGIX}'>Codigix</a></li></ul></main>")
    r = one(html, url="https://linkdump.example.org/list")
    assert r["link_class"] == "unknown" and not r["is_editorial_candidate"]


def test_profile_and_directory_evidence_in_conflict_is_unknown():
    ld = '<script type="application/ld+json">{"@type":"ProfilePage"}</script><script type="application/ld+json">{"@type":"ItemList"}</script><meta property="og:type" content="profile">'
    listing = "".join(f"<div class='business-listing'><address>{i} Main</address></div>" for i in range(4))
    html = f"<html><head><title>Codigix Infotech - Pune IT directory</title>{ld}</head><body><main><h1>Codigix Infotech</h1><p>{FILL}{FILL}</p>{listing}<p><a href='{CODIGIX}'>Website</a></p></main></body></html>"
    r = one(html, url="https://www.infobel.com/en/profile/codigix")
    assert r["link_class"] == "unknown" and not r["is_editorial_candidate"]


# ====================================================================== own property
def test_confirmed_owned_domain_sister_company_and_internal_link():
    t = bc.Target("codigix.co", owned_domains=frozenset({"codigixinfotech.com", "sister-company.in"}))
    html = article(f"<p>Our group company shares tools with <a href='{CODIGIX}'>Codigix</a> for client work and long running support projects of every kind.</p>")
    check(one(html, url="https://www.codigixinfotech.com/blog/a", target=t), "own_property", 0.95, candidate=False)
    check(one(html, url="https://blog.sister-company.in/a", target=t), "own_property", 0.95)
    check(one(html, url="https://codigix.co/blog/a"), "own_property", 0.95)  # internal link: same domain
    check(one(html, url="https://blog.codigix.co/a"), "own_property", 0.95)  # subdomain of the target


def test_self_published_blog_on_a_shared_platform_needs_a_configured_account():
    html = article(f"<p>Healthcare marketing in Pune keeps changing, and the team at <a href='{CODIGIX}'>Codigix Infotech</a> explains the details in their guide for clinics.</p>")
    t = bc.Target("codigix.co", owned_accounts=("tumblr.com/codigixinfotech",))
    check(one(html, url="https://www.tumblr.com/codigixinfotech/tagged/seo", target=t), "own_property", 0.9)
    check(one(html, url="https://codigixinfotech.tumblr.com/post/1", target=bc.Target("codigix.co", owned_accounts=("codigixinfotech.tumblr.com",))), "own_property", 0.9)
    unrelated = one(html, url="https://www.tumblr.com/somebodyelse/post/1", target=t)
    assert unrelated["link_class"] == "editorial" and unrelated["link_class"] != "own_property"  # the platform is not owned, only the account
    assert one(html, url="https://www.tumblr.com/codigixinfotech/tagged/seo")["link_class"] == "unknown"  # nothing configured: nothing is assumed
    assert bc.describe_domain("tumblr.com", bc.Target("codigix.co", owned_domains=frozenset({"tumblr.com"})))["owned_domain"] is False  # a bare platform is never "owned"


def test_similar_brand_name_without_confirmed_ownership_is_not_own_property():
    html = article(f"<p>Honestly the best experience we had with web agencies was <a href='{CODIGIX}'>Codigix</a> which handled the clinic site migration smoothly for us.</p>")
    r = one(html, url="https://codigix-reviews.com/post/1")
    assert r["link_class"] == "unknown" and "not confirmed" in r["reason"] and r["link_class"] != "own_property" and not r["is_editorial_candidate"]
    assert bc.describe_domain("codigix-reviews.com", T)["brand_similar"] is True


# ====================================================================== unknown / other / edge cases
def test_insufficient_context_and_image_links_are_other_not_editorial():
    r1 = one(article(f"<ul><li><a href='{CODIGIX}'>Codigix</a></li></ul>"))
    check(r1, "other", candidate=False)
    r2 = one(article(f"<p><a href='{CODIGIX}'><img src='l.png' alt='Codigix logo'></a></p>"))
    check(r2, "other", candidate=False)
    r3 = one(article(f"<p>Ready to start your project? <a class='btn btn-primary' href='{CODIGIX}'>Get started</a></p>"))
    assert r3["link_class"] == "other" and not r3["is_editorial_candidate"]


def test_empty_anchor_thin_page_and_poor_extraction_are_unknown():
    thin = one(page(f"<main><p>A short note about <a href='{CODIGIX}'>Codigix</a> in just a few words.</p></main>"))
    check(thin, "unknown", 0.0, 0.5, candidate=False)
    js = one(f"<html><body><div id='root'></div><noscript>Enable JavaScript</noscript><a href='{CODIGIX}'>Codigix</a></body></html>")
    check(js, "unknown", 0.0, 0.4, candidate=False)
    assert any("page_extraction" in e for e in js["evidence"])


def test_unknown_placement_is_never_assumed_editorial_or_sitewide():
    html = page(f"<main><p>{FILL}{FILL}</p></main><div><ul><li>Our partner list: <a href='{CODIGIX}'>Codigix</a> provides several useful services for clinics and hospitals nearby.</li></ul></div>")
    r = one(html)
    assert r["position"] == "unknown" and r["link_class"] == "unknown" and not r["is_editorial_candidate"] and "placement could not be identified" in r["reason"]


def test_missing_page_evidence_cannot_produce_editorial():
    link = (CODIGIX, "Codigix", None, "html", {"position": "content", "link_context": {"context_quality": "sufficient", "sentence": "a b c d e f g h i j k l m n", "text_before": "a b c d e f g h i j",
                                                                                           "block_kind": "paragraph"}})
    r = bc.classify_link(T, "https://blog.example.org/x", link, None)
    check(r, "unknown", 0.0, 0.3, candidate=False)
    r2 = bc.classify_link(T, {"url": "https://blog.example.org/x"}, {"dest": CODIGIX, "anchor": "Codigix", "extra": link[4]}, {})
    assert r2["link_class"] == "unknown"


def test_non_target_and_non_anchor_links_are_other():
    f = engine.page_facts(BeautifulSoup(article("<p>See <a href='https://elsewhere.org/'>elsewhere</a> for more details about many other things and topics.</p>"), "lxml"), "https://b.example.org/1")
    assert bc.classify_page([T], "https://b.example.org/1", f[0], f.content) == []  # not a link to the target: not classified at all
    r = bc.classify_link(T, "https://b.example.org/1", f[0][0], f.content)
    check(r, "other", 0.9)
    canon = ("https://codigix.co/", "", None, "canonical", {})
    check(bc.classify_link(T, "https://b.example.org/1", canon, f.content), "other", 0.9)


def test_generic_page_with_many_links_and_conflicting_page_types_stay_unknown_or_low():
    ld = '<script type="application/ld+json">{"@type":["Organization","BlogPosting","ItemList"]}</script><meta property="og:type" content="website">'
    others = "".join(f"<a href='https://x{i}.example.net/'>x{i}</a> " for i in range(30))
    r = one(article(f"<p>{others}Contact the people at <a href='{CODIGIX}'>Codigix</a> whenever you need a new website or maintenance for the clinic and the hospital.</p>", ld),
            url="https://hub.example.org/")
    check(r, "other", candidate=False)


def test_thresholds_are_configurable():
    html = article(f"<p>Our favourite agency for clinic websites is <a href='{CODIGIX}'>Codigix</a>, and the team has helped us many times in the past years.</p>")
    strict = bc.Thresholds(editorial_candidate_min=0.99)
    r = one(html, thresholds=strict)
    assert r["link_class"] == "editorial" and not r["is_editorial_candidate"]
    assert one(html, thresholds=bc.Thresholds(min_context_words=60))["link_class"] == "unknown"


# ====================================================================== multi-link pages
def test_one_page_four_links_four_classifications():
    html = page(f"<header class='site-header'><nav class='primary-nav'><a href='{CODIGIX}services'>Services</a></nav></header>"
                f"<main><article><h1>Partners</h1><p>{FILL}</p><p>When our hospital needed a patient portal we asked several agencies and in the end we hired "
                f"<a href='{CODIGIX}case'>Codigix Infotech</a> because they listened carefully to what the doctors needed.</p><ul><li><a href='{CODIGIX}directory'>Codigix</a></li></ul>"
                f"</article></main><footer><p>© 2026 Hospital. Website designed by <a href='{CODIGIX}'>Codigix Infotech</a></p></footer>")
    got = {d.removeprefix(CODIGIX) or "home": r for d, r in classify(html)}
    assert got["services"]["link_class"] == "sitewide" and got["case"]["link_class"] == "editorial" and got["directory"]["link_class"] == "other" and got["home"]["link_class"] == "client_credit"
    assert [got[k]["position"] for k in ("services", "case", "directory", "home")] == ["nav", "content", "content", "footer"]


# ====================================================================== the audited real-world structures
MORAYA = ("<body><div class='theme-layout'><nav><a href='/'>Home</a></nav></div><section class='cols'><p>%s</p></section><footer></footer>"
          f"<div class='bottom-footer'><div class='container'><p>© Moraya Hospital | Website Designed by <a href='{CODIGIX}'>Codigix infotech Pvt Ltd</a></p></div></div></body>" % (FILL * 2))
KITCHEN = (f"<body><div id='page'><main class='main-content'><header class='header js-header-sticky'><div class='mobile__footer'><div class='mobile__copyright'><p>© Kitchen Canvas. Design By "
           f"<a href='{CODIGIX}'>Codigix Infotech</a></p></div></div></header><div id='content'><p>{FILL}{FILL}</p><footer class='footer'><div class='footer__bottom_text'>© 2026 kitchen Canvas. All rights reserved. Design By "
           f"<a href='{CODIGIX}'>Codigix Infotech</a></div></footer></div></main></div></body>")
SANSKRUTI = ("<body class='single-post'><div class='elementor elementor-2473'><p>%s</p></div><div class='elementor elementor-67 elementor-location-footer'><div class='e-con-inner'>"
             f"<div class='elementor-widget-image'><a href='http://codigix.co'><img src='c.png' alt='Codigix Infotech'></a></div></div></div></body>" % (FILL * 2))
CORPLEGAL = (f"<body><div id='page'><div><p>{FILL}{FILL}</p></div><footer id='colophon' role='contentinfo'><div class='footer-width-fixer'><div class='elementor'>"
             "<div class='elementor-widget-image'><a href='http://codigix.co'><img alt=''></a></div></div></div></footer></div></body>")


def test_codigix_audit_sites_without_relationship_configuration():
    check(one(MORAYA, url="https://morayamultispecialitycharitablehospital.com/treatments.html"), "client_credit", 0.85, candidate=False)
    kc = classify(KITCHEN, url="https://kitchen-canvas.in/")
    assert len(kc) == 2 and all(r["link_class"] == "client_credit" for _, r in kc)  # header mobile-menu credit and the desktop footer credit, both with credit wording / footer
    # logo-only credits carry no wording: without a confirmed relationship they are only sitewide
    check(one(SANSKRUTI, url="https://sanskrutiagrotourism.com/picnic-venue-near-pune/"), "sitewide", 0.85)
    check(one(CORPLEGAL, url="https://corplegalsolutions.in/"), "sitewide", 0.85)


def test_codigix_audit_sites_with_confirmed_clients():
    t = bc.Target("codigix.co", client_domains=frozenset({"sanskrutiagrotourism.com", "corplegalsolutions.in", "kitchen-canvas.in"}))
    check(one(SANSKRUTI, url="https://sanskrutiagrotourism.com/picnic-venue-near-pune/", target=t), "client_credit", 0.85)
    check(one(CORPLEGAL, url="https://corplegalsolutions.in/", target=t), "client_credit", 0.85)
    for _, r in classify(KITCHEN, url="https://kitchen-canvas.in/", target=t):
        check(r, "client_credit", 0.9)


def test_growthpixel_and_brainmine_footer_header_and_nav_links_are_not_editorial():
    gp = bc.Target("growthpixelagency.com")
    foot = one(page(f"<main><p>{FILL}{FILL}</p></main><div class='footer-box'><p>Copyright 2026 Dr Clinic. <a href='https://growthpixelagency.com/'>GrowthPixel Pvt Ltd</a></p></div>"),
               url="https://daaskdesign.com/", target=gp)
    check(foot, "sitewide", 0.85, candidate=False)
    bm = bc.Target("brainminetech.com")
    nav = one(page(f"<header class='site-header'><ul class='nav'><li><a href='https://www.brainminetech.com/'>Brainmine</a></li></ul></header><main><p>{FILL}{FILL}</p></main>"),
              url="https://brainmine.co.in/", target=bm)
    check(nav, "sitewide", candidate=False)
    both = classify(article("<p>Reviewing providers of healthcare websites we found that <a href='https://www.brainminetech.com/seo'>Brainmine</a> documents its process in unusual detail "
                            "for prospective customers.</p>", extra_body="<footer><a href='https://www.brainminetech.com/'>Brainmine</a></footer>"), target=bm)
    got = dict(both)
    assert got["https://www.brainminetech.com/seo"]["link_class"] == "editorial" and got["https://www.brainminetech.com/"]["link_class"] == "sitewide"


def test_ruby_hospital_style_wordpress_article_vs_directory_label():
    rh = bc.Target("rubyhospital.com")
    art = one(article("<p>A bone marrow transplant in Kolkata is offered by a small number of tertiary centres, among them <a href='https://www.rubyhospital.com/24-hour-emergency'>24 hour emergency "
                      "and trauma</a> care at Ruby General Hospital, which combines haematology and intensive care.</p>"), url="https://someone.wordpress.com/2026/05/bone-marrow", target=rh)
    check(art, "editorial", 0.7)  # hosted on a shared platform, but the account is not owned and the link has a real sentence
    label = one(page("<main><h1>Hospitals</h1><p>Hospital lists for Kolkata.</p><p>Website: <a href='https://www.rubyhospital.com/'>https://www.rubyhospital.com/</a></p></main>"),
                url="https://swheal.com/hospital/ruby", target=rh)
    assert label["link_class"] == "unknown" and not label["is_editorial_candidate"]


# ====================================================================== pipeline integration (real crawl, scratch schema)
def _crawl(env, html, classifier, **kw):
    SITES["a.localhost"] = {"/": (200, "text/html", html)}
    c = cfg(target_domain="codigix.co", max_depth=0, **kw)
    c.classifier = classifier  # per-crawl attribute (not a dataclass field)
    asyncio.run(engine.run_crawl(c, [env("a")]))
    return c


def test_classifier_runs_inside_the_real_crawl_without_touching_storage(env):
    clf = bc.PageClassifier()
    html = page(f"<main><article><h1>T</h1><p>{FILL}{FILL}</p><p>The hospital chose <a href='{CODIGIX}case'>Codigix Infotech</a> after comparing agencies for the patient portal.</p></article></main>"
                f"<footer>Website Designed by <a href='{CODIGIX}'>Codigix Infotech</a></footer><a href='https://other.org/'>other</a>")
    c = _crawl(env, html, clf)
    (url, res), = clf.results.items()
    classes = sorted(r["link_class"] for _, r in res)
    assert classes == ["client_credit", "editorial"] and len(res) == 2  # the link to other.org is not a link to the monitored target
    assert url.startswith("http://a.localhost")
    cols = {r["column_name"] for r in rows("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='outgoing_links'")}
    assert not cols & {"link_class", "classification", "link_class_confidence"}  # nothing is persisted in Phase 3
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE link_type='html'")[0]["n"] == 3 and "classifier" not in engine.config_dict(c)  # storage and config dicts unchanged


def test_a_failing_classifier_never_aborts_the_page_transaction(env):
    class Boom:
        def classify_page_facts(self, *a, **k):
            raise RuntimeError("boom")
    _crawl(env, page(f"<main><p>{FILL}{FILL}</p></main><footer><a href='{CODIGIX}'>c</a></footer>"), Boom())
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url LIKE 'http://a.localhost%'")[0]["crawl_status"] == "completed"
    assert rows("SELECT COUNT(*) n FROM outgoing_links WHERE link_type='html'")[0]["n"] == 1


def test_a_failing_evidence_sink_no_longer_aborts_the_page(env, monkeypatch):
    monkeypatch.setattr(engine, "EVIDENCE_SINK", lambda *a: (_ for _ in ()).throw(RuntimeError("sink")))
    _crawl(env, page(f"<main><p>{FILL}{FILL}</p></main>"), None)
    assert rows("SELECT crawl_status FROM discovered_urls WHERE url LIKE 'http://a.localhost%'")[0]["crawl_status"] == "completed"


def test_playwright_rendered_page_is_classified_from_the_rendered_html(env):
    clf = bc.PageClassifier()
    js = ("<html><head><title>Shell</title></head><body><div id='root'></div><script>document.getElementById('root').innerHTML="
          "'<main><article><h1>Rendered</h1><p>" + (FILL * 2).replace("'", "") + " We hired <a href=\"https://codigix.co/js\">Codigix Infotech</a> for the portal and the support work afterwards.</p></article></main>'</script></body></html>")
    _crawl(env, js, clf, use_playwright=True)
    (url, res), = clf.results.items()
    assert [r["link_class"] for _, r in res] == ["editorial"] and res[0][1]["is_editorial_candidate"]


def test_classifiers_are_isolated_per_crawl_and_threadsafe():
    a, b = bc.PageClassifier(relationships={"codigix.co": bc.Target("codigix.co", client_domains=frozenset({"x.example.org"}))}), bc.PageClassifier()
    html = page(f"<main><p>{FILL}{FILL}</p></main><footer><a href='{CODIGIX}'><img src='logo.png' alt='Codigix'></a></footer>")

    def run(clf, n):
        for i in range(n):
            u = f"https://x.example.org/p{i}"
            clf.classify_page_facts(u, engine.page_facts(BeautifulSoup(html, "lxml"), u), ["codigix.co"])
    ts = [threading.Thread(target=run, args=(c, 40)) for c in (a, b, a, b)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(a.results) == len(b.results) == 40
    assert {r["link_class"] for res in a.results.values() for _, r in res} == {"client_credit"} and {r["link_class"] for res in b.results.values() for _, r in res} == {"sitewide"}
    assert a.tracker is not b.tracker


def test_load_relationships_file(tmp_path):
    p = tmp_path / "rel.json"
    p.write_text('{"targets": {"https://www.Codigix.co/": {"name": "Codigix Infotech", "brands": ["codigix"], "owned_domains": ["codigixinfotech.com"], '
                 '"owned_accounts": ["Tumblr.com/CodigixInfotech/"], "client_domains": ["clinic.example.org"]}}}', encoding="utf-8")
    t = bc.load_relationships(str(p))["codigix.co"]
    assert t.name == "Codigix Infotech" and "clinic.example.org" in t.client_domains and t.owned_accounts == ("tumblr.com/codigixinfotech",)


def test_brand_lookalike_account_on_a_shared_platform_is_unknown_until_confirmed():
    html = article(f"<p>Healthcare marketing in Pune keeps changing, and the team at <a href='{CODIGIX}'>Codigix Infotech</a> explains the details in their guide for clinics.</p>")
    for u in ("https://www.tumblr.com/codigixinfotech/post/1", "https://codigixinfotech.tumblr.com/post/1"):
        r = one(html, url=u)
        assert r["link_class"] == "unknown" and "account resembles the brand" in r["reason"] and not r["is_editorial_candidate"], u
    assert one(html, url="https://www.tumblr.com/somebodyelse/post/1")["link_class"] == "editorial"  # an unrelated account on the same platform is judged on its content
    t = bc.Target("codigix.co", owned_accounts=("tumblr.com/codigixinfotech",))
    assert one(html, url="https://www.tumblr.com/codigixinfotech/post/1", target=t)["link_class"] == "own_property"  # confirmed: ownership wins


def test_one_of_many_links_in_a_sentence_is_a_link_list_not_editorial():
    others = ", ".join(f"<a href='https://tool{i}.example.net/'>Tool{i}</a>" for i in range(4))
    r = one(article(f"<ul><li>Tools for web work: {others}, <a href='{CODIGIX}'>Codigix</a> and a few more names that people use for projects.</li></ul>"))
    assert r["link_class"] == "other" and "link list" in r["reason"] and not r["is_editorial_candidate"]
    assert one(article(f"<p>For clinic websites we use <a href='{CODIGIX}'>Codigix</a> and <a href='https://x.example.net/'>X</a>, which has served us well for several years.</p>"))["link_class"] == "editorial"


def test_category_labels_with_colon_are_not_credit_wording():
    r = one(article(f"<ul><li>Web Development: <a href='{CODIGIX}'>Codigix</a>, a long standing Pune team that builds websites and web applications for clinics.</li></ul>"))
    assert r["link_class"] == "editorial" and "credit-style" not in r["reason"]


def test_reconcile_crawl_order_retries_and_denominator():
    from itertools import permutations

    html = article(f"<p>We regularly recommend <a href='{CODIGIX}'>Codigix</a> for teams that need help maintaining complex clinic websites.</p>")
    empty = article("<p>No partner appears on this page.</p>")
    facts = [engine.page_facts(BeautifulSoup(h, "lxml"), "https://publisher.org/") for h in (html, empty)]
    urls = [f"https://publisher.org/{i}" for i in range(4)]
    expected = None
    for order in permutations(range(4)):
        emitted = []
        clf = bc.PageClassifier(on_page=lambda u, rs: emitted.append((u, rs)))
        for i in order:
            first = clf.classify_page_facts(urls[i], facts[i == 3], [T.domain])
            if i != 3:
                assert first[0][1]["provisional"]
            clf.classify_page_facts(urls[i], facts[i == 3], [T.domain])
        final = clf.finalize()
        assert all(final[urls[i]][0][1]["link_class"] == "sitewide" for i in range(3))
        assert all(not r["provisional"] for rows in final.values() for _, r in rows)
        assert final[urls[3]] == []
        assert [u for u, _ in emitted[-4:]] == sorted(urls)
        assert clf.tracker.context_for("publisher.org", CODIGIX, {}).pages_seen == 4
        assert clf.finalize() == final and len(emitted) == 12
        if expected is None:
            expected = final
        assert final == expected


def test_replaced_page_evidence_and_streaming_final_results():
    url = "https://publisher.org/"
    clf = bc.PageClassifier(keep_results=False, on_page=lambda u, rows: seen.append(rows))
    seen = []
    for h in (article(f"<footer><a href='{CODIGIX}'>Codigix</a></footer>"), article("")):
        clf.classify_page_facts(url, engine.page_facts(BeautifulSoup(h, "lxml"), url), [T.domain])
    assert clf.tracker.context_for("publisher.org", CODIGIX, {}).pages_with_link == 0
    assert clf.finalize() == {} and seen[-1] == []


def test_confirmed_client_can_publish_editorial_and_navigation_links():
    target = bc.Target("codigix.co", client_domains=frozenset({"clinic.org"}))
    html = article(f"<p>Our research cites <a href='{CODIGIX}research'>Codigix</a> for its detailed comparison of regional clinic websites over several years.</p>",
                   extra_body=f"<nav><a href='{CODIGIX}partners'>Partners</a></nav>")
    rows = dict(classify(html, url="https://clinic.org/blog", target=target))
    check(rows[CODIGIX + "research"], "editorial", candidate=True)
    check(rows[CODIGIX + "partners"], "sitewide", candidate=False)


def test_credit_phrase_for_another_party_does_not_transfer():
    html = page(f"<footer><p>Website developed by <a href='https://designer.org'>Design Studio</a> | Partner <a href='{CODIGIX}'>Codigix</a></p></footer>")
    check(one(html), "sitewide")


def test_bare_platform_account_cannot_confirm_every_publisher(tmp_path):
    path = tmp_path / "relationships.json"
    path.write_text('{"targets":{"codigix.co":{"owned_accounts":["medium.com"]}}}', encoding="utf8")
    target = bc.load_relationships(path)["codigix.co"]
    html = article(f"<p>Our research cites <a href='{CODIGIX}'>Codigix</a> for its detailed comparison of regional clinic websites over several years.</p>")
    check(one(html, url="https://medium.com/@unrelated/article", target=target), "editorial")


def test_unrelated_sentences_do_not_lend_editorial_context():
    html = article(f"<p>{FILL} Visit <a href='{CODIGIX}'>Codigix</a>.</p>")
    check(one(html), "unknown", conf_max=0.4, candidate=False)


def test_shared_publishers_do_not_supply_repetition_for_each_other():
    clf = bc.PageClassifier()
    html = article(f"<p>Our research cites <a href='{CODIGIX}'>Codigix</a> for its detailed comparison of regional clinic websites over several years.</p>")
    for account in ("science", "practice", "clinics"):
        url = f"https://medium.com/@{account}/notes"
        clf.classify_page_facts(url, engine.page_facts(BeautifulSoup(html, "lxml"), url), [T.domain])
    assert [rows[0][1]["link_class"] for rows in clf.finalize().values()] == ["editorial"] * 3


def test_latest_pages_can_weaken_template_evidence():
    clf = bc.PageClassifier()
    for i in range(6):
        url = f"https://publisher.org/{i}"
        html = article(f"<p>Our research cites <a href='{CODIGIX}'>Codigix</a> for its detailed comparison of regional clinic websites over several years.</p>" if i < 3 else "")
        rows = clf.classify_page_facts(url, engine.page_facts(BeautifulSoup(html, "lxml"), url), [T.domain])
        if i == 2:
            assert rows[0][1]["link_class"] == "sitewide" and rows[0][1]["provisional"]
    assert [r["link_class"] for rows in clf.finalize().values() for _, r in rows] == ["editorial"] * 3


def test_daask_live_footer_wording():
    # Minimal transcription of the footer link fetched from https://daaskdesign.com/ during the audit.
    footer = "<footer><p>Copyright AT DAASK CONSTRUCTION PVT LTD 2026 | Design &amp; Developed By <a href='https://growthpixelagency.com/'>GrowthPixel Pvt Ltd</a></p></footer>"
    check(one(page(footer), url="https://daaskdesign.com/", target=bc.Target("growthpixelagency.com")), "client_credit", 0.88, 0.88)


def test_short_direct_footer_credit_without_copyright():
    check(one(page(f"<footer>Designed by <a href='{CODIGIX}'>Codigix</a></footer>")), "client_credit")


def test_context_itself_must_repeat_often_enough():
    clf = bc.PageClassifier()
    for i in range(5):
        url = f"https://publisher.org/{i}"
        lead = "Our research cites" if i < 2 else "Historical records credit"
        html = article(f"<p>{lead} <a href='{CODIGIX}'>Codigix</a> for its detailed comparison of regional clinic websites over several years.</p>" if i < 3 else "")
        clf.classify_page_facts(url, engine.page_facts(BeautifulSoup(html, "lxml"), url), [T.domain])
    assert [r["link_class"] for rows in clf.finalize().values() for _, r in rows] == ["editorial"] * 3

"""Re-implementation of the PPCBlogPro SEO / AEO / GEO checker (48 weighted pass/fail checks).

Score per pillar = passed weight / total weight; overall = mean of the three pillars.
Weights and check names come from the checker's own report; the pass/fail rules are
heuristics tuned until they reproduced that report for codigix.co.
"""
import json
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

GENERIC_ANCHORS = {"", "click here", "read more", "learn more", "more", "here", "link", "this", "view", "view more",
                   "know more", "get started", "contact", "submit", "go", "see more", "details"}
QUESTION_START = re.compile(r"^(what|how|why|when|where|who|which|can|should|is|are|do|does|will)\b", re.I)


def _schema_nodes(soup):
    nodes = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except ValueError:
            continue
        stack = [data]
        while stack:
            n = stack.pop()
            if isinstance(n, list):
                stack += n
            elif isinstance(n, dict):
                nodes.append(n)
                stack += [v for v in n.values() if isinstance(v, (dict, list))]
    return nodes


def _types(nodes):
    out = []
    for n in nodes:
        t = n.get("@type")
        out += t if isinstance(t, list) else [t] if t else []
    return out


def analyze(html: str, url: str, topic: str = "") -> dict:
    soup = BeautifulSoup(html, "lxml")
    host = urlparse(url).netloc.removeprefix("www.")
    meta = lambda **a: (soup.find("meta", attrs=a) or {}).get("content", "") or ""
    title = soup.title.get_text(strip=True) if soup.title else ""
    desc = meta(name="description")

    types = _types(_schema_nodes(soup))
    nodes = _schema_nodes(soup)
    for s in soup(["script", "style", "noscript", "template"]):
        s.decompose()
    body = soup.body or soup
    text = body.get_text(" ", strip=True)
    low = text.lower()
    words = len(text.split())
    h = [(int(t.name[1]), t.get_text(" ", strip=True)) for t in body.find_all(re.compile(r"^h[1-6]$"))]
    h1s = [t for lv, t in h if lv == 1]
    h2n = sum(1 for lv, _ in h if lv == 2)
    skips = any(b - a > 1 for (a, _), (b, _) in zip(h, h[1:]))
    paras = [p.get_text(" ", strip=True) for p in body.find_all("p")]
    paras = [p for p in paras if p]
    avg_para = sum(len(p.split()) for p in paras) / len(paras) if paras else 0
    long_paras = sum(len(p.split()) > 150 for p in paras)

    internal, external, anchors = set(), set(), []
    for a in body.find_all("a", href=True):
        href = a["href"].strip()
        label = a.get_text(" ", strip=True) or a.get("aria-label", "") or a.get("title", "")
        anchors.append(label.lower())
        if href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        h_host = urlparse(href).netloc.removeprefix("www.")
        (internal if not h_host or h_host == host else external).add(href)
    descriptive = sum(1 for t in anchors if t not in GENERIC_ANCHORS and len(t.split()) >= 2)

    imgs = body.find_all("img")
    alt_ok = sum(1 for i in imgs if (i.get("alt") or "").strip())
    dim_ok = sum(1 for i in imgs if i.get("width") and i.get("height"))
    q_heads = [(i, t) for i, (_, t) in enumerate(h) if t.endswith("?") or QUESTION_START.match(t)]
    answered = 0
    for tag in body.find_all(re.compile(r"^h[1-6]$")):
        t = tag.get_text(" ", strip=True)
        if t.endswith("?") or QUESTION_START.match(t):
            nxt = tag.find_next(["p", "ul", "ol", "table"])
            if nxt and len(nxt.get_text(" ", strip=True).split()) >= 15:
                answered += 1
    head_text = " ".join(text.split()[:150]).lower()
    has = lambda pat, s=low: re.search(pat, s) is not None
    summary_sig = has(r"\b(in short|tl;?dr|in summary|key takeaway|bottom line|the short answer|quick answer|at a glance)\b", head_text)
    audience_sig = has(r"\b(who (this|it) is for|is for|ideal for|designed for|best for|perfect for|use cases?|suited for)\b")
    lists = len(body.find_all(["ul", "ol"]))
    tables = len(body.find_all("table"))
    og = {k: bool(meta(property=f"og:{k}")) for k in ("title", "description", "image")}
    robots = meta(name="robots").lower()
    path = urlparse(url).path or "/"
    t_words = {w for w in re.findall(r"[a-z]{4,}", title.lower())}
    h_words = {w for w in re.findall(r"[a-z]{4,}", (h1s[0] if h1s else "").lower())}
    topic_l = topic.lower().strip()
    topic_hits = [x for x in (title, h1s[0] if h1s else "", desc, head_text) if topic_l and topic_l in x.lower()]
    type_set = set(types)
    has_org = bool(type_set & {"Organization", "Person", "LocalBusiness"})

    P = lambda ok, d: (bool(ok), d)
    seo = [
        ("Title length", 10, P(30 <= len(title) <= 60, f"{len(title)} characters")),
        ("Meta description", 10, P(110 <= len(desc) <= 165, f"{len(desc)} characters")),
        ("HTTPS URL", 5, P(url.startswith("https://"), url)),
        ("Readable URL slug", 5, P(len(path) < 100 and "?" not in path, path)),
        ("One clear H1", 8, P(len(h1s) == 1, f"H1 count: {len(h1s)}")),
        ("Title and H1 alignment", 6, P(len(t_words & h_words) >= 2 and title.strip().lower() != (h1s[0].lower() if h1s else ""),
                                        f"shared words: {sorted(t_words & h_words)}")),
        ("Main topic placement", 12, P(not topic_l or len(topic_hits) >= 2, "no topic given" if not topic_l else f"{len(topic_hits)}/4 places")),
        ("Enough content depth", 10, P(words >= 300, f"{words} words")),
        ("Useful heading structure", 8, P(h2n >= 2 and not skips, f"H2 count {h2n}; skips levels: {skips}")),
        ("Scannable paragraphs", 6, P(avg_para <= 40 and long_paras == 0, f"avg {avg_para:.0f} words; long {long_paras}")),
        ("Internal links", 8, P(len(internal) >= 1, f"{len(internal)} unique")),
        ("External/source links", 6, P(len(external) >= 1, f"{len(external)} unique")),
        ("Descriptive anchor text", 5, P(anchors and descriptive / len(anchors) >= 0.7, f"{descriptive}/{len(anchors)}")),
        ("Image alt text", 8, P(not imgs or alt_ok / len(imgs) >= 0.8, f"{alt_ok}/{len(imgs)}")),
        ("Image dimensions", 4, P(not imgs or dim_ok / len(imgs) >= 0.8, f"{dim_ok}/{len(imgs)}")),
        ("Canonical URL", 6, P(soup.find("link", rel="canonical") is not None, "canonical tag")),
        ("Indexable page", 6, P("noindex" not in robots, robots or "no robots meta")),
        ("Mobile viewport", 5, P(bool(meta(name="viewport")), meta(name="viewport"))),
        ("Language attribute", 4, P(bool(soup.html and soup.html.get("lang")), soup.html.get("lang", "") if soup.html else "")),
        ("Social preview metadata", 4, P(all(og.values()), str(og))),
        ("Breadcrumb schema", 4, P("BreadcrumbList" in type_set, "BreadcrumbList")),
        ("Page schema", 8, P(bool(type_set), ", ".join(sorted(type_set)))),
    ]
    aeo = [
        ("Question-style headings", 16, P(len(q_heads) >= 1, f"{len(q_heads)} found")),
        ("Answers after questions", 14, P(q_heads and answered / len(q_heads) >= 0.5, f"{answered}/{len(q_heads)}")),
        ("Direct answer signals", 16, P(has(r"\b(the answer is|simply put|in simple terms|refers to|is defined as|yes,|no,)\b"), "")),
        ("Top summary answer", 12, P(summary_sig, "")),
        ("Definition support", 8, P(has(r"\b(is defined as|refers to|\w+ means|is the process of|is a type of)\b"), "")),
        ("FAQ section", 14, P(has(r"\bfaqs?\b|frequently asked"), "")),
        ("Lists or steps", 14, P(lists >= 1, f"{lists} lists")),
        ("Conversational query coverage", 12, P(has(r"\b(what is|what are|how to|how do|should i|can i)\b"), "")),
        ("Key takeaway or summary", 12, P(summary_sig, "")),
        ("Structured answer support", 10, P(lists + tables >= 1, f"tables {tables}, lists {lists}")),
        ("Comparison or decision support", 8, P(has(r"\b(vs\.?|versus|compare|comparison|choose|difference|alternatives?|pros and cons|best|top|better)\b"), "")),
        ("Audience and use-case clarity", 8, P(audience_sig, "")),
        ("FAQ or HowTo schema", 12, P(bool(type_set & {"FAQPage", "HowTo"}), "")),
    ]
    geo = [
        ("Entity clarity", 14, P(has(r"\b(about us|our team|our company|we are|founded|our agency)\b"), "")),
        ("Audience and use-case clarity", 10, P(audience_sig, "")),
        ("Ownership signal", 12, P(has_org, "")),
        ("Freshness or page schema", 10, P(bool(type_set & {"WebPage", "Article", "BlogPosting"}) or any("dateModified" in n for n in nodes), "")),
        ("Original experience", 16, P(has(r"\b(our experience|we have|we've|years of experience|case stud(y|ies)|in our)\b"), "")),
        ("Proof and examples", 12, P(has(r"\b(case stud(y|ies)|for example|testimonials?|results|proven|clients?)\b"), "")),
        ("Data and statistics", 10, P(has(r"\d+\s?%|\b\d{2,}\+|\b(report|benchmark|statistics)\b"), "")),
        ("Credible citations", 14, P(len(external) >= 1, f"{len(external)} external")),
        ("Organization identity schema", 10, P(has_org, "")),
        ("Entity disambiguation", 8, P(any(n.get("sameAs") for n in nodes), "")),
        ("Page / organization schema", 12, P(bool(type_set), "")),
        ("Clear summary for AI extraction", 12, P(summary_sig, "")),
        ("Depth and topical coverage", 10, P(words >= 300 and h2n >= 2, f"{words} words, {h2n} H2")),
    ]

    out = {"url": url, "words": words}
    for name, checks in (("seo", seo), ("aeo", aeo), ("geo", geo)):
        total = sum(w for _, w, _ in checks)
        got = sum(w for _, w, (ok, _) in checks if ok)
        out[name] = {
            "score": round(100 * got / total),
            "passed": f"{sum(ok for _, _, (ok, _) in checks)}/{len(checks)}",
            "checks": [{"name": n, "weight": w, "status": "PASS" if ok else "FIX", "detail": d} for n, w, (ok, d) in checks],
        }
    out["overall"] = round(sum(out[p]["score"] for p in ("seo", "aeo", "geo")) / 3)
    return out

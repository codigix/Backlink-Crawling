"""serper.dev as a search backend: result mapping, empty vs failure, the daily credit limit, backend order. No network: the HTTP call is replaced."""
import pytest

import ddg_lookup as dl


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "test-key-not-real")
    monkeypatch.setattr(dl, "_serper_day", ["", 0])
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)


def test_results_are_mapped_and_page_is_sent(monkeypatch):
    sent = []
    monkeypatch.setattr(dl, "_serper_post", lambda p: sent.append(p) or {"organic": [{"link": "https://a.example/p", "title": "t", "snippet": "s"}, {"title": "no link"}]})
    assert dl._page('"x.com" -site:x.com', 3, 2, backend="serper") == [{"href": "https://a.example/p", "title": "t", "body": "s"}]
    assert sent == [{"q": "x.com -site:x.com", "page": 3, "num": 10, "gl": "in"}]  # quotes dropped: a free account refuses them
    monkeypatch.setenv("SERPER_QUOTES", "1")
    dl._page('"x.com" -site:x.com', 1, 2, backend="serper")
    assert sent[-1]["q"] == '"x.com" -site:x.com'  # paid plan: exact match kept


def test_no_organic_is_an_empty_page_and_an_error_is_a_failure(monkeypatch):
    monkeypatch.setattr(dl, "_serper_post", lambda p: {})
    assert dl._page("q", 9, 4, backend="serper") == []  # Google has no more results: empty, not an error
    def boom(p): raise TimeoutError("slow")
    monkeypatch.setattr(dl, "_serper_post", boom)
    with pytest.raises(TimeoutError):
        dl._page("q", 1, 2, backend="serper")  # a failure is never an empty page


def test_refused_key_and_daily_limit_stop_at_once_without_retries(monkeypatch):
    calls = []
    def refuse(p): calls.append(1); raise dl.SerperStop("serper refused the request (HTTP 401)")
    monkeypatch.setattr(dl, "_serper_post", refuse)
    with pytest.raises(dl.SerperStop):
        dl._page("q", 1, 4, backend="serper")
    assert len(calls) == 1  # no backoff loop on a bad key
    monkeypatch.setattr(dl, "SERPER_DAILY_LIMIT", 2)
    monkeypatch.setattr(dl, "_serper_day", ["", 0])
    monkeypatch.setattr(dl, "_serper_post", lambda p: {"organic": []})
    dl._page("q", 1, 1, backend="serper"), dl._page("q", 2, 1, backend="serper")
    with pytest.raises(dl.SerperStop, match="daily limit"):
        dl._page("q", 3, 1, backend="serper")


def test_key_never_appears_in_an_error(monkeypatch):
    import httpx
    monkeypatch.setattr(httpx, "post", lambda *a, **k: httpx.Response(500, request=httpx.Request("POST", a[0])))
    with pytest.raises(httpx.HTTPStatusError) as e:
        dl._page("q", 1, 1, backend="serper")
    assert "test-key-not-real" not in str(e.value)


def test_backend_order_is_dogpile_then_ddgs_then_serper(monkeypatch):
    monkeypatch.setenv("DOGPILE", "1")
    monkeypatch.setenv("DDGS_BACKENDS", "bing,yahoo")
    assert dl._backends_from_env() == ["dogpile", "bing", "yahoo", "serper"]
    monkeypatch.setenv("DDGS_BACKENDS", "dogpile,serper,bing")
    assert dl._backends_from_env() == ["dogpile", "serper", "bing"]  # not added twice
    monkeypatch.delenv("SERPER_API_KEY")
    monkeypatch.setenv("DDGS_BACKENDS", "bing,yahoo")
    assert dl._backends_from_env() == ["dogpile", "bing", "yahoo"]
    monkeypatch.setenv("DOGPILE", "0")
    assert dl._backends_from_env() == ["bing", "yahoo"]


def test_dogpile_redirect_is_unwrapped():
    assert dl._dogpile_url("https://www.dogpile.com/r?ru=https%3A%2F%2Fa.com%2Fx") == "https://a.com/x"
    assert dl._dogpile_url("https://b.com/y") == "https://b.com/y"


def test_adult_results_are_dropped_and_ordinary_sites_kept():
    run, found = dl.Run(persist=False), {}
    hrefs = ["https://www.xnxx.com/hits/1", "https://mobile-xvideos.com/tags/x", "https://anyporn.com/c/", "https://www.redtube.com/1",
             "https://www.essexlive.news/a", "https://blog.example.com/post", "https://shop.x.com/self"]
    assert dl._collect(run, hrefs, "qh", "x.com", found, set()) == 2
    assert list(found) == ["https://www.essexlive.news/a", "https://blog.example.com/post"]  # own site and 18+ sites never become candidates


def test_tests_never_reach_the_real_serper(monkeypatch):
    """A developer .env with a real key: conftest blanks it before import, so serper is not a backend, and a real request fails the test."""
    assert "serper" not in dl.BACKENDS  # computed at import, after conftest blanked the key
    monkeypatch.setenv("SERPER_API_KEY", "")  # the state every other test runs in (this file's fixture set a dummy key)
    with pytest.raises(pytest.fail.Exception, match="real serper.dev request"):
        dl._serper_post({"q": "x"})

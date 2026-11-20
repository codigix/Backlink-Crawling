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


def test_serper_leads_the_backends_only_when_a_key_exists(monkeypatch):
    monkeypatch.setenv("DDGS_BACKENDS", "bing,yahoo")
    assert dl._backends_from_env() == ["serper", "bing", "yahoo"]
    monkeypatch.setenv("DDGS_BACKENDS", "serper,bing")
    assert dl._backends_from_env() == ["serper", "bing"]  # not added twice
    monkeypatch.delenv("SERPER_API_KEY")
    monkeypatch.setenv("DDGS_BACKENDS", "bing,yahoo")
    assert dl._backends_from_env() == ["bing", "yahoo"]

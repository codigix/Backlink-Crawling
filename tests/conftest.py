import os

import pytest

# Read BEFORE crawler_core loads .env: there DATABASE_URL is the SSH tunnel to production (:5433). Tests create and drop schemas, so they
# need an explicitly set local scratch database.
if not os.environ.get("DATABASE_URL") or ":5433" in os.environ["DATABASE_URL"]:
    pytest.exit("set DATABASE_URL to a local scratch database (e.g. postgresql://postgres:postgres@localhost:5432/phase1_api_test); "
                ".env points at the production tunnel", returncode=2)

# .env may say USE_CELERY=1 (production); tests must never push work into the real Redis queue. Celery tests set it themselves.
# Several tests simulate N workers as N threads of ONE process, each holding a connection (a barrier, an advisory lock plus a nested claim). In production
# every worker is its own process with its own pool; the test process gets the per-process capacity these tests were written against (the old cap of 20).
os.environ.setdefault("DB_POOL_MAX", "20")
os.environ["USE_CELERY"] = "0"
os.environ["DOGPILE"] = "0"  # tests fake the search engines; none may start a real browser (the dogpile tests turn it back on)
# .env holds a real, paid serper.dev key; set (empty) here, load_dotenv never overrides it, so serper is not among the backends.
# Serper tests set a dummy key and fake the HTTP call themselves.
os.environ["SERPER_API_KEY"] = ""


@pytest.fixture(autouse=True)
def _no_real_serper(monkeypatch):
    """Fail fast instead of spending credits: a test that reaches serper.dev for real fails (pytest.fail is not swallowed by `except Exception`)."""
    import httpx
    real = httpx.post

    def guarded(url, *a, **kw):
        if "serper.dev" in str(url):
            pytest.fail(f"a test tried a real serper.dev request ({url}); fake ddg_lookup._serper_post instead")
        return real(url, *a, **kw)
    monkeypatch.setattr(httpx, "post", guarded)

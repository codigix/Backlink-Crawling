"""Phase 8: db_indexes.py fails closed. These tests never open a database connection: `connect` is a recorder and `apply` is replaced, so they prove only
what the guard lets through, and (with a subprocess) that the real command line refuses before connecting. The target strings are mock configurations."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

import crawler_core as core
import db_indexes as di

PASSWORD = "s3cr3t-Pass"
RAW_AT_PASSWORD = "s3cr3t-P@ss!"  # an unescaped @ in a URL: libpq reads the part after it as the host
LOCAL = f"postgresql://idxuser:{PASSWORD}@db1.example.test:5432/appdb"
TUNNEL = f"postgresql://idxuser:{PASSWORD}@127.0.0.1:5433/appdb"  # what a development .env holds: the production tunnel
LABEL = "db1.example.test:5432/appdb"


class Recorder:
    def __init__(self, result=None):
        self.connected, self.applied, self.closed, self.result = [], [], 0, result or {"idx_discovered_first_seen": "exists"}

    def connect(self, url):
        self.connected.append(url)
        outer = self

        class Conn:
            def close(self):
                outer.closed += 1
        return Conn()

    def apply(self, conn, dry_run=False):
        self.applied.append(dry_run)
        return self.result


@pytest.fixture()
def rec(monkeypatch):
    r = Recorder()
    monkeypatch.setattr(di, "apply", r.apply)
    return r


def run(rec, argv, env):
    return di.main(argv, environ=env, connect=rec.connect)


def assert_untouched(rec):
    assert rec.connected == [] and rec.applied == []


def test_apply_never_falls_back_to_database_url_even_when_it_looks_safe(rec, capsys):
    assert run(rec, ["--apply", "--confirm-target", LABEL], {"DATABASE_URL": LOCAL}) == di.EXIT_REFUSED
    assert_untouched(rec)
    assert "INDEX_DATABASE_URL" in capsys.readouterr().err


def test_apply_without_a_confirmation_is_refused(rec, capsys):
    assert run(rec, ["--apply"], {"INDEX_DATABASE_URL": LOCAL}) == di.EXIT_REFUSED
    assert_untouched(rec)
    err = capsys.readouterr().err
    assert f"--confirm-target {LABEL}" in err and PASSWORD not in err  # it names the target to type, never the credentials


@pytest.mark.parametrize("wrong", ["", "appdb", "db1.example.test:5432/other", "db2.example.test:5432/appdb", "db1.example.test:5433/appdb", "db1.example.test/appdb"])
def test_apply_with_a_confirmation_that_does_not_match_is_refused(rec, wrong):
    assert run(rec, ["--apply", "--confirm-target", wrong], {"INDEX_DATABASE_URL": LOCAL}) == di.EXIT_REFUSED
    assert_untouched(rec)


def test_apply_with_the_matching_confirmation_connects_to_that_url_and_builds(rec, capsys):
    env = {"INDEX_DATABASE_URL": LOCAL, "DATABASE_URL": TUNNEL}  # the real situation: .env has the tunnel, the operator sets the explicit target
    assert run(rec, ["--apply", "--confirm-target", LABEL.upper()], env) == 0  # case-insensitive
    assert rec.connected == [LOCAL] and rec.applied == [False] and rec.closed == 1  # the explicit URL, not the inherited tunnel
    out = capsys.readouterr()
    assert LABEL in out.out and PASSWORD not in out.out + out.err and "idxuser" not in out.out + out.err


@pytest.mark.parametrize("url, env", [
    (f"postgresql://u:{PASSWORD}@h.example.test:5433/appdb", {}),  # explicit port
    (f"postgres://u:{PASSWORD}@h.example.test:5433/appdb?sslmode=require", {}),
    (f"postgresql://u:{PASSWORD}@h.example.test/appdb", {"PGPORT": "5433"}),  # port comes from the environment
    (f"postgresql://u:{PASSWORD}@h1.example.test:5432,h2.example.test:5433/appdb", {}),  # one of several hosts
    (f"postgresql://u:{PASSWORD}@h.example.test:5433,5432/appdb", {}),
    (f"postgresql://u:{PASSWORD}@h.example.test/appdb?port=5433", {}),
    (f"host=h.example.test port=5433 dbname=appdb user=u password={PASSWORD}", {}),  # key=value form
])
def test_port_5433_is_refused_for_apply_and_for_the_dry_run(rec, capsys, url, env):
    for argv, name in ((["--apply", "--confirm-target", "h.example.test:5433/appdb"], "INDEX_DATABASE_URL"), ([], "INDEX_DATABASE_URL"), ([], "DATABASE_URL")):
        assert run(rec, argv, {**env, name: url}) == di.EXIT_REFUSED, (argv, name)
    assert_untouched(rec)
    out = capsys.readouterr()
    assert PASSWORD not in out.out + out.err and out.err.count("REFUSED") == 3


def test_the_development_env_scenario_dry_run_and_apply_both_refuse_without_connecting(rec):
    env = {"DATABASE_URL": TUNNEL}
    assert run(rec, [], env) == di.EXIT_REFUSED
    assert run(rec, ["--apply", "--confirm-target", "127.0.0.1:5433/appdb"], env) == di.EXIT_REFUSED
    assert_untouched(rec)


def test_dry_run_may_use_database_url_when_it_is_not_the_tunnel_and_builds_nothing(rec, capsys):
    assert run(rec, [], {"DATABASE_URL": LOCAL}) == 0
    assert rec.connected == [LOCAL] and rec.applied == [True]  # dry_run=True
    out = capsys.readouterr().out
    assert LABEL in out and "dry run" in out and PASSWORD not in out


def test_dry_run_prefers_the_explicit_url_over_database_url(rec):
    assert run(rec, [], {"DATABASE_URL": TUNNEL, "INDEX_DATABASE_URL": LOCAL}) == 0
    assert rec.connected == [LOCAL]


@pytest.mark.parametrize("env", [{}, {"INDEX_DATABASE_URL": ""}, {"DATABASE_URL": ""}])
def test_no_url_at_all_is_refused(rec, env):
    assert run(rec, [], env) == di.EXIT_REFUSED
    assert_untouched(rec)


@pytest.mark.parametrize("bad", ["not a url at all " + PASSWORD, "postgresql://", f"postgresql://u:{PASSWORD}@/appdb", f"postgresql://u:{PASSWORD}@h.example.test:abc/appdb",
                                 f"postgresql://u:{PASSWORD}@h.example.test:5432"])
def test_unparsable_or_incomplete_urls_are_refused_without_echoing_them(rec, capsys, bad):
    assert run(rec, ["--apply", "--confirm-target", "x"], {"INDEX_DATABASE_URL": bad}) == di.EXIT_REFUSED
    assert_untouched(rec)
    out = capsys.readouterr()
    assert PASSWORD not in out.out + out.err


def test_pgdatabase_and_pghost_fill_gaps_but_the_label_always_shows_them(rec, capsys):
    env = {"INDEX_DATABASE_URL": f"postgresql://u:{PASSWORD}@/", "PGHOST": "envhost.example.test", "PGDATABASE": "envdb"}
    assert run(rec, ["--apply", "--confirm-target", "envhost.example.test:5432/envdb"], env) == 0
    assert "envhost.example.test:5432/envdb" in capsys.readouterr().out


def test_problems_in_the_result_give_exit_code_1_and_the_connection_is_closed(monkeypatch):
    r = Recorder({"a": "exists", "b": "INVALID (an earlier build failed); not touched. To rebuild, review it, then run by hand: DROP INDEX CONCURRENTLY b"})
    monkeypatch.setattr(di, "apply", r.apply)
    assert run(r, [], {"DATABASE_URL": LOCAL}) == di.EXIT_PROBLEM and r.closed == 1


def test_default_connect_uses_the_verified_url_not_what_dotenv_loaded(monkeypatch, rec):
    seen = []
    monkeypatch.setattr(core, "DATABASE_URL", TUNNEL)  # what crawler_core loaded from a development .env
    monkeypatch.setattr(core, "connect_unpooled", lambda: (seen.append(core.DATABASE_URL), type("C", (), {"close": lambda self: None})())[1])
    assert di.main(["--apply", "--confirm-target", LABEL], environ={"INDEX_DATABASE_URL": LOCAL, "DATABASE_URL": TUNNEL}) == 0
    assert seen == [LOCAL]


def test_the_command_line_refuses_before_connecting(tmp_path):
    """The real entry point with the development situation (DATABASE_URL = tunnel, nothing explicit): exit 2, no password anywhere in the output."""
    root = Path(__file__).resolve().parent.parent
    env = {k: v for k, v in os.environ.items() if k not in ("INDEX_DATABASE_URL", "PGPORT", "PGHOST", "PGDATABASE")}
    env["DATABASE_URL"] = TUNNEL
    for argv in ([], ["--apply", "--confirm-target", "127.0.0.1:5433/appdb"], ["--apply", "--confirm-target", LABEL]):
        p = subprocess.run([sys.executable, str(root / "db_indexes.py"), *argv], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
        assert p.returncode == di.EXIT_REFUSED, (argv, p.stdout, p.stderr)
        assert PASSWORD not in p.stdout + p.stderr and "REFUSED" in p.stderr


@pytest.mark.parametrize("url", [f"postgresql://idxuser:{RAW_AT_PASSWORD}@db1.example.test:5432/appdb", f"postgresql://idxuser:pa/ss@db1.example.test:5432/appdb",
                                 f"postgresql://idxuser:{RAW_AT_PASSWORD}@db1.example.test/appdb?x=1"])
def test_a_url_whose_password_is_not_escaped_is_refused_and_no_fragment_of_it_is_shown(rec, capsys, url):
    """libpq would read part of the password as the host or database name; the label built from it would print that fragment."""
    for argv in ([], ["--apply", "--confirm-target", "db1.example.test:5432/appdb"]):
        assert run(rec, argv, {"INDEX_DATABASE_URL": url}) == di.EXIT_REFUSED
    assert_untouched(rec)
    out = capsys.readouterr()
    assert "ss!" not in out.out + out.err and "pa/ss" not in out.out + out.err and "s3cr3t" not in out.out + out.err


def test_an_escaped_password_with_special_characters_works_and_is_not_shown(rec, capsys):
    url = "postgresql://idxuser:s3cr3t-P%40ss%21@db1.example.test:5432/appdb"
    assert run(rec, ["--apply", "--confirm-target", LABEL], {"INDEX_DATABASE_URL": url}) == 0
    out = capsys.readouterr()
    assert "s3cr3t" not in out.out + out.err and rec.connected == [url]

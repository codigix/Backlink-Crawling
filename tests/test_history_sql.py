"""Phase 5: /history reads link_history with the window, host filter, order and cut in PostgreSQL. The oracle is `legacy_history` (test_api_perf.py), the
original algorithm that loaded up to 100000 raw rows into Python. Every case below must give the SAME answer (or the same exception) as the oracle.
Local scratch database, one schema per test."""

import pytest

import crawler_api
import crawler_core as core
from api_data import EVIL, SUB, TARGET
from test_api_perf import db, legacy_history  # noqa: F401 - fixture

_TS = "to_char(timestamp '2026-01-01' + (({e}) || ' minutes')::interval, 'YYYY-MM-DD HH24:MI:SS')"


def TS(e: str) -> str:
    return _TS.format(e=e)


def outcome(f, *a):
    try:
        return "ok", f(*a)
    except Exception as e:  # a crash of the old code (NULL timestamps) must stay a crash of the same kind, not silently become an answer
        return "err", type(e).__name__


def same(target=TARGET, limits=(1, 2, 3, 7, 50, 200, 1000), old_may_crash=False):
    """Identical answers. old_may_crash: where the ORIGINAL raised TypeError (NULL timestamps compared with text) the new code may answer or raise,
    but it must never fail where the original answered."""
    for limit in limits:
        new, old = outcome(crawler_api.backlink_history, target, limit), outcome(legacy_history, target, limit)
        assert new == old or (old_may_crash and old == ("err", "TypeError") and new[0] in ("ok", "err")), (target, limit, new[0], old[0])


def hist(c, rows):
    """rows: (id, source_url, destination_url, anchor, event_type, detected_at | None), all of the TARGET's registrable domain"""
    for r in rows:
        c.execute("INSERT INTO link_history (id, source_url, destination_url, anchor_text, event_type, detected_at, destination_domain) VALUES (?,?,?,?,?,?,?)", (*r, TARGET))


def removed(c, source, dest, anchor, lost_at, active=0):
    c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, link_type, first_seen, last_seen, is_active, lost_at, source_domain)"
              " VALUES (?,?,?,?, 'html', '2026-01-01 00:00:00', '2026-01-01 00:00:00', ?, ?, ?)",
              (source, dest, TARGET, anchor, active, lost_at, core.hostname(source)))


def test_empty_history_and_unknown_domain(db):
    same(TARGET)
    same("nothing-here.org")


def test_id_order_and_detected_at_order_disagree(db):
    with core.connect() as c:  # detected_at is a scrambled function of id, so "newest by id" and "newest by time" are different rows
        c.execute(f"INSERT INTO link_history (id, source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT g, 'https://s'||g||'.example/p', 'https://{TARGET}/x'||(g%9), '{TARGET}', 'a'||(g%4),"
                  f" (ARRAY['lost','suspected_removed','reappeared'])[1+g%3], {TS('(g*7919) % 997')} FROM generate_series(1,600) g")
    same()


def test_equal_timestamps_are_ordered_by_id_descending_and_ties_across_sources_keep_the_old_order(db):
    with core.connect() as c:
        c.execute(f"INSERT INTO link_history (id, source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT g, 'https://s'||g||'.example/p', 'https://{TARGET}/x', '{TARGET}', '', 'lost', '2026-05-05 05:05:05' FROM generate_series(1,40) g")
        removed(c, "https://r.example/p", f"https://{TARGET}/gone", "", "2026-05-05 05:05:05")  # a fallback event with the very same timestamp
        c.execute("INSERT INTO outgoing_links (source_url, destination_url, destination_domain, anchor_text, source_domain, first_seen)"
                  f" VALUES ('https://n.example/p','https://{TARGET}/n','{TARGET}','','n.example','2026-05-05 05:05:05')")  # and a 'new' event
    same()


def test_missing_timestamps_behave_exactly_as_before(db):
    with core.connect() as c:
        hist(c, [(1, "https://a.example/p", f"https://{TARGET}/x", "", "lost", None), (2, "https://b.example/p", f"https://{TARGET}/x", "", "lost", None)])
    same(old_may_crash=True)  # all NULL: the old final sort raises TypeError (None < None) once two events meet; with limit 1 the new code has only one
    with core.connect() as c:
        hist(c, [(3, "https://c.example/p", f"https://{TARGET}/x", "", "lost", "2026-01-01 00:00:00")])
    same(old_may_crash=True)  # NULL and text mixed: TypeError in the old code


def test_several_events_for_one_link_and_duplicates_are_all_kept(db):
    with core.connect() as c:
        s, d = "https://s.example/p", f"https://{TARGET}/x"
        hist(c, [(1, s, d, "a", "suspected_removed", "2026-01-01 00:00:00"), (2, s, d, "a", "lost", "2026-01-02 00:00:00"), (3, s, d, "a", "reappeared", "2026-01-03 00:00:00"),
                 (4, s, d, "a", "lost", "2026-01-04 00:00:00"), (5, s, d, "a", "lost", "2026-01-04 00:00:00"), (6, s, d, "b", "lost", "2026-01-04 00:00:00")])
        removed(c, s, d, "a", "2026-01-04 00:00:00")  # has 'lost' events -> no fallback event
        removed(c, s, d, "zzz", "2026-01-04 00:00:00")  # different anchor -> its own fallback event
    same()


def test_links_on_and_off_the_target_host(db):
    with core.connect() as c:
        hist(c, [(1, "https://a.example/p", f"https://{TARGET}/x", "", "lost", "2026-01-01 00:00:01"),
                 (2, "https://a.example/p", f"https://{SUB}/x", "", "lost", "2026-01-01 00:00:02"),  # subdomain of the target: on it
                 (3, "https://a.example/p", f"https://{EVIL}/x", "", "lost", "2026-01-01 00:00:03"),  # look-alike: off it
                 (4, f"https://www.{TARGET}/p", f"https://{TARGET}/x", "", "lost", "2026-01-01 00:00:04"),  # source on the target: dropped
                 (5, f"https://{TARGET}./p", f"https://{TARGET}/x", "", "lost", "2026-01-01 00:00:05"),  # trailing dot host
                 (6, "https://a.example/p", f"https://{TARGET}./x", "", "lost", "2026-01-01 00:00:06"),
                 (7, "https://a.example:8443/p", f"https://{TARGET}:8443/x", "", "lost", "2026-01-01 00:00:07"),  # port
                 (8, "https://u@a.example/p", f"https://U:p@{TARGET.upper()}/x", "", "lost", "2026-01-01 00:00:08"),  # userinfo, upper case
                 (9, "HTTPS://A.EXAMPLE/p", f"HTTPS://{TARGET.upper()}/x", "", "reappeared", "2026-01-01 00:00:09"),
                 (10, "https://bücher.example/p", f"https://{TARGET}/x", "", "lost", "2026-01-01 00:00:10"),  # unicode host
                 (11, "ftp://a.example/p", f"ftp://{TARGET}/x", "", "lost", "2026-01-01 00:00:11"),  # other scheme
                 (12, "https://a.example/p?x=1#f", f"https://{TARGET}?q=1", "", "lost", "2026-01-01 00:00:12"),  # no path
                 (13, "https://a.example", f"https://{TARGET}", "", "lost", "2026-01-01 00:00:13"),  # no slash at all
                 (14, "not a url", "also not a url", "", "lost", "2026-01-01 00:00:14")])
    for t in (TARGET, SUB, EVIL):
        same(t)


def test_lost_link_fallback_skips_exactly_the_links_history_already_knows_as_lost(db):
    with core.connect() as c:
        for i in range(30):
            removed(c, f"https://r{i}.example/p", f"https://{TARGET}/g{i}", "a", f"2026-02-{1 + i % 28:02d} 00:00:00")
        hist(c, [(10 + i, f"https://r{i}.example/p", f"https://{TARGET}/g{i}", "a", "lost", "2026-03-01 00:00:00") for i in range(0, 30, 3)])  # every third is known
        hist(c, [(100, "https://r1.example/p", f"https://{TARGET}/g1", "a", "reappeared", "2026-03-01 00:00:00")])  # a non-lost event does not suppress
        removed(c, "https://r2.example/p", f"https://{TARGET}/g2", "b", "2026-02-02 00:00:00", active=1)  # active again: no fallback
    same(limits=(1, 3, 5, 10, 25, 50, 200))


@pytest.mark.parametrize("rows", [100_000, 100_001])
def test_the_100000_row_window_is_exactly_the_old_one(db, rows):
    """The domain's newest 100000 rows by id are read; with 100001 the OLDEST one (id 1) falls out, and with it its 'lost' event: its removed link then
    reappears as a lost_at fallback event and its (newest-of-all) timestamp disappears. Other domains' rows are interleaved in the id space."""
    with core.connect() as c:
        c.execute(f"INSERT INTO link_history (id, source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT g*2, 'https://s'||(g%500)||'.example/p'||g, 'https://{TARGET}/x', '{TARGET}', '', (ARRAY['suspected_removed','reappeared','lost'])[1+g%3],"
                  f" {TS('g % 20000')} FROM generate_series(2,{rows}) g")
        c.execute(f"INSERT INTO link_history (id, source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT g*2+1, 'https://o'||g||'.example/p', 'https://other.org/x', 'other.org', '', 'lost', '2031-01-01 00:00:00' FROM generate_series(1,{rows}) g")
        hist(c, [(2, "https://old.example/p", f"https://{TARGET}/old", "", "lost", "2030-01-01 00:00:00")])  # id 2 is the domain's oldest row
        removed(c, "https://old.example/p", f"https://{TARGET}/old", "", "2026-06-06 00:00:00")
        c.execute("ANALYZE link_history")
    got = crawler_api.backlink_history(TARGET, 1000)
    assert got == legacy_history(TARGET, 1000)
    in_window = any(e["at"] == "2030-01-01 00:00:00" for e in got["events"])
    assert in_window == (rows == 100_000), "the oldest row must be read at exactly 100000 rows and ignored at 100001"
    assert any(e["at"] == "2026-06-06 00:00:00" for e in got["events"]) == (rows == 100_001), "its fallback event exists only once its 'lost' event left the window"
    same(limits=(1, 10, 200))


@pytest.mark.parametrize("n", [1, 5, 99, 1000])
def test_small_and_large_domains_mixed_in_one_table(db, n):
    with core.connect() as c:
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://s'||g||'.example/p', 'https://{TARGET}/x', '{TARGET}', '', 'lost', {TS('(g*31) % 400')} FROM generate_series(1,{n}) g")
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://s'||g||'.example/p', 'https://{SUB}/x', '{TARGET}', '', 'reappeared', {TS('(g*17) % 300')} FROM generate_series(1,{n * 3}) g")
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://s'||g||'.example/p', 'https://{EVIL}/x', '{EVIL}', '', 'lost', {TS('g')} FROM generate_series(1,{n * 5}) g")
    same(TARGET)
    same(SUB)
    same(EVIL)


def test_rows_returned_from_postgresql_are_bounded_by_the_limit_not_by_the_window(db, monkeypatch):
    """The point of the change: a hot domain no longer sends its history into Python."""
    with core.connect() as c:
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://s'||g||'.example/p', 'https://{TARGET}/x', '{TARGET}', '', 'reappeared', {TS('g')} FROM generate_series(1,20000) g")
    fetched = []
    real = core.Cursor.fetchall

    def counting(self):
        rows = real(self)
        fetched.append(len(rows))
        return rows
    monkeypatch.setattr(core.Cursor, "fetchall", counting)
    assert len(crawler_api.backlink_history(TARGET, 50)["events"]) == 50
    assert max(fetched) <= 256, fetched  # one first batch (max(2*limit, 256) rows), never the 20000-row window


def test_mostly_rejected_rows_make_the_scan_grow_until_enough_events_are_found(db):
    """The newest 3000 rows are all from the target's own host (rejected), far more than the first batches read: the answer must still be the
    accepted older rows, exactly as the old full pass found them."""
    with core.connect() as c:
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://www.{TARGET}/p'||g, 'https://{TARGET}/x', '{TARGET}', '', 'lost', {TS('100000 + g')} FROM generate_series(1,3000) g")
        c.execute(f"INSERT INTO link_history (source_url, destination_url, destination_domain, anchor_text, event_type, detected_at)"
                  f" SELECT 'https://s'||g||'.example/p', 'https://{TARGET}/x', '{TARGET}', '', 'reappeared', {TS('g')} FROM generate_series(1,400) g")
    same(limits=(1, 50, 200, 1000))
    assert len(crawler_api.backlink_history(TARGET, 1000)["events"]) == 400

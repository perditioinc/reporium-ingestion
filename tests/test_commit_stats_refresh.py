"""Tests for the commit-stats refresh phase (fixes the corpus-wide last7Days=0
freeze). Covers upstream targeting for forks, fresh-count + activity_score
application, and preserve-on-unavailable (never overwrite with 0)."""

from unittest import mock

import pytest

from ingestion.main import _windows_from_weeks, _refresh_commit_stats_blocking

pytestmark = pytest.mark.no_db


def _payload(name="repo", owner="perditioinc", forked_from=None):
    return {
        "name": name,
        "owner": owner,
        "forked_from": forked_from,
        "commits_last_7_days": 0,
        "commits_last_30_days": 0,
        "commits_last_90_days": 0,
        "activity_score": 0,
        "activity_score_breakdown": None,
    }


def test_windows_from_weeks_collapses_52_weeks():
    weeks = [{"total": i} for i in range(1, 53)]  # totals 1..52, oldest first
    c7, c30, c90 = _windows_from_weeks(weeks)
    assert c7 == 52                         # most recent week
    assert c30 == 49 + 50 + 51 + 52         # last 4 weeks
    assert c90 == sum(range(40, 53))        # last 13 weeks


def test_refresh_targets_upstream_and_applies_fresh_counts():
    payload = _payload(name="my-fork", forked_from="acme/lib")
    weeks = [{"total": 0}] * 48 + [{"total": 3}, {"total": 4}, {"total": 5}, {"total": 6}]
    captured = {}

    def fake_fetch(client, target, headers, **_):
        captured["target"] = target
        setattr(client, "_last_rate_limit_remaining", 5000)
        return weeks

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        stats = _refresh_commit_stats_blocking([(payload, 100, 20, False)], "tok")

    assert captured["target"] == "acme/lib"          # upstream, NOT the empty fork
    assert payload["commits_last_7_days"] == 6
    assert payload["commits_last_30_days"] == 3 + 4 + 5 + 6
    assert payload["activity_score"] > 0             # recomputed from fresh counts
    assert payload["activity_score_breakdown"] is not None
    assert stats == {"updated": 1, "skipped": 0, "errors": 0, "deadline_deferred": 0}


def test_refresh_non_fork_targets_owner_name():
    payload = _payload(name="own-repo", owner="perditioinc", forked_from=None)
    captured = {}

    def fake_fetch(client, target, headers, **_):
        captured["target"] = target
        setattr(client, "_last_rate_limit_remaining", 5000)
        return [{"total": 1}]

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        _refresh_commit_stats_blocking([(payload, 0, 0, False)], "tok")

    assert captured["target"] == "perditioinc/own-repo"


def test_refresh_preserves_on_unavailable():
    # Pre-existing payload values must NOT be zeroed when stats are unavailable —
    # None signals the API to keep the stored value.
    payload = _payload()
    payload["commits_last_7_days"] = 9

    def fake_fetch(client, target, headers, **_):
        setattr(client, "_last_rate_limit_remaining", 5000)
        return None

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        stats = _refresh_commit_stats_blocking([(payload, 0, 0, False)], "tok")

    assert payload["commits_last_7_days"] is None
    assert payload["commits_last_30_days"] is None
    assert payload["commits_last_90_days"] is None
    assert payload["activity_score"] is None
    assert stats == {"updated": 0, "skipped": 1, "errors": 0, "deadline_deferred": 0}


def test_refresh_preserves_on_empty_weeks():
    payload = _payload()

    def fake_fetch(client, target, headers, **_):
        setattr(client, "_last_rate_limit_remaining", 5000)
        return []  # brand-new repo, no week data yet

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        stats = _refresh_commit_stats_blocking([(payload, 0, 0, False)], "tok")

    assert payload["commits_last_7_days"] is None
    assert stats["skipped"] == 1


def test_refresh_stops_early_on_low_rate_limit_and_preserves_unreached():
    p1, p2 = _payload(name="a"), _payload(name="b")
    p1["commits_last_7_days"] = 7  # stale pre-refresh value that must NOT be posted
    calls = []

    def fake_fetch(client, target, headers, **_):
        calls.append(target)
        setattr(client, "_last_rate_limit_remaining", 10)  # below the 100 floor
        return [{"total": 1}]

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        _refresh_commit_stats_blocking([(p1, 0, 0, False), (p2, 0, 0, False)], "tok")

    assert len(calls) == 1  # stopped after the first repo
    # Neither payload was successfully refreshed → both carry the preserve
    # sentinel so the API keeps their stored values (no stale-zero overwrite).
    assert p1["commits_last_7_days"] is None
    assert p2["commits_last_7_days"] is None


def test_refresh_max_repos_cap_preserves_uncapped():
    payloads = [(_payload(name=f"r{i}"), 0, 0, False) for i in range(5)]
    calls = []

    def fake_fetch(client, target, headers, **_):
        calls.append(target)
        setattr(client, "_last_rate_limit_remaining", 5000)
        return [{"total": 1}]

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main.time.sleep"):
        _refresh_commit_stats_blocking(payloads, "tok", max_repos=2)

    assert len(calls) == 2
    assert payloads[0][0]["commits_last_7_days"] == 1   # refreshed
    assert payloads[1][0]["commits_last_7_days"] == 1   # refreshed
    # Repos beyond the cap are never reached → preserve sentinel, not stale 0.
    assert payloads[2][0]["commits_last_7_days"] is None
    assert payloads[4][0]["commits_last_7_days"] is None


def test_refresh_preserves_on_fetch_exception():
    payload = _payload()
    payload["commits_last_7_days"] = 9  # stale value that must not be posted

    def boom(client, target, headers, **_):
        raise RuntimeError("transient network error")

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", boom), \
         mock.patch("ingestion.main.time.sleep"):
        stats = _refresh_commit_stats_blocking([(payload, 0, 0, False)], "tok")

    assert payload["commits_last_7_days"] is None  # preserved, not overwritten
    assert payload["activity_score"] is None
    assert stats["errors"] == 1


def test_refresh_no_partial_mutation_if_scoring_raises():
    # If activity-score computation raises AFTER counts are derived, the payload
    # must not be left half-updated — all fields stay the preserve sentinel.
    payload = _payload()

    def fake_fetch(client, target, headers, **_):
        setattr(client, "_last_rate_limit_remaining", 5000)
        return [{"total": 4}]

    with mock.patch("scripts.fetch_commit_stats.fetch_commit_activity", fake_fetch), \
         mock.patch("ingestion.main._compute_activity_score", side_effect=ValueError("boom")), \
         mock.patch("ingestion.main.time.sleep"):
        stats = _refresh_commit_stats_blocking([(payload, 0, 0, False)], "tok")

    assert payload["commits_last_7_days"] is None
    assert payload["commits_last_30_days"] is None
    assert payload["commits_last_90_days"] is None
    assert payload["activity_score"] is None
    assert stats["errors"] == 1
    assert stats["updated"] == 0

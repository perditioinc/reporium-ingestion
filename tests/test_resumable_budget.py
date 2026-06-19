"""Unit tests for the resumable per-run budgeting + work selection.

Fix under test: a single `python -m ingestion run` invocation must process
only a bounded, prioritised slice of the pending corpus so it finishes well
under the 3600s Cloud Run task timeout, while the durable cache lets the next
run resume from where it left off. See ingestion/budget.py.

These are pure unit tests: no GitHub API, no Anthropic API, no DB, no network.
Deterministic and runnable offline ($0). Fakes stand in for GitHubRepo and the
durable RepoCacheRow so the change-detection / priority / cap / freshness logic
is exercised in isolation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from ingestion.budget import (
    DEFAULT_FRESHNESS_SLO_HOURS,
    DEFAULT_MAX_REPOS_PER_RUN,
    DEFAULT_RUN_TIME_BUDGET_SECONDS,
    compute_freshness,
    freshness_slo_hours,
    is_checkpointed,
    max_repos_per_run,
    run_time_budget_seconds,
    select_work_for_run,
)

# Pure logic -- no PostgreSQL fixture needed.
pytestmark = pytest.mark.no_db


# -- Minimal fakes (duck-typed to GitHubRepo / RepoCacheRow) -------------------


@dataclass
class FakeRepo:
    name: str
    updated_at: str


@dataclass
class FakeCacheRow:
    name: str
    github_updated_at: str | None = None
    daily_fetched_at: str | None = None


def _cache(*rows: FakeCacheRow) -> dict[str, FakeCacheRow]:
    return {r.name: r for r in rows}


def _iso(dt: datetime) -> str:
    """Render an aware datetime as the trailing-Z form GitHub emits."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- is_checkpointed ------------------------------------------------------------


def test_checkpointed_when_row_daily_and_unchanged():
    repo = FakeRepo("rag", "2026-06-10T00:00:00Z")
    cached = _cache(
        FakeCacheRow("rag", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T03:00:00Z")
    )
    assert is_checkpointed(repo, cached) is True


def test_not_checkpointed_when_no_row():
    repo = FakeRepo("brand-new", "2026-06-17T00:00:00Z")
    assert is_checkpointed(repo, _cache()) is False


def test_not_checkpointed_when_never_daily_fetched():
    repo = FakeRepo("perm-only", "2026-06-10T00:00:00Z")
    cached = _cache(
        FakeCacheRow("perm-only", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at=None)
    )
    assert is_checkpointed(repo, cached) is False


def test_not_checkpointed_when_github_moved():
    repo = FakeRepo("active", "2026-06-15T12:00:00Z")
    cached = _cache(
        FakeCacheRow("active", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T03:00:00Z")
    )
    assert is_checkpointed(repo, cached) is False


# -- select_work_for_run: pending/done partition -------------------------------


def test_select_skips_checkpointed_and_keeps_pending():
    done = FakeRepo("done", "2026-06-10T00:00:00Z")
    new = FakeRepo("new", "2026-06-17T00:00:00Z")
    changed = FakeRepo("changed", "2026-06-16T00:00:00Z")
    cached = _cache(
        FakeCacheRow("done", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T00:00:00Z"),
        FakeCacheRow("changed", github_updated_at="2026-06-01T00:00:00Z",
                     daily_fetched_at="2026-06-02T00:00:00Z"),
    )
    sel = select_work_for_run([done, new, changed], cached, max_repos=0)
    names = {r.name for r in sel.selected}
    assert names == {"new", "changed"}
    assert "done" not in names
    assert sel.pending_total == 2
    assert sel.deferred == 0
    assert sel.is_complete is True


def test_select_empty_when_corpus_fully_checkpointed():
    a = FakeRepo("a", "2026-06-10T00:00:00Z")
    b = FakeRepo("b", "2026-06-10T00:00:00Z")
    cached = _cache(
        FakeCacheRow("a", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T00:00:00Z"),
        FakeCacheRow("b", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T00:00:00Z"),
    )
    sel = select_work_for_run([a, b], cached)
    assert sel.selected == []
    assert sel.pending_total == 0
    assert sel.is_complete is True


# -- select_work_for_run: prioritise NEW repos, newest first --------------------


def test_new_repos_prioritised_ahead_of_changed():
    # A changed repo with a MORE-recent updated_at than the new repos must
    # still rank behind every brand-new repo (freshness recovery for forks).
    new_old = FakeRepo("new-old", "2026-06-12T00:00:00Z")
    new_recent = FakeRepo("new-recent", "2026-06-17T00:00:00Z")
    changed_newest = FakeRepo("changed-newest", "2026-06-18T00:00:00Z")
    cached = _cache(
        FakeCacheRow("changed-newest", github_updated_at="2026-06-01T00:00:00Z",
                     daily_fetched_at="2026-06-02T00:00:00Z"),
    )
    sel = select_work_for_run(
        [changed_newest, new_old, new_recent], cached, max_repos=0
    )
    order = [r.name for r in sel.selected]
    # Both new repos first (newest-first among themselves), then the changed one.
    assert order == ["new-recent", "new-old", "changed-newest"]
    assert sel.new_count == 2
    assert sel.changed_count == 1


# -- select_work_for_run: per-run cap (batching) --------------------------------


def test_max_repos_caps_selection_and_defers_rest():
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(5)]
    sel = select_work_for_run(repos, _cache(), max_repos=2)
    assert len(sel.selected) == 2
    assert sel.pending_total == 5
    assert sel.deferred == 3
    assert sel.capped is True
    assert sel.is_complete is False
    # Newest-first: the two highest-dated new repos are selected.
    assert {r.name for r in sel.selected} == {"r4", "r3"}


def test_resume_drains_remaining_after_checkpoint():
    """Simulate run 1 -> checkpoint -> run 2: the cap means run 1 takes the
    top 2, and after they are checkpointed run 2 takes the next 2, etc.,
    until the corpus is fully drained (idempotent resume)."""
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(5)]
    cached: dict[str, FakeCacheRow] = {}

    drained: list[str] = []
    for _ in range(10):  # generous loop bound; should converge in 3 runs
        sel = select_work_for_run(repos, cached, max_repos=2)
        if not sel.selected:
            break
        for repo in sel.selected:
            # Checkpoint exactly as the fetcher would: row + daily + matching ts.
            cached[repo.name] = FakeCacheRow(
                repo.name,
                github_updated_at=repo.updated_at,
                daily_fetched_at="2026-06-18T00:00:00Z",
            )
            drained.append(repo.name)

    assert sorted(drained) == ["r0", "r1", "r2", "r3", "r4"]
    # No repo processed twice (idempotent).
    assert len(drained) == len(set(drained))
    # Final selection is empty -> fully drained.
    assert select_work_for_run(repos, cached, max_repos=2).is_complete is True


def test_max_repos_zero_means_no_cap():
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(5)]
    sel = select_work_for_run(repos, _cache(), max_repos=0)
    assert len(sel.selected) == 5
    assert sel.capped is False
    assert sel.deferred == 0


def test_select_does_not_mutate_input_order():
    repos = [
        FakeRepo("a", "2026-06-10T00:00:00Z"),
        FakeRepo("b", "2026-06-17T00:00:00Z"),
    ]
    snapshot = [r.name for r in repos]
    select_work_for_run(repos, _cache(), max_repos=1)
    assert [r.name for r in repos] == snapshot


def test_unparseable_timestamp_sorts_last_not_crash():
    good = FakeRepo("good", "2026-06-17T00:00:00Z")
    bad = FakeRepo("bad", "not-a-date")
    sel = select_work_for_run([bad, good], _cache(), max_repos=0)
    assert [r.name for r in sel.selected] == ["good", "bad"]


# -- compute_freshness: SLO metric ----------------------------------------------


def test_freshness_none_when_fully_fresh():
    repo = FakeRepo("a", "2026-06-10T00:00:00Z")
    cached = _cache(
        FakeCacheRow("a", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T00:00:00Z")
    )
    rep = compute_freshness([repo], cached)
    assert rep.pending_total == 0
    assert rep.lag_hours is None
    assert rep.breached is False


def test_freshness_lag_from_newest_pending():
    now = datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc)
    # Newest pending repo updated 10h ago; an older pending one 50h ago.
    recent = FakeRepo("recent", _iso(now - timedelta(hours=10)))
    older = FakeRepo("older", _iso(now - timedelta(hours=50)))
    rep = compute_freshness([older, recent], _cache(), now=now, slo_hours=48)
    assert rep.pending_total == 2
    assert rep.newest_pending_repo == "recent"
    assert rep.lag_hours == pytest.approx(10.0, abs=0.01)
    assert rep.breached is False  # 10h < 48h SLO


def test_freshness_breach_when_oldest_only_pending():
    now = datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc)
    stale = FakeRepo("stale", _iso(now - timedelta(hours=72)))
    rep = compute_freshness([stale], _cache(), now=now, slo_hours=48)
    assert rep.lag_hours == pytest.approx(72.0, abs=0.01)
    assert rep.breached is True


def test_freshness_metrics_dict_shape():
    now = datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc)
    repo = FakeRepo("r", _iso(now - timedelta(hours=5)))
    rep = compute_freshness([repo], _cache(), now=now, slo_hours=48)
    m = rep.as_metrics()
    assert set(m) == {
        "freshness_pending_total",
        "freshness_newest_pending_repo",
        "freshness_lag_hours",
        "freshness_slo_hours",
        "freshness_slo_breached",
    }
    assert m["freshness_pending_total"] == 1
    assert m["freshness_newest_pending_repo"] == "r"
    assert m["freshness_lag_hours"] == pytest.approx(5.0, abs=0.01)
    assert m["freshness_slo_breached"] is False


# -- env tunables ----------------------------------------------------------------


def test_env_tunables_default_when_unset(monkeypatch):
    monkeypatch.delenv("MAX_REPOS_PER_RUN", raising=False)
    monkeypatch.delenv("RUN_TIME_BUDGET_SECONDS", raising=False)
    monkeypatch.delenv("FRESHNESS_SLO_HOURS", raising=False)
    assert max_repos_per_run() == DEFAULT_MAX_REPOS_PER_RUN
    assert run_time_budget_seconds() == DEFAULT_RUN_TIME_BUDGET_SECONDS
    assert freshness_slo_hours() == DEFAULT_FRESHNESS_SLO_HOURS


def test_env_tunables_override(monkeypatch):
    monkeypatch.setenv("MAX_REPOS_PER_RUN", "25")
    monkeypatch.setenv("RUN_TIME_BUDGET_SECONDS", "600")
    monkeypatch.setenv("FRESHNESS_SLO_HOURS", "12")
    assert max_repos_per_run() == 25
    assert run_time_budget_seconds() == 600
    assert freshness_slo_hours() == 12


def test_env_tunable_ignores_garbage(monkeypatch):
    monkeypatch.setenv("MAX_REPOS_PER_RUN", "not-a-number")
    assert max_repos_per_run() == DEFAULT_MAX_REPOS_PER_RUN


# -- commit-stats deadline guard (partial-completion safety) --------------------


def test_commit_stats_stops_at_deadline(monkeypatch):
    """The blocking commit-stats refresh must stop once the wall-clock deadline
    is crossed, deferring the rest -- so the slowest phase can never push a
    single invocation past the task timeout. Patched to avoid all network/sleep.
    """
    from ingestion import main as main_mod

    # Stub the network fetch and time.sleep so the test is instant + offline.
    monkeypatch.setattr(
        "scripts.fetch_commit_stats.fetch_commit_activity",
        lambda client, target, headers: [{"total": 1}] * 13,
        raising=False,
    )
    monkeypatch.setattr(main_mod.time, "sleep", lambda _s: None)

    # Deadline already in the past -> the loop should stop on the FIRST repo,
    # deferring all of them.
    items = [
        ({"owner": "o", "name": f"r{i}", "forked_from": None}, 1, 1, False)
        for i in range(5)
    ]
    past_deadline = time.monotonic() - 1.0
    result = main_mod._refresh_commit_stats_blocking(
        items, "fake-token", deadline=past_deadline
    )
    assert result["updated"] == 0
    assert result["deadline_deferred"] == 5
    # Every payload retains the preserve sentinel (None) -> API skip-empty keeps
    # the stored value; a partial run never blanks real commit counts.
    for payload, *_ in items:
        assert payload["commits_last_7_days"] is None
        assert payload["activity_score"] is None


def test_commit_stats_runs_all_when_deadline_far(monkeypatch):
    from ingestion import main as main_mod

    monkeypatch.setattr(
        "scripts.fetch_commit_stats.fetch_commit_activity",
        lambda client, target, headers: [{"total": 2}] * 13,
        raising=False,
    )
    monkeypatch.setattr(main_mod.time, "sleep", lambda _s: None)

    items = [
        ({"owner": "o", "name": f"r{i}", "forked_from": None}, 5, 2, False)
        for i in range(3)
    ]
    far_deadline = time.monotonic() + 3600.0
    result = main_mod._refresh_commit_stats_blocking(
        items, "fake-token", deadline=far_deadline
    )
    assert result["updated"] == 3
    assert result["deadline_deferred"] == 0
    for payload, *_ in items:
        assert payload["commits_last_7_days"] == 2  # last week total

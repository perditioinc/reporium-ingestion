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
    # Fix #1 (lost-work): the COMPLETED checkpoint is distinct from the fetcher's
    # daily_fetched_at. It is set only AFTER the repo is fully posted to the API.
    completed_at: str | None = None
    completed_github_updated_at: str | None = None


def _cache(*rows: FakeCacheRow) -> dict[str, FakeCacheRow]:
    return {r.name: r for r in rows}


def _done(name: str, updated_at: str) -> FakeCacheRow:
    """A fully-COMPLETED checkpoint row (fetched AND posted to the API)."""
    return FakeCacheRow(
        name,
        github_updated_at=updated_at,
        daily_fetched_at="2026-06-11T03:00:00Z",
        completed_at="2026-06-11T03:05:00Z",
        completed_github_updated_at=updated_at,
    )


def _iso(dt: datetime) -> str:
    """Render an aware datetime as the trailing-Z form GitHub emits."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- is_checkpointed ------------------------------------------------------------


def test_checkpointed_when_completed_and_unchanged():
    repo = FakeRepo("rag", "2026-06-10T00:00:00Z")
    cached = _cache(_done("rag", "2026-06-10T00:00:00Z"))
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
    cached = _cache(_done("active", "2026-06-10T00:00:00Z"))
    assert is_checkpointed(repo, cached) is False


# -- Fix #1: LOST-WORK CHECKPOINT ----------------------------------------------
# A repo fetched (daily_fetched_at + matching github_updated_at) but KILLED
# before summarise/enrich/commit-stats/API-post must NOT count as done, or the
# next run skips it and the work is lost. Only the COMPLETED marker, set after
# the API post, makes a repo "done".


def test_not_checkpointed_when_fetched_but_not_completed():
    # The fetcher wrote daily_fetched_at + matching github_updated_at, but the
    # run died before the API post -> completed_at is still None.
    repo = FakeRepo("half-done", "2026-06-17T00:00:00Z")
    cached = _cache(
        FakeCacheRow(
            "half-done",
            github_updated_at="2026-06-17T00:00:00Z",
            daily_fetched_at="2026-06-18T03:00:00Z",
            completed_at=None,
            completed_github_updated_at=None,
        )
    )
    assert is_checkpointed(repo, cached) is False


def test_not_checkpointed_when_completed_marker_is_stale():
    # Completed on a PRIOR github_updated_at, but GitHub has since moved. The
    # completed marker must match the CURRENT updated_at, else re-process.
    repo = FakeRepo("moved", "2026-06-18T00:00:00Z")
    cached = _cache(
        FakeCacheRow(
            "moved",
            github_updated_at="2026-06-18T00:00:00Z",
            daily_fetched_at="2026-06-18T03:00:00Z",
            completed_at="2026-06-11T03:00:00Z",
            completed_github_updated_at="2026-06-10T00:00:00Z",
        )
    )
    assert is_checkpointed(repo, cached) is False


def test_fetched_but_not_completed_repo_is_reselected():
    # End-to-end: a fetched-but-not-completed repo must be picked again so its
    # work is never lost.
    repo = FakeRepo("half-done", "2026-06-17T00:00:00Z")
    cached = _cache(
        FakeCacheRow(
            "half-done",
            github_updated_at="2026-06-17T00:00:00Z",
            daily_fetched_at="2026-06-18T03:00:00Z",
        )
    )
    sel = select_work_for_run([repo], cached, max_repos=0)
    assert [r.name for r in sel.selected] == ["half-done"]
    assert sel.pending_total == 1


# -- select_work_for_run: pending/done partition -------------------------------


def test_select_skips_checkpointed_and_keeps_pending():
    done = FakeRepo("done", "2026-06-10T00:00:00Z")
    new = FakeRepo("new", "2026-06-17T00:00:00Z")
    changed = FakeRepo("changed", "2026-06-16T00:00:00Z")
    cached = _cache(
        _done("done", "2026-06-10T00:00:00Z"),
        _done("changed", "2026-06-01T00:00:00Z"),  # completed at an OLD ts
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
        _done("a", "2026-06-10T00:00:00Z"),
        _done("b", "2026-06-10T00:00:00Z"),
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
        _done("changed-newest", "2026-06-01T00:00:00Z"),  # completed at an OLD ts
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
    # reserve_oldest=0 => pure newest-first (legacy behaviour).
    sel = select_work_for_run(repos, _cache(), max_repos=2, reserve_oldest=0.0)
    assert len(sel.selected) == 2
    assert sel.pending_total == 5
    assert sel.deferred == 3
    assert sel.capped is True
    assert sel.is_complete is False
    # Newest-first: the two highest-dated new repos are selected.
    assert {r.name for r in sel.selected} == {"r4", "r3"}


def test_default_reserve_oldest_picks_one_oldest_when_capped():
    # With the DEFAULT reservation, a cap of 2 keeps the freshest (r4) AND
    # guarantees the OLDEST pending (r0) a slot so it can never starve.
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(5)]
    sel = select_work_for_run(repos, _cache(), max_repos=2)  # default reserve
    assert len(sel.selected) == 2
    names = {r.name for r in sel.selected}
    assert "r4" in names   # freshness head
    assert "r0" in names   # oldest reserved
    assert sel.reserved_oldest == 1


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
            # Checkpoint exactly as the pipeline does AFTER a successful API
            # post: set the COMPLETED marker (not just daily_fetched_at).
            cached[repo.name] = _done(repo.name, repo.updated_at)
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
    cached = _cache(_done("a", "2026-06-10T00:00:00Z"))
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
        "freshness_oldest_pending_repo",
        "freshness_oldest_lag_hours",
        "freshness_slo_hours",
        "freshness_slo_breached",
    }
    assert m["freshness_pending_total"] == 1
    assert m["freshness_newest_pending_repo"] == "r"
    assert m["freshness_lag_hours"] == pytest.approx(5.0, abs=0.01)
    assert m["freshness_slo_breached"] is False


# -- Fix #3: BACKLOG DRAIN FAIRNESS (no starvation) ----------------------------
# Under continuous new arrivals, a pure newest-first selection lets old deferred
# work starve forever. select_work_for_run must reserve capacity for the OLDEST
# pending repos so the backlog drains in bounded time.


def test_oldest_pending_reserved_against_new_arrivals():
    # 4 old deferred repos (changed long ago) + 4 brand-new repos this run.
    # cap=4. A pure newest-first policy would take all 4 new and the old ones
    # would starve. With a reservation, at least one OLD repo must be selected.
    old = [
        FakeRepo("old0", "2026-01-01T00:00:00Z"),
        FakeRepo("old1", "2026-01-02T00:00:00Z"),
        FakeRepo("old2", "2026-01-03T00:00:00Z"),
        FakeRepo("old3", "2026-01-04T00:00:00Z"),
    ]
    new = [FakeRepo(f"new{i}", f"2026-06-{14 + i:02d}T00:00:00Z") for i in range(4)]
    cached = _cache(*[_done(r.name, "2025-12-01T00:00:00Z") for r in old])
    # mark them changed: GitHub moved them past the completed marker
    for r in old:
        cached[r.name].completed_github_updated_at = "2025-12-01T00:00:00Z"
        cached[r.name].github_updated_at = "2025-12-01T00:00:00Z"

    sel = select_work_for_run(old + new, cached, max_repos=4, reserve_oldest=0.25)
    names = {r.name for r in sel.selected}
    assert len(sel.selected) == 4
    # At least one of the oldest deferred repos is guaranteed a slot.
    assert names & {"old0", "old1"}, f"oldest starved: {names}"


def test_backlog_drains_under_continuous_new_arrivals():
    """The headline guarantee: even with a NEW repo arriving every run and a
    per-run cap smaller than the backlog, every repo (old and new) is
    eventually processed -- the backlog never starves."""
    # Seed 6 old pending repos (changed, never completed at current ts).
    repos: list[FakeRepo] = [
        FakeRepo(f"old{i}", "2026-01-01T00:00:00Z") for i in range(6)
    ]
    cached: dict[str, FakeCacheRow] = {}
    drained: set[str] = set()
    arrival = 0

    for run in range(40):  # generous bound; must converge well before this
        # A brand-new repo arrives every run (steady inflow).
        new_name = f"arrival{arrival}"
        repos.append(FakeRepo(new_name, f"2026-07-01T{arrival:02d}:00:00Z"))
        arrival += 1
        if arrival >= 12:
            # Stop new arrivals eventually so the loop can fully drain.
            pass

        sel = select_work_for_run(repos, cached, max_repos=3, reserve_oldest=0.34)
        for repo in sel.selected:
            cached[repo.name] = _done(repo.name, repo.updated_at)
            drained.add(repo.name)
        if arrival >= 12 and sel.is_complete:
            break

    # Every original old repo must have been drained (none starved).
    for i in range(6):
        assert f"old{i}" in drained, f"old{i} starved under continuous arrivals"


def test_reserve_oldest_does_not_exceed_cap():
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(10)]
    sel = select_work_for_run(repos, _cache(), max_repos=3, reserve_oldest=0.5)
    assert len(sel.selected) == 3
    assert sel.deferred == 7


# -- Fix #7: TIMESTAMP COMPARISON (mixed datetime/string/precision) ------------
# is_checkpointed must parse both sides to UTC instants before comparing, or a
# datetime-vs-string (or differing-precision) mismatch makes equality fail and
# EVERY repo stays pending -- the backlog never drains.


def test_checkpointed_mixed_datetime_and_string():
    # Repo carries a datetime; cache stored an ISO string for the SAME instant.
    repo = FakeRepo("dt", datetime(2026, 6, 10, 0, 0, 0, tzinfo=timezone.utc))
    cached = _cache(
        FakeCacheRow(
            "dt",
            github_updated_at="2026-06-10T00:00:00Z",
            daily_fetched_at="2026-06-11T03:00:00Z",
            completed_at="2026-06-11T03:05:00Z",
            completed_github_updated_at="2026-06-10T00:00:00Z",
        )
    )
    assert is_checkpointed(repo, cached) is True


def test_checkpointed_precision_mismatch_same_instant():
    # Same instant, different ISO precision / offset spelling.
    repo = FakeRepo("p", "2026-06-10T00:00:00.000+00:00")
    cached = _cache(
        FakeCacheRow(
            "p",
            github_updated_at="2026-06-10T00:00:00Z",
            daily_fetched_at="2026-06-11T03:00:00Z",
            completed_at="2026-06-11T03:05:00Z",
            completed_github_updated_at="2026-06-10T00:00:00Z",
        )
    )
    assert is_checkpointed(repo, cached) is True


def test_checkpointed_string_offset_equals_z():
    repo = FakeRepo("z", "2026-06-10T05:00:00+00:00")
    cached = _cache(
        FakeCacheRow(
            "z",
            github_updated_at="2026-06-10T05:00:00Z",
            daily_fetched_at="2026-06-11T03:00:00Z",
            completed_at="2026-06-11T03:05:00Z",
            completed_github_updated_at="2026-06-10T05:00:00Z",
        )
    )
    assert is_checkpointed(repo, cached) is True


def test_not_checkpointed_when_instants_actually_differ():
    repo = FakeRepo("diff", "2026-06-11T00:00:00Z")
    cached = _cache(
        FakeCacheRow(
            "diff",
            github_updated_at="2026-06-11T00:00:00Z",
            daily_fetched_at="2026-06-11T03:00:00Z",
            completed_at="2026-06-11T03:05:00Z",
            completed_github_updated_at="2026-06-10T00:00:00Z",
        )
    )
    assert is_checkpointed(repo, cached) is False


# -- OPTIONAL: freshness also reports OLDEST pending age + processed counts -----


def test_freshness_reports_oldest_pending_age():
    now = datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc)
    recent = FakeRepo("recent", _iso(now - timedelta(hours=10)))
    older = FakeRepo("older", _iso(now - timedelta(hours=200)))
    rep = compute_freshness([older, recent], _cache(), now=now, slo_hours=48)
    assert rep.oldest_pending_repo == "older"
    assert rep.oldest_lag_hours == pytest.approx(200.0, abs=0.01)
    m = rep.as_metrics()
    assert m["freshness_oldest_pending_repo"] == "older"
    assert m["freshness_oldest_lag_hours"] == pytest.approx(200.0, abs=0.01)


def test_selection_reports_corpus_and_processed_counts():
    repos = [FakeRepo(f"r{i}", f"2026-06-{10 + i:02d}T00:00:00Z") for i in range(5)]
    sel = select_work_for_run(repos, _cache(), max_repos=2)
    assert sel.corpus_size == 5
    assert sel.selected_count == 2


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


# -- Fix #4: end-to-end MONOTONIC budget guard ---------------------------------


def test_budget_exhausted_uses_monotonic_deadline():
    from ingestion import main as main_mod

    # None deadline (budget disabled) never trips.
    assert main_mod._budget_exhausted(None) is False
    # Past monotonic deadline -> exhausted; far-future -> not.
    assert main_mod._budget_exhausted(time.monotonic() - 1.0) is True
    assert main_mod._budget_exhausted(time.monotonic() + 3600.0) is False


def test_budget_exhausted_immune_to_wallclock_jump(monkeypatch):
    """The guard must read time.monotonic(), NOT time.time(): a wall-clock jump
    (NTP step) must not prematurely abort or extend the budget."""
    from ingestion import main as main_mod

    # Freeze monotonic just before a far-future deadline.
    base = 1000.0
    monkeypatch.setattr(main_mod.time, "monotonic", lambda: base)
    deadline = base + 100.0
    # Even if wall-clock (time.time) leaps forward, the monotonic guard holds.
    monkeypatch.setattr(main_mod.time, "time", lambda: 9_999_999_999.0)
    assert main_mod._budget_exhausted(deadline) is False
    # Advance monotonic past the deadline -> now exhausted.
    monkeypatch.setattr(main_mod.time, "monotonic", lambda: base + 200.0)
    assert main_mod._budget_exhausted(deadline) is True


# -- Fix #5: run status recomputed from TRUE pending state ---------------------


def test_run_status_completed_only_when_drained():
    from ingestion import main as main_mod

    repos = [FakeRepo("a", "2026-06-10T00:00:00Z"), FakeRepo("b", "2026-06-10T00:00:00Z")]
    # Both completed -> drained -> "completed".
    cached = _cache(_done("a", "2026-06-10T00:00:00Z"), _done("b", "2026-06-10T00:00:00Z"))
    status, pending = main_mod._compute_run_status(repos, cached)
    assert status == "completed"
    assert pending == 0


def test_run_status_partial_when_uncheckpointed_remains():
    from ingestion import main as main_mod

    repos = [FakeRepo("a", "2026-06-10T00:00:00Z"), FakeRepo("b", "2026-06-10T00:00:00Z")]
    # 'a' completed; 'b' only fetched (not completed) -> a mid-pipeline kill or
    # failure left it pending. Old `deferred>0` logic with deferred==0 would
    # have wrongly said "completed"; the recompute catches it.
    cached = _cache(
        _done("a", "2026-06-10T00:00:00Z"),
        FakeCacheRow("b", github_updated_at="2026-06-10T00:00:00Z",
                     daily_fetched_at="2026-06-11T00:00:00Z"),  # fetched, not completed
    )
    status, pending = main_mod._compute_run_status(repos, cached)
    assert status == "partial"
    assert pending == 1


def test_run_status_fix_mode_always_completed():
    from ingestion import main as main_mod

    repos = [FakeRepo("a", "2026-06-10T00:00:00Z")]
    status, pending = main_mod._compute_run_status(repos, _cache(), fix_mode=True)
    assert status == "completed"
    assert pending == 0


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

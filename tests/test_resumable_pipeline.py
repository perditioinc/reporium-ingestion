"""End-to-end (mocked) tests for the resumable/budgeted run_ingestion wiring.

These exercise the REAL ``ingestion.main.run_ingestion`` with every external
dependency stubbed (no GitHub, no Anthropic, no DB, no network), so the
Codex-audit fixes that live in the orchestrator -- not just the pure budget
helpers -- are covered offline and deterministically:

  * Fix #2  full corpus vs slice: global phases (trend snapshot) see the FULL
            corpus size, not the budgeted slice.
  * Fix #1  lost-work checkpoint: repos are marked COMPLETED only AFTER the
            API post, and only when the post had no errors.
  * Fix #4  end-to-end monotonic budget: an already-exhausted deadline skips
            the per-repo phases (nothing fetched) yet still finishes cleanly.
  * Fix #5  run status: reported "partial" when the corpus is not drained.

Pure offline -- marked no_db so the PostgreSQL fixture is not required.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from ingestion.config import RunMode
from ingestion import main as main_module

pytestmark = pytest.mark.no_db


@dataclass
class _FakeGHRepo:
    name: str
    updated_at: str
    stars: int = 1
    forks_count: int = 0
    is_archived: bool = False
    is_fork: bool = False
    forked_from: str | None = None


@dataclass
class _FakeCacheRow:
    name: str
    github_updated_at: str | None = None
    daily_fetched_at: str | None = None
    completed_at: str | None = None
    completed_github_updated_at: str | None = None


class _FakeDB:
    """Minimal async cache stub that records mark_completed calls."""

    def __init__(self, rows: list[_FakeCacheRow]):
        self._rows = {r.name: r for r in rows}
        self.completed: list[tuple[str, str | None]] = []
        self.finished_status: str | None = None
        self.init = AsyncMock()
        self.start_run = AsyncMock(return_value=1)

    async def get_all_repos(self):
        return list(self._rows.values())

    async def mark_completed(self, name: str, github_updated_at):
        self.completed.append((name, github_updated_at))
        self._rows[name] = _FakeCacheRow(
            name=name,
            github_updated_at=github_updated_at,
            daily_fetched_at="2026-06-18T00:00:00Z",
            completed_at="2026-06-18T00:05:00Z",
            completed_github_updated_at=github_updated_at,
        )

    async def finish_run(self, *, run_id, repos_processed, repos_updated,
                         api_calls_made, rate_limit_hits, status):
        self.finished_status = status


def _wire(monkeypatch, *, corpus, db, upsert_errors=None,
          budget_seconds=2700, fetched_names=None):
    """Stub every run_ingestion dependency. Returns (fake_api, trends_holder)."""
    fake_rl = MagicMock()
    fake_rl.calls_this_run = 0
    fake_rl.remaining = 4500
    fake_rl.estimate_calls = MagicMock(return_value=10)
    fake_rl.check_budget = AsyncMock(
        return_value=MagicMock(ok=True, message="", wait_seconds=0)
    )
    monkeypatch.setattr(main_module, "RateLimitManager", lambda **kw: fake_rl)
    monkeypatch.setattr(main_module, "CacheDatabase", lambda *a, **kw: db)
    monkeypatch.setattr(main_module, "RepoSummarizer", lambda *a, **kw: MagicMock())

    fake_api = MagicMock()
    fake_api.upsert_repos = AsyncMock(
        return_value=MagicMock(upserted=len(corpus), errors=upsert_errors or [])
    )
    fake_api.post_trend_snapshot = AsyncMock()
    fake_api.post_gap_analysis = AsyncMock()
    monkeypatch.setattr(main_module, "ReporiumAPIClient", lambda *a, **kw: fake_api)

    fake_gh = MagicMock()
    fake_gh.__aenter__ = AsyncMock(return_value=fake_gh)
    fake_gh.__aexit__ = AsyncMock(return_value=None)
    fake_gh.get_rate_limit = AsyncMock()
    fake_gh.get_repos = AsyncMock(return_value=list(corpus))
    fake_gh.hydrate_fork_parents = AsyncMock()
    monkeypatch.setattr(main_module, "GitHubClient", lambda *a, **kw: fake_gh)

    # Fetcher returns a FetchedRepo-like object per selected repo.
    fetched_holder: dict = {"selected": None}

    async def _fetch_changed(selected, mode):
        fetched_holder["selected"] = list(selected)
        out = []
        for r in selected:
            f = MagicMock()
            f.github_repo = r
            out.append(f)
        return out

    fake_fetcher = MagicMock()
    fake_fetcher.fetch_changed_repos = AsyncMock(side_effect=_fetch_changed)
    monkeypatch.setattr(main_module, "RepoFetcher", lambda *a, **kw: fake_fetcher)

    async def _fake_payload(fetched, summarizer):
        return {
            "name": fetched.github_repo.name,
            "tags": ["Active"],
            "categories": [],
            "github_updated_at": fetched.github_repo.updated_at,
        }
    monkeypatch.setattr(main_module, "_to_api_payload", _fake_payload)

    # No AI enrichment / commit-stats network: gate to empty + disable.
    monkeypatch.setattr(main_module, "_select_payloads_for_enrichment", lambda pairs: [])
    monkeypatch.setenv("COMMIT_STATS_ENABLED", "0")
    monkeypatch.setenv("RUN_TIME_BUDGET_SECONDS", str(budget_seconds))
    monkeypatch.setenv("MAX_REPOS_PER_RUN", "0")  # no cap; isolate budget/status logic

    fake_settings = MagicMock()
    fake_settings.cache_db_path = ":memory:"
    fake_settings.gh_username = "perditioinc"
    fake_settings.gh_token = ""          # commit-stats off anyway
    fake_settings.min_rate_limit_buffer = 100
    fake_settings.reporium_api_url = "http://localhost:8000"
    fake_settings.reporium_api_key = ""
    fake_settings.ingest_api_key = ""
    fake_settings.database_url = ""
    fake_settings.anthropic_api_key = ""
    monkeypatch.setattr(main_module, "get_settings", lambda: fake_settings)

    fake_pubsub = MagicMock()
    fake_pubsub.publish_repo_ingested = MagicMock()
    sys.modules["ingestion.events.pubsub"] = fake_pubsub

    return fake_api, fetched_holder


@pytest.mark.asyncio
async def test_trend_snapshot_reports_full_corpus_not_slice(monkeypatch):
    # Fix #2: 3 repos in corpus; all pending. The trend snapshot total_repos
    # must equal the FULL corpus size even though it is built from payloads.
    corpus = [
        _FakeGHRepo("a", "2026-06-10T00:00:00Z"),
        _FakeGHRepo("b", "2026-06-11T00:00:00Z"),
        _FakeGHRepo("c", "2026-06-12T00:00:00Z"),
    ]
    db = _FakeDB([])
    fake_api, _ = _wire(monkeypatch, corpus=corpus, db=db)

    await main_module.run_ingestion(RunMode.QUICK)

    snap = fake_api.post_trend_snapshot.await_args.args[0]
    assert snap.total_repos == 3  # full corpus, not a slice


@pytest.mark.asyncio
async def test_completed_marker_set_after_successful_post(monkeypatch):
    # Fix #1: a clean post marks every fetched repo COMPLETED.
    corpus = [
        _FakeGHRepo("a", "2026-06-10T00:00:00Z"),
        _FakeGHRepo("b", "2026-06-11T00:00:00Z"),
    ]
    db = _FakeDB([])
    _wire(monkeypatch, corpus=corpus, db=db)

    await main_module.run_ingestion(RunMode.QUICK)

    assert sorted(n for n, _ in db.completed) == ["a", "b"]
    # Drained -> completed status.
    assert db.finished_status == "completed"


@pytest.mark.asyncio
async def test_completed_marker_skipped_on_post_errors(monkeypatch):
    # Fix #1 + #5: API errors -> no completed markers, status partial.
    corpus = [_FakeGHRepo("a", "2026-06-10T00:00:00Z")]
    db = _FakeDB([])
    _wire(monkeypatch, corpus=corpus, db=db, upsert_errors=["HTTP 500: boom"])

    await main_module.run_ingestion(RunMode.QUICK)

    assert db.completed == []          # nothing checkpointed
    assert db.finished_status == "partial"


@pytest.mark.asyncio
async def test_exhausted_budget_skips_per_repo_work_but_finishes(monkeypatch):
    # Fix #4: when the per-run budget is exhausted, the per-repo phases are
    # skipped (NO repos fetched), yet the run finishes cleanly, runs the GLOBAL
    # phase with the true corpus size, and reports "partial" (corpus pending).
    corpus = [
        _FakeGHRepo("a", "2026-06-10T00:00:00Z"),
        _FakeGHRepo("b", "2026-06-11T00:00:00Z"),
    ]
    db = _FakeDB([])
    fake_api, fetched = _wire(monkeypatch, corpus=corpus, db=db, budget_seconds=2700)

    # Trip the monotonic budget guard without touching the real clock (patching
    # time.monotonic globally breaks the asyncio event loop). Stub the guard so
    # it reports "exhausted" only AFTER the deadline has been captured.
    real_guard = main_module._budget_exhausted

    def _always_exhausted(deadline):
        # deadline is a captured float when the budget is active; trip on it.
        return deadline is not None
    monkeypatch.setattr(main_module, "_budget_exhausted", _always_exhausted)

    await main_module.run_ingestion(RunMode.QUICK)

    # Nothing fetched (selected_repos emptied by the budget guard).
    assert fetched["selected"] == []
    assert db.completed == []
    assert db.finished_status == "partial"
    # Global phase still ran with the true corpus size.
    snap = fake_api.post_trend_snapshot.await_args.args[0]
    assert snap.total_repos == 2


@pytest.mark.asyncio
async def test_status_partial_when_some_repos_left_uncheckpointed(monkeypatch):
    # Fix #5: even with NO selection cap, if the end-state cache still has a
    # pending (uncheckpointed) repo, status is "partial".
    corpus = [
        _FakeGHRepo("a", "2026-06-10T00:00:00Z"),
        _FakeGHRepo("b", "2026-06-11T00:00:00Z"),
    ]
    # Pre-seed 'b' as fetched-but-not-completed and make the post error so it is
    # never completed this run -> corpus not drained.
    db = _FakeDB([
        _FakeCacheRow("b", github_updated_at="2026-06-11T00:00:00Z",
                      daily_fetched_at="2026-06-12T00:00:00Z"),
    ])
    _wire(monkeypatch, corpus=corpus, db=db, upsert_errors=["partial failure"])

    await main_module.run_ingestion(RunMode.QUICK)

    assert db.finished_status == "partial"


@pytest.mark.asyncio
async def test_budget_deferred_enrichment_repos_not_marked_completed(monkeypatch):
    """Fix #1/#4 follow-up: when AI enrichment is skipped because the per-run
    budget is exhausted, the repos that WERE selected for enrichment must NOT be
    marked completed -- otherwise they are skipped next run and never enriched
    until their GitHub updated_at changes. They are still posted (freshness), but
    left un-checkpointed so the next run re-enriches them."""
    corpus = [
        _FakeGHRepo("a", "2026-06-10T00:00:00Z"),
        _FakeGHRepo("b", "2026-06-11T00:00:00Z"),
    ]
    db = _FakeDB([])
    fake_api, _ = _wire(monkeypatch, corpus=corpus, db=db)
    # The enrichment gate selects ALL posted payloads this run.
    monkeypatch.setattr(
        main_module, "_select_payloads_for_enrichment",
        lambda pairs: [p for p, _ in pairs],
    )
    # Budget: NOT exhausted at the fetch guard (1st call) so fetch+post proceed;
    # exhausted at the enrichment guard (subsequent calls) so enrichment defers.
    state = {"calls": 0}

    def fake_exhausted(deadline):
        state["calls"] += 1
        return state["calls"] > 1

    monkeypatch.setattr(main_module, "_budget_exhausted", fake_exhausted)

    await main_module.run_ingestion(RunMode.QUICK)

    fake_api.upsert_repos.assert_awaited()  # repos WERE posted (freshness kept)
    completed = {n for n, _ in db.completed}
    assert completed == set(), (
        f"budget-deferred-enrichment repos must not be marked completed; got {completed}"
    )

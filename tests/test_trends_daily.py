"""
KAN-DRAFT-trends-daily-snapshot: ensure the daily Cloud Run Job (QUICK mode)
writes a trend_snapshot to reporium-api on every run.

Background:
    The daily Cloud Run Job invokes `python -m ingestion run --mode quick`
    (see deploy/job.yaml). Until this fix, the trend-snapshot writer was
    gated behind `if mode in (RunMode.WEEKLY, RunMode.FULL)`, so the daily
    path never produced snapshots and `/trends/report` reported
    `period.snapshots: 0` indefinitely.

These tests pin the new contract: regardless of run mode, the snapshot +
gap-analysis writers fire after a successful upsert.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestion.config import RunMode
from ingestion import main as main_module


def _patch_run_ingestion_dependencies(monkeypatch, *, mode_under_test: RunMode):
    """
    Stub out every external dependency of run_ingestion so we can assert
    on the trend-snapshot wiring in isolation.

    Returns the AsyncMock instance attached as ReporiumAPIClient so callers
    can inspect post_trend_snapshot / post_gap_analysis call args.
    """
    # --- Cache DB (sqlite) ---
    fake_db = MagicMock()
    fake_db.init = AsyncMock()
    fake_db.start_run = AsyncMock(return_value=1)
    fake_db.finish_run = AsyncMock()
    fake_db.get_all_repos = AsyncMock(return_value=[])
    monkeypatch.setattr(main_module, "CacheDatabase", lambda *a, **kw: fake_db)

    # --- Rate limiter ---
    fake_rl = MagicMock()
    fake_rl.calls_this_run = 0
    fake_rl.remaining = 4500
    fake_rl.estimate_calls = MagicMock(return_value=10)
    fake_rl.check_budget = AsyncMock(return_value=MagicMock(ok=True, message="", wait_seconds=0))
    monkeypatch.setattr(main_module, "RateLimitManager", lambda **kw: fake_rl)

    # --- Summarizer (no Anthropic call) ---
    fake_summarizer = MagicMock()
    monkeypatch.setattr(main_module, "RepoSummarizer", lambda *a, **kw: fake_summarizer)

    # --- API client: capture post_trend_snapshot / post_gap_analysis ---
    fake_api = MagicMock()
    fake_api.upsert_repos = AsyncMock(return_value=MagicMock(upserted=1, errors=[]))
    fake_api.post_trend_snapshot = AsyncMock()
    fake_api.post_gap_analysis = AsyncMock()
    monkeypatch.setattr(main_module, "ReporiumAPIClient", lambda *a, **kw: fake_api)

    # --- GitHub client (async context manager) ---
    fake_repo = MagicMock()
    fake_repo.name = "test-repo"
    fake_repo.updated_at = "2026-04-30T00:00:00Z"

    fake_gh = MagicMock()
    fake_gh.__aenter__ = AsyncMock(return_value=fake_gh)
    fake_gh.__aexit__ = AsyncMock(return_value=None)
    fake_gh.get_rate_limit = AsyncMock()
    fake_gh.get_repos = AsyncMock(return_value=[fake_repo])
    fake_gh.hydrate_fork_parents = AsyncMock()
    monkeypatch.setattr(main_module, "GitHubClient", lambda *a, **kw: fake_gh)

    # --- Fetcher: pretend we fetched one repo ---
    fake_fetcher = MagicMock()
    fake_fetched = MagicMock()
    fake_fetcher.fetch_changed_repos = AsyncMock(return_value=[fake_fetched])
    monkeypatch.setattr(main_module, "RepoFetcher", lambda *a, **kw: fake_fetcher)

    # --- _to_api_payload: skip the real enrichment chain ---
    async def _fake_payload(fetched, summarizer):
        return {"name": "test-repo", "tags": ["Active"], "categories": []}
    monkeypatch.setattr(main_module, "_to_api_payload", _fake_payload)

    # --- Settings (avoid loading .env / GCP secrets) ---
    fake_settings = MagicMock()
    fake_settings.cache_db_path = ":memory:"
    fake_settings.gh_username = "perditioinc"
    fake_settings.min_rate_limit_buffer = 100
    fake_settings.reporium_api_url = "http://localhost:8000"
    fake_settings.reporium_api_key = ""
    fake_settings.ingest_api_key = ""
    monkeypatch.setattr(main_module, "get_settings", lambda: fake_settings)

    # --- Pubsub publisher (imported lazily inside run_ingestion) ---
    import sys
    fake_pubsub = MagicMock()
    fake_pubsub.publish_repo_ingested = MagicMock()
    sys.modules["ingestion.events.pubsub"] = fake_pubsub

    return fake_api


@pytest.mark.asyncio
async def test_quick_mode_writes_trend_snapshot(monkeypatch):
    """The daily Cloud Run Job invokes QUICK mode — it MUST write a snapshot."""
    fake_api = _patch_run_ingestion_dependencies(monkeypatch, mode_under_test=RunMode.QUICK)

    await main_module.run_ingestion(RunMode.QUICK)

    assert fake_api.post_trend_snapshot.await_count == 1, (
        "QUICK mode must call post_trend_snapshot exactly once per run "
        "(daily Cloud Run Job is the only thing keeping /trends/report fresh)"
    )
    snapshot_arg = fake_api.post_trend_snapshot.await_args.args[0]
    assert snapshot_arg.total_repos == 1
    assert snapshot_arg.captured_at  # non-empty ISO timestamp


@pytest.mark.asyncio
async def test_quick_mode_writes_gap_analysis(monkeypatch):
    """Gap analysis travels with the snapshot — both fire on QUICK runs."""
    fake_api = _patch_run_ingestion_dependencies(monkeypatch, mode_under_test=RunMode.QUICK)

    await main_module.run_ingestion(RunMode.QUICK)

    assert fake_api.post_gap_analysis.await_count == 1


@pytest.mark.asyncio
async def test_weekly_mode_still_writes_trend_snapshot(monkeypatch):
    """Regression guard: WEEKLY behavior must be unchanged."""
    fake_api = _patch_run_ingestion_dependencies(monkeypatch, mode_under_test=RunMode.WEEKLY)

    await main_module.run_ingestion(RunMode.WEEKLY)

    assert fake_api.post_trend_snapshot.await_count == 1
    assert fake_api.post_gap_analysis.await_count == 1


@pytest.mark.asyncio
async def test_full_mode_still_writes_trend_snapshot(monkeypatch):
    """Regression guard: FULL behavior must be unchanged."""
    fake_api = _patch_run_ingestion_dependencies(monkeypatch, mode_under_test=RunMode.FULL)

    await main_module.run_ingestion(RunMode.FULL)

    assert fake_api.post_trend_snapshot.await_count == 1
    assert fake_api.post_gap_analysis.await_count == 1

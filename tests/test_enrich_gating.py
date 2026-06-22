"""KAN-230: gate per-repo AI enrichment so unchanged, already-enriched repos
are skipped instead of re-sent to Claude on every nightly run.

Root cause (live-verified 2026-05-18): `ingestion.main` builds a payload for
every fetched repo and calls `_enrich_payloads_with_ai` on the ENTIRE
~1866-repo corpus every run. One Claude call per payload with no
"already enriched / unchanged" filter blows past the Cloud Run Job timeout,
so `integration_tags` never persist and `COMPATIBLE_WITH` edges stay dead.

The durable, post-#96 (PostgreSQL-backed cache) signal that a repo has
already been fully processed is its `repo_cache` row:

  * a row exists, AND
  * ``daily_fetched_at`` is set (it went through a full fetch at least once), AND
  * ``github_updated_at`` equals the repo's current GitHub ``updated_at``
    (GitHub has not changed it since we last processed it).

When all three hold the repo is unchanged-and-processed and must be skipped.
Never-fetched / changed / forced repos must still be enriched. An
``ENRICH_FORCE_ALL=1`` override bypasses the gate for a full re-enrich.

These are pure unit tests: no DB, no network, Anthropic SDK stubbed. They
exercise the gating predicate and the gated selection at the seam where
Claude is called.
"""

from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import patch

import pytest

from ingestion.cache.models import RepoCacheRow

# No DB needed — gating reads the in-memory FetchedRepo.cache snapshot.
pytestmark = pytest.mark.no_db


# ── Minimal stubs (mirror tests/test_ai_enrichment_in_nightly.py) ─────────────


class _StubUsage:
    def __init__(self, in_tokens: int = 100, out_tokens: int = 200) -> None:
        self.input_tokens = in_tokens
        self.output_tokens = out_tokens


class _StubContent:
    def __init__(self, text: str) -> None:
        self.text = text


class _StubMessage:
    def __init__(self, text: str) -> None:
        self.content = [_StubContent(text)]
        self.usage = _StubUsage()


_RESPONSE = json.dumps(
    {
        "readme_summary": (
            "A retrieval-augmented generation framework for production LLM "
            "applications used by enterprise search teams."
        ),
        "integration_tags": ["langchain", "fastapi", "pgvector"],
        "skill_areas": ["Retrieval-Augmented Generation"],
        "industries": ["Developer Tools"],
        "use_cases": ["Document Question Answering"],
        "modalities": ["Text"],
        "ai_trends": ["Compound AI Systems"],
        "deployment_context": ["Self-hosted"],
        "quality_assessment": "high",
        "maturity_level": "production",
    }
)


class _AsyncStubMessages:
    def __init__(self, response_text: str) -> None:
        self._response_text = response_text
        self.calls: list[dict[str, Any]] = []

    async def create(self, *, model: str, max_tokens: int, messages: list[dict]) -> _StubMessage:
        self.calls.append({"model": model, "max_tokens": max_tokens, "messages": messages})
        return _StubMessage(self._response_text)


class _StubAsyncAnthropicClient:
    last_instance: "_StubAsyncAnthropicClient | None" = None

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.messages = _AsyncStubMessages(_RESPONSE)
        _StubAsyncAnthropicClient.last_instance = self

    async def close(self) -> None:
        pass


@pytest.fixture
def stub_anthropic(monkeypatch):
    original = sys.modules.get("anthropic")
    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = _StubAsyncAnthropicClient  # unused here
    fake_mod.AsyncAnthropic = _StubAsyncAnthropicClient

    class _APIError(Exception):
        pass

    fake_mod.APIError = _APIError
    sys.modules["anthropic"] = fake_mod
    _StubAsyncAnthropicClient.last_instance = None
    yield fake_mod
    if original is None:
        del sys.modules["anthropic"]
    else:
        sys.modules["anthropic"] = original


# ── FetchedRepo factory ───────────────────────────────────────────────────────


def _fetched(
    name: str,
    *,
    github_updated_at: str = "2026-05-10T00:00:00Z",
    cache_github_updated_at: str | None = None,
    cache_daily_fetched_at: str | None = None,
    has_cache_row: bool = True,
):
    """Build a FetchedRepo with a controllable durable-cache snapshot.

    `cache_github_updated_at` defaults to the repo's own updated_at (the
    "unchanged" case). Set `has_cache_row=False` for a never-seen repo.
    """
    from ingestion.github.client import GitHubRepo
    from ingestion.github.fetcher import FetchedRepo

    repo = GitHubRepo(
        name=name,
        full_name=f"perditioinc/{name}",
        owner="perditioinc",
        description="A RAG framework.",
        is_fork=False,
        is_private=False,
        forked_from=None,
        primary_language="Python",
        github_url=f"https://github.com/perditioinc/{name}",
        updated_at=github_updated_at,
        pushed_at=github_updated_at,
        created_at="2025-01-01T00:00:00Z",
        default_branch="main",
        stars=10,
        forks_count=2,
        open_issues_count=1,
        topics=[],
        is_archived=False,
        license_spdx="MIT",
    )

    if has_cache_row:
        cache = RepoCacheRow(
            name=name,
            github_updated_at=(
                cache_github_updated_at
                if cache_github_updated_at is not None
                else github_updated_at
            ),
            daily_fetched_at=cache_daily_fetched_at,
        )
    else:
        cache = None

    return FetchedRepo(repo, cache)


# ── Gating predicate ──────────────────────────────────────────────────────────


def test_predicate_skips_unchanged_and_already_processed():
    """Cache row exists, daily_fetched_at set, github_updated_at matches →
    repo is unchanged & already processed → do NOT enrich."""
    from ingestion.main import _needs_ai_enrichment

    f = _fetched(
        "rag-pipeline",
        github_updated_at="2026-05-10T00:00:00Z",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T03:00:00Z",
    )
    assert _needs_ai_enrichment(f, force_all=False) is False


def test_predicate_enriches_never_seen_repo():
    """No durable cache row at all → must enrich."""
    from ingestion.main import _needs_ai_enrichment

    f = _fetched("brand-new-repo", has_cache_row=False)
    assert _needs_ai_enrichment(f, force_all=False) is True


def test_predicate_enriches_fetched_but_never_daily():
    """Cache row exists but daily_fetched_at is None → never fully
    processed → must enrich."""
    from ingestion.main import _needs_ai_enrichment

    f = _fetched(
        "permanent-only-repo",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at=None,
    )
    assert _needs_ai_enrichment(f, force_all=False) is True


def test_predicate_enriches_changed_repo():
    """GitHub updated_at moved since last process → re-enrich."""
    from ingestion.main import _needs_ai_enrichment

    f = _fetched(
        "actively-developed",
        github_updated_at="2026-05-18T12:00:00Z",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T03:00:00Z",
    )
    assert _needs_ai_enrichment(f, force_all=False) is True


def test_predicate_force_all_overrides_skip():
    """ENRICH_FORCE_ALL → enrich even an unchanged, processed repo."""
    from ingestion.main import _needs_ai_enrichment

    f = _fetched(
        "rag-pipeline",
        github_updated_at="2026-05-10T00:00:00Z",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T03:00:00Z",
    )
    assert _needs_ai_enrichment(f, force_all=True) is True


# ── Gated selection at the Claude seam ────────────────────────────────────────


def _payload_for(fetched) -> dict:
    """A minimal tagger-shaped payload paired with a FetchedRepo."""
    r = fetched.github_repo
    return {
        "name": r.name,
        "owner": r.owner,
        "description": r.description,
        "is_fork": r.is_fork,
        "primary_language": r.primary_language,
        "forked_from": r.forked_from,
        "dependencies": ["langchain"],
        "skill_areas": [],
        "industries": [],
        "use_cases": [],
        "modalities": [],
        "ai_trends": [],
        "deployment_context": [],
        "maturity_level": None,
        "quality_assessment": None,
        "integration_tags": [],
        "readme_summary": "short",
    }


@pytest.mark.asyncio
async def test_select_payloads_for_enrichment_filters_corpus(monkeypatch):
    """The exported selection helper returns ONLY new/changed/forced repos
    out of a mixed corpus — the headline KAN-230 behaviour."""
    from ingestion.main import _select_payloads_for_enrichment

    monkeypatch.delenv("ENRICH_FORCE_ALL", raising=False)

    new = _fetched("new-repo", has_cache_row=False)
    changed = _fetched(
        "changed-repo",
        github_updated_at="2026-05-18T00:00:00Z",
        cache_github_updated_at="2026-05-01T00:00:00Z",
        cache_daily_fetched_at="2026-05-02T00:00:00Z",
    )
    unchanged = _fetched(
        "unchanged-repo",
        github_updated_at="2026-05-10T00:00:00Z",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T00:00:00Z",
    )

    pairs = [
        (_payload_for(new), new),
        (_payload_for(changed), changed),
        (_payload_for(unchanged), unchanged),
    ]
    selected = _select_payloads_for_enrichment(pairs)
    selected_names = sorted(p["name"] for p in selected)

    assert selected_names == ["changed-repo", "new-repo"]
    assert "unchanged-repo" not in selected_names


@pytest.mark.asyncio
async def test_select_payloads_force_all_returns_everything(monkeypatch):
    """ENRICH_FORCE_ALL=1 → the gate is bypassed, every payload selected."""
    from ingestion.main import _select_payloads_for_enrichment

    monkeypatch.setenv("ENRICH_FORCE_ALL", "1")

    unchanged_a = _fetched(
        "u-a",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T00:00:00Z",
    )
    unchanged_b = _fetched(
        "u-b",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T00:00:00Z",
    )
    pairs = [
        (_payload_for(unchanged_a), unchanged_a),
        (_payload_for(unchanged_b), unchanged_b),
    ]
    selected = _select_payloads_for_enrichment(pairs)
    assert sorted(p["name"] for p in selected) == ["u-a", "u-b"]


@pytest.mark.asyncio
async def test_only_selected_payloads_hit_claude(stub_anthropic, monkeypatch):
    """End-to-end at the seam: with one unchanged and one new repo, only the
    new repo's payload is sent to Claude and gets integration_tags."""
    from ingestion.main import (
        _select_payloads_for_enrichment,
        _enrich_payloads_with_ai,
    )

    monkeypatch.delenv("ENRICH_FORCE_ALL", raising=False)

    new = _fetched("new-repo", has_cache_row=False)
    unchanged = _fetched(
        "unchanged-repo",
        cache_github_updated_at="2026-05-10T00:00:00Z",
        cache_daily_fetched_at="2026-05-11T00:00:00Z",
    )
    p_new = _payload_for(new)
    p_unchanged = _payload_for(unchanged)

    to_enrich = _select_payloads_for_enrichment(
        [(p_new, new), (p_unchanged, unchanged)]
    )
    stats = await _enrich_payloads_with_ai(
        to_enrich,
        api_key="sk-ant-test",
        model="claude-sonnet-4-20250514",
        provider="frontier",
    )

    assert stats["attempted"] == 1
    assert stats["enriched"] == 1
    # New repo enriched; unchanged repo never touched Claude.
    assert p_new["integration_tags"] == ["langchain", "fastapi", "pgvector"]
    assert p_unchanged["integration_tags"] == []
    client = _StubAsyncAnthropicClient.last_instance
    assert client is not None
    assert len(client.messages.calls) == 1

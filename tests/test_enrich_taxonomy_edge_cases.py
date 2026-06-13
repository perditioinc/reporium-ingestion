"""Enrichment gating + taxonomy edge cases at the Claude seam.

Complements tests/test_enrich_gating.py (cache-driven gating) and
tests/test_ai_enrichment_in_nightly.py (happy-path wiring) with the awkward
inputs the corpus actually produces:

  * a FORK with NO upstream tags - Claude legitimately returns an empty
    ``integration_tags`` list, and that empty result must NOT clobber whatever
    the deterministic tagger already put on the payload;
  * an EMPTY-README repo - Claude returns an empty / null ``readme_summary``,
    which the parser must normalize to ``None`` and the merge must not use to
    overwrite a non-empty fallback summary;
  * the gating predicate must treat forks exactly like non-forks (the cache
    change-signal, not fork status, decides re-enrichment).

All Claude calls are mocked (no live API, no key, no network) using the same
stub shape as tests/test_ai_enrichment_in_nightly.py. No DB.
"""

from __future__ import annotations

import json
import sys
import types
from typing import Any

import pytest

pytestmark = pytest.mark.no_db


# -- Anthropic stub (mirrors tests/test_ai_enrichment_in_nightly.py) -----------


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


class _AsyncStubMessages:
    def __init__(self, response_text: str) -> None:
        self._response_text = response_text
        self.calls: list[dict[str, Any]] = []

    async def create(self, *, model: str, max_tokens: int, messages: list[dict]) -> _StubMessage:
        self.calls.append({"model": model, "max_tokens": max_tokens, "messages": messages})
        return _StubMessage(self._response_text)


class _StubAsyncAnthropicClient:
    last_instance: "_StubAsyncAnthropicClient | None" = None
    # Each test sets this before constructing the client (via the fixture).
    _next_response: str = "{}"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.messages = _AsyncStubMessages(_StubAsyncAnthropicClient._next_response)
        _StubAsyncAnthropicClient.last_instance = self

    async def close(self) -> None:
        pass


@pytest.fixture
def stub_anthropic(monkeypatch):
    original = sys.modules.get("anthropic")
    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = _StubAsyncAnthropicClient
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


def _set_response(payload: dict) -> None:
    _StubAsyncAnthropicClient._next_response = json.dumps(payload)


# -- _build_repo_context: fork-with-no-tag and empty-readme shapes -------------


def test_context_omits_forked_from_line_when_not_a_fork():
    """A non-fork (or a fork whose upstream is unknown - forked_from=None) must
    NOT emit a 'Forked from:' line, so Claude isn't told about an upstream that
    doesn't exist."""
    from ingestion.enrichers.ai_enricher import _build_repo_context

    ctx = _build_repo_context(
        {
            "name": "standalone",
            "owner": "perditioinc",
            "description": "A standalone tool.",
            "primary_language": "Python",
            "forked_from": None,
        }
    )
    assert "Forked from" not in ctx
    assert "perditioinc/standalone" in ctx


def test_context_includes_forked_from_line_for_real_fork():
    """A fork WITH a known upstream surfaces it to Claude so the model can
    inherit upstream context the empty local fork lacks."""
    from ingestion.enrichers.ai_enricher import _build_repo_context

    ctx = _build_repo_context(
        {
            "name": "langchain-fork",
            "owner": "perditioinc",
            "description": "A fork.",
            "primary_language": "Python",
            "forked_from": "langchain-ai/langchain",
        }
    )
    assert "Forked from: langchain-ai/langchain" in ctx


def test_context_handles_empty_readme_repo_with_missing_fields():
    """An empty-README repo often arrives with no description and no detected
    language. The context builder must still produce a usable, non-crashing
    string with explicit placeholders rather than the literal word 'None' for
    the language."""
    from ingestion.enrichers.ai_enricher import _build_repo_context

    ctx = _build_repo_context(
        {
            "name": "bare-repo",
            "owner": "perditioinc",
            "description": None,
            "primary_language": None,
            "forked_from": None,
        }
    )
    assert "perditioinc/bare-repo" in ctx
    # Missing language is rendered as the explicit 'Unknown' placeholder.
    assert "Primary Language: Unknown" in ctx
    # No dependencies line when none were extracted.
    assert "Dependencies:" not in ctx


# -- _parse_enrichment_response: no-tag fork + empty-readme normalization -------


def test_parse_no_integration_tags_yields_empty_list_not_crash():
    """A fork with no upstream signal: Claude returns no integration_tags. The
    parser must coerce the absence to an empty list (never None), so the
    downstream merge's truthiness guard works."""
    from ingestion.enrichers.ai_enricher import _parse_enrichment_response

    parsed = _parse_enrichment_response(json.dumps({"readme_summary": "x"}))
    assert parsed["integration_tags"] == []
    assert isinstance(parsed["integration_tags"], list)


def test_parse_empty_readme_summary_normalized_to_none():
    """An empty-README repo: Claude returns an empty-string readme_summary. The
    parser must normalize '' (and null) to None so the merge step's
    truthiness guard skips it and the local fallback survives."""
    from ingestion.enrichers.ai_enricher import _parse_enrichment_response

    parsed_empty = _parse_enrichment_response(json.dumps({"readme_summary": ""}))
    assert parsed_empty["readme_summary"] is None

    parsed_null = _parse_enrichment_response(json.dumps({"readme_summary": None}))
    assert parsed_null["readme_summary"] is None


def test_parse_invalid_quality_and_maturity_fall_back():
    """Out-of-taxonomy quality/maturity values (a common low-signal-fork
    failure mode) must clamp: quality -> 'medium', maturity -> None."""
    from ingestion.enrichers.ai_enricher import _parse_enrichment_response

    parsed = _parse_enrichment_response(
        json.dumps({"quality_assessment": "stellar", "maturity_level": "vapor"})
    )
    assert parsed["quality_assessment"] == "medium"
    assert parsed["maturity_level"] is None


def test_parse_lowercases_and_dedupes_integration_tags():
    """integration_tags must be lowercased, de-duplicated, order-preserving, and
    free of non-strings - the tag-hygiene contract the COMPATIBLE_WITH edge
    builder downstream depends on."""
    from ingestion.enrichers.ai_enricher import _parse_enrichment_response

    parsed = _parse_enrichment_response(
        json.dumps(
            {"integration_tags": ["LangChain", "langchain", "  FastAPI ", "", 7, "pgVector"]}
        )
    )
    assert parsed["integration_tags"] == ["langchain", "fastapi", "pgvector"]


# -- _merge_ai_fields_into_payload: empty AI output must not clobber -----------


def test_merge_empty_fork_result_preserves_tagger_tags():
    """The headline fork/no-tag case: Claude returns nothing useful for a fork,
    but the deterministic tagger already populated integration_tags. The empty
    AI result must NOT wipe them."""
    from ingestion.main import _merge_ai_fields_into_payload

    payload = {
        "integration_tags": ["pytorch", "transformers"],
        "readme_summary": "Deterministic fallback summary that clears the floor.",
        "skill_areas": ["Existing Skill"],
    }
    # AI parsed result for a no-signal fork: empty list + None summary.
    ai_data = {"integration_tags": [], "readme_summary": None, "skill_areas": []}

    _merge_ai_fields_into_payload(payload, ai_data)

    assert payload["integration_tags"] == ["pytorch", "transformers"]
    assert payload["readme_summary"] == "Deterministic fallback summary that clears the floor."
    assert payload["skill_areas"] == ["Existing Skill"]


def test_merge_nonempty_ai_overwrites_tagger_values():
    """Symmetric to the guard above: when Claude DOES return values, they take
    precedence over the deterministic fallback (richer taxonomy wins)."""
    from ingestion.main import _merge_ai_fields_into_payload

    payload = {
        "integration_tags": ["pytorch"],
        "readme_summary": "short",
        "skill_areas": [],
    }
    ai_data = {
        "integration_tags": ["langchain", "fastapi"],
        "readme_summary": "A richer AI-generated summary well past the 50 char floor here.",
        "skill_areas": ["Retrieval-Augmented Generation"],
    }
    _merge_ai_fields_into_payload(payload, ai_data)

    assert payload["integration_tags"] == ["langchain", "fastapi"]
    assert payload["readme_summary"].startswith("A richer AI-generated")
    assert payload["skill_areas"] == ["Retrieval-Augmented Generation"]


# -- End-to-end at the seam: fork-with-no-tag through _enrich_payloads_with_ai -


@pytest.mark.asyncio
async def test_enrich_payload_fork_no_tags_keeps_existing_tags(stub_anthropic):
    """Full per-payload pass for a fork where Claude returns empty tags + empty
    summary: the call is counted as enriched (no error), but the payload's
    pre-existing tagger tags and fallback summary are preserved."""
    from ingestion.main import _enrich_payloads_with_ai

    _set_response(
        {
            "readme_summary": "",
            "problem_solved": None,
            "quality_assessment": "medium",
            "maturity_level": None,
            "integration_tags": [],
            "skill_areas": [],
            "industries": [],
            "use_cases": [],
            "modalities": [],
            "ai_trends": [],
            "deployment_context": [],
        }
    )

    payload = {
        "name": "langchain-fork",
        "owner": "perditioinc",
        "description": "A fork with no distinctive README.",
        "is_fork": True,
        "primary_language": "Python",
        "forked_from": "langchain-ai/langchain",
        "dependencies": ["langchain"],
        "skill_areas": [],
        "industries": [],
        "use_cases": [],
        "modalities": [],
        "ai_trends": [],
        "deployment_context": [],
        "maturity_level": None,
        "quality_assessment": None,
        "integration_tags": ["langchain"],  # deterministic tagger already set this
        "readme_summary": "Deterministic fallback summary that clears the probe floor.",
    }

    stats = await _enrich_payloads_with_ai(
        [payload],
        api_key="sk-ant-test",
        model="claude-sonnet-4-20250514",
    )

    # The call succeeded (no error), even though it added nothing.
    assert stats["attempted"] == 1
    assert stats["enriched"] == 1
    assert stats["errors"] == 0

    # Empty AI output must not have clobbered the tagger's work.
    assert payload["integration_tags"] == ["langchain"]
    assert payload["readme_summary"].startswith("Deterministic fallback")

    # Claude was actually invoked once, and the prompt carried the upstream.
    client = _StubAsyncAnthropicClient.last_instance
    assert client is not None and len(client.messages.calls) == 1
    sent_prompt = client.messages.calls[0]["messages"][0]["content"]
    assert "langchain-ai/langchain" in sent_prompt


@pytest.mark.asyncio
async def test_enrich_payload_empty_readme_repo_gets_ai_summary(stub_anthropic):
    """An empty-README repo where Claude DOES produce a real summary: the AI
    summary supersedes the (too-short) local fallback that would otherwise fail
    the quality probe's char floor."""
    from ingestion.main import _enrich_payloads_with_ai

    _set_response(
        {
            "readme_summary": (
                "A minimal but well-named utility for batch image resizing in "
                "data pipelines, used by ML teams preprocessing vision datasets."
            ),
            "integration_tags": ["pillow", "numpy"],
            "skill_areas": ["Computer Vision"],
            "industries": [],
            "use_cases": ["Dataset Preprocessing"],
            "modalities": ["Image"],
            "ai_trends": [],
            "deployment_context": ["Self-hosted"],
            "quality_assessment": "medium",
            "maturity_level": "prototype",
        }
    )

    payload = {
        "name": "img-resize",
        "owner": "perditioinc",
        "description": None,  # empty-readme shape: little local signal
        "is_fork": False,
        "primary_language": None,
        "forked_from": None,
        "dependencies": [],
        "skill_areas": [],
        "industries": [],
        "use_cases": [],
        "modalities": [],
        "ai_trends": [],
        "deployment_context": [],
        "maturity_level": None,
        "quality_assessment": None,
        "integration_tags": [],
        "readme_summary": "tiny",  # below the 50-char probe floor
    }

    stats = await _enrich_payloads_with_ai(
        [payload],
        api_key="sk-ant-test",
        model="claude-sonnet-4-20250514",
    )

    assert stats["enriched"] == 1 and stats["errors"] == 0
    assert payload["integration_tags"] == ["pillow", "numpy"]
    assert payload["modalities"] == ["Image"]
    assert payload["maturity_level"] == "prototype"
    # The richer AI summary replaced the sub-floor fallback.
    assert payload["readme_summary"].startswith("A minimal but well-named")
    assert len(payload["readme_summary"]) >= 50


# -- Gating predicate: forks are gated by the cache signal, not fork status -----


def test_gating_treats_fork_like_any_other_repo(stub_anthropic):
    """A fork that is unchanged-and-already-processed must be SKIPPED, exactly
    like a non-fork. Fork status must not force (or suppress) re-enrichment."""
    from ingestion.cache.models import RepoCacheRow
    from ingestion.github.client import GitHubRepo
    from ingestion.github.fetcher import FetchedRepo
    from ingestion.main import _needs_ai_enrichment

    repo = GitHubRepo(
        name="settled-fork",
        full_name="perditioinc/settled-fork",
        owner="perditioinc",
        description="A fork that hasn't moved.",
        is_fork=True,
        is_private=False,
        forked_from="upstream/thing",
        primary_language="Python",
        github_url="https://github.com/perditioinc/settled-fork",
        updated_at="2026-05-10T00:00:00Z",
        pushed_at="2026-05-10T00:00:00Z",
        created_at="2025-01-01T00:00:00Z",
        default_branch="main",
        stars=3,
        forks_count=0,
        open_issues_count=0,
        topics=[],
        is_archived=False,
        license_spdx="MIT",
    )
    cache = RepoCacheRow(
        name="settled-fork",
        github_updated_at="2026-05-10T00:00:00Z",
        daily_fetched_at="2026-05-11T03:00:00Z",
    )
    fetched = FetchedRepo(repo, cache)

    # Unchanged + already processed -> skip, fork or not.
    assert _needs_ai_enrichment(fetched, force_all=False) is False
    # force_all still overrides for a fork.
    assert _needs_ai_enrichment(fetched, force_all=True) is True


def test_gating_enriches_changed_fork(stub_anthropic):
    """A fork whose upstream-sync moved its GitHub updated_at must be
    re-enriched - the change signal applies to forks too."""
    from ingestion.cache.models import RepoCacheRow
    from ingestion.github.client import GitHubRepo
    from ingestion.github.fetcher import FetchedRepo
    from ingestion.main import _needs_ai_enrichment

    repo = GitHubRepo(
        name="active-fork",
        full_name="perditioinc/active-fork",
        owner="perditioinc",
        description="A fork that just synced from upstream.",
        is_fork=True,
        is_private=False,
        forked_from="upstream/thing",
        primary_language="Python",
        github_url="https://github.com/perditioinc/active-fork",
        updated_at="2026-05-20T00:00:00Z",  # moved forward
        pushed_at="2026-05-20T00:00:00Z",
        created_at="2025-01-01T00:00:00Z",
        default_branch="main",
        stars=3,
        forks_count=0,
        open_issues_count=0,
        topics=[],
        is_archived=False,
        license_spdx="MIT",
    )
    cache = RepoCacheRow(
        name="active-fork",
        github_updated_at="2026-05-10T00:00:00Z",  # stale snapshot
        daily_fetched_at="2026-05-11T03:00:00Z",
    )
    fetched = FetchedRepo(repo, cache)

    assert _needs_ai_enrichment(fetched, force_all=False) is True

"""KAN-199: AI enrichment is wired into the nightly Cloud Run Job entry point.

Closes the regression surfaced by KAN-191's quality probe: every nightly run
since PR #64 cutover shipped empty `integration_tags` because `ingestion.main`
never invoked the AI enricher (RCA: `.audit/2026-05-03-12h-run/
enrichment-regression-rca.md`).

These tests exercise the wiring at the seam where Claude is called, with the
Anthropic SDK mocked. They do not need a live API key and do not hit the live
API. They also do not need a database — the wiring writes AI fields into the
in-memory API payload before the structural API post.
"""

from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import patch

import pytest

# Ensure tests in this file are not subject to the conftest DB-setup autouse
# fixture; they don't touch the DB.
pytestmark = pytest.mark.no_db


# ── Lightweight Anthropic SDK stub ────────────────────────────────────────────
#
# tests/test_backfill.py already skips when `anthropic` isn't importable; we
# install a minimal in-process stub here so the import succeeds and we can
# patch it. The stub mirrors only the surface ai_enricher.py + main.py rely
# on: anthropic.Anthropic(api_key=...).messages.create(...) returning an
# object with .content[0].text and .usage.{input,output}_tokens.


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


class _StubMessages:
    def __init__(self, response_text: str) -> None:
        self._response_text = response_text
        self.calls: list[dict[str, Any]] = []

    def create(self, *, model: str, max_tokens: int, messages: list[dict]) -> _StubMessage:
        self.calls.append({"model": model, "max_tokens": max_tokens, "messages": messages})
        return _StubMessage(self._response_text)


class _StubAnthropicClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.messages = _StubMessages(_StubAnthropicClient._next_response)

    # Class-level slot so each test can configure what the stub returns.
    _next_response: str = json.dumps(
        {
            "readme_summary": (
                "A retrieval-augmented generation framework for production LLM "
                "applications. Used by teams building enterprise search and "
                "knowledge assistants over private corpora."
            ),
            "problem_solved": (
                "Composes vector search, reranking, and prompt orchestration "
                "into a single deployable pipeline."
            ),
            "quality_assessment": "high",
            "maturity_level": "production",
            "skill_areas": ["Retrieval-Augmented Generation", "Vector Search"],
            "industries": ["Developer Tools"],
            "use_cases": ["Document Question Answering"],
            "modalities": ["Text"],
            "ai_trends": ["Compound AI Systems"],
            "deployment_context": ["Self-hosted", "Cloud API"],
            "integration_tags": ["langchain", "fastapi", "pgvector", "openai"],
        }
    )


@pytest.fixture
def stub_anthropic(monkeypatch):
    """Install a stub `anthropic` module so `import anthropic` succeeds."""
    if "anthropic" in sys.modules:
        original = sys.modules["anthropic"]
    else:
        original = None

    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = _StubAnthropicClient

    class _APIError(Exception):
        pass

    fake_mod.APIError = _APIError
    sys.modules["anthropic"] = fake_mod

    yield fake_mod

    if original is None:
        del sys.modules["anthropic"]
    else:
        sys.modules["anthropic"] = original


def _make_payload(name: str = "rag-pipeline", owner: str = "perditioinc") -> dict:
    """A plausible tagger-built API payload, with empty AI fields."""
    return {
        "name": name,
        "owner": owner,
        "description": "A RAG framework for production LLM applications.",
        "is_fork": False,
        "is_private": False,
        "forked_from": None,
        "primary_language": "Python",
        "github_url": f"https://github.com/{owner}/{name}",
        "tags": ["RAG", "Python"],
        "categories": [],
        "builders": [{"login": owner}],
        "pm_skills": [],
        "skill_areas": [],
        "industries": [],
        "use_cases": [],
        "modalities": [],
        "ai_trends": [],
        "deployment_context": [],
        "maturity_level": None,
        "quality_assessment": None,
        "has_tests": True,
        "has_ci": True,
        "integration_tags": [],
        "dependencies": ["langchain", "fastapi"],
        "license_spdx": "MIT",
        "languages": [],
        "commits": [],
        "readme_summary": "Short fallback summary.",
    }


# ── Per-payload AI enrichment ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_payloads_populates_integration_tags(stub_anthropic):
    """The wire-up populates non-empty integration_tags for at least one repo."""
    from ingestion.main import _enrich_payloads_with_ai

    payloads = [_make_payload(name="rag-pipeline")]

    stats = await _enrich_payloads_with_ai(
        payloads,
        api_key="sk-ant-test-fake-key",
        model="claude-sonnet-4-20250514",
    )

    assert stats["attempted"] == 1
    assert stats["enriched"] == 1
    assert stats["errors"] == 0

    p = payloads[0]
    # The headline regression: integration_tags must be non-empty after wiring.
    assert p["integration_tags"], "integration_tags should be populated by AI enrichment"
    assert "langchain" in p["integration_tags"]

    # Open-taxonomy dimensions also flow through the API path.
    assert p["skill_areas"]
    assert p["industries"] == ["Developer Tools"]
    assert p["use_cases"] == ["Document Question Answering"]
    assert p["modalities"] == ["Text"]
    assert p["ai_trends"] == ["Compound AI Systems"]
    assert p["deployment_context"] == ["Self-hosted", "Cloud API"]
    assert p["quality_assessment"] == "high"
    assert p["maturity_level"] == "production"

    # AI summary supersedes the local fallback (which often falls below the
    # probe's 50-char floor — KAN-196 RCA §2).
    assert p["readme_summary"].startswith("A retrieval-augmented")


@pytest.mark.asyncio
async def test_enrich_payloads_skips_when_api_key_missing(stub_anthropic):
    """No api_key → no Claude calls, no error, payloads unchanged."""
    from ingestion.main import _enrich_payloads_with_ai

    payloads = [_make_payload()]
    before = json.dumps(payloads, sort_keys=True)

    stats = await _enrich_payloads_with_ai(
        payloads,
        api_key="",
        model="claude-sonnet-4-20250514",
    )

    assert stats["enriched"] == 0
    assert stats["errors"] == 0
    assert json.dumps(payloads, sort_keys=True) == before


@pytest.mark.asyncio
async def test_enrich_payloads_skips_empty_list(stub_anthropic):
    """No payloads → no Claude calls, zero stats."""
    from ingestion.main import _enrich_payloads_with_ai

    stats = await _enrich_payloads_with_ai(
        [],
        api_key="sk-ant-test",
        model="claude-sonnet-4-20250514",
    )

    assert stats == {
        "attempted": 0,
        "enriched": 0,
        "errors": 0,
        "input_tokens": 0,
        "output_tokens": 0,
    }


@pytest.mark.asyncio
async def test_enrich_payloads_continues_on_per_repo_failure(stub_anthropic, monkeypatch):
    """A single repo's AI failure must not break the run for sibling repos.

    This is the explicit KAN-199 design constraint: AI enrichment failure
    shouldn't kill the entire ingestion run.

    KAN-230: per-payload calls now run concurrently under a semaphore, so
    "the boom call is the second one" no longer maps to "boom-repo fails".
    The fixture targets the failure by repo name (deterministic regardless
    of scheduling order) and assertions are order-independent.
    """
    from ingestion.main import _enrich_payloads_with_ai

    payloads = [
        _make_payload(name="ok-repo-1"),
        _make_payload(name="boom-repo"),
        _make_payload(name="ok-repo-2"),
    ]

    real_create = _StubMessages.create

    def flaky_create(self, *, model, max_tokens, messages):
        # The first user message contains the rendered prompt with the repo
        # name embedded — match on that to fail deterministically for the
        # named repo, no matter what order the concurrent calls run in.
        prompt = (messages[0] or {}).get("content", "")
        if "boom-repo" in prompt:
            raise RuntimeError("simulated Claude transient")
        return real_create(self, model=model, max_tokens=max_tokens, messages=messages)

    monkeypatch.setattr(_StubMessages, "create", flaky_create)

    stats = await _enrich_payloads_with_ai(
        payloads,
        api_key="sk-ant-test",
        model="claude-sonnet-4-20250514",
    )

    assert stats["attempted"] == 3
    assert stats["enriched"] == 2
    assert stats["errors"] == 1
    # boom-repo (index 1) failed; the two ok-repos succeeded.
    by_name = {p["name"]: p for p in payloads}
    assert by_name["ok-repo-1"]["integration_tags"]
    assert by_name["ok-repo-2"]["integration_tags"]
    assert by_name["boom-repo"]["integration_tags"] == []


@pytest.mark.asyncio
async def test_merge_ai_fields_does_not_overwrite_with_empty(stub_anthropic):
    """Empty AI output should not clobber existing payload values."""
    from ingestion.main import _merge_ai_fields_into_payload

    payload = _make_payload()
    payload["integration_tags"] = ["pre-existing-tag"]

    # AI output omits / empties everything.
    _merge_ai_fields_into_payload(payload, {})

    assert payload["integration_tags"] == ["pre-existing-tag"]
    assert payload["readme_summary"] == "Short fallback summary."


# ── Import-level wiring assertions ────────────────────────────────────────────


def test_ingestion_main_imports_run_ai_enrichment():
    """KAN-199: `ingestion.main` must import ai_enricher.run_ai_enrichment so
    the corpus catch-up phase has it in scope."""
    import ingestion.main as main_mod

    assert hasattr(main_mod, "run_ai_enrichment"), (
        "ingestion.main must import run_ai_enrichment from ai_enricher"
    )
    assert hasattr(main_mod, "_enrich_payloads_with_ai"), (
        "ingestion.main must expose the per-payload enrichment helper"
    )

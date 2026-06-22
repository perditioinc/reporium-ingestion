"""#148: unit tests for the local-first enrichment provider seam.

These exercise the flag-gate logic (local / frontier / auto + escalation) and the
local enricher's shape parity WITHOUT touching the live Ollama server or any paid
API. The local client and the anthropic SDK are both stubbed, so the suite runs
in CI with no GPU, no server, and no key.

A separate live test (``tests/test_local_enrichment_live.py``, marked ``live``)
proves the real local model on the dev box; it is skipped here.
"""

from __future__ import annotations

import json
import sys
import types

import pytest

pytestmark = pytest.mark.no_db


# --- A fake local_inference.LocalClient ------------------------------------
# Mirrors only the surface LocalEnricher uses: classify_json(...) -> object with
# .data / .text / .repaired, and confidence_of(text) -> float.


class _FakeStructured:
    def __init__(self, data: dict, *, repaired: bool = False) -> None:
        self.data = data
        self.text = json.dumps(data)
        self.repaired = repaired


_FAKE_TAXONOMY = {
    "readme_summary": "A local-model enrichment of a RAG framework for LLM apps.",
    "problem_solved": "Composes retrieval and orchestration into one pipeline.",
    "quality_assessment": "high",
    "maturity_level": "beta",
    "skill_areas": ["Retrieval-Augmented Generation"],
    "industries": ["Developer Tools"],
    "use_cases": ["Document Question Answering"],
    "modalities": ["Text"],
    "ai_trends": ["Compound AI Systems"],
    "deployment_context": ["Self-hosted"],
    "integration_tags": ["LangChain", "FastAPI", "langchain"],  # dupe + case to test cleaning
}


class _FakeLocalClient:
    """Stand-in for local_inference.LocalClient. ``behavior`` controls success/raise."""

    behavior = "ok"  # 'ok' | 'transport' | 'structured'
    confidence_value = 0.9
    last_prompt = None

    def __init__(self, *, host=None, chat_model=None, keep_alive=None) -> None:
        self.host = host
        self.chat_model = chat_model

    def classify_json(self, prompt, schema, *, num_predict=256, temperature=0.0, timeout=180.0):
        type(self).last_prompt = prompt
        if self.behavior == "transport":
            from local_inference import LocalInferenceError

            raise LocalInferenceError("ollama unreachable")
        if self.behavior == "structured":
            from local_inference import StructuredError

            raise StructuredError("could not produce valid JSON", stage="validate")
        return _FakeStructured(dict(_FAKE_TAXONOMY))

    def confidence_of(self, text: str) -> float:
        return type(self).confidence_value


@pytest.fixture
def fake_local(monkeypatch):
    """Install a fake `local_inference` module exposing LocalClient + the error
    types LocalEnricher imports."""
    real = sys.modules.get("local_inference")

    mod = types.ModuleType("local_inference")
    mod.LocalClient = _FakeLocalClient

    class LocalInferenceError(Exception):
        pass

    class StructuredError(Exception):
        def __init__(self, message, *, stage="", raw=""):
            super().__init__(message)
            self.stage = stage
            self.raw = raw

    mod.LocalInferenceError = LocalInferenceError
    mod.StructuredError = StructuredError
    sys.modules["local_inference"] = mod
    # Reset class-level knobs each test.
    _FakeLocalClient.behavior = "ok"
    _FakeLocalClient.confidence_value = 0.9
    _FakeLocalClient.last_prompt = None
    yield mod
    if real is not None:
        sys.modules["local_inference"] = real
    else:
        del sys.modules["local_inference"]


# --- LocalEnricher shape parity --------------------------------------------


def test_local_enricher_returns_parsed_taxonomy_shape(fake_local):
    from ingestion.enrichers.local_enricher import LocalEnricher

    enr = LocalEnricher()
    res = enr.enrich(
        {
            "owner": "langchain-ai",
            "name": "langchain",
            "description": "Build LLM apps.",
            "primary_language": "Python",
            "forked_from": None,
            "dependencies": ["pydantic", "openai"],
        }
    )
    d = res.data
    # All eleven fields present and normalized like the Claude path.
    assert d["readme_summary"]
    assert d["quality_assessment"] == "high"
    assert d["maturity_level"] == "beta"
    # _clean_list dedups + lowercases integration_tags.
    assert d["integration_tags"] == ["langchain", "fastapi"]
    assert res.confidence == 0.9
    assert res.input_tokens == 0  # local is $0
    assert res.model == "qwen2.5:7b-instruct-q4_K_M"


def test_local_enricher_prompt_includes_repo_context(fake_local):
    from ingestion.enrichers.local_enricher import LocalEnricher

    LocalEnricher().enrich(
        {"owner": "acme", "name": "widget", "description": "A widget.", "primary_language": "Go"}
    )
    assert "acme/widget" in _FakeLocalClient.last_prompt


def test_local_enricher_transport_failure_raises_unavailable(fake_local):
    from ingestion.enrichers.local_enricher import LocalEnricher, LocalEnricherUnavailable

    _FakeLocalClient.behavior = "transport"
    with pytest.raises(LocalEnricherUnavailable):
        LocalEnricher().enrich({"owner": "a", "name": "b"})


# --- Provider routing -------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_local_uses_local_no_key(fake_local):
    from ingestion.enrichers.provider import EnrichmentProvider

    prov = EnrichmentProvider(provider="local")
    res = await prov.enrich_one({"owner": "a", "name": "b"}, api_key="")
    assert res.backend.startswith("local:")
    assert res.escalated is False
    assert res.data["integration_tags"] == ["langchain", "fastapi"]


@pytest.mark.asyncio
async def test_provider_local_raises_when_local_down(fake_local):
    from ingestion.enrichers.provider import EnrichmentProvider
    from ingestion.enrichers.local_enricher import LocalEnricherUnavailable

    _FakeLocalClient.behavior = "transport"
    prov = EnrichmentProvider(provider="local")
    with pytest.raises(LocalEnricherUnavailable):
        await prov.enrich_one({"owner": "a", "name": "b"}, api_key="sk-ant-test")


@pytest.mark.asyncio
async def test_provider_auto_escalates_on_low_confidence(fake_local, monkeypatch):
    """auto: low local confidence + a key => escalate to the frontier stub."""
    from ingestion.enrichers.provider import EnrichmentProvider

    # Install a stub anthropic so the frontier escalation path is callable.
    _install_stub_anthropic(monkeypatch)
    _FakeLocalClient.confidence_value = 0.1  # below threshold

    prov = EnrichmentProvider(provider="auto", escalation_threshold=0.5)
    res = await prov.enrich_one(
        {"owner": "a", "name": "b"}, api_key="sk-ant-test", frontier_model="claude-x"
    )
    assert res.backend.startswith("frontier:")
    assert res.escalated is True


@pytest.mark.asyncio
async def test_provider_auto_stays_local_on_high_confidence(fake_local):
    from ingestion.enrichers.provider import EnrichmentProvider

    _FakeLocalClient.confidence_value = 0.95
    prov = EnrichmentProvider(provider="auto", escalation_threshold=0.5)
    res = await prov.enrich_one({"owner": "a", "name": "b"}, api_key="sk-ant-test")
    assert res.backend.startswith("local:")
    assert res.escalated is False


@pytest.mark.asyncio
async def test_provider_auto_no_key_stays_local(fake_local):
    """auto with low confidence but NO key must NOT escalate (stays $0)."""
    from ingestion.enrichers.provider import EnrichmentProvider

    _FakeLocalClient.confidence_value = 0.0
    prov = EnrichmentProvider(provider="auto", escalation_threshold=0.5)
    res = await prov.enrich_one({"owner": "a", "name": "b"}, api_key="")
    assert res.backend.startswith("local:")
    assert res.escalated is False


def test_resolve_provider_defaults_to_local(monkeypatch):
    from ingestion.enrichers.provider import resolve_provider

    monkeypatch.delenv("ENRICHMENT_PROVIDER", raising=False)
    assert resolve_provider() == "local"
    assert resolve_provider("frontier") == "frontier"
    assert resolve_provider("AUTO") == "auto"
    assert resolve_provider("nonsense") == "local"  # unknown -> safe default


def test_resolve_provider_reads_env(monkeypatch):
    from ingestion.enrichers.provider import resolve_provider

    monkeypatch.setenv("ENRICHMENT_PROVIDER", "frontier")
    assert resolve_provider() == "frontier"


# --- frontier stub helper ---------------------------------------------------


def _install_stub_anthropic(monkeypatch):
    class _Usage:
        input_tokens = 50
        output_tokens = 120

    class _Content:
        def __init__(self, text):
            self.text = text

    class _Msg:
        def __init__(self, text):
            self.content = [_Content(text)]
            self.usage = _Usage()

    class _Messages:
        async def create(self, *, model, max_tokens, messages):
            return _Msg(json.dumps(_FAKE_TAXONOMY))

    class _AsyncAnthropic:
        def __init__(self, api_key):
            self.messages = _Messages()

        async def close(self):
            pass

    mod = types.ModuleType("anthropic")
    mod.AsyncAnthropic = _AsyncAnthropic
    mod.Anthropic = _AsyncAnthropic

    class _APIError(Exception):
        pass

    mod.APIError = _APIError
    monkeypatch.setitem(sys.modules, "anthropic", mod)

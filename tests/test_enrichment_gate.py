"""#148: CI-safe tests for the enrichment no-regression gate wiring.

These do NOT call the live Ollama model or the HHEM weights (both unavailable in
CI). They exercise:

  * the golden set loads and every record carries the taxonomy SHAPE contract;
  * the deterministic tier (model-free) scores a real local-style taxonomy JSON
    at 1.0 and a degenerate/empty answer below floor - so the gate's ground-truth
    Tier 1 actually discriminates good from regressed enrichment.

The full three-tier gate against the live model is proven by
``eval/run_enrichment_gate.py`` (run on the dev box, result captured in
``eval/gate_result.json``); this file guards the harness plumbing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# The eval gate reuses the estate `local-inference` harness. It is installed on
# the dev box (pip install -e .../local-inference) but is NOT on PyPI, so it is
# absent in the hosted CI runner. Skip this whole module cleanly when it is not
# importable; the live gate proof lives in eval/run_enrichment_gate.py +
# eval/gate_result.json regardless.
local_inference = pytest.importorskip("local_inference")

from local_inference.eval.deterministic import score_deterministic  # noqa: E402
from local_inference.eval.goldenset import load_goldenset  # noqa: E402

pytestmark = pytest.mark.no_db

GOLDEN = Path(__file__).resolve().parents[1] / "eval" / "enrichment_goldenset.jsonl"

# A valid local-style taxonomy answer (what qwen emits via constrained decoding).
_GOOD = json.dumps(
    {
        "readme_summary": "A framework for building LLM apps with retrieval and agents.",
        "problem_solved": "Composes retrieval and orchestration into one pipeline.",
        "quality_assessment": "high",
        "maturity_level": "beta",
        "skill_areas": ["Retrieval-Augmented Generation"],
        "industries": ["Developer Tools"],
        "use_cases": ["Document Question Answering"],
        "modalities": ["Text"],
        "ai_trends": ["Agentic AI"],
        "deployment_context": ["Self-hosted"],
        "integration_tags": ["openai", "pydantic"],
    }
)

# A regressed answer: the KAN-191 failure mode - valid JSON but empty
# integration_tags (and missing fields).
_REGRESSED = json.dumps({"readme_summary": "x", "integration_tags": []})


def test_golden_set_loads_and_has_shape_contract():
    records = load_goldenset(str(GOLDEN))
    assert len(records) >= 8
    for r in records:
        kinds = {c.get("kind") for c in r.checks}
        assert "json_valid" in kinds
        assert "json_key" in kinds
        # The rendered enrichment prompt is the input the candidate sees.
        assert "Repository information:" in r.input


def test_deterministic_tier_passes_good_taxonomy():
    """A valid taxonomy JSON scores 1.0 on the langchain record's shape checks."""
    records = load_goldenset(str(GOLDEN))
    rec = next(r for r in records if r.id == "langchain-ai/langchain")
    res = score_deterministic(_GOOD, rec.checks)
    assert res.score == 1.0, [o.reason for o in res.outcomes if not o.passed]


def test_deterministic_tier_flags_regressed_taxonomy():
    """The KAN-191 regression (empty tags + missing fields) scores well below 1.0,
    so the gate's ground-truth tier discriminates it from a good answer."""
    records = load_goldenset(str(GOLDEN))
    rec = next(r for r in records if r.id == "langchain-ai/langchain")
    res = score_deterministic(_REGRESSED, rec.checks)
    assert res.score < 0.6


def test_gate_systems_importable_without_live_model():
    """The candidate/baseline factories import cleanly; building the baseline
    (reference) system needs no model and answers from the golden set."""
    from eval.run_enrichment_gate import make_reference_system

    records = load_goldenset(str(GOLDEN))
    ref_sys = make_reference_system(records)
    answer = ref_sys(records[0].input)
    assert answer == records[0].reference

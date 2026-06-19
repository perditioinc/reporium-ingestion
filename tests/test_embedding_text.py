"""Tests for the enriched embedding text builder (scripts/generate_embeddings.py).

Front-loading curated metadata (taxonomy values + category names) before the
readme is a PROVEN retrieval win (offline eval: realistic-query dense MRR +0.128,
nDCG@10 +0.084, both 95% CI>0; stacks with reranking to +33% nDCG). It lands the
structured signal inside the embedding model's ~256-token window.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from generate_embeddings import build_embedding_text

pytestmark = pytest.mark.no_db


def test_frontloads_taxonomy_and_categories_before_readme():
    row = {
        "name": "vllm", "forked_from": "vllm-project/vllm",
        "taxonomy_values": "Model Serving LLM Inference",
        "category_names": "Inference & Serving",
        "description": "high throughput", "readme_summary": "serve models fast",
        "problem_solved": "inference",
    }
    t = build_embedding_text(row)
    assert "Model Serving" in t and "Inference & Serving" in t
    assert t.index("Model Serving") < t.index("serve models fast")
    assert t.index("vllm") < t.index("serve models fast")


def test_missing_enrichment_fields_ok():
    assert build_embedding_text({"name": "x"}).strip() == "x"


def test_integration_tags_still_supported():
    t = build_embedding_text({"name": "x", "integration_tags": ["OpenAI", "Anthropic"]})
    assert "OpenAI" in t and "Anthropic" in t


def test_truncates_to_model_max():
    assert len(build_embedding_text({"description": "x" * 9999})) <= 2048

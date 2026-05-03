"""
Tests for scripts/backfill_no_tag_forks.py.

No DB, no network. Exercises the deterministic tag-computation path that the
backfill uses so a regression in taxonomy/tagger wiring is caught in CI.
"""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


def _load_backfill_module():
    root = Path(__file__).resolve().parent.parent
    # Script imports from ``ingestion.*`` via sys.path insertion — make sure the
    # repo root is importable before we load the script.
    sys.path.insert(0, str(root))
    script_path = root / "scripts" / "backfill_no_tag_forks.py"
    spec = importlib.util.spec_from_file_location("backfill_no_tag_forks", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_category_id_slugifies_name():
    bf = _load_backfill_module()
    assert bf._category_id("RAG & Retrieval") == "rag-and-retrieval"
    assert bf._category_id("Industry: Healthcare") == "industry-healthcare"
    assert bf._category_id("AI Agents") == "ai-agents"


def test_no_tag_sql_targets_only_forks_with_zero_tags():
    bf = _load_backfill_module()
    sql = bf.NO_TAG_FORKS_SQL
    # Must restrict to forks
    assert "is_fork = TRUE" in sql
    # Must require a known upstream to fetch against
    assert "forked_from IS NOT NULL" in sql
    # Must only surface repos with zero tag rows
    assert "LEFT JOIN repo_tags" in sql
    assert "HAVING COUNT(rt.tag) = 0" in sql


def test_tagger_recovers_tags_from_upstream_readme():
    """The whole point: a fork with empty topics and a tiny summary still gets
    tags when we feed the UPSTREAM README through the deterministic tagger."""
    from ingestion.enrichment.tagger import enrich_tags
    from ingestion.enrichment.taxonomy import assign_all_categories

    upstream_readme = (
        "LangChain is a framework for developing applications powered by "
        "large language models. It provides tools for building AI agents "
        "with prompt engineering, RAG, and vector database integration. "
        "Built on top of PyTorch and HuggingFace transformers."
    )

    tags = enrich_tags(
        language="Python",
        topics=[],  # forks inherit no topics — this is the #240 scenario
        stars=0,  # fork's own star count
        updated_at=datetime.now(timezone.utc).isoformat(),
        is_fork=True,
        is_archived=False,
        readme_text=upstream_readme,
    )

    # Meta tags (language + fork marker) always fire
    assert "Python" in tags
    assert "Forked" in tags
    # Keyword tagger picks up AI framework terms from the upstream README
    ai_like = {"LangChain", "Large Language Models", "AI Agents", "RAG",
               "Vector Database", "HuggingFace", "PyTorch", "Prompt Engineering"}
    assert ai_like & set(tags), f"expected AI tags, got {tags}"

    # Categories derived from tags must be non-empty
    cats = assign_all_categories(tags)
    assert cats, "expected at least one category"


def test_dry_run_computes_without_db_writes(monkeypatch):
    """backfill_one in --dry-run mode must not touch the DB cursor."""
    bf = _load_backfill_module()

    class FakeCursor:
        def execute(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError("dry-run must not execute SQL")

    class FakeConn:
        def commit(self):  # pragma: no cover
            raise AssertionError("dry-run must not commit")
        def rollback(self):  # pragma: no cover
            raise AssertionError("dry-run must not rollback")

    monkeypatch.setattr(bf, "fetch_upstream_topics", lambda upstream, token: ["llm", "agents"])
    monkeypatch.setattr(
        bf,
        "fetch_upstream_readme",
        lambda upstream, token: "A LangChain fork that adds RAG pipelines.",
    )

    repo = {
        "id": "00000000-0000-0000-0000-000000000001",
        "name": "langchain-fork",
        "owner": "perditioinc",
        "forked_from": "langchain-ai/langchain",
        "primary_language": "Python",
        "stars": 0,
        "updated_at": datetime.now(timezone.utc),
        "is_archived": False,
    }

    stats = bf.backfill_one(FakeConn(), FakeCursor(), repo, token="x", dry_run=True)
    assert stats["tag_count"] > 0
    assert stats["category_count"] > 0
    assert stats["topics_from_upstream"] == 2
    assert stats["readme_bytes"] > 0

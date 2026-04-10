"""
Regression tests for knowledge graph construction.

These run against a real PostgreSQL database (same pgvector image as production)
to catch schema-level regressions like the DEPENDS_ON bug where the script
silently returned [] when the dependencies column was dropped.

Requires DATABASE_URL to be set (provided by CI pgvector service container).
"""

import json
import sys
import os

import psycopg2
import pytest

# Add scripts/ to path so we can import build_knowledge_graph
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from build_knowledge_graph import (
    build_compatible_with,
    build_alternative_to,
    build_depends_on,
    insert_edges,
    record_history,
    verify_table,
)
from conftest import make_repo


# ---------------------------------------------------------------------------
# DEPENDS_ON — THE regression test
# ---------------------------------------------------------------------------

class TestBuildDependsOn:
    """Tests for DEPENDS_ON edge construction from repo_dependencies table."""

    def test_returns_edges_from_repo_dependencies(self, db_conn):
        """DEPENDS_ON edges are built from repo_dependencies, not repos.dependencies."""
        cur = db_conn.cursor()

        # Create two repos
        repo_a = make_repo(cur, name="my-app", owner="org")
        repo_b = make_repo(cur, name="my-lib", owner="org")

        # Add dependency: my-app depends on my-lib
        cur.execute(
            """INSERT INTO repo_dependencies (id, repo_id, package_name, package_ecosystem, is_direct)
               VALUES (gen_random_uuid(), %s, 'my-lib', 'pypi', true)""",
            (repo_a,),
        )
        db_conn.commit()

        edges = build_depends_on(cur)

        assert len(edges) >= 1
        edge = edges[0]
        assert str(edge["source"]) == repo_a or edge["source"] == repo_a
        assert edge["confidence"] == 0.95
        assert edge["metadata"]["method"] == "repo_dependencies"

    def test_empty_table_returns_empty_not_crash(self, db_conn):
        """With zero rows in repo_dependencies, returns [] — no crash."""
        cur = db_conn.cursor()
        make_repo(cur, name="lonely-repo", owner="org")
        db_conn.commit()

        edges = build_depends_on(cur)
        assert edges == []

    def test_skips_sentinel_rows(self, db_conn):
        """Sentinel rows (__none__) should not produce edges."""
        cur = db_conn.cursor()
        repo_a = make_repo(cur, name="checked-repo", owner="org")
        repo_b = make_repo(cur, name="__none__", owner="org")

        # Sentinel row
        cur.execute(
            """INSERT INTO repo_dependencies (id, repo_id, package_name, package_ecosystem, is_direct)
               VALUES (gen_random_uuid(), %s, '__none__', '__sentinel__', false)""",
            (repo_a,),
        )
        db_conn.commit()

        edges = build_depends_on(cur)
        assert len(edges) == 0

    def test_skips_self_reference(self, db_conn):
        """A repo cannot depend on itself."""
        cur = db_conn.cursor()
        repo_a = make_repo(cur, name="self-dep", owner="org")

        cur.execute(
            """INSERT INTO repo_dependencies (id, repo_id, package_name, package_ecosystem, is_direct)
               VALUES (gen_random_uuid(), %s, 'self-dep', 'pypi', true)""",
            (repo_a,),
        )
        db_conn.commit()

        edges = build_depends_on(cur)
        assert len(edges) == 0

    def test_no_dependencies_column_used(self, db_conn):
        """
        THE REGRESSION TEST: verify the code does NOT query repos.dependencies.
        The dependencies column was dropped in migration 014.  If the code
        tried to query it, this test would fail because the column doesn't
        exist in our test schema (which mirrors production).
        """
        cur = db_conn.cursor()

        # Verify the column does NOT exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'repos' AND column_name = 'dependencies'
        """)
        assert cur.fetchone() is None, "repos.dependencies column should not exist"

        # build_depends_on should work fine without it
        repo_a = make_repo(cur, name="app", owner="org")
        repo_b = make_repo(cur, name="lib", owner="org")
        cur.execute(
            """INSERT INTO repo_dependencies (id, repo_id, package_name, package_ecosystem, is_direct)
               VALUES (gen_random_uuid(), %s, 'lib', 'pypi', true)""",
            (repo_a,),
        )
        db_conn.commit()

        edges = build_depends_on(cur)
        assert len(edges) >= 1


# ---------------------------------------------------------------------------
# COMPATIBLE_WITH
# ---------------------------------------------------------------------------

class TestBuildCompatibleWith:
    """Tests for COMPATIBLE_WITH edge construction from integration tags."""

    def test_requires_2_shared_tags(self, db_conn):
        """Pairs with only 1 shared tag should NOT produce an edge."""
        cur = db_conn.cursor()
        make_repo(cur, name="repo-a", owner="org",
                  integration_tags=["RAG", "Python"])
        make_repo(cur, name="repo-b", owner="org",
                  integration_tags=["RAG", "TypeScript"])
        db_conn.commit()

        edges = build_compatible_with(cur)
        # Only 1 shared tag ("rag" after lowering), so no edge
        assert len(edges) == 0

    def test_two_shared_tags_produce_edge(self, db_conn):
        """Pairs with 2+ shared tags produce an edge with correct confidence."""
        cur = db_conn.cursor()
        make_repo(cur, name="repo-a", owner="org",
                  integration_tags=["RAG", "Vector Database", "Python"])
        make_repo(cur, name="repo-b", owner="org",
                  integration_tags=["RAG", "Vector Database", "TypeScript"])
        db_conn.commit()

        edges = build_compatible_with(cur)
        assert len(edges) == 1
        edge = edges[0]
        # 2 shared tags / 5 = 0.4
        assert edge["confidence"] == pytest.approx(0.4, abs=0.01)
        assert edge["metadata"]["count"] == 2

    def test_confidence_caps_at_085(self, db_conn):
        """Confidence should not exceed 0.85 even with many shared tags."""
        cur = db_conn.cursor()
        many_tags = ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7"]
        make_repo(cur, name="repo-a", owner="org", integration_tags=many_tags)
        make_repo(cur, name="repo-b", owner="org", integration_tags=many_tags)
        db_conn.commit()

        edges = build_compatible_with(cur)
        assert len(edges) == 1
        # 7 shared / 5 = 1.4, but capped at 0.85
        assert all(edge["confidence"] <= 0.85 for edge in edges)


# ---------------------------------------------------------------------------
# ALTERNATIVE_TO
# ---------------------------------------------------------------------------

class TestBuildAlternativeTo:
    """Tests for ALTERNATIVE_TO edge construction."""

    def test_uses_primary_category(self, db_conn):
        """Repos with the same primary_category produce edges."""
        cur = db_conn.cursor()
        make_repo(cur, name="llm-a", owner="org", primary_category="Foundation Models")
        make_repo(cur, name="llm-b", owner="org", primary_category="Foundation Models")
        # Add to repo_categories too (the function checks cat_count)
        cur.execute("INSERT INTO repo_categories VALUES ((SELECT id FROM repos WHERE name='llm-a'), 'foundation-models', 'Foundation Models', true)")
        cur.execute("INSERT INTO repo_categories VALUES ((SELECT id FROM repos WHERE name='llm-b'), 'foundation-models', 'Foundation Models', true)")
        db_conn.commit()

        edges = build_alternative_to(cur)
        assert len(edges) >= 1
        assert edges[0]["confidence"] == 0.7
        assert edges[0]["metadata"]["method"] == "primary_category"

    def test_keyword_fallback_when_no_categories(self, db_conn):
        """When repo_categories is empty, falls back to keyword matching."""
        cur = db_conn.cursor()
        make_repo(cur, name="vec-a", owner="org",
                  problem_solved="A vector database for similarity search")
        make_repo(cur, name="vec-b", owner="org",
                  problem_solved="Fast vector search and embedding storage")
        db_conn.commit()

        edges = build_alternative_to(cur)
        assert len(edges) >= 1
        assert edges[0]["confidence"] == 0.4
        assert edges[0]["metadata"]["method"] == "problem_solved_keywords"


# ---------------------------------------------------------------------------
# Confidence scores
# ---------------------------------------------------------------------------

class TestConfidenceScores:
    """Verify confidence values match the spec."""

    def test_depends_on_confidence_095(self, db_conn):
        cur = db_conn.cursor()
        a = make_repo(cur, name="app", owner="org")
        b = make_repo(cur, name="lib", owner="org")
        cur.execute(
            """INSERT INTO repo_dependencies (id, repo_id, package_name, package_ecosystem, is_direct)
               VALUES (gen_random_uuid(), %s, 'lib', 'npm', true)""",
            (a,),
        )
        db_conn.commit()
        edges = build_depends_on(cur)
        assert edges[0]["confidence"] == 0.95

    def test_compatible_with_confidence_formula(self, db_conn):
        cur = db_conn.cursor()
        make_repo(cur, name="a", owner="o",
                  integration_tags=["RAG", "Vector Database", "Embeddings"])
        make_repo(cur, name="b", owner="o",
                  integration_tags=["RAG", "Vector Database", "LlamaIndex"])
        db_conn.commit()
        edges = build_compatible_with(cur)
        assert len(edges) == 1
        # 2 shared / 5 = 0.4
        assert edges[0]["confidence"] == pytest.approx(0.4, abs=0.01)

    def test_alternative_to_category_confidence_07(self, db_conn):
        cur = db_conn.cursor()
        make_repo(cur, name="x", owner="o", primary_category="AI Agents")
        make_repo(cur, name="y", owner="o", primary_category="AI Agents")
        cur.execute("INSERT INTO repo_categories VALUES ((SELECT id FROM repos WHERE name='x'), 'ai-agents', 'AI Agents', true)")
        cur.execute("INSERT INTO repo_categories VALUES ((SELECT id FROM repos WHERE name='y'), 'ai-agents', 'AI Agents', true)")
        db_conn.commit()
        edges = build_alternative_to(cur)
        assert edges[0]["confidence"] == 0.7

    def test_alternative_to_keyword_confidence_04(self, db_conn):
        cur = db_conn.cursor()
        make_repo(cur, name="m", owner="o",
                  problem_solved="An LLM framework for language model apps")
        make_repo(cur, name="n", owner="o",
                  problem_solved="Large language model serving framework")
        db_conn.commit()
        edges = build_alternative_to(cur)
        assert edges[0]["confidence"] == 0.4


# ---------------------------------------------------------------------------
# Edge insertion
# ---------------------------------------------------------------------------

class TestInsertEdges:
    """Tests for batch edge insertion."""

    def test_insert_edges_batch(self, db_conn):
        """Batch insert creates edges in the database."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="a", owner="o")
        b = make_repo(cur, name="b", owner="o")
        db_conn.commit()

        edges = [{
            "source": a,
            "target": b,
            "weight": 0.8,
            "confidence": 0.6,
            "metadata": {"test": True},
        }]
        count = insert_edges(cur, edges, "TEST_EDGE")
        db_conn.commit()

        assert count == 1
        cur.execute("SELECT confidence FROM repo_edges WHERE edge_type = 'TEST_EDGE'")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == pytest.approx(0.6)

    def test_upsert_updates_existing(self, db_conn):
        """Re-inserting the same edge updates weight/confidence."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="a", owner="o")
        b = make_repo(cur, name="b", owner="o")
        db_conn.commit()

        edges_v1 = [{"source": a, "target": b, "weight": 0.5, "confidence": 0.3, "metadata": {}}]
        insert_edges(cur, edges_v1, "TEST")
        db_conn.commit()

        edges_v2 = [{"source": a, "target": b, "weight": 0.9, "confidence": 0.8, "metadata": {"updated": True}}]
        insert_edges(cur, edges_v2, "TEST")
        db_conn.commit()

        cur.execute("SELECT weight, confidence FROM repo_edges WHERE edge_type = 'TEST'")
        row = cur.fetchone()
        assert row[0] == pytest.approx(0.9)
        assert row[1] == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# History recording
# ---------------------------------------------------------------------------

class TestRecordHistory:
    """Tests for edge history tracking."""

    def test_records_counts(self, db_conn):
        cur = db_conn.cursor()
        record_history(cur, {"COMPATIBLE_WITH": 100, "DEPENDS_ON": 50})
        db_conn.commit()

        cur.execute("SELECT edge_type, edge_count FROM repo_edges_history ORDER BY edge_type")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert ("COMPATIBLE_WITH", 100) in rows
        assert ("DEPENDS_ON", 50) in rows


# ---------------------------------------------------------------------------
# Table verification
# ---------------------------------------------------------------------------

class TestVerifyTable:
    """Tests for the migration verification check."""

    def test_passes_when_table_exists(self, db_conn):
        """Should not raise when repo_edges exists."""
        cur = db_conn.cursor()
        verify_table(cur)  # Should not raise

    def test_fails_when_table_missing(self, db_conn):
        """Should raise RuntimeError when repo_edges is absent."""
        cur = db_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS repo_edges CASCADE")
        db_conn.commit()

        with pytest.raises(RuntimeError, match="repo_edges table does not exist"):
            verify_table(cur)

        # Recreate for other tests — schema must match migration 033
        cur.execute("""
            CREATE TABLE repo_edges (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_repo_id UUID NOT NULL REFERENCES repos(id),
                target_repo_id UUID NOT NULL REFERENCES repos(id),
                edge_type TEXT NOT NULL,
                weight FLOAT DEFAULT 1.0,
                confidence FLOAT DEFAULT 0.5,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (source_repo_id, target_repo_id, edge_type)
            )
        """)
        db_conn.commit()

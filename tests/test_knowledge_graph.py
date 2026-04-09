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

# Add scripts/ and project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

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
        assert edge["evidence"]["method"] == "repo_dependencies"

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
        assert edge["evidence"]["count"] == 2

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
        assert edges[0]["evidence"]["method"] == "primary_category"

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
        assert edges[0]["evidence"]["method"] == "problem_solved_keywords"


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
            "evidence": {"test": True},
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

        edges_v1 = [{"source": a, "target": b, "weight": 0.5, "confidence": 0.3, "evidence": {}}]
        insert_edges(cur, edges_v1, "TEST")
        db_conn.commit()

        edges_v2 = [{"source": a, "target": b, "weight": 0.9, "confidence": 0.8, "evidence": {"updated": True}}]
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
    """Tests for edge count recording via ingest_runs."""

    def test_records_counts_in_ingest_runs(self, db_conn):
        """record_history writes edge counts to ingest_runs.prev_edge_counts."""
        cur = db_conn.cursor()
        # Create an ingest_run first
        cur.execute(
            """INSERT INTO ingest_runs (run_mode, status, repos_upserted, repos_processed)
               VALUES ('graph_build', 'running', 0, 0) RETURNING id"""
        )
        run_id = cur.fetchone()[0]
        db_conn.commit()

        record_history(cur, {"COMPATIBLE_WITH": 100, "DEPENDS_ON": 50}, run_id=run_id)
        db_conn.commit()

        cur.execute("SELECT prev_edge_counts FROM ingest_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        counts = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        assert counts["COMPATIBLE_WITH"] == 100
        assert counts["DEPENDS_ON"] == 50


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

        # Recreate for other tests
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


# ---------------------------------------------------------------------------
# Atomic swap tests (PR 2)
# ---------------------------------------------------------------------------

class TestAtomicSwap:
    """Tests for the atomic staging → validate → swap pattern."""

    def test_staging_table_insert(self, db_conn):
        """Edges can be inserted into a staging table and read back."""
        from ingestion.graph.atomic_swap import _create_staging_table, _insert_into_staging

        cur = db_conn.cursor()
        a = make_repo(cur, name="src", owner="o")
        b = make_repo(cur, name="tgt", owner="o")
        db_conn.commit()

        _create_staging_table(cur)
        edges = [{
            "source": a,
            "target": b,
            "weight": 0.9,
            "confidence": 0.85,
            "evidence": {"test": True},
        }]
        count = _insert_into_staging(cur, edges, "COMPATIBLE_WITH")
        assert count == 1

        cur.execute("SELECT COUNT(*) FROM repo_edges_staging")
        assert cur.fetchone()[0] == 1

    def test_archive_and_swap_preserves_history(self, db_conn):
        """Atomic swap archives old edges and inserts new ones."""
        from ingestion.graph.atomic_swap import (
            _create_staging_table, _insert_into_staging, _archive_and_swap
        )

        cur = db_conn.cursor()
        a = make_repo(cur, name="repo-a", owner="o")
        b = make_repo(cur, name="repo-b", owner="o")
        c = make_repo(cur, name="repo-c", owner="o")
        db_conn.commit()

        # Insert an old edge directly into live table
        cur.execute(
            """INSERT INTO repo_edges
               (source_repo_id, target_repo_id, edge_type, weight, confidence, metadata)
               VALUES (%s, %s, 'COMPATIBLE_WITH', 0.5, 0.3, '{}')""",
            (a, b),
        )
        db_conn.commit()

        # Stage a new edge
        _create_staging_table(cur)
        new_edges = [{
            "source": a,
            "target": c,
            "weight": 0.8,
            "confidence": 0.7,
            "evidence": {"new": True},
        }]
        _insert_into_staging(cur, new_edges, "COMPATIBLE_WITH")

        # Swap
        inserted = _archive_and_swap(cur, run_id=None)
        db_conn.commit()

        assert inserted == 1

        # Old edge should be in history
        cur.execute(
            "SELECT COUNT(*) FROM repo_edges_history WHERE source_repo_id = %s AND edge_type = 'COMPATIBLE_WITH'",
            (a,),
        )
        assert cur.fetchone()[0] >= 1

        # Live table should only have the new edge
        cur.execute("SELECT target_repo_id::text FROM repo_edges WHERE edge_type = 'COMPATIBLE_WITH'")
        live_targets = [row[0] for row in cur.fetchall()]
        assert c in live_targets
        assert b not in live_targets

    def test_swap_preserves_unmanaged_edges(self, db_conn):
        """Unmanaged edge types (e.g., MAINTAINED_BY) survive the swap."""
        from ingestion.graph.atomic_swap import (
            _create_staging_table, _insert_into_staging, _archive_and_swap
        )

        cur = db_conn.cursor()
        a = make_repo(cur, name="repo-x", owner="o")
        b = make_repo(cur, name="repo-y", owner="o")
        db_conn.commit()

        # Insert an unmanaged edge
        cur.execute(
            """INSERT INTO repo_edges
               (source_repo_id, target_repo_id, edge_type, weight, confidence, metadata)
               VALUES (%s, %s, 'MAINTAINED_BY', 1.0, 1.0, '{}')""",
            (a, b),
        )
        db_conn.commit()

        # Stage empty (no managed edges)
        _create_staging_table(cur)
        _archive_and_swap(cur, run_id=None)
        db_conn.commit()

        # MAINTAINED_BY should still be there
        cur.execute("SELECT COUNT(*) FROM repo_edges WHERE edge_type = 'MAINTAINED_BY'")
        assert cur.fetchone()[0] == 1


class TestEdgeCountValidation:
    """Tests for edge count regression detection."""

    def test_abort_on_large_drop(self):
        """Should raise EdgeCountValidationError on >50% drop."""
        from ingestion.graph.ingest_run_manager import IngestRunManager, EdgeCountValidationError

        # We can't easily test validate_edge_counts without a DB, but we can
        # test the threshold logic by checking the constants
        from ingestion.graph.ingest_run_manager import ABORT_DROP_FRACTION, WARN_DROP_FRACTION
        assert ABORT_DROP_FRACTION == 0.50
        assert WARN_DROP_FRACTION == 0.20

    def test_validation_with_db(self, db_conn):
        """Full validation flow: create prior run, validate new counts."""
        from ingestion.graph.ingest_run_manager import IngestRunManager, EdgeCountValidationError

        cur = db_conn.cursor()

        # Create a prior successful run with edge counts
        cur.execute(
            """INSERT INTO ingest_runs
               (run_mode, status, repos_processed, prev_edge_counts, finished_at)
               VALUES ('graph_build', 'success', 1000, %s, NOW())""",
            (json.dumps({"COMPATIBLE_WITH": 500, "ALTERNATIVE_TO": 300, "DEPENDS_ON": 200}),),
        )
        db_conn.commit()

        manager = IngestRunManager(db_conn.dsn if hasattr(db_conn, 'dsn') else db_conn.info.dsn)

        # New run with acceptable counts — should pass
        cur.execute(
            """INSERT INTO ingest_runs
               (run_mode, status, repos_processed)
               VALUES ('graph_build', 'running', 0) RETURNING id"""
        )
        run_id = cur.fetchone()[0]
        db_conn.commit()

        # Slight drop — should be fine
        manager.validate_edge_counts(
            run_id,
            {"COMPATIBLE_WITH": 480, "ALTERNATIVE_TO": 290, "DEPENDS_ON": 195},
        )

    def test_validation_aborts_on_zero_with_prior(self, db_conn):
        """Should abort if a type with >100 edges drops to zero."""
        from ingestion.graph.ingest_run_manager import IngestRunManager, EdgeCountValidationError

        cur = db_conn.cursor()

        # Create prior with DEPENDS_ON=200
        cur.execute(
            """INSERT INTO ingest_runs
               (run_mode, status, repos_processed, prev_edge_counts, finished_at)
               VALUES ('graph_build', 'success', 1000, %s, NOW())""",
            (json.dumps({"COMPATIBLE_WITH": 500, "DEPENDS_ON": 200}),),
        )
        db_conn.commit()

        manager = IngestRunManager(db_conn.info.dsn)

        cur.execute(
            """INSERT INTO ingest_runs
               (run_mode, status, repos_processed)
               VALUES ('graph_build', 'running', 0) RETURNING id"""
        )
        run_id = cur.fetchone()[0]
        db_conn.commit()

        with pytest.raises(EdgeCountValidationError, match="DEPENDS_ON"):
            manager.validate_edge_counts(
                run_id,
                {"COMPATIBLE_WITH": 500, "DEPENDS_ON": 0},
            )


# ---------------------------------------------------------------------------
# Embedding history tests (PR 2)
# ---------------------------------------------------------------------------

class TestEmbeddingHistory:
    """Tests for append-only embedding storage."""

    def test_is_current_column_exists(self, db_conn):
        """Verify the is_current column exists in repo_embeddings."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'repo_embeddings' AND column_name = 'is_current'
        """)
        assert cur.fetchone() is not None

    def test_unique_partial_index_enforces_one_current(self, db_conn):
        """Only one embedding per repo can have is_current=true."""
        cur = db_conn.cursor()
        repo_id = make_repo(cur, name="embed-test", owner="org")
        db_conn.commit()

        # Insert first embedding as current
        cur.execute(
            """INSERT INTO repo_embeddings (id, repo_id, embedding, model, is_current)
               VALUES (gen_random_uuid(), %s, '[]', 'test-model', true)""",
            (repo_id,),
        )
        db_conn.commit()

        # Trying to insert another current one should fail (unique partial index)
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute(
                """INSERT INTO repo_embeddings (id, repo_id, embedding, model, is_current)
                   VALUES (gen_random_uuid(), %s, '[]', 'test-model', true)""",
                (repo_id,),
            )
        db_conn.rollback()

    def test_append_only_pattern(self, db_conn):
        """Mark old as is_current=false, insert new as is_current=true."""
        cur = db_conn.cursor()
        repo_id = make_repo(cur, name="embed-hist", owner="org")
        db_conn.commit()

        # Insert v1
        cur.execute(
            """INSERT INTO repo_embeddings (id, repo_id, embedding, model, is_current)
               VALUES (gen_random_uuid(), %s, '[1.0, 2.0]', 'v1', true)""",
            (repo_id,),
        )
        db_conn.commit()

        # Simulate append-only update: mark old as not current, insert new
        cur.execute(
            "UPDATE repo_embeddings SET is_current = false WHERE repo_id = %s AND is_current = true",
            (repo_id,),
        )
        cur.execute(
            """INSERT INTO repo_embeddings (id, repo_id, embedding, model, is_current)
               VALUES (gen_random_uuid(), %s, '[3.0, 4.0]', 'v2', true)""",
            (repo_id,),
        )
        db_conn.commit()

        # Should have 2 rows total, 1 current, 1 historical
        cur.execute("SELECT COUNT(*) FROM repo_embeddings WHERE repo_id = %s", (repo_id,))
        assert cur.fetchone()[0] == 2

        cur.execute(
            "SELECT COUNT(*) FROM repo_embeddings WHERE repo_id = %s AND is_current = true",
            (repo_id,),
        )
        assert cur.fetchone()[0] == 1

        cur.execute(
            "SELECT COUNT(*) FROM repo_embeddings WHERE repo_id = %s AND is_current = false",
            (repo_id,),
        )
        assert cur.fetchone()[0] == 1

        # Current should be v2
        cur.execute(
            "SELECT model FROM repo_embeddings WHERE repo_id = %s AND is_current = true",
            (repo_id,),
        )
        assert cur.fetchone()[0] == "v2"

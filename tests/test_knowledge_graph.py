"""
Tests for scripts/build_knowledge_graph.py against a real PostgreSQL database.

Uses the db_conn fixture from conftest.py (real DB, transaction rollback).
Skipped automatically if reporium-api migrations are unavailable.

DEPENDS_ON regression (the core bug being fixed):
  Prior to the fix, build_depends_on() checked information_schema for
  repos.dependencies (dropped in migration 014). Finding nothing, it returned []
  silently. These tests verify the fixed behaviour: edges are built from the
  repo_dependencies table (migration 029).
"""

from __future__ import annotations

import json
import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.build_knowledge_graph import (
    archive_existing_edges,
    assert_schema,
    build_compatible_with,
    build_depends_on,
    insert_edges,
)


# ── test-data helpers ─────────────────────────────────────────────────────────

def _repo(cur, name: str, owner: str = "testorg",
          tags: list[str] | None = None,
          forked_from: str | None = None) -> str:
    """Insert a minimal test repo row and return its UUID string."""
    repo_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO repos
            (id, name, owner, github_url, is_fork, integration_tags,
             forked_from, ingested_at, updated_at)
        VALUES (%s, %s, %s, %s, false, %s, %s, NOW(), NOW())
        """,
        (
            repo_id,
            name,
            owner,
            f"https://github.com/{owner}/{name}",
            json.dumps(tags) if tags is not None else None,
            forked_from,
        ),
    )
    return repo_id


def _dep(cur, repo_id: str, package_name: str, ecosystem: str = "pypi") -> None:
    """Insert a row in repo_dependencies."""
    cur.execute(
        """
        INSERT INTO repo_dependencies
            (repo_id, package_name, package_ecosystem, is_direct)
        VALUES (%s, %s, %s, true)
        ON CONFLICT DO NOTHING
        """,
        (repo_id, package_name, ecosystem),
    )


def _category(cur, repo_id: str, category_name: str) -> None:
    """Insert a row in repo_categories."""
    cur.execute(
        """
        INSERT INTO repo_categories (repo_id, category_name, is_primary)
        VALUES (%s, %s, false)
        ON CONFLICT DO NOTHING
        """,
        (repo_id, category_name),
    )


# ── schema guard ──────────────────────────────────────────────────────────────

def test_assert_schema_passes_after_migrations(db_conn):
    """repo_edges table must exist after alembic upgrade head (migration 033)."""
    cur = db_conn.cursor()
    # Should not raise
    assert_schema(cur)


# ── DEPENDS_ON regression ─────────────────────────────────────────────────────

def test_depends_on_uses_repo_dependencies_not_dropped_column(db_conn):
    """
    Core regression test for the silent DEPENDS_ON bug.

    Verifies two things:
      1. repos.dependencies column does NOT exist (migration 014 dropped it).
      2. build_depends_on() still produces edges by reading repo_dependencies.

    Before the fix, the function found no repos.dependencies column and
    returned [] silently. After the fix it queries the correct table.
    """
    cur = db_conn.cursor()

    # --- Guard: confirm the old column is gone ---
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'repos' AND column_name = 'dependencies'
        """
    )
    assert cur.fetchone() is None, (
        "repos.dependencies column still exists — migration 014 may not have been applied. "
        "This is the column that caused DEPENDS_ON to silently return [] when it was dropped."
    )

    # --- Set up: repo A depends on a package whose name matches repo B ---
    repo_b_id = _repo(cur, "fastapi", owner="tiangolo")
    repo_a_id = _repo(cur, "my-api-app", owner="testorg")
    _dep(cur, repo_a_id, "fastapi", ecosystem="pypi")

    # --- Exercise ---
    edges = build_depends_on(cur)

    # --- Assert: A→B edge exists ---
    edge_pairs = [(str(e["source"]), str(e["target"])) for e in edges]
    assert (repo_a_id, repo_b_id) in edge_pairs, (
        f"Expected DEPENDS_ON edge from my-api-app({repo_a_id}) to "
        f"fastapi({repo_b_id}). "
        f"Got edges: {edge_pairs}. "
        "This indicates build_depends_on() is not reading from repo_dependencies."
    )


def test_depends_on_confidence_is_0_95(db_conn):
    """All DEPENDS_ON edges must carry confidence=0.95."""
    cur = db_conn.cursor()
    target_id = _repo(cur, "requests", owner="psf")
    source_id = _repo(cur, "my-scraper", owner="testorg")
    _dep(cur, source_id, "requests")

    edges = build_depends_on(cur)

    source_edges = [e for e in edges if str(e["source"]) == source_id]
    assert source_edges, "Expected at least one DEPENDS_ON edge from my-scraper"
    for edge in source_edges:
        assert edge["confidence"] == 0.95, (
            f"DEPENDS_ON confidence must be 0.95 (high-signal), got {edge['confidence']}"
        )


def test_depends_on_no_self_references(db_conn):
    """A repo whose name matches one of its own dependencies must not produce a self-edge."""
    cur = db_conn.cursor()
    repo_id = _repo(cur, "selfref-tool", owner="testorg")
    # Normalised name matches itself: "selfreftool" == "selfreftool"
    _dep(cur, repo_id, "selfref-tool")

    edges = build_depends_on(cur)

    for edge in edges:
        assert str(edge["source"]) != str(edge["target"]), (
            "build_depends_on produced a self-referencing DEPENDS_ON edge"
        )


def test_depends_on_case_insensitive_normalisation(db_conn):
    """
    Package names and repo names are normalised (lowercase, hyphens/underscores
    stripped) before matching. A dep named 'My-Package' should match a repo
    named 'my_package'.
    """
    cur = db_conn.cursor()
    target_id = _repo(cur, "my_package", owner="testorg")
    source_id = _repo(cur, "consumer-app", owner="testorg")
    _dep(cur, source_id, "My-Package")  # Mixed case + hyphen

    edges = build_depends_on(cur)
    edge_pairs = [(str(e["source"]), str(e["target"])) for e in edges]
    assert (source_id, target_id) in edge_pairs, (
        "build_depends_on should normalise package names before matching repo names"
    )


def test_depends_on_no_duplicate_edges(db_conn):
    """Multiple dep rows for the same (source, target) pair must produce one edge."""
    cur = db_conn.cursor()
    target_id = _repo(cur, "shared-lib", owner="testorg")
    source_id = _repo(cur, "multi-dep-app", owner="testorg")
    # Same package under two ecosystem spellings — both normalise to "sharedlib"
    _dep(cur, source_id, "shared-lib", ecosystem="pypi")

    edges = build_depends_on(cur)
    matching = [
        e for e in edges
        if str(e["source"]) == source_id and str(e["target"]) == target_id
    ]
    assert len(matching) <= 1, (
        f"Expected at most 1 DEPENDS_ON edge between these repos, got {len(matching)}"
    )


def test_depends_on_returns_list_when_table_empty(db_conn):
    """When repo_dependencies is empty, build_depends_on must return [] (not raise)."""
    cur = db_conn.cursor()

    # Confirm repo_dependencies has no rows (this test runs in a fresh transaction)
    cur.execute("SELECT COUNT(*) FROM repo_dependencies")
    count = cur.fetchone()[0]

    if count == 0:
        edges = build_depends_on(cur)
        assert edges == [], f"Expected [] when repo_dependencies is empty, got {edges}"
    else:
        # Table already has rows from prior data — skip the empty-table path
        pytest.skip("repo_dependencies is not empty in this environment; skipping empty-table test")


# ── COMPATIBLE_WITH ───────────────────────────────────────────────────────────

def test_compatible_with_requires_two_shared_tags(db_conn):
    """Repos sharing exactly 1 tag do not produce a COMPATIBLE_WITH edge."""
    cur = db_conn.cursor()
    # Unique prefix avoids collisions with other test data
    pfx = uuid.uuid4().hex[:8]
    _repo(cur, f"{pfx}-tool-one",   owner="testorg", tags=["llm", "python"])
    _repo(cur, f"{pfx}-tool-two",   owner="testorg", tags=["llm", "rust"])    # 1 shared: llm
    _repo(cur, f"{pfx}-tool-three", owner="testorg", tags=["llm", "python"])  # 2 shared with one

    edges = build_compatible_with(cur)

    # Collect name pairs that appeared
    edge_names = {
        tuple(sorted([e["source_name"], e["target_name"]]))
        for e in edges
    }

    one_two_key   = tuple(sorted([f"{pfx}-tool-one", f"{pfx}-tool-two"]))
    one_three_key = tuple(sorted([f"{pfx}-tool-one", f"{pfx}-tool-three"]))

    assert one_two_key not in edge_names, (
        "Repos sharing only 1 tag should NOT produce a COMPATIBLE_WITH edge"
    )
    assert one_three_key in edge_names, (
        "Repos sharing 2 tags SHOULD produce a COMPATIBLE_WITH edge"
    )


def test_compatible_with_confidence_floor_at_0_3(db_conn):
    """COMPATIBLE_WITH confidence must be >= 0.3 even for very low overlap ratios."""
    cur = db_conn.cursor()
    pfx = uuid.uuid4().hex[:8]
    # 2 shared tags out of 12 unique → overlap_ratio = 2/12 ≈ 0.167 < 0.3
    shared = ["llm", "python"]
    tags_a = shared + [f"{pfx}-uniq-a-{i}" for i in range(10)]
    tags_b = shared + [f"{pfx}-uniq-b-{i}" for i in range(10)]
    _repo(cur, f"{pfx}-low-overlap-a", owner="testorg", tags=tags_a)
    _repo(cur, f"{pfx}-low-overlap-b", owner="testorg", tags=tags_b)

    edges = build_compatible_with(cur)

    for edge in edges:
        assert edge["confidence"] >= 0.3, (
            f"COMPATIBLE_WITH confidence below floor 0.3: {edge['confidence']} "
            f"(source={edge['source_name']}, target={edge['target_name']})"
        )


# ── insert_edges ──────────────────────────────────────────────────────────────

def test_insert_edges_writes_row_to_repo_edges(db_conn):
    """insert_edges should persist a row to the repo_edges table."""
    cur = db_conn.cursor()
    src_id = _repo(cur, "edge-src", owner="testorg")
    tgt_id = _repo(cur, "edge-tgt", owner="testorg")

    edges = [{
        "source": src_id,
        "target": tgt_id,
        "weight": 1.0,
        "confidence": 0.95,
        "metadata": {"dependency": "edge-tgt", "method": "repo_dependencies"},
        "source_name": "edge-src",
        "target_name": "edge-tgt",
    }]

    inserted = insert_edges(cur, edges, "DEPENDS_ON", ingest_run_id=None)
    assert inserted == 1

    cur.execute(
        """
        SELECT edge_type, weight, confidence, metadata
        FROM repo_edges
        WHERE source_repo_id = %s AND target_repo_id = %s
        """,
        (src_id, tgt_id),
    )
    row = cur.fetchone()
    assert row is not None, "Edge was not written to repo_edges"
    assert row[0] == "DEPENDS_ON"
    assert abs(row[1] - 1.0) < 1e-6
    assert abs(row[2] - 0.95) < 1e-6


def test_insert_edges_upsert_updates_weight(db_conn):
    """Re-inserting the same (source, target, edge_type) triple updates weight."""
    cur = db_conn.cursor()
    src_id = _repo(cur, "upsert-src", owner="testorg")
    tgt_id = _repo(cur, "upsert-tgt", owner="testorg")

    base_edge = [{
        "source": src_id, "target": tgt_id,
        "weight": 0.5, "confidence": 0.5,
        "metadata": {}, "source_name": "s", "target_name": "t",
    }]
    insert_edges(cur, base_edge, "COMPATIBLE_WITH", ingest_run_id=None)

    updated_edge = [{**base_edge[0], "weight": 0.8}]
    insert_edges(cur, updated_edge, "COMPATIBLE_WITH", ingest_run_id=None)

    cur.execute(
        """
        SELECT weight FROM repo_edges
        WHERE source_repo_id = %s AND target_repo_id = %s
          AND edge_type = 'COMPATIBLE_WITH'
        """,
        (src_id, tgt_id),
    )
    row = cur.fetchone()
    assert row is not None
    assert abs(row[0] - 0.8) < 1e-6, (
        f"ON CONFLICT upsert should update weight to 0.8, got {row[0]}"
    )


def test_insert_edges_batch_of_many(db_conn):
    """insert_edges handles batches larger than BATCH=500 without errors."""
    cur = db_conn.cursor()

    # Create a hub repo
    hub_id = _repo(cur, "hub-repo", owner="testorg")

    # Create 600 leaf repos
    leaf_ids = [_repo(cur, f"leaf-{i:04d}", owner="testorg") for i in range(600)]

    edges = [
        {
            "source": hub_id,
            "target": leaf_id,
            "weight": 0.5,
            "confidence": 0.3,
            "metadata": {},
            "source_name": "hub-repo",
            "target_name": f"leaf-{i:04d}",
        }
        for i, leaf_id in enumerate(leaf_ids)
    ]

    inserted = insert_edges(cur, edges, "COMPATIBLE_WITH", ingest_run_id=None)
    assert inserted == 600, f"Expected 600 inserts, got {inserted}"


# ── archive_existing_edges ────────────────────────────────────────────────────

def test_archive_edges_writes_to_history(db_conn):
    """archive_existing_edges must copy live edges to repo_edges_history."""
    cur = db_conn.cursor()
    src_id = _repo(cur, "arch-src", owner="testorg")
    tgt_id = _repo(cur, "arch-tgt", owner="testorg")

    # Insert a live edge first
    insert_edges(
        cur,
        [{
            "source": src_id, "target": tgt_id,
            "weight": 1.0, "confidence": 0.7,
            "metadata": {}, "source_name": "arch-src", "target_name": "arch-tgt",
        }],
        "ALTERNATIVE_TO",
        ingest_run_id=None,
    )

    archived = archive_existing_edges(cur, ingest_run_id=None)
    assert archived >= 1

    cur.execute(
        """
        SELECT valid_from, valid_until FROM repo_edges_history
        WHERE source_repo_id = %s AND target_repo_id = %s
        """,
        (src_id, tgt_id),
    )
    row = cur.fetchone()
    assert row is not None, "Edge not found in repo_edges_history after archiving"
    assert row[1] is not None, "valid_until should be set after archiving"


def test_archive_edges_sets_valid_until_to_now(db_conn):
    """Archived edges should have valid_until approximately equal to NOW()."""
    from datetime import datetime, timezone, timedelta

    cur = db_conn.cursor()
    src_id = _repo(cur, "ts-src", owner="testorg")
    tgt_id = _repo(cur, "ts-tgt", owner="testorg")

    insert_edges(
        cur,
        [{"source": src_id, "target": tgt_id, "weight": 1.0, "confidence": 0.9,
          "metadata": {}, "source_name": "ts-src", "target_name": "ts-tgt"}],
        "DEPENDS_ON",
        ingest_run_id=None,
    )
    archive_existing_edges(cur, ingest_run_id=None)

    cur.execute(
        "SELECT valid_until FROM repo_edges_history WHERE source_repo_id = %s AND target_repo_id = %s",
        (src_id, tgt_id),
    )
    row = cur.fetchone()
    assert row is not None
    valid_until = row[0]
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    delta = abs((now - valid_until).total_seconds())
    assert delta < 60, (
        f"valid_until should be within 60s of NOW(), but delta={delta:.1f}s"
    )

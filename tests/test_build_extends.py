"""
Tests for build_extends() — EXTENDS edges via shared-upstream-owner heuristic.

Semantic (KAN-228, originally implemented on the KAN-164 branch and pulled
forward into main): repos sharing the same upstream OWNER get linked. The
prior fork-resolution implementation (KAN-155) is replaced because
perditioinc/* repos never have their `forked_from` strings land back in the
same `repos` table — fork-of-fork relationships do not exist in this corpus,
so that algorithm produced 0 edges in production.

Contract verified here:
  - Repos with no parsable forked_from are ignored.
  - Repos are grouped by lowercased owner of forked_from.
  - Groups smaller than MIN_GROUP_SIZE (2) or larger than MAX_GROUP_SIZE (30)
    are skipped.
  - Each pair within an eligible group produces an edge.
  - Per-repo cap is MAX_PER_REPO (20) strongest edges.
  - confidence=0.6, evidence.method="shared_upstream_owner".
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from build_knowledge_graph import build_extends
from conftest import make_repo


def _mark_fork(cur, repo_id: str, forked_from: str):
    """make_repo() doesn't expose forked_from — patch it after insert."""
    cur.execute(
        "UPDATE repos SET is_fork = true, forked_from = %s WHERE id = %s",
        (forked_from, repo_id),
    )


class TestBuildExtends:
    """Validates the shared-upstream-owner heuristic builder."""

    def test_two_repos_same_upstream_owner(self, db_conn):
        """2 forks of the same upstream org → 1 edge between them."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="extends-pair-a", owner="perditioinc")
        b = make_repo(cur, name="extends-pair-b", owner="perditioinc")
        _mark_fork(cur, a, forked_from="upstream-org/lib-a")
        _mark_fork(cur, b, forked_from="upstream-org/lib-b")
        db_conn.commit()

        edges = build_extends(cur)

        assert len(edges) == 1
        edge = edges[0]
        assert edge["confidence"] == 0.6
        assert edge["evidence"]["method"] == "shared_upstream_owner"
        assert edge["evidence"]["upstream_owner"] == "upstream-org"
        # The pair must connect a and b in some direction.
        ids = {str(edge["source"]), str(edge["target"])}
        assert ids == {a, b}

    def test_solo_upstream_owner_skipped(self, db_conn):
        """Group of 1 (only one repo with that upstream owner) → 0 edges."""
        cur = db_conn.cursor()
        solo = make_repo(cur, name="extends-solo", owner="perditioinc")
        _mark_fork(cur, solo, forked_from="lonely-org/only-lib")
        db_conn.commit()

        edges = build_extends(cur)
        assert edges == []

    def test_no_fork_metadata_skipped(self, db_conn):
        """Repos with NULL forked_from (non-forks) → not included in any group."""
        cur = db_conn.cursor()
        non_fork = make_repo(cur, name="extends-non-fork-a", owner="perditioinc")
        # Leave forked_from as NULL — make_repo default.
        # Add a real fork that would otherwise create no group of its own.
        solo = make_repo(cur, name="extends-non-fork-pair", owner="perditioinc")
        _mark_fork(cur, solo, forked_from="some-org/some-lib")
        db_conn.commit()

        edges = build_extends(cur)
        # non_fork is filtered by the WHERE clause; solo's group is size 1.
        assert edges == []

    def test_owner_case_insensitive(self, db_conn):
        """Owners are lowercased before grouping — Facebook ≡ facebook."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="extends-case-a", owner="perditioinc")
        b = make_repo(cur, name="extends-case-b", owner="perditioinc")
        _mark_fork(cur, a, forked_from="Facebook/react")
        _mark_fork(cur, b, forked_from="facebook/react-native")
        db_conn.commit()

        edges = build_extends(cur)

        assert len(edges) == 1
        assert edges[0]["evidence"]["upstream_owner"] == "facebook"

    def test_three_repos_same_owner_three_edges(self, db_conn):
        """Group of 3 → C(3,2) = 3 pair edges."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="extends-trio-a", owner="perditioinc")
        b = make_repo(cur, name="extends-trio-b", owner="perditioinc")
        c = make_repo(cur, name="extends-trio-c", owner="perditioinc")
        _mark_fork(cur, a, forked_from="trio-org/lib-a")
        _mark_fork(cur, b, forked_from="trio-org/lib-b")
        _mark_fork(cur, c, forked_from="trio-org/lib-c")
        db_conn.commit()

        edges = build_extends(cur)
        assert len(edges) == 3
        assert all(e["evidence"]["upstream_owner"] == "trio-org" for e in edges)
        assert all(e["confidence"] == 0.6 for e in edges)

    def test_giant_group_skipped(self, db_conn):
        """Group exceeding MAX_GROUP_SIZE (30) is skipped (too noisy)."""
        cur = db_conn.cursor()
        # 31 repos all forked from the same org → group size 31 > 30 → skipped.
        for i in range(31):
            r = make_repo(cur, name=f"extends-giant-{i:02d}", owner="perditioinc")
            _mark_fork(cur, r, forked_from=f"giant-org/lib-{i:02d}")
        db_conn.commit()

        edges = build_extends(cur)
        # Edges may exist from other groups in the corpus (none here), so we
        # check no edges have giant-org as upstream_owner.
        giant_edges = [e for e in edges if e["evidence"]["upstream_owner"] == "giant-org"]
        assert giant_edges == []

    def test_evidence_shape(self, db_conn):
        """Edge dict has the expected fields and types."""
        cur = db_conn.cursor()
        a = make_repo(cur, name="extends-shape-a", owner="perditioinc")
        b = make_repo(cur, name="extends-shape-b", owner="perditioinc")
        _mark_fork(cur, a, forked_from="shape-org/x")
        _mark_fork(cur, b, forked_from="shape-org/y")
        db_conn.commit()

        edges = build_extends(cur)
        assert len(edges) == 1
        edge = edges[0]
        # Required fields
        for k in ("source", "target", "weight", "confidence", "evidence",
                  "source_name", "target_name"):
            assert k in edge, f"missing field {k}"
        assert edge["evidence"]["method"] == "shared_upstream_owner"
        assert edge["evidence"]["upstream_owner"] == "shape-org"
        # source_name and target_name fall back to forked_from when present
        names = {edge["source_name"], edge["target_name"]}
        assert names == {"shape-org/x", "shape-org/y"}

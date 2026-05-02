"""
Tests for build_extends() — EXTENDS edges from fork relationships.

Mirrors the validity predicate at
reporium-api/app/routers/platform.py:386-398, which is what
/metrics/graph-quality uses to compute precision_proxy. Before KAN-155,
no builder existed, so the metric stayed at 0.0 against 1564 stale
legacy edges held over from the migration-033 table rename.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from build_knowledge_graph import build_extends
from conftest import make_repo


def _mark_fork(cur, repo_id: str, forked_from: str):
    """make_repo() doesn't expose is_fork — patch it after insert."""
    cur.execute(
        "UPDATE repos SET is_fork = true, forked_from = %s WHERE id = %s",
        (forked_from, repo_id),
    )


class TestBuildExtends:
    """Validates the fork_resolution predicate as a builder."""

    def test_resolvable_fork(self, db_conn):
        """is_fork=true + forked_from resolves to a tracked repo → 1 edge."""
        cur = db_conn.cursor()
        upstream = make_repo(cur, name="parent-lib", owner="upstream-org")
        fork = make_repo(cur, name="parent-lib", owner="fork-org")
        _mark_fork(cur, fork, forked_from="upstream-org/parent-lib")
        db_conn.commit()

        edges = build_extends(cur)

        assert len(edges) == 1
        edge = edges[0]
        assert str(edge["source"]) == fork
        assert str(edge["target"]) == upstream
        assert edge["confidence"] == 0.95
        assert edge["weight"] == 1.0
        assert edge["evidence"]["method"] == "fork_resolution"
        assert edge["evidence"]["forked_from"] == "upstream-org/parent-lib"

    def test_unresolvable_fork(self, db_conn):
        """is_fork=true but forked_from points outside the tracked set → 0 edges."""
        cur = db_conn.cursor()
        fork = make_repo(cur, name="ghost-fork", owner="fork-org")
        _mark_fork(cur, fork, forked_from="some-other-org/never-ingested")
        db_conn.commit()

        edges = build_extends(cur)
        assert edges == []

    def test_non_fork(self, db_conn):
        """is_fork=false → 0 edges, even if forked_from happens to be set."""
        cur = db_conn.cursor()
        # Two real repos
        parent = make_repo(cur, name="real-parent", owner="org")
        # A repo with is_fork=false that should never produce an edge
        bystander = make_repo(cur, name="standalone", owner="org")
        # Defensive: even if a stray forked_from string was set, is_fork governs.
        cur.execute(
            "UPDATE repos SET forked_from = 'org/real-parent' WHERE id = %s",
            (bystander,),
        )
        db_conn.commit()

        edges = build_extends(cur)
        assert edges == []

    def test_self_reference(self, db_conn):
        """is_fork=true and forked_from resolves to itself → 0 edges (filtered)."""
        cur = db_conn.cursor()
        repo_id = make_repo(cur, name="self-fork", owner="org")
        # Point forked_from at itself: "org/self-fork" → resolves to repo_id
        _mark_fork(cur, repo_id, forked_from="org/self-fork")
        db_conn.commit()

        edges = build_extends(cur)
        assert edges == []

    def test_multiple_forks_partial_resolution(self, db_conn):
        """5 forks, 3 with resolvable upstreams → 3 edges."""
        cur = db_conn.cursor()

        # 3 upstream repos that exist in our DB
        u1 = make_repo(cur, name="lib-one", owner="upstream")
        u2 = make_repo(cur, name="lib-two", owner="upstream")
        u3 = make_repo(cur, name="lib-three", owner="upstream")

        # 5 forks, 3 resolvable, 2 unresolvable
        f1 = make_repo(cur, name="lib-one", owner="forker-a")
        _mark_fork(cur, f1, forked_from="upstream/lib-one")

        f2 = make_repo(cur, name="lib-two", owner="forker-b")
        _mark_fork(cur, f2, forked_from="upstream/lib-two")

        f3 = make_repo(cur, name="lib-three", owner="forker-c")
        _mark_fork(cur, f3, forked_from="upstream/lib-three")

        f4 = make_repo(cur, name="lib-missing", owner="forker-d")
        _mark_fork(cur, f4, forked_from="not-tracked/lib-missing")

        f5 = make_repo(cur, name="lib-other", owner="forker-e")
        _mark_fork(cur, f5, forked_from="also-not-tracked/lib-other")

        db_conn.commit()

        edges = build_extends(cur)
        assert len(edges) == 3

        sources = {str(e["source"]) for e in edges}
        targets = {str(e["target"]) for e in edges}
        assert sources == {f1, f2, f3}
        assert targets == {u1, u2, u3}
        # All edges must carry the canonical confidence
        assert all(e["confidence"] == 0.95 for e in edges)
        assert all(e["evidence"]["method"] == "fork_resolution" for e in edges)

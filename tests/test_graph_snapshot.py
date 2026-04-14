import json

import pytest

from ingestion.graph_snapshot import (
    GRAPH_SNAPSHOT_VERSION,
    GraphSnapshotConfig,
    build_graph_snapshot,
    publish_graph_snapshot,
)

pytestmark = pytest.mark.no_db


class FakeCursor:
    def __init__(self, responses):
        self._responses = list(responses)
        self._current = None

    def execute(self, sql, params=None):
        if not self._responses:
            raise AssertionError(f"Unexpected query: {sql}")
        self._current = self._responses.pop(0)

    def fetchall(self):
        if isinstance(self._current, dict):
            return self._current.get("all", [])
        return self._current

    def fetchone(self):
        if isinstance(self._current, dict):
            return self._current.get("one")
        return self._current[0] if self._current else None


def test_build_graph_snapshot_collects_nodes_and_edges():
    cursor = FakeCursor(
        [
            [
                (
                    "repo-1",
                    "repo-a",
                    "perditioinc",
                    "Repo A",
                    "ai-agents",
                    100,
                    {"overall": 0.9},
                    _utc_datetime("2026-04-13T01:00:00+00:00"),
                ),
                (
                    "repo-2",
                    "repo-b",
                    "perditioinc",
                    "Repo B",
                    "rag-retrieval",
                    50,
                    {"overall": 0.8},
                    _utc_datetime("2026-04-13T01:00:00+00:00"),
                ),
            ],
            {"one": (2, 2)},
            [
                ("repo-1", "repo-2", 1, 0.82),
                ("repo-2", "repo-1", 1, 0.82),
            ],
            [
                ("repo-2", "repo-1", "DEPENDS_ON", 1.0),
            ],
        ]
    )

    snapshot = build_graph_snapshot(cursor, max_similarity_neighbours=5, min_similarity=0.4)

    assert snapshot["snapshot_version"] == GRAPH_SNAPSHOT_VERSION
    assert snapshot["stats"]["total_public_repos"] == 2
    assert snapshot["stats"]["repos_with_embeddings"] == 2
    assert len(snapshot["nodes"]) == 2
    assert len(snapshot["similarity_edges"]) == 2
    assert snapshot["typed_edges"][0]["edge_type"] == "DEPENDS_ON"


def test_publish_graph_snapshot_writes_local_file(tmp_path):
    destination = tmp_path / "graph-snapshot.json"
    snapshot = {
        "snapshot_version": GRAPH_SNAPSHOT_VERSION,
        "generated_at": "2026-04-13T01:00:00+00:00",
        "stats": {"total_public_repos": 1, "repos_with_embeddings": 1},
        "nodes": [],
        "similarity_edges": [],
        "typed_edges": [],
    }

    result = publish_graph_snapshot(
        snapshot,
        GraphSnapshotConfig(local_path=str(destination)),
    )

    assert str(destination) in result["destinations"]
    assert json.loads(destination.read_text(encoding="utf-8"))["snapshot_version"] == GRAPH_SNAPSHOT_VERSION


def _utc_datetime(value: str):
    from datetime import datetime

    return datetime.fromisoformat(value)

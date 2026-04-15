import json

import pytest

from ingestion.graph_snapshot import (
    GRAPH_SNAPSHOT_VERSION,
    GraphSnapshotConfig,
    _TYPED_EDGE_PER_TYPE_MAX,
    _balance_typed_edges,
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


# ---------------------------------------------------------------------------
# KAN-121: Typed edge balancing
# ---------------------------------------------------------------------------

def _make_typed_edge(source: str, target: str, edge_type: str, weight: float = 1.0) -> dict:
    return {
        "source_repo_id": source,
        "target_repo_id": target,
        "edge_type": edge_type,
        "weight": weight,
    }


def test_balance_typed_edges_preserves_all_depends_on_when_under_cap():
    """All DEPENDS_ON edges survive when count < _TYPED_EDGE_PER_TYPE_MAX."""
    depends_on_edges = [
        _make_typed_edge(f"src-{i}", f"tgt-{i}", "DEPENDS_ON")
        for i in range(60)
    ]
    alt_edges = [
        _make_typed_edge(f"a-{i}", f"b-{i}", "ALTERNATIVE_TO")
        for i in range(_TYPED_EDGE_PER_TYPE_MAX + 200)
    ]
    result = _balance_typed_edges(depends_on_edges + alt_edges)

    result_by_type: dict[str, list] = {}
    for edge in result:
        result_by_type.setdefault(edge["edge_type"], []).append(edge)

    # All 60 DEPENDS_ON edges must survive
    assert len(result_by_type.get("DEPENDS_ON", [])) == 60
    # ALTERNATIVE_TO capped at per-type max
    assert len(result_by_type.get("ALTERNATIVE_TO", [])) == _TYPED_EDGE_PER_TYPE_MAX


def test_balance_typed_edges_priority_order():
    """DEPENDS_ON appears before ALTERNATIVE_TO in the output list."""
    edges = (
        [_make_typed_edge(f"a-{i}", f"b-{i}", "ALTERNATIVE_TO") for i in range(5)]
        + [_make_typed_edge(f"d-{i}", f"e-{i}", "DEPENDS_ON") for i in range(5)]
    )
    result = _balance_typed_edges(edges)
    types_in_order = [e["edge_type"] for e in result]
    last_depends_on = max(
        (i for i, t in enumerate(types_in_order) if t == "DEPENDS_ON"), default=-1
    )
    first_alternative = min(
        (i for i, t in enumerate(types_in_order) if t == "ALTERNATIVE_TO"), default=999
    )
    assert last_depends_on < first_alternative, (
        "DEPENDS_ON must appear before ALTERNATIVE_TO in the balanced output"
    )


def test_balance_typed_edges_caps_each_type_independently():
    """Each type is capped independently; combined count can exceed _TYPED_EDGE_PER_TYPE_MAX."""
    edges = []
    for et in ("DEPENDS_ON", "EXTENDS", "COMPATIBLE_WITH", "ALTERNATIVE_TO"):
        edges += [
            _make_typed_edge(f"{et}-src-{i}", f"{et}-tgt-{i}", et)
            for i in range(_TYPED_EDGE_PER_TYPE_MAX + 50)
        ]
    result = _balance_typed_edges(edges)

    result_by_type: dict[str, int] = {}
    for edge in result:
        result_by_type[edge["edge_type"]] = result_by_type.get(edge["edge_type"], 0) + 1

    for et in ("DEPENDS_ON", "EXTENDS", "COMPATIBLE_WITH", "ALTERNATIVE_TO"):
        assert result_by_type.get(et, 0) == _TYPED_EDGE_PER_TYPE_MAX, (
            f"{et} should be capped at {_TYPED_EDGE_PER_TYPE_MAX}"
        )


def test_snapshot_regression_guard_depends_on_not_dropped():
    """Regression guard: snapshot with 60 DEPENDS_ON edges retains all of them after build."""
    depends_on_rows = [
        (f"src-{i}", f"tgt-{i}", "DEPENDS_ON", 1.0)
        for i in range(60)
    ]
    alt_rows = [
        (f"a-{i}", f"b-{i}", "ALTERNATIVE_TO", 1.0)
        for i in range(800)
    ]
    all_typed_rows = depends_on_rows + alt_rows

    cursor = FakeCursor(
        [
            # repos query
            [
                (
                    "repo-1", "repo-a", "perditioinc", "Repo A",
                    "ai-agents", 100, {"overall": 0.9},
                    _utc_datetime("2026-04-13T01:00:00+00:00"),
                ),
            ],
            # counts query
            {"one": (1, 1)},
            # similarity edges
            [],
            # typed edges (60 DEPENDS_ON + 800 ALTERNATIVE_TO)
            all_typed_rows,
        ]
    )
    snapshot = build_graph_snapshot(cursor)

    by_type: dict[str, int] = {}
    for edge in snapshot["typed_edges"]:
        by_type[edge["edge_type"]] = by_type.get(edge["edge_type"], 0) + 1

    assert by_type.get("DEPENDS_ON", 0) == 60, (
        "Regression guard: all 60 DEPENDS_ON edges must survive balancing"
    )
    # 800 ALTERNATIVE_TO edges < 1000 cap, so all survive (no trimming needed here).
    # Trimming behaviour is covered by test_balance_typed_edges_caps_each_type_independently.
    assert by_type.get("ALTERNATIVE_TO", 0) == 800, (
        "Regression guard: ALTERNATIVE_TO edges must be preserved when under the cap"
    )

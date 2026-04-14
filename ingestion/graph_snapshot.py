from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

GRAPH_SNAPSHOT_VERSION = 1
DEFAULT_GRAPH_SNAPSHOT_OBJECT = "reporium/graph/knowledge-graph.json"


@dataclass(frozen=True)
class GraphSnapshotConfig:
    bucket_name: str = ""
    object_name: str = DEFAULT_GRAPH_SNAPSHOT_OBJECT
    local_path: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.bucket_name or self.local_path)


def resolve_graph_snapshot_config() -> GraphSnapshotConfig:
    return GraphSnapshotConfig(
        bucket_name=os.getenv("GRAPH_SNAPSHOT_BUCKET", "").strip(),
        object_name=os.getenv("GRAPH_SNAPSHOT_OBJECT", DEFAULT_GRAPH_SNAPSHOT_OBJECT).strip(),
        local_path=os.getenv("GRAPH_SNAPSHOT_LOCAL_PATH", "").strip(),
    )


def _extract_quality(quality_signals: dict[str, Any] | None) -> float | None:
    if not quality_signals:
        return None
    overall = quality_signals.get("overall")
    if overall is None:
        return None
    return round(float(overall), 4)


def _log_scale_stars(stars: int | None) -> float:
    if not stars or stars <= 0:
        return 0.0
    return round(math.log10(stars + 1), 4)


def validate_graph_snapshot(snapshot: dict[str, Any]) -> None:
    """
    Validate that a graph snapshot has expected structure and data integrity.
    Raises ValueError if validation fails to prevent publishing broken snapshots.
    """
    typed_edges = snapshot.get("typed_edges", [])
    if not typed_edges:
        raise ValueError("Snapshot has zero typed edges - check repo_edges table in database")

    # Verify all 4 edge types are present
    edge_types = set(e.get("edge_type") for e in typed_edges)
    required_types = {"ALTERNATIVE_TO", "COMPATIBLE_WITH", "DEPENDS_ON", "EXTENDS"}
    missing_types = required_types - edge_types
    if missing_types:
        raise ValueError(f"Snapshot missing required edge types: {missing_types}")

    # Count edges by type for diagnostics
    type_counts = {}
    for e in typed_edges:
        etype = e.get("edge_type")
        type_counts[etype] = type_counts.get(etype, 0) + 1

    # Sanity check: DEPENDS_ON should have reasonable count
    depends_on_count = type_counts.get("DEPENDS_ON", 0)
    if depends_on_count < 50:
        raise ValueError(
            f"Suspiciously low DEPENDS_ON edge count: {depends_on_count} "
            f"(expected >50; check dependency extraction is running)"
        )

    # Sanity check: no single type should dominate >80% (indicates edge balancing broken)
    total_typed = len(typed_edges)
    alt_to_count = type_counts.get("ALTERNATIVE_TO", 0)
    alt_to_percent = (alt_to_count / total_typed * 100) if total_typed > 0 else 0
    if alt_to_percent > 80:
        raise ValueError(
            f"ALTERNATIVE_TO edges dominate {alt_to_percent:.1f}% of typed edges "
            f"({alt_to_count}/{total_typed}); edge type balancing may be broken"
        )

    logger.info(
        "Snapshot validation passed: %s",
        {k: v for k, v in sorted(type_counts.items())}
    )


def build_graph_snapshot(
    cur,
    *,
    max_similarity_neighbours: int = 12,
    min_similarity: float = 0.4,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            id::text,
            name,
            owner,
            description,
            primary_category,
            stargazers_count,
            quality_signals,
            updated_at
        FROM repos
        WHERE is_private = false
        ORDER BY owner, name
        """
    )
    nodes = []
    for row in cur.fetchall():
        repo_id, name, owner, description, category, stars, quality_signals, updated_at = row
        nodes.append(
            {
                "repo_id": repo_id,
                "name": name,
                "owner": owner,
                "description": description,
                "primary_category": category,
                "stars": int(stars or 0),
                "stars_log": _log_scale_stars(stars),
                "quality": _extract_quality(quality_signals),
                "updated_at": updated_at.astimezone(timezone.utc).isoformat() if updated_at else None,
            }
        )

    cur.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM repos WHERE is_private = false) AS total_public,
            (SELECT COUNT(DISTINCT re.repo_id)
             FROM repo_embeddings re
             JOIN repos r ON r.id = re.repo_id
             WHERE r.is_private = false
               AND re.embedding_vec IS NOT NULL) AS with_embeddings
        """
    )
    counts_row = cur.fetchone() or (0, 0)
    total_public_repos, repos_with_embeddings = counts_row

    cur.execute(
        """
        WITH public_embeddings AS (
            SELECT re.repo_id, re.embedding_vec
            FROM repo_embeddings re
            JOIN repos r ON r.id = re.repo_id
            WHERE r.is_private = false
              AND re.embedding_vec IS NOT NULL
        ),
        ranked AS (
            SELECT
                e1.repo_id AS source_id,
                e2.repo_id AS target_id,
                ROW_NUMBER() OVER (
                    PARTITION BY e1.repo_id
                    ORDER BY e1.embedding_vec <=> e2.embedding_vec
                ) AS source_rank,
                1 - (e1.embedding_vec <=> e2.embedding_vec) AS similarity
            FROM public_embeddings e1
            CROSS JOIN LATERAL (
                SELECT e2_inner.repo_id, e2_inner.embedding_vec
                FROM public_embeddings e2_inner
                WHERE e2_inner.repo_id != e1.repo_id
                ORDER BY e1.embedding_vec <=> e2_inner.embedding_vec
                LIMIT %s
            ) e2
        ),
        filtered AS (
            SELECT source_id, target_id, source_rank, similarity
            FROM ranked
            WHERE similarity >= %s
        ),
        orphan_edges AS (
            SELECT source_id, target_id, source_rank, similarity
            FROM ranked r
            WHERE r.source_rank = 1
              AND NOT EXISTS (
                  SELECT 1 FROM filtered f WHERE f.source_id = r.source_id
              )
        )
        SELECT source_id::text, target_id::text, source_rank, similarity
        FROM filtered
        UNION ALL
        SELECT source_id::text, target_id::text, source_rank, similarity
        FROM orphan_edges
        ORDER BY similarity DESC, source_rank ASC
        """,
        (max_similarity_neighbours, min_similarity),
    )
    similarity_edges = [
        {
            "source_repo_id": source_id,
            "target_repo_id": target_id,
            "rank": int(source_rank),
            "weight": round(float(similarity), 4),
        }
        for source_id, target_id, source_rank, similarity in cur.fetchall()
    ]

    cur.execute(
        """
        SELECT
            re.source_repo_id::text,
            re.target_repo_id::text,
            re.edge_type,
            COALESCE(re.weight, 0.5)
        FROM repo_edges re
        JOIN repos r1 ON r1.id = re.source_repo_id AND r1.is_private = false
        JOIN repos r2 ON r2.id = re.target_repo_id AND r2.is_private = false
        WHERE re.edge_type IN ('ALTERNATIVE_TO', 'COMPATIBLE_WITH', 'DEPENDS_ON', 'EXTENDS')
        ORDER BY re.weight DESC NULLS LAST, re.edge_type, re.source_repo_id, re.target_repo_id
        """
    )
    typed_edges = [
        {
            "source_repo_id": source_repo_id,
            "target_repo_id": target_repo_id,
            "edge_type": edge_type,
            "weight": round(float(weight), 4),
        }
        for source_repo_id, target_repo_id, edge_type, weight in cur.fetchall()
    ]

    snapshot = {
        "snapshot_version": GRAPH_SNAPSHOT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "max_similarity_neighbours": max_similarity_neighbours,
            "min_similarity": min_similarity,
        },
        "stats": {
            "total_public_repos": int(total_public_repos or 0),
            "repos_with_embeddings": int(repos_with_embeddings or 0),
            "similarity_edges": len(similarity_edges),
            "typed_edges": len(typed_edges),
        },
        "nodes": nodes,
        "similarity_edges": similarity_edges,
        "typed_edges": typed_edges,
    }

    # Validate before returning to prevent silent failures
    validate_graph_snapshot(snapshot)

    return snapshot


def publish_graph_snapshot(
    snapshot: dict[str, Any],
    config: GraphSnapshotConfig | None = None,
) -> dict[str, Any]:
    config = config or resolve_graph_snapshot_config()
    payload = json.dumps(snapshot, separators=(",", ":"), sort_keys=True)
    destinations: list[str] = []

    if config.local_path:
        path = Path(config.local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        destinations.append(str(path))

    if config.bucket_name:
        from google.cloud import storage

        client = storage.Client()
        blob = client.bucket(config.bucket_name).blob(config.object_name)
        blob.cache_control = "no-cache, max-age=0"
        blob.upload_from_string(payload, content_type="application/json")
        destinations.append(f"gs://{config.bucket_name}/{config.object_name}")

    if not destinations:
        logger.info("Graph snapshot publication skipped; no destination configured")

    return {
        "destinations": destinations,
        "size_bytes": len(payload.encode("utf-8")),
        "stats": snapshot.get("stats", {}),
    }

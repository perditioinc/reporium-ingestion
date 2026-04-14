"""
Publish the current knowledge graph snapshot without rebuilding repo_edges.

This script is read-only against the production database and exists so we can
refresh the serving artifact independently from graph edge generation.

Usage:
    DATABASE_URL=... GRAPH_SNAPSHOT_BUCKET=... python scripts/publish_graph_snapshot.py
"""

import logging
import os

import psycopg2

from ingestion.graph_snapshot import build_graph_snapshot, publish_graph_snapshot, resolve_graph_snapshot_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Set DATABASE_URL")

    config = resolve_graph_snapshot_config()
    if not config.enabled:
        raise RuntimeError(
            "Set GRAPH_SNAPSHOT_BUCKET or GRAPH_SNAPSHOT_LOCAL_PATH before publishing a graph snapshot"
        )

    conn = psycopg2.connect(database_url)
    try:
        cur = conn.cursor()
        snapshot = build_graph_snapshot(cur)
        result = publish_graph_snapshot(snapshot, config)
    finally:
        conn.close()

    logger.info(
        "Published knowledge graph snapshot (%s bytes) to %s",
        result["size_bytes"],
        ", ".join(result["destinations"]),
    )
    logger.info("Snapshot stats: %s", result["stats"])


if __name__ == "__main__":
    main()

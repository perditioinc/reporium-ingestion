"""
Atomic graph rebuild via staging table swap.

Pattern:
  1. Build all edges into repo_edges_staging (temp table, no FK constraints)
  2. Validate edge counts vs prior run (abort >50% drop or zero on high-count type)
  3. Single transaction:
       a. Archive repo_edges → repo_edges_history (valid_until = NOW())
       b. DELETE managed edge types from repo_edges
       c. INSERT INTO repo_edges SELECT FROM staging
       d. COMMIT

Crash safety:
  - Crash BEFORE step 3: repo_edges untouched. Staging table is orphan.
  - Crash DURING step 3: PostgreSQL rolls back the transaction.
  - Crash AFTER COMMIT: swap succeeded.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import psycopg2

from ingestion.graph.ingest_run_manager import (
    IngestRunManager,
    EdgeCountValidationError,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

# Edge types managed by this builder (others like MAINTAINED_BY are preserved)
MANAGED_TYPES = ("COMPATIBLE_WITH", "ALTERNATIVE_TO", "DEPENDS_ON")


# ── staging table ops ────────────────────────────────────────────────────────

def _create_staging_table(cur):
    """Create temporary staging table matching repo_edges columns."""
    cur.execute("DROP TABLE IF EXISTS repo_edges_staging")
    cur.execute("""
        CREATE TEMP TABLE repo_edges_staging (
            source_repo_id UUID NOT NULL,
            target_repo_id UUID NOT NULL,
            edge_type VARCHAR(32) NOT NULL,
            weight FLOAT NOT NULL DEFAULT 1.0,
            confidence FLOAT NOT NULL DEFAULT 0.5,
            metadata JSONB DEFAULT '{}'
        )
    """)


def _insert_into_staging(cur, edges: list[dict], edge_type: str) -> int:
    """Batch-insert edges into the staging table."""
    inserted = 0
    for i in range(0, len(edges), BATCH_SIZE):
        batch = edges[i:i + BATCH_SIZE]
        values = []
        params: list[Any] = []
        for e in batch:
            values.append("(%s, %s, %s, %s, %s, %s)")
            params.extend([
                str(e["source"]), str(e["target"]), edge_type,
                e["weight"], e.get("confidence", 0.5),
                json.dumps(e.get("evidence", e.get("metadata", {}))),
            ])
        sql = (
            "INSERT INTO repo_edges_staging "
            "(source_repo_id, target_repo_id, edge_type, weight, confidence, metadata) "
            "VALUES " + ", ".join(values)
        )
        cur.execute(sql, params)
        inserted += len(batch)
    return inserted


# ── atomic swap ──────────────────────────────────────────────────────────────

def _archive_and_swap(cur, run_id: int | None):
    """
    Single-transaction atomic swap.

    Archives current managed edges to history → deletes them → inserts from staging.
    Must be called inside a transaction (conn.autocommit=False).
    """
    # Archive current live edges to history.
    # We do NOT select ingest_run_id from repo_edges because that column may not
    # exist on databases that are behind on the migration chain.  The archival
    # run_id (passed in as run_id) is recorded instead — it identifies the run
    # that is *replacing* these edges, which is the most useful provenance for
    # post-mortem queries.
    cur.execute("""
        INSERT INTO repo_edges_history
            (source_repo_id, target_repo_id, edge_type, weight, confidence,
             metadata, ingest_run_id, valid_from, valid_until)
        SELECT
            source_repo_id, target_repo_id, edge_type, weight, confidence,
            metadata, %s, created_at, NOW()
        FROM repo_edges
        WHERE edge_type = ANY(%s)
    """, (run_id, list(MANAGED_TYPES),))
    archived = cur.rowcount
    logger.info("Archived %d live edges to history", archived)

    # Delete managed edge types (preserve MAINTAINED_BY etc.)
    cur.execute(
        "DELETE FROM repo_edges WHERE edge_type = ANY(%s)",
        (list(MANAGED_TYPES),),
    )
    deleted = cur.rowcount
    logger.info("Deleted %d old managed edges", deleted)

    # Insert from staging to live.
    # Use a conditional INSERT that includes ingest_run_id only when the column
    # exists (migration 033 adds it; older schemas may not have it yet).
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'repo_edges' AND column_name = 'ingest_run_id'
    """)
    has_run_id_col = cur.fetchone() is not None

    if has_run_id_col:
        cur.execute("""
            INSERT INTO repo_edges
                (source_repo_id, target_repo_id, edge_type, weight, confidence,
                 metadata, ingest_run_id)
            SELECT
                source_repo_id, target_repo_id, edge_type, weight, confidence,
                metadata, %s
            FROM repo_edges_staging
        """, (run_id,))
    else:
        cur.execute("""
            INSERT INTO repo_edges
                (source_repo_id, target_repo_id, edge_type, weight, confidence,
                 metadata)
            SELECT
                source_repo_id, target_repo_id, edge_type, weight, confidence,
                metadata
            FROM repo_edges_staging
        """)
    inserted = cur.rowcount
    logger.info("Inserted %d new edges from staging", inserted)

    return inserted


# ── orchestrator ─────────────────────────────────────────────────────────────

def build_and_swap(db_url: str, run_id: int, run_manager: IngestRunManager) -> dict:
    """
    Full atomic rebuild pipeline.

    1. Connect, verify schema
    2. Build all three edge types (read-only) — imported from build_knowledge_graph
    3. Stage into temp table
    4. Validate counts (abort if >50% drop)
    5. Atomic archive+swap in a single transaction
    6. Return edge counts

    Returns dict of {edge_type: count}.
    Raises EdgeCountValidationError if counts fail validation.
    """
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

    # Import edge builders from the script module (single source of truth)
    from scripts.build_knowledge_graph import (
        build_compatible_with,
        build_alternative_to,
        build_depends_on,
        verify_table,
    )

    t0 = time.monotonic()

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Verify migration
        verify_table(cur)
        logger.info("repo_edges table verified")

        # Checkpoint: starting build phase
        run_manager.save_checkpoint(run_id, {"phase": "building_edges"})

        # Build edges (read-only queries against repos, repo_categories, repo_dependencies)
        compatible_edges = build_compatible_with(cur)
        alternative_edges = build_alternative_to(cur)
        depends_edges = build_depends_on(cur)

        new_counts = {
            "COMPATIBLE_WITH": len(compatible_edges),
            "ALTERNATIVE_TO": len(alternative_edges),
            "DEPENDS_ON": len(depends_edges),
        }
        logger.info("New edge counts: %s", new_counts)

        # Checkpoint: edges built
        run_manager.save_checkpoint(run_id, {
            "phase": "edges_built",
            "edge_counts": new_counts,
        })

        # Validate against prior successful run (raises EdgeCountValidationError on abort)
        run_manager.validate_edge_counts(run_id, new_counts)

        # Stage edges into temp table
        _create_staging_table(cur)
        s1 = _insert_into_staging(cur, compatible_edges, "COMPATIBLE_WITH")
        s2 = _insert_into_staging(cur, alternative_edges, "ALTERNATIVE_TO")
        s3 = _insert_into_staging(cur, depends_edges, "DEPENDS_ON")
        logger.info("Staged %d edges total", s1 + s2 + s3)

        # Checkpoint: staged, about to swap
        run_manager.save_checkpoint(run_id, {
            "phase": "staged",
            "edge_counts": new_counts,
        })

        # Atomic swap (all within the same transaction)
        _archive_and_swap(cur, run_id=run_id)
        conn.commit()
        logger.info("Atomic swap committed successfully")

        elapsed = time.monotonic() - t0
        logger.info("Graph build completed in %.1fs", elapsed)

        edge_counts = {
            "COMPATIBLE_WITH": s1,
            "ALTERNATIVE_TO": s2,
            "DEPENDS_ON": s3,
        }

        # Print examples for operator visibility
        for label, edges_list in [
            ("COMPATIBLE_WITH", compatible_edges),
            ("ALTERNATIVE_TO", alternative_edges),
            ("DEPENDS_ON", depends_edges),
        ]:
            if edges_list:
                print(f"\nEXAMPLE {label} EDGES:")
                for e in edges_list[:3]:
                    c = e.get("confidence", "?")
                    src = e.get("source_name", e["source"])
                    tgt = e.get("target_name", e["target"])
                    print(f"  {src} <-> {tgt} (confidence={c})")
                    print(f"    evidence: {e.get('evidence', e.get('metadata', {}))}")

        return edge_counts

    except EdgeCountValidationError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

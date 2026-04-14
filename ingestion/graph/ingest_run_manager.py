"""
IngestRunManager — crash-safe pipeline run tracking via ingest_runs table.

Responsibilities:
  - Record pipeline run start/complete/fail in ingest_runs (reporium-api DB)
  - Persist checkpoint_data JSONB so a crashed run can be resumed
  - Load a prior crashed run's checkpoint for resume
  - Validate edge counts against the previous run (warn >20% drop, abort >50% or zero)

Usage in build_knowledge_graph.py:
    rm = IngestRunManager(db_url)
    run_id = rm.start(triggered_by="schedule")
    try:
        rm.save_checkpoint(run_id, {"phase": "COMPATIBLE_WITH", "progress": 42})
        ...
        rm.complete(run_id, edge_counts={"DEPENDS_ON": 1200, ...})
    except Exception as exc:
        rm.fail(run_id, exc)
        raise
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)

WARN_DROP_FRACTION = 0.20
ABORT_DROP_FRACTION = 0.50
MIN_EDGES_FOR_ZERO_ABORT = 100


class EdgeCountValidationError(RuntimeError):
    """Raised when edge counts fail validation and the swap should be aborted."""


class IngestRunManager:
    def __init__(self, db_url: str) -> None:
        self._db_url = db_url

    def _connect(self):
        return psycopg2.connect(self._db_url)

    def start(self, triggered_by: str = "manual") -> int:
        git_sha = _get_git_sha()
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO ingest_runs (run_mode, status, triggered_by, git_sha, started_at) "
                    "VALUES ('graph_build', 'running', %s, %s, NOW()) RETURNING id",
                    (triggered_by, git_sha),
                )
                run_id: int = cur.fetchone()[0]
                logger.info("IngestRun started: id=%d triggered_by=%s", run_id, triggered_by)
                return run_id
        finally:
            conn.close()

    def save_checkpoint(self, run_id: int, data: dict[str, Any]) -> None:
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ingest_runs SET checkpoint_data = %s WHERE id = %s",
                    (json.dumps(data), run_id),
                )
        finally:
            conn.close()

    def load_checkpoint(self, run_id: int) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT checkpoint_data FROM ingest_runs WHERE id = %s", (run_id,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return None
        finally:
            conn.close()

    def complete(self, run_id: int, edge_counts: dict[str, int]) -> None:
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ingest_runs SET status='success', finished_at=NOW(), "
                    "checkpoint_data=NULL, prev_edge_counts=%s WHERE id=%s",
                    (json.dumps(edge_counts), run_id),
                )
                logger.info("IngestRun %d completed: %s", run_id, edge_counts)
        finally:
            conn.close()

    def fail(self, run_id: int, exc: Exception) -> None:
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ingest_runs SET status='failed', finished_at=NOW(), errors=%s WHERE id=%s",
                    (json.dumps({"error": str(exc), "type": type(exc).__name__}), run_id),
                )
                logger.error("IngestRun %d failed: %s", run_id, exc)
        finally:
            conn.close()

    def find_crashed_run(self) -> int | None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM ingest_runs WHERE status='running' AND checkpoint_data IS NOT NULL "
                "ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def validate_edge_counts(self, run_id: int, new_counts: dict[str, int]) -> None:
        prior_counts = self._get_prior_edge_counts()
        if prior_counts is None:
            logger.info("No prior edge count baseline — skipping regression check")
            return

        prior_total = sum(prior_counts.values())
        new_total = sum(new_counts.values())

        if prior_total > 0:
            drop_fraction = (prior_total - new_total) / prior_total
            if drop_fraction > ABORT_DROP_FRACTION:
                raise EdgeCountValidationError(
                    f"ABORT: total edge count dropped {drop_fraction:.0%} "
                    f"({prior_total} -> {new_total}). Swap cancelled."
                )
            if drop_fraction > WARN_DROP_FRACTION:
                logger.warning(
                    "Edge count dropped %.0f%% (%d -> %d). Below abort threshold. Proceeding.",
                    drop_fraction * 100, prior_total, new_total,
                )

        for edge_type, prior_count in prior_counts.items():
            if prior_count >= MIN_EDGES_FOR_ZERO_ABORT and new_counts.get(edge_type, 0) == 0:
                raise EdgeCountValidationError(
                    f"ABORT: edge type {edge_type!r} dropped to 0 "
                    f"(prior: {prior_count}). Swap cancelled."
                )

        logger.info("Edge count validation passed. Prior: %d, New: %d", prior_total, new_total)

    def _get_prior_edge_counts(self) -> dict[str, int] | None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT prev_edge_counts FROM ingest_runs "
                "WHERE status='success' AND prev_edge_counts IS NOT NULL "
                "ORDER BY finished_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return None
        finally:
            conn.close()


def _get_git_sha() -> str | None:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(__file__),
        )
        return sha.decode().strip()
    except Exception:
        return None

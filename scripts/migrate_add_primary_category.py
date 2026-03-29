#!/usr/bin/env python3
"""
KAN-41 — Migration: add primary_category and secondary_categories columns to repos.

Safe to run multiple times (checks before altering).
Run this BEFORE backfill_primary_category.py.

Usage:
    DATABASE_URL=<psycopg2-url> python scripts/migrate_add_primary_category.py
"""

import logging
import os
import sys

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

VALID_CATEGORIES = {
    "agents", "rag-retrieval", "llm-serving", "fine-tuning", "evaluation",
    "orchestration", "vector-databases", "observability", "security-safety",
    "code-generation", "data-processing", "computer-vision", "nlp-text",
    "speech-audio", "generative-media", "infrastructure",
}


def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Fall back to GCP secret (set externally)
        raise RuntimeError(
            "DATABASE_URL not set. Export it before running:\n"
            "  export DATABASE_URL=$(gcloud secrets versions access latest "
            "--secret=reporium-db-url-async --project=perditio-platform | "
            "sed 's/+asyncpg//' | sed 's/?ssl=require/?sslmode=require/')"
        )
    # Normalise: asyncpg → psycopg2
    url = url.replace("+asyncpg", "")
    url = url.replace("?ssl=require", "?sslmode=require")
    return url


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def run_migration(db_url: str) -> None:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        changed = False

        # 1. primary_category TEXT
        if not column_exists(cur, "repos", "primary_category"):
            logger.info("Adding column: repos.primary_category TEXT")
            cur.execute("ALTER TABLE repos ADD COLUMN primary_category TEXT")
            changed = True
        else:
            logger.info("Column already exists: repos.primary_category — skipping")

        # 2. secondary_categories TEXT[]
        if not column_exists(cur, "repos", "secondary_categories"):
            logger.info("Adding column: repos.secondary_categories TEXT[]")
            cur.execute("ALTER TABLE repos ADD COLUMN secondary_categories TEXT[]")
            changed = True
        else:
            logger.info("Column already exists: repos.secondary_categories — skipping")

        # 3. Optional CHECK constraint on primary_category (advisory, not enforced)
        #    We skip a hard constraint so the backfill can write freely; we validate
        #    in application code instead.

        if changed:
            conn.commit()
            logger.info("Migration committed successfully.")
        else:
            logger.info("Nothing to migrate — schema already up to date.")

        # Sanity check
        cur.execute(
            "SELECT COUNT(*) FROM repos WHERE primary_category IS NOT NULL"
        )
        classified = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM repos")
        total = cur.fetchone()[0]
        logger.info("Current state: %d / %d repos have primary_category", classified, total)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    db_url = get_db_url()
    run_migration(db_url)

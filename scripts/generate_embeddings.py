"""
Generate embeddings for all enriched repos using sentence-transformers.
Cost: $0 — runs locally.

Append-only strategy (migration 034):
  - Each re-embedding produces a NEW row with is_current=TRUE
  - The previous row is marked is_current=FALSE (preserved for history)
  - Historical embeddings enable drift analysis and rollback

Usage:
    python scripts/generate_embeddings.py
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from sentence_transformers import SentenceTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

MODEL_NAME = "all-MiniLM-L6-v2"  # 384-dim, fast, good for semantic similarity
BATCH_SIZE = 64


def get_db_url() -> str:
    """Get DB URL from Secret Manager, .env, or environment. Strip whitespace."""
    # Try GCP Secret Manager first
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT", "perditio-platform")
        name = f"projects/{project_id}/secrets/reporium-db-url/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception:
        pass

    # Fall back to environment
    url = os.getenv("DATABASE_URL", "")
    if url:
        return url.strip()

    raise RuntimeError("No DATABASE_URL found in Secret Manager or environment")


def build_embedding_text(row: dict) -> str:
    """
    Combine all enriched fields into a single text for embedding.
    Fields: name, description, readme_summary, problem_solved, integration_tags
    """
    parts = []

    if row.get("name"):
        parts.append(row["name"])
    if row.get("forked_from"):
        parts.append(row["forked_from"])
    if row.get("description"):
        parts.append(row["description"])
    if row.get("readme_summary"):
        parts.append(row["readme_summary"])
    if row.get("problem_solved"):
        parts.append(row["problem_solved"])

    tags = row.get("integration_tags")
    if tags:
        if isinstance(tags, str):
            tags = json.loads(tags)
        if tags:
            parts.append("integrations: " + " ".join(tags))

    return " ".join(parts)[:2048]  # model max input


def _has_is_current_column(cur) -> bool:
    """Check if repo_embeddings has the is_current column (migration 034)."""
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'repo_embeddings' AND column_name = 'is_current'
    """)
    return cur.fetchone() is not None


def _get_repos_needing_embeddings(cur, append_only: bool):
    """
    Get repos that need new embeddings.

    In append-only mode: all repos (we always generate fresh + mark old as historical).
    In legacy mode: only repos without any embedding row.
    """
    if append_only:
        # Re-embed all repos (append-only: old ones get is_current=FALSE)
        cur.execute("""
            SELECT r.id, r.name, r.forked_from, r.description,
                   r.readme_summary, r.problem_solved, r.integration_tags
            FROM repos r
            ORDER BY r.parent_stars DESC NULLS LAST;
        """)
    else:
        # Legacy: only repos without embeddings
        cur.execute("""
            SELECT r.id, r.name, r.forked_from, r.description,
                   r.readme_summary, r.problem_solved, r.integration_tags
            FROM repos r
            LEFT JOIN repo_embeddings e ON r.id = e.repo_id
            WHERE e.repo_id IS NULL
            ORDER BY r.parent_stars DESC NULLS LAST;
        """)
    return cur.fetchall(), [d[0] for d in cur.description]


def _insert_append_only(cur, conn, repo_id, embedding_json, model_name, now, run_id):
    """
    Append-only insert: mark existing current embedding as historical,
    then insert new one as current.
    """
    # Mark existing current embedding as historical
    cur.execute(
        """UPDATE repo_embeddings
           SET is_current = false
           WHERE repo_id = %s AND is_current = true""",
        (str(repo_id),),
    )

    # Insert new embedding as current
    cur.execute(
        """INSERT INTO repo_embeddings
           (id, repo_id, embedding, model, generated_at, is_current, ingest_run_id)
           VALUES (gen_random_uuid(), %s, %s, %s, %s, true, %s)""",
        (str(repo_id), embedding_json, model_name, now, run_id),
    )


def _insert_legacy(cur, conn, repo_id, embedding_json, model_name, now):
    """Legacy insert with ON CONFLICT DO UPDATE (overwrites, no history)."""
    cur.execute(
        """INSERT INTO repo_embeddings (repo_id, embedding, model, generated_at)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (repo_id) DO UPDATE SET
             embedding = EXCLUDED.embedding,
             model = EXCLUDED.model,
             generated_at = EXCLUDED.generated_at;""",
        (str(repo_id), embedding_json, model_name, now),
    )


def main():
    logger.info("Generating embeddings with sentence-transformers (local, $0)")
    logger.info(f"Model: {MODEL_NAME}")

    t0 = time.monotonic()

    # Load model
    logger.info("Loading model...")
    model = SentenceTransformer(MODEL_NAME)
    logger.info(f"Model loaded in {time.monotonic() - t0:.1f}s")

    # Connect to DB
    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Check if append-only mode is available (migration 034)
    append_only = _has_is_current_column(cur)
    if append_only:
        logger.info("Append-only mode: migration 034 detected")
    else:
        logger.info("Legacy mode: migration 034 not applied yet")

    # Count existing embeddings
    if append_only:
        cur.execute("SELECT COUNT(*) FROM repo_embeddings WHERE is_current = true;")
    else:
        cur.execute("SELECT COUNT(*) FROM repo_embeddings;")
    existing = cur.fetchone()[0]
    logger.info(f"Existing current embeddings: {existing}")

    # Create ingest_run for traceability (if table exists)
    run_id = None
    if append_only:
        try:
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'ingest_runs'
            """)
            if cur.fetchone():
                cur.execute(
                    """INSERT INTO ingest_runs (run_mode, status, repos_upserted, repos_processed)
                       VALUES ('embedding_gen', 'running', 0, 0)
                       RETURNING id"""
                )
                run_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"Ingest run created: {run_id}")
        except Exception as ex:
            logger.warning(f"Could not create ingest_run: {ex}")
            conn.rollback()

    # Get repos needing embeddings
    rows, cols = _get_repos_needing_embeddings(cur, append_only)

    if not rows:
        logger.info("No repos need embeddings. Nothing to do.")
        conn.close()
        return

    logger.info(f"Repos to process: {len(rows)}")

    # Build texts
    texts = []
    repo_ids = []
    for row in rows:
        row_dict = dict(zip(cols, row))
        text = build_embedding_text(row_dict)
        texts.append(text)
        repo_ids.append(row_dict["id"])

    # Generate embeddings in batches
    logger.info(f"Generating embeddings in batches of {BATCH_SIZE}...")
    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        all_embeddings.extend(batch_embeddings)

        done = min(i + BATCH_SIZE, len(texts))
        if done % 200 == 0 or done == len(texts):
            logger.info(f"  Encoded {done}/{len(texts)} repos")

    # Insert into database
    logger.info("Writing embeddings to database...")
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    historical_marked = 0

    for repo_id, embedding in zip(repo_ids, all_embeddings):
        embedding_json = json.dumps(embedding.tolist())
        try:
            if append_only:
                # Check if there's an existing current embedding
                cur.execute(
                    "SELECT 1 FROM repo_embeddings WHERE repo_id = %s AND is_current = true",
                    (str(repo_id),),
                )
                had_existing = cur.fetchone() is not None
                _insert_append_only(cur, conn, repo_id, embedding_json, MODEL_NAME, now, run_id)
                if had_existing:
                    historical_marked += 1
            else:
                _insert_legacy(cur, conn, repo_id, embedding_json, MODEL_NAME, now)
            inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert embedding for {repo_id}: {e}")
            conn.rollback()
            # Reconnect
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

        if inserted % 200 == 0 and inserted > 0:
            conn.commit()
            logger.info(f"  Committed {inserted}/{len(repo_ids)} embeddings")

    conn.commit()
    elapsed = time.monotonic() - t0

    # Finalize ingest run
    if run_id:
        try:
            cur.execute(
                """UPDATE ingest_runs
                   SET status = 'success', finished_at = NOW(),
                       repos_processed = %s
                   WHERE id = %s""",
                (inserted, run_id),
            )
            conn.commit()
        except Exception:
            pass

    logger.info(f"Embedding generation complete: {inserted} embeddings in {elapsed:.1f}s")
    logger.info(f"Model: {MODEL_NAME}, Dimensions: {len(all_embeddings[0])}")
    if append_only:
        logger.info(f"Historical embeddings preserved: {historical_marked}")
    logger.info(f"Cost: $0.00 (local sentence-transformers)")

    # Verify
    if append_only:
        cur.execute("SELECT COUNT(*) FROM repo_embeddings WHERE is_current = true;")
        current = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM repo_embeddings WHERE is_current = false;")
        historical = cur.fetchone()[0]
        logger.info(f"Current embeddings: {current}, Historical: {historical}")
    else:
        cur.execute("SELECT COUNT(*) FROM repo_embeddings;")
        total = cur.fetchone()[0]
        logger.info(f"Total embeddings in DB: {total}")

    conn.close()

    logger.info("Embedding generation finished")


if __name__ == "__main__":
    main()

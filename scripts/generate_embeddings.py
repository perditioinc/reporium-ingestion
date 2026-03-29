"""
Phase 3: Generate embeddings for all enriched repos using sentence-transformers.
Cost: $0 — runs locally.

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


def main():
    logger.info("Phase 3: Generating embeddings with sentence-transformers (local, $0)")
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

    # Count existing embeddings to skip
    cur.execute("SELECT COUNT(*) FROM repo_embeddings;")
    existing = cur.fetchone()[0]
    logger.info(f"Existing embeddings: {existing}")

    # Get all repos that need embeddings
    cur.execute("""
        SELECT r.id, r.name, r.forked_from, r.description,
               r.readme_summary, r.problem_solved, r.integration_tags
        FROM repos r
        LEFT JOIN repo_embeddings e ON r.id = e.repo_id
        WHERE e.repo_id IS NULL
        ORDER BY r.parent_stars DESC NULLS LAST;
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    if not rows:
        logger.info("All repos already have embeddings. Nothing to do.")
        conn.close()
        return

    logger.info(f"Repos needing embeddings: {len(rows)}")

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

    for repo_id, embedding in zip(repo_ids, all_embeddings):
        embedding_json = json.dumps(embedding.tolist())
        try:
            cur.execute(
                """INSERT INTO repo_embeddings (repo_id, embedding, model, generated_at)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (repo_id) DO UPDATE SET
                     embedding = EXCLUDED.embedding,
                     model = EXCLUDED.model,
                     generated_at = EXCLUDED.generated_at;""",
                (str(repo_id), embedding_json, MODEL_NAME, now),
            )
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

    logger.info(f"Phase 3 complete: {inserted} embeddings generated in {elapsed:.1f}s")
    logger.info(f"Model: {MODEL_NAME}, Dimensions: {len(all_embeddings[0])}")
    logger.info(f"Cost: $0.00 (local sentence-transformers)")

    # Verify
    cur.execute("SELECT COUNT(*) FROM repo_embeddings;")
    total = cur.fetchone()[0]
    logger.info(f"Total embeddings in DB: {total}")

    conn.close()

    # Update RESUME.md
    resume = f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: COMPLETE — 826/826 enriched, 0 errors, $2.5213 spent
Phase 3: COMPLETE — {inserted} embeddings generated, {elapsed:.1f}s, $0.00
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Model: {MODEL_NAME} ({len(all_embeddings[0])}-dim)
Next phase: 4 (knowledge graph)
"""
    with open("RESUME.md", "w") as f:
        f.write(resume)

    logger.info("RESUME.md updated")


if __name__ == "__main__":
    main()

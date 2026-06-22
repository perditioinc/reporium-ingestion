"""
Generate repo_embeddings using the local stub embedder (Ollama-compatible).

generate_embeddings.py (the production script) imports sentence-transformers
directly and downloads a model. For a fast, $0, dependency-light smoke we
instead reuse the same embedding *text* builder from that script but fetch the
384-dim vector from the local stub embedder service over the Ollama API the
app already speaks (ingestion/enrichment/embeddings.py). Result lands in the
same `repo_embeddings` table the graph snapshot reads.

This is a substrate convenience for the smoke path only; the real
generate_embeddings.py still works unchanged against the local Postgres if you
have sentence-transformers installed.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

import psycopg2


def build_embedding_text(row: dict) -> str:
    """Inlined copy of scripts.generate_embeddings.build_embedding_text.

    Inlined so this smoke helper does not import that module (which pulls in
    sentence-transformers at import time). Keep in sync if the field set
    changes; the production embeddings script remains the source of truth.
    """
    parts = []
    for key in ("name", "forked_from", "description", "readme_summary", "problem_solved"):
        if row.get(key):
            parts.append(row[key])
    tags = row.get("integration_tags")
    if tags:
        if isinstance(tags, str):
            tags = json.loads(tags)
        if tags:
            parts.append("integrations: " + " ".join(tags))
    return " ".join(parts)[:2048]


DB_URL = os.environ["DATABASE_URL"]
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("EMBEDDING_MODEL", "stub-embed-384")


def embed(text: str) -> list[float]:
    body = json.dumps({"model": MODEL, "prompt": text}).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def main() -> int:
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, forked_from, description, readme_summary, "
        "problem_solved, integration_tags FROM repos"
    )
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        print("no repos to embed", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc).isoformat()
    n = 0
    for row in rows:
        rd = dict(zip(cols, row))
        vec = embed(build_embedding_text(rd))
        cur.execute(
            "UPDATE repo_embeddings SET is_current = false "
            "WHERE repo_id = %s AND is_current = true",
            (str(rd["id"]),),
        )
        cur.execute(
            "INSERT INTO repo_embeddings (repo_id, embedding, model, "
            "generated_at, is_current) VALUES (%s, %s, %s, %s, true)",
            (str(rd["id"]), json.dumps(vec), MODEL, now),
        )
        n += 1
    print(f"embedded: {n} repos ({len(vec)}-dim) via {MODEL}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

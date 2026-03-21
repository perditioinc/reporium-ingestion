"""
Phase 3 verification: Test semantic search quality.
Encodes a query with the same model, then computes cosine similarity against all repo embeddings.
"""

import json
import os
import numpy as np
import psycopg2
from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"


def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def search(query: str, model, cur, top_k: int = 5):
    """Encode query and find top-k most similar repos."""
    query_embedding = model.encode(query)

    # Get all embeddings
    cur.execute("""
        SELECT r.name, r.forked_from, r.description, r.readme_summary, r.parent_stars,
               e.embedding
        FROM repo_embeddings e
        JOIN repos r ON r.id = e.repo_id;
    """)

    results = []
    for row in cur.fetchall():
        repo_embedding = np.array(json.loads(row[5]))
        sim = cosine_similarity(query_embedding, repo_embedding)
        results.append({
            "name": row[0],
            "forked_from": row[1],
            "description": str(row[2])[:100] if row[2] else "",
            "stars": row[4],
            "similarity": float(sim),
        })

    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


def main():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("Set DATABASE_URL")

    print("Loading model...")
    model = SentenceTransformer(MODEL_NAME)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    queries = [
        "OCR document parsing extract text from images",
        "autonomous AI agents task planning",
        "vector database similarity search embeddings",
        "real-time chat application websocket messaging",
        "kubernetes container orchestration deployment",
    ]

    for query in queries:
        print(f"\n{'='*60}")
        print(f"QUERY: {query}")
        print(f"{'='*60}")
        results = search(query, model, cur)
        for i, r in enumerate(results, 1):
            upstream = r["forked_from"] or r["name"]
            print(f"  {i}. {upstream} ({r['stars'] or 0} stars) -- sim={r['similarity']:.4f}")
            desc = r['description'].encode('ascii', 'replace').decode('ascii')
            print(f"     {desc}")

    conn.close()


if __name__ == "__main__":
    main()

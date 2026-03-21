"""
Phase 4: Build knowledge graph edges from enriched repo data.
Cost: $0 — uses existing data only.

Edge types:
  COMPATIBLE_WITH — repos sharing 2+ integration tags
  ALTERNATIVE_TO  — repos in the same category (from repo_categories)
  DEPENDS_ON      — repos where one appears in another's dependencies

Usage:
    DATABASE_URL=... python scripts/build_knowledge_graph.py
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Set DATABASE_URL")
    return url


def ensure_table(cur):
    """Create repo_edges table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_edges (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            source_repo_id UUID NOT NULL REFERENCES repos(id),
            target_repo_id UUID NOT NULL REFERENCES repos(id),
            edge_type TEXT NOT NULL,
            weight FLOAT DEFAULT 1.0,
            evidence JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(source_repo_id, target_repo_id, edge_type)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_repo_edges_source ON repo_edges(source_repo_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_repo_edges_target ON repo_edges(target_repo_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_repo_edges_type ON repo_edges(edge_type);
    """)


def build_compatible_with(cur):
    """
    Create COMPATIBLE_WITH edges for repos sharing 2+ integration tags.
    Weight = number of shared tags / max possible tags.
    """
    logger.info("Building COMPATIBLE_WITH edges...")

    # Get all repos with integration tags
    cur.execute("""
        SELECT id, name, forked_from, integration_tags
        FROM repos
        WHERE integration_tags IS NOT NULL AND integration_tags::text != '[]';
    """)

    repos_with_tags = []
    for row in cur.fetchall():
        tags = row[3] if isinstance(row[3], list) else json.loads(row[3]) if row[3] else []
        if len(tags) >= 1:
            repos_with_tags.append({
                "id": row[0],
                "name": row[1],
                "forked_from": row[2],
                "tags": set(t.lower() for t in tags),
            })

    logger.info(f"  Repos with integration tags: {len(repos_with_tags)}")

    # Build tag -> repo index for efficient lookup
    tag_to_repos = defaultdict(list)
    for repo in repos_with_tags:
        for tag in repo["tags"]:
            tag_to_repos[tag].append(repo)

    # Find pairs sharing 2+ tags using tag-indexed approach
    # For each pair of tags, find repos that have both
    edges = []
    seen = set()
    pair_shared = defaultdict(set)  # (id1, id2) -> shared tags

    tags_list = list(tag_to_repos.keys())
    for tag in tags_list:
        repos = tag_to_repos[tag]
        if len(repos) > 100:
            # Skip very common tags to avoid O(n²) blowup
            continue
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            pair_shared[pair_key].add(tag)

    for pair_key, shared_tags in pair_shared.items():
        if len(shared_tags) >= 2:
            if pair_key in seen:
                continue
            seen.add(pair_key)
            # Find the repos by id
            r1_id, r2_id = pair_key
            r1 = next((r for r in repos_with_tags if str(r["id"]) == r1_id), None)
            r2 = next((r for r in repos_with_tags if str(r["id"]) == r2_id), None)
            if r1 and r2:
                weight = len(shared_tags) / max(len(r1["tags"]), len(r2["tags"]))
                edges.append({
                    "source": r1["id"],
                    "target": r2["id"],
                    "weight": weight,
                    "evidence": {"shared_tags": sorted(shared_tags), "count": len(shared_tags)},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })
        # Cap at 5000 edges to keep manageable
        if len(edges) >= 5000:
            break

    logger.info(f"  COMPATIBLE_WITH edges found: {len(edges)}")
    return edges


def build_alternative_to(cur):
    """
    Create ALTERNATIVE_TO edges for repos in the same category.
    Uses repo_categories junction table.
    """
    logger.info("Building ALTERNATIVE_TO edges...")

    cur.execute("SELECT COUNT(*) FROM repo_categories;")
    cat_count = cur.fetchone()[0]
    logger.info(f"  Entries in repo_categories: {cat_count}")

    if cat_count == 0:
        # Fall back: use problem_solved similarity
        # Group repos by similar problem_solved text
        logger.info("  No categories in DB. Using problem_solved grouping as fallback...")

        cur.execute("""
            SELECT id, name, forked_from, problem_solved, integration_tags
            FROM repos
            WHERE problem_solved IS NOT NULL AND problem_solved != '';
        """)

        # Simple keyword-based grouping
        PROBLEM_GROUPS = {
            "llm-framework": ["llm", "language model", "large language"],
            "vector-db": ["vector database", "vector search", "similarity search", "embedding"],
            "ocr": ["ocr", "optical character", "text extraction", "document parsing"],
            "agent": ["autonomous agent", "ai agent", "agent framework", "task planning"],
            "code-editor": ["code editor", "ide", "development environment"],
            "web-framework": ["web framework", "web application", "http server"],
            "ml-framework": ["machine learning framework", "deep learning", "neural network", "training"],
            "container": ["container", "kubernetes", "docker", "orchestration"],
            "monitoring": ["monitoring", "observability", "metrics", "logging"],
            "data-pipeline": ["data pipeline", "etl", "data processing", "data flow"],
        }

        group_repos = defaultdict(list)
        for row in cur.fetchall():
            problem = (row[3] or "").lower()
            for group_name, keywords in PROBLEM_GROUPS.items():
                if any(kw in problem for kw in keywords):
                    group_repos[group_name].append({
                        "id": row[0],
                        "name": row[1],
                        "forked_from": row[2],
                    })
                    break  # Only assign to first matching group

        edges = []
        seen = set()
        for group_name, repos in group_repos.items():
            if len(repos) < 2:
                continue
            for r1, r2 in combinations(repos, 2):
                pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)
                edges.append({
                    "source": r1["id"],
                    "target": r2["id"],
                    "weight": 0.7,  # Lower confidence since keyword-based
                    "evidence": {"category": group_name, "method": "problem_solved_keywords"},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })

        logger.info(f"  ALTERNATIVE_TO edges (keyword fallback): {len(edges)}")
        return edges

    # Use actual categories if available
    cur.execute("""
        SELECT rc.category_name, r.id, r.name, r.forked_from
        FROM repo_categories rc
        JOIN repos r ON r.id = rc.repo_id
        ORDER BY rc.category_name;
    """)

    cat_repos = defaultdict(list)
    for row in cur.fetchall():
        cat_repos[row[0]].append({"id": row[1], "name": row[2], "forked_from": row[3]})

    edges = []
    seen = set()
    MAX_EDGES = 3000
    MAX_PER_CATEGORY = 50  # Only compare top repos in each category
    for cat, repos in cat_repos.items():
        if len(repos) < 2:
            continue
        # Cap per category to avoid blowup on large categories
        subset = repos[:MAX_PER_CATEGORY]
        for r1, r2 in combinations(subset, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            edges.append({
                "source": r1["id"],
                "target": r2["id"],
                "weight": 1.0,
                "evidence": {"category": cat},
                "source_name": r1["forked_from"] or r1["name"],
                "target_name": r2["forked_from"] or r2["name"],
            })
            if len(edges) >= MAX_EDGES:
                break
        if len(edges) >= MAX_EDGES:
            break

    logger.info(f"  ALTERNATIVE_TO edges: {len(edges)}")
    return edges


def build_depends_on(cur):
    """
    Create DEPENDS_ON edges where one repo appears in another's dependencies.
    Match dependency names against repo names (case-insensitive).
    """
    logger.info("Building DEPENDS_ON edges...")

    # Get all repos with dependencies
    cur.execute("""
        SELECT id, name, forked_from, dependencies
        FROM repos
        WHERE dependencies IS NOT NULL AND dependencies::text != '[]';
    """)

    repos_with_deps = []
    for row in cur.fetchall():
        deps = row[3] if isinstance(row[3], list) else json.loads(row[3]) if row[3] else []
        if deps:
            repos_with_deps.append({
                "id": row[0],
                "name": row[1],
                "forked_from": row[2],
                "deps": [d.lower().replace("-", "").replace("_", "") for d in deps],
                "deps_raw": deps,
            })

    # Build name -> repo_id index (normalize names)
    cur.execute("SELECT id, name, forked_from FROM repos;")
    name_to_repo = {}
    for row in cur.fetchall():
        # Index by repo name (without owner)
        upstream = row[2] or row[1]
        if "/" in upstream:
            repo_name = upstream.split("/")[1]
        else:
            repo_name = upstream
        normalized = repo_name.lower().replace("-", "").replace("_", "")
        name_to_repo[normalized] = {"id": row[0], "name": row[1], "forked_from": row[2]}

    edges = []
    seen = set()
    for repo in repos_with_deps:
        for dep in repo["deps"]:
            if dep in name_to_repo:
                target = name_to_repo[dep]
                if str(repo["id"]) == str(target["id"]):
                    continue  # Skip self-reference

                pair_key = (str(repo["id"]), str(target["id"]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                edges.append({
                    "source": repo["id"],
                    "target": target["id"],
                    "weight": 1.0,
                    "evidence": {"dependency": dep, "method": "requirements.txt"},
                    "source_name": repo["forked_from"] or repo["name"],
                    "target_name": target["forked_from"] or target["name"],
                })

    logger.info(f"  DEPENDS_ON edges: {len(edges)}")
    return edges


def insert_edges(cur, edges, edge_type):
    """Insert edges into repo_edges table."""
    inserted = 0
    for e in edges:
        try:
            cur.execute("""
                INSERT INTO repo_edges (source_repo_id, target_repo_id, edge_type, weight, evidence)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_repo_id, target_repo_id, edge_type) DO UPDATE SET
                    weight = EXCLUDED.weight,
                    evidence = EXCLUDED.evidence;
            """, (str(e["source"]), str(e["target"]), edge_type, e["weight"], json.dumps(e["evidence"])))
            inserted += 1
        except Exception as ex:
            logger.warning(f"  Failed to insert edge: {ex}")
    return inserted


def main():
    t0 = time.monotonic()
    logger.info("Phase 4: Building knowledge graph edges ($0)")

    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # Create table
    ensure_table(cur)
    conn.commit()
    logger.info("repo_edges table ready")

    # Clear existing edges for clean rebuild
    cur.execute("DELETE FROM repo_edges;")
    conn.commit()

    # Build each edge type
    compatible_edges = build_compatible_with(cur)
    alternative_edges = build_alternative_to(cur)
    depends_edges = build_depends_on(cur)

    # Insert all
    c1 = insert_edges(cur, compatible_edges, "COMPATIBLE_WITH")
    conn.commit()
    c2 = insert_edges(cur, alternative_edges, "ALTERNATIVE_TO")
    conn.commit()
    c3 = insert_edges(cur, depends_edges, "DEPENDS_ON")
    conn.commit()

    elapsed = time.monotonic() - t0

    # Summary
    cur.execute("SELECT edge_type, COUNT(*) FROM repo_edges GROUP BY edge_type ORDER BY edge_type;")
    print()
    print("=" * 60)
    print("PHASE 4 SUMMARY: Knowledge Graph Edges")
    print("=" * 60)
    total = 0
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} edges")
        total += row[1]
    print(f"  TOTAL: {total} edges")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Cost: $0.00")
    print()

    # Example edges
    print("EXAMPLE COMPATIBLE_WITH EDGES:")
    for e in compatible_edges[:3]:
        print(f"  {e['source_name']} <-> {e['target_name']}")
        print(f"    shared tags: {e['evidence']['shared_tags']}")
    print()

    print("EXAMPLE ALTERNATIVE_TO EDGES:")
    for e in alternative_edges[:3]:
        print(f"  {e['source_name']} <-> {e['target_name']}")
        print(f"    evidence: {e['evidence']}")
    print()

    print("EXAMPLE DEPENDS_ON EDGES:")
    for e in depends_edges[:3]:
        print(f"  {e['source_name']} -> {e['target_name']}")
        print(f"    evidence: {e['evidence']}")

    conn.close()

    # Update RESUME.md
    resume = f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: COMPLETE -- 826/826 enriched, 0 errors, $2.5213 spent
Phase 3: COMPLETE -- 826 embeddings, 62s, $0.00
Phase 4: COMPLETE -- {total} edges ({c1} COMPATIBLE_WITH, {c2} ALTERNATIVE_TO, {c3} DEPENDS_ON), {elapsed:.1f}s, $0.00
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Next phase: 5 (/intelligence/query endpoint)
"""
    with open("RESUME.md", "w") as f:
        f.write(resume)


if __name__ == "__main__":
    main()

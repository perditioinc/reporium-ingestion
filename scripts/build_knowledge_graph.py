"""
Phase 4: Build knowledge graph edges — atomic rebuild with crash recovery.

Supersedes the previous direct-psycopg2 implementation.

Architecture:
  1. Edges are built into a staging table (repo_edges_staging_{run_id}).
  2. Edge counts are validated against the previous run (warn >20% drop, abort >50%
     or any type with >100 prior edges drops to zero).
  3. A single locked transaction: archive → truncate → swap from staging → drop staging.
  4. Crash BEFORE the LOCK leaves live repo_edges untouched.
  5. Progress is checkpointed to ingest_runs.checkpoint_data for resume after crash.

Requires:
  - repo_edges + repo_edges_history tables (migration 033 in reporium-api)
  - ingest_runs extended columns: checkpoint_data, prev_edge_counts, git_sha,
    triggered_by (migration 032 in reporium-api)

(Previous build notes below — kept for historical reference)

Edge types:
  COMPATIBLE_WITH — repos sharing 2+ integration tags
  ALTERNATIVE_TO  — repos in the same category (from repo_categories)
  DEPENDS_ON      — repos where one appears in another's repo_dependencies

Requires:
  - Migration 031 (repo_edges table with confidence column)
  - Migration 032 (repo_edges_history table)

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

from ingestion.graph_snapshot import build_graph_snapshot, publish_graph_snapshot, resolve_graph_snapshot_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Set DATABASE_URL")
    # Secret Manager stores SQLAlchemy-style 'postgresql+psycopg2://' but
    # psycopg2.connect() only accepts 'postgresql://' (no dialect suffix).
    if url.startswith("postgresql+psycopg2://"):
        url = "postgresql://" + url[len("postgresql+psycopg2://"):]
    return url


def verify_table(cur):
    """Verify repo_edges table exists (created by migration 031)."""
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'repo_edges'
    """)
    if not cur.fetchone():
        raise RuntimeError(
            "repo_edges table does not exist. "
            "Run migration 031_formalize_repo_edges first."
        )


# ---------------------------------------------------------------------------
# Edge builders
# ---------------------------------------------------------------------------

def build_compatible_with(cur):
    """
    Create COMPATIBLE_WITH edges for repos sharing 2+ integration tags.
    Weight = shared_tag_count / max(tags_a, tags_b).
    Confidence = min(0.85, shared_tag_count / 5).

    Uses a top-K-per-repo strategy: each repo keeps at most MAX_PER_REPO
    strongest matches to prevent O(n^2) explosion on ubiquitous tags.
    """
    MAX_PER_REPO = 30
    logger.info("Building COMPATIBLE_WITH edges...")

    cur.execute("""
        SELECT id, name, forked_from, integration_tags
        FROM repos
        WHERE integration_tags IS NOT NULL AND integration_tags::text != '[]';
    """)

    repos_with_tags = []
    repo_by_id = {}
    for row in cur.fetchall():
        tags = row[3] if isinstance(row[3], list) else json.loads(row[3]) if row[3] else []
        if len(tags) >= 1:
            repo = {
                "id": row[0],
                "name": row[1],
                "forked_from": row[2],
                "tags": set(t.lower() for t in tags),
            }
            repos_with_tags.append(repo)
            repo_by_id[str(row[0])] = repo

    logger.info(f"  Repos with integration tags: {len(repos_with_tags)}")

    # Build tag -> repos index
    tag_to_repos = defaultdict(list)
    for repo in repos_with_tags:
        for tag in repo["tags"]:
            tag_to_repos[tag].append(repo)

    logger.info(f"  Unique tags: {len(tag_to_repos)}")
    big_tags = {t: len(rs) for t, rs in tag_to_repos.items() if len(rs) > 300}
    if big_tags:
        logger.info(f"  Tags with >300 repos (included): {big_tags}")

    # Accumulate shared tags per pair
    pair_shared = defaultdict(set)
    for tag, repos in tag_to_repos.items():
        if len(repos) < 2:
            continue
        if len(repos) > 500:
            logger.info(f"  Processing tag '{tag}' with {len(repos)} repos...")
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            pair_shared[pair_key].add(tag)

    # Build candidate edges (pairs sharing 2+ tags)
    repo_candidates = defaultdict(list)
    for pair_key, shared_tags in pair_shared.items():
        if len(shared_tags) < 2:
            continue
        r1_id, r2_id = pair_key
        r1 = repo_by_id.get(r1_id)
        r2 = repo_by_id.get(r2_id)
        if not r1 or not r2:
            continue
        weight = len(shared_tags) / max(len(r1["tags"]), len(r2["tags"]))
        entry = (weight, pair_key, shared_tags, r1, r2)
        repo_candidates[r1_id].append(entry)
        repo_candidates[r2_id].append(entry)

    # Top-K per repo
    selected_pairs = set()
    for repo_id, candidates in repo_candidates.items():
        candidates.sort(key=lambda x: x[0], reverse=True)
        for weight, pair_key, shared_tags, r1, r2 in candidates[:MAX_PER_REPO]:
            selected_pairs.add(pair_key)

    # Build final edge list
    edges = []
    for pair_key in selected_pairs:
        shared_tags = pair_shared[pair_key]
        r1_id, r2_id = pair_key
        r1 = repo_by_id[r1_id]
        r2 = repo_by_id[r2_id]
        weight = len(shared_tags) / max(len(r1["tags"]), len(r2["tags"]))
        confidence = min(0.85, len(shared_tags) / 5)
        edges.append({
            "source": r1["id"],
            "target": r2["id"],
            "weight": weight,
            "confidence": confidence,
            "evidence": {"shared_tags": sorted(shared_tags), "count": len(shared_tags)},
            "source_name": r1["forked_from"] or r1["name"],
            "target_name": r2["forked_from"] or r2["name"],
        })

    logger.info(f"  COMPATIBLE_WITH edges (top-{MAX_PER_REPO}/repo): {len(edges)}")
    return edges


def build_alternative_to(cur):
    """
    Create ALTERNATIVE_TO edges for repos in the same category.
    Confidence = 0.7 for primary_category, 0.4 for keyword fallback.
    """
    MAX_PER_REPO = 30
    logger.info("Building ALTERNATIVE_TO edges...")

    cur.execute("SELECT COUNT(*) FROM repo_categories;")
    cat_count = cur.fetchone()[0]
    logger.info(f"  Entries in repo_categories: {cat_count}")

    if cat_count == 0:
        logger.info("  No categories in DB. Using problem_solved grouping as fallback...")

        cur.execute("""
            SELECT id, name, forked_from, problem_solved
            FROM repos
            WHERE problem_solved IS NOT NULL AND problem_solved != '';
        """)

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
                        "id": row[0], "name": row[1], "forked_from": row[2],
                    })
                    break

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
                    "weight": 0.7,
                    "confidence": 0.4,  # keyword fallback = low confidence
                    "evidence": {"category": group_name, "method": "problem_solved_keywords"},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })

        logger.info(f"  ALTERNATIVE_TO edges (keyword fallback): {len(edges)}")
        return edges

    # Use repo_categories (is_primary=true) to find primary category per repo.
    # NOTE: repos.primary_category was never added to the Alembic schema -- always
    # use the repo_categories table, not a column on repos.
    cur.execute("""
        SELECT rc.category_name, r.id, r.name, r.forked_from
        FROM repos r
        JOIN repo_categories rc ON rc.repo_id = r.id AND rc.is_primary = true
        ORDER BY rc.category_name;
    """)

    cat_repos = defaultdict(list)
    repo_by_id = {}
    for row in cur.fetchall():
        repo = {"id": row[1], "name": row[2], "forked_from": row[3]}
        cat_repos[row[0]].append(repo)
        repo_by_id[str(row[1])] = repo

    logger.info(f"  Categories (repo_categories is_primary): {len(cat_repos)}")

    # Top-K-per-repo strategy
    repo_candidates = defaultdict(list)
    seen_pairs = set()
    for cat, repos in cat_repos.items():
        if len(repos) < 2:
            continue
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            entry = (1.0, pair_key, cat)
            repo_candidates[str(r1["id"])].append(entry)
            repo_candidates[str(r2["id"])].append(entry)

    selected_pairs = set()
    for repo_id, candidates in repo_candidates.items():
        for weight, pair_key, cat in candidates[:MAX_PER_REPO]:
            selected_pairs.add((pair_key, cat))

    edges = []
    for pair_key, cat in selected_pairs:
        r1_id, r2_id = pair_key
        r1 = repo_by_id[r1_id]
        r2 = repo_by_id[r2_id]
        edges.append({
            "source": r1["id"],
            "target": r2["id"],
            "weight": 1.0,
            "confidence": 0.7,  # same DB category row
            "evidence": {"category": cat, "method": "primary_category"},
            "source_name": r1["forked_from"] or r1["name"],
            "target_name": r2["forked_from"] or r2["name"],
        })

    logger.info(f"  ALTERNATIVE_TO edges (primary_category, top-{MAX_PER_REPO}/repo): {len(edges)}")
    return edges


def build_depends_on(cur):
    """
    Create DEPENDS_ON edges from the repo_dependencies table (migration 029).
    Reads direct dependencies and matches package_name (normalized:
    lowercase, strip hyphens/underscores) against repo names in the DB.

    Confidence = 0.95 (deterministic, from actual package files).
    """
    logger.info("Building DEPENDS_ON edges...")

    # Verify repo_dependencies table exists
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'repo_dependencies'
    """)
    if not cur.fetchone():
        logger.warning("  repo_dependencies table not found - skipping DEPENDS_ON")
        return []

    # Read direct dependencies, excluding sentinel rows
    cur.execute("""
        SELECT rd.repo_id, rd.package_name, rd.package_ecosystem
        FROM repo_dependencies rd
        WHERE rd.is_direct = true
          AND rd.package_name != '__none__';
    """)
    dep_rows = cur.fetchall()
    logger.info(f"  Direct dependencies in repo_dependencies: {len(dep_rows)}")

    if not dep_rows:
        logger.warning("  No direct dependencies found - DEPENDS_ON will be empty")
        logger.warning("  Run dependency extraction to populate repo_dependencies")
        return []

    # Group by repo_id
    repo_deps = defaultdict(list)
    for repo_id, pkg_name, ecosystem in dep_rows:
        repo_deps[str(repo_id)].append((pkg_name, ecosystem))

    logger.info(f"  Repos with direct dependencies: {len(repo_deps)}")

    # Build repo name index
    cur.execute("SELECT id, name, forked_from FROM repos;")
    name_to_repo = {}
    id_to_repo = {}
    for row in cur.fetchall():
        repo_info = {"id": row[0], "name": row[1], "forked_from": row[2]}
        id_to_repo[str(row[0])] = repo_info
        upstream = row[2] or row[1]
        repo_name = upstream.split("/")[1] if "/" in upstream else upstream
        normalized = repo_name.lower().replace("-", "").replace("_", "")
        name_to_repo[normalized] = repo_info

    logger.info(f"  Repo name index size: {len(name_to_repo)}")

    # Match dependencies against repos
    edges = []
    seen = set()
    for repo_id, deps in repo_deps.items():
        src = id_to_repo.get(repo_id)
        if not src:
            continue
        for pkg_name, ecosystem in deps:
            normalized_pkg = pkg_name.lower().replace("-", "").replace("_", "")
            if normalized_pkg in name_to_repo:
                target = name_to_repo[normalized_pkg]
                if repo_id == str(target["id"]):
                    continue  # Skip self-reference
                pair_key = (repo_id, str(target["id"]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                edges.append({
                    "source": repo_id,
                    "target": target["id"],
                    "weight": 1.0,
                    "confidence": 0.95,  # deterministic, from actual package files
                    "evidence": {
                        "package": pkg_name,
                        "ecosystem": ecosystem,
                        "method": "repo_dependencies",
                    },
                    "source_name": src["forked_from"] or src["name"],
                    "target_name": target["forked_from"] or target["name"],
                })

    logger.info(f"  DEPENDS_ON edges (no cap): {len(edges)}")
    return edges


def build_extends(cur):
    """
    Create EXTENDS edges between repos that share an upstream owner.

    Semantic: "repos extending the same upstream organization's ecosystem."
    All of our repos are perditioinc/* forks of public projects, so a true
    "fork-of-fork" parent->child relationship doesn't exist in the DB.
    Instead, we group repos by the OWNER component of their `forked_from`
    upstream. Repos sharing an upstream owner (e.g. all facebook/* forks,
    all google/* forks) are linked: they extend the same project family.

    Examples:
        facebook/react       <-EXTENDS-> facebook/react-native
        google/jax           <-EXTENDS-> google/flax
        microsoft/typescript <-EXTENDS-> microsoft/vscode

    Top-K-per-repo cap protects against giant orgs (microsoft/* etc.) creating
    O(n^2) edge counts. Confidence = 0.6 (heuristic, weaker than DEPENDS_ON
    but stronger than the keyword-fallback ALTERNATIVE_TO).

    KAN-228: this replaces the previous "fork resolution" implementation
    (KAN-155) which produced 0 edges in production because perditioinc/*
    repos never have their `forked_from` string land back in the same
    `repos` table — fork-of-fork relationships do not exist in this corpus.
    The shared-upstream-owner heuristic was field-validated on the rolled-
    back KAN-164 image (1704 edges), which is the version that has actually
    been producing the live EXTENDS data.
    """
    MAX_PER_REPO = 20
    MIN_GROUP_SIZE = 2
    MAX_GROUP_SIZE = 30  # skip groups bigger than this — too noisy to be useful

    logger.info("Building EXTENDS edges...")

    cur.execute("""
        SELECT id, name, forked_from
        FROM repos
        WHERE forked_from IS NOT NULL AND forked_from != '' AND forked_from LIKE '%/%';
    """)
    rows = cur.fetchall()
    logger.info(f"  Repos with parsable forked_from: {len(rows)}")

    if not rows:
        return []

    # Group repos by upstream owner
    by_owner = defaultdict(list)
    for repo_id, name, forked_from in rows:
        upstream_owner = forked_from.split("/", 1)[0].strip().lower()
        if not upstream_owner:
            continue
        by_owner[upstream_owner].append({
            "id": repo_id,
            "name": name,
            "forked_from": forked_from,
        })

    eligible_groups = {o: r for o, r in by_owner.items()
                       if MIN_GROUP_SIZE <= len(r) <= MAX_GROUP_SIZE}
    logger.info(
        "  Upstream owners: %d total, %d eligible (size %d-%d)",
        len(by_owner), len(eligible_groups), MIN_GROUP_SIZE, MAX_GROUP_SIZE,
    )

    # Top-K-per-repo: collect candidates per repo, keep strongest by group size
    repo_candidates = defaultdict(list)
    for owner, repos in eligible_groups.items():
        # Smaller groups are more meaningful — invert size for weight
        weight = 1.0 / max(len(repos) - 1, 1)
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            entry = (weight, pair_key, owner, r1, r2)
            repo_candidates[str(r1["id"])].append(entry)
            repo_candidates[str(r2["id"])].append(entry)

    selected = {}
    for repo_id, candidates in repo_candidates.items():
        candidates.sort(key=lambda x: x[0], reverse=True)
        for weight, pair_key, owner, r1, r2 in candidates[:MAX_PER_REPO]:
            if pair_key not in selected:
                selected[pair_key] = (weight, owner, r1, r2)

    edges = []
    for pair_key, (weight, owner, r1, r2) in selected.items():
        edges.append({
            "source": r1["id"],
            "target": r2["id"],
            "weight": weight,
            "confidence": 0.6,  # heuristic — same upstream owner != strict extension
            "evidence": {
                "method": "shared_upstream_owner",
                "upstream_owner": owner,
            },
            "source_name": r1["forked_from"] or r1["name"],
            "target_name": r2["forked_from"] or r2["name"],
        })

    logger.info(
        f"  EXTENDS edges (shared upstream owner, top-{MAX_PER_REPO}/repo): {len(edges)}"
    )
    return edges


# ---------------------------------------------------------------------------
# Edge insertion
# ---------------------------------------------------------------------------

def insert_edges(cur, edges, edge_type):
    """Batch-insert edges into repo_edges table with confidence."""
    inserted = 0
    BATCH = 500
    for i in range(0, len(edges), BATCH):
        batch = edges[i:i + BATCH]
        values = []
        params = []
        for e in batch:
            values.append("(%s, %s, %s, %s, %s, %s)")
            params.extend([
                str(e["source"]), str(e["target"]), edge_type,
                e["weight"], e.get("confidence", 0.5), json.dumps(e["evidence"]),
            ])
        sql = (
            "INSERT INTO repo_edges "
            "(source_repo_id, target_repo_id, edge_type, weight, confidence, metadata) "
            "VALUES " + ", ".join(values)
            + " ON CONFLICT (source_repo_id, target_repo_id, edge_type) DO UPDATE SET"
            " weight = EXCLUDED.weight,"
            " confidence = EXCLUDED.confidence,"
            " metadata = EXCLUDED.metadata"
        )
        try:
            cur.execute(sql, params)
            inserted += len(batch)
        except Exception as ex:
            logger.warning(f"  Batch insert failed: {ex}")
        if (i + BATCH) % 5000 == 0 or i + BATCH >= len(edges):
            logger.info(f"  {edge_type}: inserted {inserted}/{len(edges)}")
    return inserted


def record_history(cur, edge_counts: dict, run_id: int | None = None):
    """Record edge counts for velocity tracking.

    Writes to ingest_runs.prev_edge_counts if the table supports it,
    otherwise logs the counts. Full edge history is archived by
    atomic_swap into repo_edges_history.
    """
    try:
        cur.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_name = 'ingest_runs' AND column_name = 'prev_edge_counts'"""
        )
        if cur.fetchone():
            if run_id:
                cur.execute(
                    "UPDATE ingest_runs SET prev_edge_counts = %s WHERE id = %s",
                    (json.dumps(edge_counts), run_id),
                )
            else:
                # No run_id — create a new run for the record
                cur.execute(
                    """INSERT INTO ingest_runs
                       (run_mode, status, repos_processed, prev_edge_counts)
                       VALUES ('graph_build', 'success', %s, %s)""",
                    (sum(edge_counts.values()), json.dumps(edge_counts)),
                )
        else:
            logger.info("Edge counts (no ingest_runs support): %s", edge_counts)
    except Exception as ex:
        logger.warning("Failed to record edge history: %s", ex)


# ---------------------------------------------------------------------------
# Main — atomic rebuild with crash recovery
# ---------------------------------------------------------------------------

def main():
    """
    Entry point for atomic graph rebuild.

    Delegates to ingestion.graph.atomic_swap (staging table swap) and
    ingestion.graph.ingest_run_manager (crash-safe ingest_runs tracking).

    Set RESUME_CRASHED_RUN=1 to resume a previously crashed run.
    """
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from ingestion.graph.atomic_swap import build_and_swap
    from ingestion.graph.ingest_run_manager import (
        IngestRunManager, EdgeCountValidationError
    )

    db_url = get_db_url()
    triggered_by = os.getenv("TRIGGERED_BY", "manual").strip()
    resume_crashed = os.getenv("RESUME_CRASHED_RUN", "").strip().lower() in ("1", "true", "yes")

    run_manager = IngestRunManager(db_url)
    run_id = None

    if resume_crashed:
        crashed = run_manager.find_crashed_run()
        if crashed:
            logger.info("Resuming crashed run id=%d", crashed)
            checkpoint = run_manager.load_checkpoint(crashed) or {}
            logger.info("Prior checkpoint: %s", checkpoint)
            run_id = crashed
        else:
            logger.info("No crashed run found — starting fresh")

    if run_id is None:
        run_id = run_manager.start(triggered_by=triggered_by)

    logger.info("Graph build starting [run_id=%d triggered_by=%s]", run_id, triggered_by)

    try:
        edge_counts = build_and_swap(db_url, run_id, run_manager)
        run_manager.complete(run_id, edge_counts)

        total = sum(edge_counts.values())
        print()
        print("=" * 60)
        print("KNOWLEDGE GRAPH SUMMARY (atomic swap)")
        print("=" * 60)
        for etype, count in sorted(edge_counts.items()):
            print(f"  {etype}: {count} edges")
        print(f"  TOTAL: {total} edges")
        print(f"  run_id: {run_id}")
        print()

        if edge_counts.get("DEPENDS_ON", 0) == 0:
            logger.warning(
                "::warning::DEPENDS_ON edge count is 0 — "
                "check that repo_dependencies is populated"
            )

        snapshot_config = resolve_graph_snapshot_config()
        if snapshot_config.enabled:
            conn = psycopg2.connect(db_url)
            try:
                cur = conn.cursor()
                snapshot = build_graph_snapshot(cur)
                snapshot_result = publish_graph_snapshot(snapshot, snapshot_config)
            finally:
                conn.close()

            logger.info(
                "Published knowledge graph snapshot (%s bytes) to %s",
                snapshot_result["size_bytes"],
                ", ".join(snapshot_result["destinations"]),
            )
        else:
            logger.info("Skipping knowledge graph snapshot publish; no destination configured")

    except EdgeCountValidationError as exc:
        logger.error("Edge count validation failed — swap aborted: %s", exc)
        run_manager.fail(run_id, exc)
        print(f"\n::warning::Graph swap aborted: {exc}")
        sys.exit(1)

    except Exception as exc:
        logger.error("Graph build failed: %s", exc, exc_info=True)
        run_manager.fail(run_id, exc)
        raise


if __name__ == "__main__":
    main()

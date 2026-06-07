#!/usr/bin/env bash
# $0 / OSS / local smoke test for the reporium-ingestion substrate.
#
# Brings up the OSS substitutes, exercises the ingestion write + embeddings +
# knowledge-graph-snapshot path against them, asserts results, and (unless
# KEEP_UP=1) tears everything down with volumes. Prints PASS or FAIL.
#
# Cloud deps are NOT touched: no GitHub, no Anthropic, no GCP. The seed posts
# synthetic repos through the real ReporiumAPIClient -> stub API -> Postgres
# contract; embeddings come from the local stub embedder; the graph snapshot is
# written to a local file (GCS substitute).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE="docker compose -f $HERE/docker-compose.yml"

# Host-side env for the python helpers (talk to the published container ports).
export REPORIUM_API_URL="http://localhost:58000"
export REPORIUM_API_KEY="local-dev"
export INGEST_API_KEY="local-dev"
export DATABASE_URL="postgresql://reporium:reporium@localhost:55432/reporium"
export OLLAMA_URL="http://localhost:11434"
export EMBEDDING_MODEL="stub-embed-384"
export GRAPH_SNAPSHOT_LOCAL_PATH="$ROOT/local/out/knowledge-graph.json"
export GH_TOKEN="${GH_TOKEN:-}"
export GH_USERNAME="${GH_USERNAME:-perditioinc}"
export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"
export PUBSUB_REPO_INGESTED_TOPIC=""
export CACHE_DB_PATH="$ROOT/local/out/cache.db"

PY="${PYTHON:-python}"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export GH_TOKEN="${GH_TOKEN:-local-smoke-no-github}"

fail() { echo ""; echo "SMOKE RESULT: FAIL — $1"; teardown; exit 1; }

teardown() {
  if [ "${KEEP_UP:-0}" = "1" ]; then
    echo "KEEP_UP=1 set — leaving substrate running."
  else
    echo "--- tearing down (down -v) ---"
    $COMPOSE down -v --remove-orphans || true
  fi
}

mkdir -p "$ROOT/local/out"

echo "=== 1/6 compose up --wait (build + health) ==="
$COMPOSE up -d --build --wait || fail "compose up did not become healthy"
$COMPOSE ps

# Create the GCS-substitute bucket in MinIO (best-effort; the snapshot itself
# uses the local-file path, so this is for parity, not required by the smoke).
# MSYS_NO_PATHCONV stops git-bash on Windows from mangling the container path.
MSYS_NO_PATHCONV=1 docker run --rm \
  --network reporium-ingestion-local_default \
  --entrypoint /bin/sh minio/mc:latest -c \
  'mc alias set l http://minio:9000 reporium reporium-local && mc mb --ignore-existing l/reporium-graph' \
  >/dev/null 2>&1 \
  && echo "minio bucket reporium-graph ready" \
  || echo "note: minio bucket create skipped (non-fatal; snapshot uses local file)"

echo "=== 2/6 stub reporium-api /health ==="
code="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:58000/health)"
[ "$code" = "200" ] || fail "reporium-api stub /health returned $code"
echo "health ok ($code)"

echo "=== 3/6 seed repos via real ReporiumAPIClient -> stub API -> Postgres ==="
( cd "$ROOT" && $PY local/seed/seed_repos.py ) || fail "seed failed"

count="$(curl -s http://localhost:58000/repos/count | $PY -c 'import sys,json;print(json.load(sys.stdin)["count"])')"
echo "repos in DB: $count"
[ "$count" -ge 3 ] || fail "expected >=3 repos, got $count"

echo "=== 4/6 generate embeddings via local stub embedder + pgvector ==="
( cd "$ROOT" && $PY local/seed/embed_via_stub.py ) || fail "embedding generation failed"

echo "=== 5/6 build + publish knowledge graph snapshot (pgvector <=> ) ==="
( cd "$ROOT" && $PY scripts/publish_graph_snapshot.py ) || fail "graph snapshot publish failed"
[ -s "$GRAPH_SNAPSHOT_LOCAL_PATH" ] || fail "snapshot file not written"

echo "=== 6/6 assert snapshot contents ==="
$PY - "$GRAPH_SNAPSHOT_LOCAL_PATH" <<'PYEOF'
import json, sys
snap = json.load(open(sys.argv[1], encoding="utf-8"))
nodes = snap.get("nodes", [])
stats = snap.get("stats", {})
sim = snap.get("similarity_edges", [])
print(f"snapshot: nodes={len(nodes)} "
      f"with_embeddings={stats.get('repos_with_embeddings')} "
      f"similarity_edges={len(sim)} typed_edges={len(snap.get('typed_edges', []))}")
assert len(nodes) >= 3, "expected >=3 nodes in snapshot"
assert stats.get("repos_with_embeddings", 0) >= 3, "embeddings not reflected"
assert len(sim) >= 1, "expected at least one pgvector similarity edge"
print("snapshot assertions passed")
PYEOF
[ $? -eq 0 ] || fail "snapshot content assertions failed"

echo ""
echo "SMOKE RESULT: PASS"
teardown

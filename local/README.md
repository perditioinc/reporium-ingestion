# Local $0 / OSS development substrate

A self-contained, additive, local-only dev environment for `reporium-ingestion`.
It stands up open-source substitutes for every cloud dependency the pipeline
talks to, so the ingestion write path, embeddings, and knowledge-graph snapshot
can be exercised end to end with no cloud account, no secrets, and no spend.

Nothing here touches production or any live cloud project. It is purely
additive: no production config or existing CI is modified.

## Cloud to OSS map

| Production dependency | Local OSS substitute | How |
|-----------------------|----------------------|-----|
| Cloud SQL (Postgres + pgvector) | `pgvector/pgvector:pg16` | service `postgres`, host port 55432, schema in `sql/init.sql` |
| reporium-api (Cloud Run) | FastAPI stub | service `reporium-api-stub`, host port 58000, `stub-api/app.py` |
| GCS (graph snapshot bucket) | MinIO + native local-file path | service `minio` (bucket `reporium-graph`); snapshot also written to `GRAPH_SNAPSHOT_LOCAL_PATH` |
| GCP Pub/Sub | Pub/Sub emulator | service `pubsub`, host port 58085; topic unset = graceful no-op |
| GCP Secret Manager | env vars | `.env.local` — no secrets needed locally |
| Anthropic Claude (paid enrichment) | not run | `ANTHROPIC_API_KEY` unset; enrichment degrades gracefully |
| Paid embeddings / Ollama | stub embedder | service `embedder`, Ollama-compatible API, deterministic 384-dim vectors |

The app code is not modified. Every substitute is reached through an env var the
app already reads (`DATABASE_URL`, `REPORIUM_API_URL`, `OLLAMA_URL`,
`GRAPH_SNAPSHOT_LOCAL_PATH`, `PUBSUB_*`).

## Quick start

```bash
# from the repo root
make local-up        # build + start all substitutes, wait for health
make local-smoke     # full up -> seed -> embed -> graph -> assert -> down -v
make local-down      # stop + remove containers and volumes
```

Or from inside `local/`:

```bash
make up
make smoke
make down
```

## What the smoke test does

1. `docker compose up --build --wait` — all five services to healthy.
2. Asserts the stub reporium-api `/health` returns 200.
3. Seeds synthetic repos through the **real** `ReporiumAPIClient` -> stub API ->
   Postgres (exercises the actual ingestion write contract).
4. Generates embeddings via the local stub embedder and stores them in pgvector.
5. Builds + publishes the knowledge graph snapshot using pgvector cosine
   distance (`<=>`), writing the artifact to a local file.
6. Asserts the snapshot has nodes, embedding-backed repos, and similarity edges.
7. Tears down with `down -v` (set `KEEP_UP=1` to leave it running).

## Driving a real ingestion run against the substrate

The smoke is hermetic (no GitHub). To run the full pipeline against the local
substitutes, copy `.env.local.example` to `.env.local`, set a real `GH_TOKEN`
(GitHub API is free), `source` it, then:

```bash
make local-up
python -m ingestion run --mode quick
```

This fetches real repos from GitHub but writes everything to the local Postgres
via the stub API, embeds via the local embedder, and publishes the graph
snapshot locally. Claude enrichment stays off unless you set `ANTHROPIC_API_KEY`
(which would no longer be $0).

## Ports

| Service | Host port | Purpose |
|---------|-----------|---------|
| postgres | 55432 | Postgres + pgvector |
| reporium-api-stub | 58000 | ingest contract |
| minio | 59000 / 59001 | S3 API / console (user `reporium` / `reporium-local`) |
| pubsub | 58085 | Pub/Sub emulator |
| embedder | 11434 | Ollama-compatible embeddings |

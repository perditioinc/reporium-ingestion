# reporium-ingestion

AI-native ingestion pipeline for Reporium. Runs on a Mac Mini, never exposed to the public internet. Currently tracking **~1,544 repos** (as of March 2026).

Fetches GitHub repositories, enriches them with **Claude** (8-dimension open taxonomy), generates embeddings locally with sentence-transformers, and writes to reporium-api. Publishes GCP Pub/Sub events after each run so the API auto-refreshes taxonomy and portfolio intelligence.

> API-call counts below are directional estimates based on the current ~1,544-repo corpus. Figures scale with live repo count and cache state. The March 2026 initial milestone had 826 repos — those historical figures appear in COST_REPORT.md and ARCHITECTURE_DECISIONS.md.

---

## Stack

- **Python 3.12+**
- `httpx` — async HTTP (GitHub API + reporium-api)
- `aiosqlite` — local SQLite cache
- `anthropic` — Claude API for AI enrichment (8-dimension open taxonomy)
- `sentence-transformers` — local 384-dim embeddings (all-MiniLM-L6-v2, free, no API cost)
- `APScheduler` — job scheduling
- `Rich` — terminal UI
- `Pydantic v2` — data validation
- `google-cloud-pubsub` — optional, for post-ingestion event publishing

---

## Mac Mini Setup

### 1. Install Python 3.12+

```bash
brew install python@3.12
```

### 2. Clone and install

```bash
cd ~/Developer
git clone <repo-url> reporium-ingestion
cd reporium-ingestion
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
# GitHub
GH_TOKEN=your_github_pat
GH_USERNAME=perditioinc

# reporium-api
REPORIUM_API_URL=http://localhost:8000
REPORIUM_API_KEY=your_api_key
INGEST_API_KEY=your_ingest_key        # X-Ingest-Key header for protected endpoints

# Claude AI enrichment
ANTHROPIC_API_KEY=your_anthropic_key
ENRICHMENT_MODEL=claude-sonnet-4-20250514

# Events (optional — GCP Pub/Sub)
PUBSUB_REPO_INGESTED_TOPIC=projects/perditio-platform/topics/repo-ingested

# Knowledge graph snapshot publication
GRAPH_SNAPSHOT_BUCKET=perditio-platform-bucket
GRAPH_SNAPSHOT_OBJECT=reporium/graph/knowledge-graph.json
```

> **GCP Secret Manager:** In production, `ANTHROPIC_API_KEY`, `REPORIUM_API_KEY`, `INGEST_API_KEY`, and `DATABASE_URL` are resolved automatically from Secret Manager — no `.env` needed on Cloud infra.

### 4. Bootstrap (first run)

```bash
python scripts/bootstrap.py
```

Checks all connections and runs a full ingestion.

---

## Usage

```bash
# Quick incremental update (default — only changed repos)
python -m ingestion run

# Weekly refresh (parent stats, languages, fork sync)
python -m ingestion run --mode weekly

# Full refresh — re-fetches everything (use monthly)
python -m ingestion run --mode full

# Fix specific repos
python -m ingestion fix --repos repo1 repo2

# Check rate limit and cache stats
python -m ingestion status

# Cache commands
python -m ingestion cache stats
python -m ingestion cache clean

# Start scheduled daemon (daily/weekly/monthly)
python -m ingestion schedule
```

---

## Run Modes

| Mode | GitHub API Calls | When | What |
|------|-----------------|------|------|
| `quick` | ~240 | Daily | Only repos changed since last run |
| `weekly` | ~1,500 | Sunday | Refresh parent stats, languages, fork sync |
| `full` | ~9,300 | Monthly | Everything — re-generates all embeddings |
| `fix` | ~3/repo | On-demand | Specific repos only |

---

## AI Enrichment — 8 Taxonomy Dimensions

Every repo is enriched by Claude with **open-ended** values across 8 dimensions. There are no hardcoded lists — values are generated freely from each repo's README, topics, and description, then stored in the database and assigned via pgvector cosine similarity.

| Dimension | Description | Examples |
|-----------|-------------|---------|
| `skill_area` | Core AI/ML competency | `RAG & Retrieval`, `Fine-tuning`, `Agents & Orchestration` |
| `industry` | Target industry vertical | `Healthcare`, `Finance`, `Developer Tools` |
| `use_case` | Problem being solved | `Document Q&A`, `Code generation`, `Anomaly detection` |
| `modality` | Data modality | `Text`, `Vision`, `Audio`, `Multimodal` |
| `ai_trend` | Emerging AI trend | `Agentic AI`, `Reasoning Models`, `Long Context` |
| `deployment_context` | Where it runs | `Edge`, `Cloud`, `On-premise`, `Serverless` |
| `tags` | Cross-cutting labels | `production-ready`, `research`, `benchmark` |
| `maturity_level` | Repo maturity | `prototype`, `production`, `research` |

**Cost:** ~$0.003/repo for Claude enrichment. Adding new taxonomy values costs ~$0.00001 (one local embedding) — no Claude re-enrichment of existing repos required.

---

## Embeddings

Local sentence-transformers (`all-MiniLM-L6-v2`, 384-dim). Runs entirely on-device — no API cost, no data leaves the Mac Mini.

Used by reporium-api for:
- Semantic search (`/search?q=` with `mode=semantic`)
- Taxonomy value assignment (pgvector cosine similarity, threshold 0.65)
- Similar repo discovery (`/repos/{name}/similar`)

---

## Event Publishing (Pub/Sub)

After each successful run, publishes a `repo.ingested` event to GCP Pub/Sub:

```json
{
  "event": "repo.ingested",
  "run_mode": "quick",
  "upserted": 42,
  "repo_count": 42,
  "repo_names": ["repo1", "repo2"],
  "published_at": "2026-03-24T09:00:00+00:00"
}
```

The API's push subscription (`POST /ingest/events/repo-ingested`) receives this and automatically triggers taxonomy embedding, similarity assignment, and portfolio intelligence cache refresh.

Set `PUBSUB_REPO_INGESTED_TOPIC` to enable. Falls back silently if unset or `google-cloud-pubsub` is not installed.

---

## Knowledge Graph Snapshot Publication

The production graph should be served from a durable snapshot artifact rather than request-time database queries.

Snapshot publication paths:

- `python scripts/build_knowledge_graph.py` rebuilds `repo_edges` and republishes the graph snapshot
- `python scripts/publish_graph_snapshot.py` republishes the current graph snapshot without rebuilding `repo_edges`

Environment variables:

- `GRAPH_SNAPSHOT_BUCKET` â€” GCS bucket for the published artifact
- `GRAPH_SNAPSHOT_OBJECT` â€” object path, default `reporium/graph/knowledge-graph.json`
- `GRAPH_SNAPSHOT_LOCAL_PATH` â€” optional local file target for development or tests

---

## Four-Tier Cache

| Tier | Re-fetch | What |
|------|----------|------|
| `PERMANENT` | Never | `upstream_created_at`, `original_owner` |
| `WEEKLY` | Every 7 days | Parent stats, language breakdown |
| `DAILY` | When repo updated | README, commits, releases |
| `REALTIME` | Active forks only | Fork sync status |

---

## Default Schedule

```
Daily at 9am      → quick mode
Sunday at 2am     → weekly mode
1st of month 3am  → full mode
```

Configure via: `QUICK_SCHEDULE`, `WEEKLY_SCHEDULE`, `FULL_SCHEDULE` env vars.

---

## Rate Limit Safety

- Always checks remaining before batch operations
- Pauses automatically if remaining < `MIN_RATE_LIMIT_BUFFER` (default: 100)
- Switches to sequential requests if remaining < 500
- On 429: waits 30s, retries once, marks unknown, continues — never crashes
- Logs every API call to SQLite

---

## Cloud SQL Access in CI

The nightly GitHub Actions workflow (`Nightly Graph Build`) rebuilds the knowledge graph by running `scripts/build_knowledge_graph.py`, which populates ALTERNATIVE_TO, COMPATIBLE_WITH, and DEPENDS_ON edges in Cloud SQL.

Since the Cloud SQL instance (`reporium-db`) has no public IP and uses private networking, the workflow uses **Cloud SQL Auth Proxy** to establish a secure tunnel on `127.0.0.1:5432`.

### Setup (One-time)

1. Create a GCP service account scoped to Cloud SQL:
   ```bash
   gcloud iam service-accounts create reporium-ingestion-ci
   gcloud projects add-iam-policy-binding <project> \
     --member="serviceAccount:reporium-ingestion-ci@<project>.iam.gserviceaccount.com" \
     --role="roles/cloudsql.client"
   gcloud projects add-iam-policy-binding <project> \
     --member="serviceAccount:reporium-ingestion-ci@<project>.iam.gserviceaccount.com" \
     --role="roles/cloudsql.instanceUser"
   ```

2. Generate a JSON key and upload to GitHub Secrets:
   ```bash
   gcloud iam service-accounts keys create /tmp/key.json \
     --iam-account=reporium-ingestion-ci@<project>.iam.gserviceaccount.com
   gh secret set GCP_SA_KEY --repo perditioinc/reporium-ingestion < /tmp/key.json
   rm /tmp/key.json
   ```

3. Also set database and connection name secrets:
   ```bash
   gh secret set DATABASE_URL --repo perditioinc/reporium-ingestion --body "postgresql://user:pass@127.0.0.1:5432/reporium"
   gh secret set CLOUD_SQL_CONNECTION_NAME --repo perditioinc/reporium-ingestion --body "project:region:instance"
   ```

### Key Rotation

The JSON key above should be rotated every **90 days** (next due: July 15, 2026). To rotate:

```bash
# List old keys
gcloud iam service-accounts keys list --iam-account=reporium-ingestion-ci@<project>.iam.gserviceaccount.com

# Delete old keys (keep at most 1 active)
gcloud iam service-accounts keys delete <key-id> \
  --iam-account=reporium-ingestion-ci@<project>.iam.gserviceaccount.com

# Generate new key and update GitHub secret
gcloud iam service-accounts keys create /tmp/key.json \
  --iam-account=reporium-ingestion-ci@<project>.iam.gserviceaccount.com
gh secret set GCP_SA_KEY --repo perditioinc/reporium-ingestion < /tmp/key.json
rm /tmp/key.json
```

---

## Tests

```bash
pip install pytest pytest-asyncio
pytest tests/
```

---

## Hard Rules

- NEVER call GitHub API without checking rate limit first
- NEVER re-fetch data that hasn't changed (`github_updated_at`)
- Fork sync: max 1 concurrent request, 1000ms delay
- On 429: wait 30s, retry once, mark unknown, continue — never crash
- AI enrichment is always optional — degrades gracefully if `ANTHROPIC_API_KEY` is unset
- All data goes through reporium-api — never write directly to its database
- Log every API call to SQLite for debugging

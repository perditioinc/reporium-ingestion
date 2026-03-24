# reporium-ingestion

AI-native ingestion pipeline for Reporium. Runs on a Mac Mini, never exposed to the public internet.

Fetches GitHub repositories, enriches them with local AI (Ollama), and writes to reporium-api.

Historical note: API-call counts and throughput examples in this README were captured during the March 2026 milestone corpus. Treat them as directional operating examples, not as the current live corpus size or current run cost.

---

## Stack

- Python 3.12+
- `httpx` — async HTTP (GitHub API + reporium-api)
- `aiosqlite` — local SQLite cache
- `Ollama` — local AI (summaries, embeddings)
- `APScheduler` — job scheduling
- `Rich` — terminal UI
- `Pydantic v2` — data validation

---

## Mac Mini Setup

### 1. Install Python 3.12+

```bash
brew install python@3.12
```

### 2. Install Ollama

```bash
brew install ollama
ollama serve &
ollama pull llama3.1:8b
ollama pull nomic-embed-text
```

### 3. Clone and install

```bash
cd ~/Developer
git clone <repo-url> reporium-ingestion
cd reporium-ingestion
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Configure environment

```bash
cp .env.example .env
# Edit .env with your values:
# GH_TOKEN=your_github_pat
# GH_USERNAME=perditioinc
# REPORIUM_API_URL=http://localhost:8000
# REPORIUM_API_KEY=your_api_key
```

### 5. Bootstrap (first run)

```bash
python scripts/bootstrap.py
```

This checks all connections and runs a full ingestion.

---

## Usage

```bash
# Quick incremental update (default, historical March 2026 estimate: ~127 API calls)
python -m ingestion run

# Weekly refresh (historical March 2026 estimate: ~800 API calls)
python -m ingestion run --mode weekly

# Full refresh — use sparingly (historical March 2026 estimate: ~5000 API calls)
python -m ingestion run --mode full

# Fix specific repos (after rate limit recovery)
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

These API-call counts are historical March 2026 estimates from the 826-repo milestone corpus. Current runs scale with the live repo count and current cache state.

| Mode | API Calls | When |
|------|-----------|------|
| `quick` | ~127 | Daily — fetches only changed repos |
| `weekly` | ~800 | Sunday — refreshes parent stats, languages, fork sync |
| `full` | ~5000 | Monthly — everything, re-generates all embeddings |
| `fix` | ~3/repo | Emergency — specific repos only |

---

## Four-Tier Cache

| Tier | Re-fetch | What |
|------|----------|------|
| PERMANENT | Never | upstream_created_at, original_owner |
| WEEKLY | Every 7 days | parent stats, language breakdown |
| DAILY | When repo updated | README, commits, releases |
| REALTIME | Active forks only | fork sync status |

---

## Default Schedule

```
Daily at 9am     → quick mode
Sunday at 2am    → weekly mode
1st of month 3am → full mode
```

Configure via environment variables: `QUICK_SCHEDULE`, `WEEKLY_SCHEDULE`, `FULL_SCHEDULE`.

---

## Rate Limit Safety

- Always checks remaining before batch operations
- Pauses automatically if remaining < `MIN_RATE_LIMIT_BUFFER` (default: 100)
- Switches to sequential requests if remaining < 500
- On 429 (abuse detection): waits 30s, retries once, marks unknown, continues — never crashes
- Logs every API call to SQLite

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
- Ollama/AI enrichment is always optional — degrades gracefully
- All data goes through reporium-api — never write directly to its database
- Log every API call to SQLite for debugging

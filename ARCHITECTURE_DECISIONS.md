# Reporium Ingestion — Architecture Decisions

## Decision 1: Sentence-Transformers Over Paid Embedding APIs

**Context:** Needed embeddings for 826 repos for semantic search.

**Options:**
1. OpenAI text-embedding-3-small — $0.02/1M tokens, ~$0.01 for 826 repos
2. Voyage AI — $0.01/1M tokens
3. Sentence-transformers locally — $0.00, runs on GPU

**Decision:** sentence-transformers with all-MiniLM-L6-v2 (384-dim)

**Why:** Already in requirements.txt. Runs locally in 62 seconds on GPU. Zero cost. 384 dimensions is sufficient for repo-level semantic similarity. No API dependency — works offline, no rate limits, no vendor lock-in.

**Tradeoff:** Lower dimensional embeddings than OpenAI (384 vs 1536). Acceptable — we're comparing repos not individual sentences. The semantic search tests prove quality is sufficient.

## Decision 2: Claude Sonnet for Enrichment

**Context:** Needed to generate readme_summary, problem_solved, and integration_tags for 826 repos.

**Options:**
1. Claude Haiku — cheapest, fastest, less accurate
2. Claude Sonnet — mid-tier, good balance
3. Claude Opus — most accurate, expensive

**Decision:** claude-sonnet-4-20250514

**Why:** Good quality at $0.003/repo. Total cost $2.52 for 826 repos. Haiku would save ~60% but integration tag quality matters for the knowledge graph. Opus would cost 5x more with marginal quality improvement for this use case.

**Measured result:** 826/826 enriched, 0 errors, 613/826 with integration tags. OCR repos correctly tagged, ML frameworks correctly identified. Quality verified manually on 5 sample repos.

## Decision 3: Knowledge Graph in PostgreSQL, Not Neo4j

**Context:** Needed to store repo relationships (compatible with, alternative to, depends on).

**Options:**
1. Neo4j — purpose-built graph database, powerful traversal
2. PostgreSQL repo_edges table — simple, already have the database

**Decision:** PostgreSQL repo_edges table with source_repo_id, target_repo_id, edge_type, weight, evidence JSONB.

**Why:** 5,418 edges is tiny. Neo4j adds infrastructure complexity for a problem that fits in a SQL table. Traversal queries (find repos compatible with X) are simple JOINs at this scale. If we reach 100K+ edges, revisit.

**Tradeoff:** No native graph traversal. Acceptable — we're doing 1-hop queries (find direct neighbors), not multi-hop path finding.

## Decision 4: Cosine Similarity in Python, Not pgvector

**Context:** repo_embeddings table stores embeddings as TEXT (JSON arrays), not as pgvector VECTOR type.

**Options:**
1. Migrate to pgvector VECTOR column — native similarity operators, indexed
2. Load all embeddings into Python, compute cosine similarity with numpy

**Decision:** Python-side cosine similarity for now.

**Why:** 826 embeddings × 384 dimensions fits in memory easily (~1.2 MB). Loading all and computing similarity takes <100ms. pgvector would require schema migration and the Neon free tier may have vector index limitations.

**When to migrate:** When repos > 10K, pgvector HNSW index becomes necessary for sub-100ms queries. Migration path is clear: ALTER COLUMN embedding TYPE vector(384), CREATE INDEX USING hnsw.

## Decision 5: API Key Auth on /intelligence/query

**Context:** The query endpoint calls Claude API (~$0.01/query). Must not be public.

**Decision:** Require `Authorization: Bearer {REPORIUM_API_KEY}` header. Same HTTPBearer auth as ingest endpoints.

**Why:** Simple, already implemented. Prevents anonymous users from running up Claude API costs. A future version could add per-user API keys with rate limiting.

## Decision 6: Dependencies from Requirements Files, Not AI

**Context:** Phase 1 needed to extract repo dependencies.

**Options:**
1. Parse requirements.txt/package.json from GitHub raw files — free, deterministic
2. Ask Claude to infer dependencies from README — paid, non-deterministic

**Decision:** Parse actual dependency files from GitHub.

**Why:** Free, accurate, deterministic. If a repo doesn't have requirements.txt, it doesn't have known dependencies — that's honest. 392/826 repos (47%) had parseable dependency files. The missing 53% are repos without standard dependency manifests (awesome lists, docs, notebooks).

**Tradeoff:** Misses dependencies declared in setup.py, pyproject.toml [tool.poetry], or Dockerfile. Could add parsers for these formats in a future iteration.

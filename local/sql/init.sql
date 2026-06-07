-- Local-only minimal schema for the reporium-ingestion $0 substrate.
--
-- In production these tables live in Cloud SQL and are owned by reporium-api's
-- Alembic migrations. This file is a LOCAL stand-in so the ingestion pipeline,
-- the stub API, the embeddings script, and the graph-snapshot path all run
-- end-to-end against a local Postgres. It is NOT a migration and never runs
-- against any cloud database.
--
-- Column set is the subset the ingestion code + stub API read/write. Anything
-- the pipeline does not touch is omitted.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- ── repos ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS repos (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                     TEXT NOT NULL UNIQUE,
    owner                    TEXT,
    description              TEXT,
    is_fork                  BOOLEAN DEFAULT FALSE,
    is_private               BOOLEAN DEFAULT FALSE,
    is_archived              BOOLEAN DEFAULT FALSE,
    forked_from              TEXT,
    primary_language         TEXT,
    primary_category         TEXT,
    github_url               TEXT,
    stargazers_count         INTEGER DEFAULT 0,
    parent_stars             INTEGER,
    forks_count              INTEGER DEFAULT 0,
    open_issues_count        INTEGER DEFAULT 0,
    commits_last_7_days      INTEGER,
    commits_last_30_days     INTEGER,
    commits_last_90_days     INTEGER,
    activity_score           INTEGER,
    activity_score_breakdown JSONB,
    quality_signals          JSONB,
    readme_summary           TEXT,
    problem_solved           TEXT,
    maturity_level           TEXT,
    quality_assessment       TEXT,
    integration_tags         JSONB DEFAULT '[]'::jsonb,
    dependencies             JSONB,
    license_spdx             TEXT,
    github_created_at        TEXT,
    github_updated_at        TEXT,
    your_last_push_at        TEXT,
    upstream_last_push_at    TEXT,
    forked_at                TEXT,
    created_at               TIMESTAMPTZ DEFAULT now(),
    updated_at               TIMESTAMPTZ DEFAULT now()
);

-- ── repo_embeddings (pgvector) ───────────────────────────────────────────────
-- generate_embeddings.py writes JSON-encoded vectors into `embedding`; the
-- graph snapshot reads `embedding_vec` for cosine distance. A trigger keeps
-- the vector column in sync so both the legacy and the snapshot path work.
CREATE TABLE IF NOT EXISTS repo_embeddings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repo_id       UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    embedding     TEXT,
    embedding_vec vector(384),
    model         TEXT,
    generated_at  TEXT,
    is_current    BOOLEAN DEFAULT TRUE,
    ingest_run_id BIGINT
);

CREATE OR REPLACE FUNCTION sync_embedding_vec() RETURNS trigger AS $$
BEGIN
    IF NEW.embedding IS NOT NULL THEN
        BEGIN
            NEW.embedding_vec := NEW.embedding::vector;
        EXCEPTION WHEN others THEN
            NEW.embedding_vec := NULL;
        END;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_embedding_vec ON repo_embeddings;
CREATE TRIGGER trg_sync_embedding_vec
    BEFORE INSERT OR UPDATE ON repo_embeddings
    FOR EACH ROW EXECUTE FUNCTION sync_embedding_vec();

-- ── repo_edges (knowledge graph) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS repo_edges (
    id             BIGSERIAL PRIMARY KEY,
    source_repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    target_repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    edge_type      TEXT NOT NULL,
    weight         DOUBLE PRECISION DEFAULT 0.5,
    confidence     DOUBLE PRECISION DEFAULT 0.5,
    created_at     TIMESTAMPTZ DEFAULT now()
);

-- ── ingest_runs (reporium-api naming; used by embeddings + graph scripts) ─────
CREATE TABLE IF NOT EXISTS ingest_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_mode        TEXT,
    status          TEXT DEFAULT 'running',
    repos_upserted  INTEGER DEFAULT 0,
    repos_processed INTEGER DEFAULT 0,
    errors          JSONB,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    checkpoint_data JSONB,
    prev_edge_counts JSONB,
    git_sha         TEXT,
    triggered_by    TEXT
);

-- ── trend snapshots + gaps (stub API write targets) ──────────────────────────
CREATE TABLE IF NOT EXISTS trend_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    captured_at  TEXT,
    payload      JSONB,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gaps (
    id          BIGSERIAL PRIMARY KEY,
    payload     JSONB,
    created_at  TIMESTAMPTZ DEFAULT now()
);

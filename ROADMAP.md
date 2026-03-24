# reporium-ingestion Roadmap

## Current State (March 2026)

`reporium-ingestion` is the operator-run pipeline that fetches GitHub data, enriches repos, and writes the results into `reporium-api`.

- Claude enrichment is active for repo summaries, taxonomy hints, and related enrichment fields
- Local sentence-transformer embeddings support semantic search and taxonomy assignment downstream
- Dependency parsing is implemented for 6 formats:
  - `requirements.txt`
  - `pyproject.toml`
  - `package.json`
  - `setup.py`
  - `go.mod`
  - `Cargo.toml`
- Quality signal extraction includes `has_tests` and `has_ci`
- Re-enrichment tooling exists for targeted or follow-up runs
- Run completion is recorded back to the API for dashboard visibility
- Pub/Sub publishing is available after successful ingestion runs

## Recent Platform Additions

- Open-ended 8-dimension taxonomy enrichment aligned to the database-driven taxonomy model
- License capture from GitHub metadata
- Expanded dependency extraction used for taxonomy and portfolio analysis
- Run-history recording for `/admin/runs`
- Event publishing so downstream taxonomy and intelligence refresh can happen automatically

## Historical Context

Some cost and scale references in this repository describe the March 2026 milestone corpus.
Those figures are historical planning snapshots, not fixed assumptions about the current live repo count.

## What Is Next

- Cloud deployment of the ingestion pipeline on managed infrastructure
- Nightly enrichment cron wiring so new and changed repos are processed automatically
- Scale the ingestion and enrichment path to 10K repos
- Public query UI safeguards that depend on fresh downstream data
- Commit-stat refresh integration so activity data stays current

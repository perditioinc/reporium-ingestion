# Changelog

## [Unreleased] - 2026-04-08

### Changed
- **activity_score formula**: Expanded from commit-only to multi-signal scoring:
  - Commit velocity: `commits_30d * 3 + commits_7d * 5` (up to 60 pts)
  - Popularity signal: `log2(stars + 1) * 2` (up to 20 pts)
  - Community engagement: `min(10, open_issues_count)` (up to 10 pts)
  - Recency bonus: 10 pts if any commits in last 90 days
  - Total capped at 100

## [Unreleased] - 2026-03-24

### Added
- Claude enrichment prompt expanded to the live 8-dimension taxonomy flow.
- SPDX license capture from the GitHub API is now forwarded in ingest payloads.
- Pub/Sub publisher for repo-ingested events after successful batch completion.
- Safer re-enrichment and repo-intake scripts with checkpoint/resume support.

### Changed
- Documentation now frames the March 2026 826-repo run as a historical milestone rather than current operating state.
- Enrichment guidance is aligned to Claude-based summary/tag generation instead of older local-model references.

### Fixed
- Silent best-effort failures now emit structured warnings instead of disappearing.
- Unused embedder allocation removed from the main ingestion path.

## [1.3.0] - 2026-03-23

### Added
- `scripts/backfill_fork_dates.py` — fetches `forked_at`, `your_last_push_at`, and `upstream_created_at`
  from GitHub GraphQL API for all forks missing these fields. Batches 50 repos per query, ~$0 cost.
  Run with `GH_TOKEN=... DATABASE_URL=... python scripts/backfill_fork_dates.py [--dry-run]`.

### Fixed
- Identified root cause of missing fork timeline dates: none of the ingest pipelines (forksync, reporium-db,
  backfill_from_library_json) ever wrote `forked_at` or `your_last_push_at` to the DB. All 1,390 forks
  have empty values for these fields. Run the new backfill script to fix.

## [1.2.0] - 2026-03-21

### Added
- CI workflow (`.github/workflows/test.yml`) — runs on push to main, 3 new unit tests
- `scripts/fetch_commit_stats.py` — populates commit counts from GitHub API /stats/commit_activity
- 395 repos now have commits_last_30_days > 0 (previously all zeros)

## [1.1.0] - 2026-03-21

### Added
- Database backfill script (`scripts/backfill_from_library_json.py`) — populates Neon from library.json
- Backfilled: 14,077 tags, 2,062 pmSkills, 918 industries, 825 builders, 4,920 categories (29 distinct)
- `repo_industries` table created for industry classification
- Ingestion log record documenting the full weekend's work

### Changed
- RESUME.md and COST_REPORT.md updated with backfill results
- BACKFILL_NEEDED.md documents the gap analysis and switch-back conditions

## [1.0.0] - 2026-03-21

### Added
- March 2026 milestone Phase 1: Dependency extraction from GitHub (392/826 repos, $0)
- March 2026 milestone Phase 2: Claude API enrichment with claude-sonnet-4-20250514 (826/826 repos, $2.52, 0 errors)
- March 2026 milestone Phase 3: Sentence-transformer embeddings (826 x 384-dim, all-MiniLM-L6-v2, 62s, $0)
- Phase 4: Knowledge graph edges (5,418 edges: 1,988 COMPATIBLE_WITH, 3,000 ALTERNATIVE_TO, 430 DEPENDS_ON)
- Phase 5: Semantic search verification (5 queries, all returning relevant results)
- Phase 6: COST_REPORT.md and ARCHITECTURE_DECISIONS.md
- Schema migration: added dependencies, problem_solved, integration_tags, quality_signals columns
- ANTHROPIC_API_KEY loaded from GCP Secret Manager with whitespace stripping

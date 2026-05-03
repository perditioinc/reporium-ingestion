# Lane: Taxonomy & Backfill Completion — JIRA Draft

- **Repo:** reporium-ingestion
- **Branch:** `claude/feature/KAN-TBD-taxonomy-backfill-completion`
- **Base:** `dev`
- **Date:** 2026-04-24
- **Status:** patch ready, PR-target `dev`, DO NOT MERGE without operator review

## Title

`KAN-TBD: Backfill 184 no-tag forks by feeding upstream topics+README through the deterministic tagger`

## Context

Two open issues in `perditioinc/reporium` describe the gap:

- **reporium#240 "P7: 184 no-tag repos — backfill strategy (not a full AI wave)"** —
  all 184 rows with zero `repo_tags` entries are forks whose GitHub
  `topics` array is empty. GitHub does not inherit topics onto forks. The
  stored `readme_summary` is a short one-liner, too thin for the keyword
  tagger. The nightly ingest does not re-tag them because `_upsert_repo` in
  `reporium-api` skips empty tag arrays to avoid accidental wipes.
- **reporium#251 "API: taxonomy_values table missing 'tags' + 'categories'
  dimensions"** — the aggregated `taxonomy_values` endpoints return empty
  for `tag` and `category` even though `repo_tags` and `repo_categories` have
  data. The issue recommends Option B (aggregate directly in the FastAPI
  handler), which is API-side work and **not in this lane**.

The issue#240 fix plan (from its own body) is exactly what this lane does,
and it is $0 — no AI spend, no Claude calls:

1. Fetch upstream topics via GitHub REST.
2. Fetch upstream full README via GitHub REST.
3. Run the deterministic keyword tagger against upstream text.
4. Add `MDX → [Docs]` to `LANGUAGE_TAGS` — **already done** in commit
   `f3ab7ae` (`fix(tagger): add MDX → [docs] in LANGUAGE_TAGS (#58)`).

## Scope in this lane

- **Owned**: `reporium-ingestion/scripts/backfill_no_tag_forks.py`,
  `reporium-ingestion/tests/test_backfill_no_tag_forks.py`,
  one-line CI addition in `.github/workflows/test.yml`, this audit directory.
- **Not touched**: `reporium-api`, `reporium` UI, any migration files, any
  existing ingestion pipeline code. No cross-repo edits.

## What the patch does

`scripts/backfill_no_tag_forks.py`:

- Finds rows in the DB where `is_fork = TRUE`, `forked_from IS NOT NULL`,
  and the `repo_tags` join returns zero rows.
- For each, hits `GET /repos/{upstream}/topics` and `GET /repos/{upstream}/readme`
  on GitHub REST.
- Runs `ingestion.enrichment.tagger.enrich_tags` with upstream topics + upstream
  README (same tagger the nightly ingest uses), then
  `ingestion.enrichment.taxonomy.assign_primary_category` /
  `assign_all_categories` on the resulting tags.
- Writes `repo_tags`, `repo_categories`, and `repo_taxonomy` (`tag` and
  `category` dimensions, `assigned_by = 'backfill_no_tag_forks'`).
- `--dry-run` runs end-to-end without any DB writes.
- `--limit N` caps the batch size.
- Final report prints the number of no-tag forks remaining so the operator
  can confirm the floor dropped.

## Why this is the right shape of change

- **Bounded**: one new script, one new test file, one CI line. No edits to
  the canonical ingestion pipeline. Easy to revert.
- **Matches precedent**: follows the same shape as
  `scripts/backfill_from_library_json.py` (direct psycopg2 writes) and
  `scripts/backfill_fork_dates.py` (GitHub REST + DB).
- **Idempotent**: once a fork picks up tags, the DELETE/INSERT path still
  works, and the candidate query stops surfacing it.
- **No AI cost**: uses deterministic keyword tagger only. Re-runnable.

## Why it is NOT merged in this lane

- Requires a live Cloud SQL connection and an authenticated GH token.
  Execution is an operator step, not a CI step.
- The `taxonomy_values` rebuild (reporium#251) is API-side and out of scope.
  The runbook documents how to kick it off post-backfill.

## Acceptance criteria (for reviewer)

- Tests pass: `pytest tests/test_backfill_no_tag_forks.py -v` (4 tests,
  no DB/network required).
- Existing tests still pass (`pytest tests/test_backfill.py
  tests/test_enrichment.py`).
- Dry run on a staging snapshot reports a non-zero `with_tags` count and
  zero errors for a sample of 10 repos.

## Operational plan (separate from code review)

See `.audit/2026-04-24/taxonomy-backfill-runbook.md` in this PR for the
step-by-step execution plan and validation checklist.

## Follow-ups (NOT in this lane)

- reporium#251 fix (API-side `/taxonomy/{dimension}` aggregation or explicit
  `taxonomy_values` rebuild for `tag` + `category` dims).
- Wire backfill into the nightly Cloud Run Job as an idempotent sweep once
  one manual run has proven the tag recovery rate.

# reporium-ingestion Operator Runbook

This is the top-level operator runbook for `reporium-ingestion`. Per-task
runbooks (e.g. taxonomy backfills, atomic graph rebuild) live alongside the
artifacts that ship them under `.audit/<date>/`. Cross-link those from here
when they land on `main`.

---

## Post-#67 backfill: reconcile `repos.primary_category` column

### Context

[reporium-ingestion#67](https://github.com/perditioinc/reporium-ingestion/pull/67)
closes the no-tag-fork gap by populating `repo_tags`, `repo_categories`, and
`repo_taxonomy`. It does **not** touch the `repos.primary_category` scalar
column. The Data Quality gate
(`/metrics/data-quality.primary_category_coverage`) reads that column, NOT
the `repo_categories.is_primary = true` junction — so the gate stays red even
after #67 finishes successfully and the next nightly enrichment run is green.

This is the column-vs-junction split documented in the memory entry
`project_dq_gate_column_vs_junction_bug.md`. The forward-fix in
[reporium-api PR #444](https://github.com/perditioinc/reporium-api/pull/444)
(merged `619325e`) keeps the column in sync going forward; the historical
drift accumulated before that fix is healed by
[reporium-api PR #445](https://github.com/perditioinc/reporium-api/pull/445)
(merged 2026-04-27), which adds a one-shot admin endpoint.

> **This step is required EVEN AFTER PR #67 merges and the next nightly
> enrichment runs successfully.** The column is a separate write target that
> the ingestion pipeline never wrote to; nothing on the ingestion side can
> close the gate.

### When to run

Run this once after either of the following:

1. PR #67 merges to `main` and the next nightly Cloud Run Job
   (`reporium-ingestion-nightly`) completes successfully, OR
2. Any backfill that newly populates `repo_categories` for repos whose
   `repos.primary_category` is `NULL`.

Once executed, the endpoint is idempotent — re-running it returns
`updated: 0` once the drift is healed.

### Step 1 — Dry run (no writes)

```bash
curl -X POST \
  "https://reporium-api-573778300586.us-central1.run.app/admin/backfill/primary_category_column?dry_run=true" \
  -H "X-Admin-Key: $ADMIN_KEY" \
  -H "X-API-Key: $REPORIUM_API_KEY"
```

Inspect `before.drift_rows` — that is the number of rows the live run will
update. Anything dramatically over the most recent gate-residual count
warrants investigation before going further.

### Step 2 — Live run

```bash
curl -X POST \
  "https://reporium-api-573778300586.us-central1.run.app/admin/backfill/primary_category_column" \
  -H "X-Admin-Key: $ADMIN_KEY" \
  -H "X-API-Key: $REPORIUM_API_KEY"
```

### Expected response shape

```json
{
  "dry_run": false,
  "updated": 4,
  "before": {
    "public_total": 1861,
    "public_with_primary_category": 1641,
    "drift_rows": 4,
    "coverage_pct": 88.1784
  },
  "after": {
    "public_total": 1861,
    "public_with_primary_category": 1645,
    "drift_rows": 0,
    "coverage_pct": 88.3934
  }
}
```

`updated` matches the rows promoted from junction → column. `after.drift_rows`
should be `0` on a healthy run; if it isn't, a junction integrity bug
(e.g. dual-primary rows for one repo) is the likely cause and needs
investigation before re-running.

### Step 3 — Verify the gate

After the live run, confirm the Data Quality gate sees the new coverage:

```bash
curl -s \
  "https://reporium-api-573778300586.us-central1.run.app/metrics/data-quality" \
  -H "X-API-Key: $REPORIUM_API_KEY" \
  | jq '.gates.primary_category_coverage'
```

Expected: `primary_category_coverage` ≥ 95% (gate target). If it is still
below 95% but `after.drift_rows: 0` from Step 2, the residual is genuine-empty
repos (no `is_primary = true` row in `repo_categories` at all, e.g. structurally
broken entries like `design.md`) — that's an ingestion-coverage problem, not a
column-sync problem, and the next action moves to #67 / nightly enrichment
follow-up rather than another column reconcile.

### Cross-links

- Memory entry: `project_dq_gate_column_vs_junction_bug.md` — full root-cause and
  decision history.
- API forward-fix: [reporium-api PR #444](https://github.com/perditioinc/reporium-api/pull/444).
- API backfill endpoint: [reporium-api PR #445](https://github.com/perditioinc/reporium-api/pull/445).
- Ingestion fork-tag backfill: [reporium-ingestion PR #67](https://github.com/perditioinc/reporium-ingestion/pull/67).

---

## Resumable ingestion runs (weekly timeout fix)

### Context

The weekly "Manual Ingestion Run" workflow executes the `reporium-enrichment`
Cloud Run Job (`python -m ingestion run`). Before this fix a single invocation
walked the ENTIRE corpus (~1900 repos) every run; the commit-stats refresh
alone sleeps 0.5s/repo, so the run could not finish inside the 3600s Cloud Run
task timeout and was KILLED. Newly-forked repos never reached the live DB, so
Reporium went stale (failed runs 2026-05-25 / 06-01 / 06-08 / 06-15; last clean
ingestion 2026-05-18).

### What changed

`ingestion/budget.py` makes each invocation process only a bounded, prioritised
slice of the PENDING set:

- Pending = repos with no durable cache row (brand-new forks) OR whose GitHub
  `updated_at` moved since the last run. Brand-new repos are processed FIRST so
  freshness recovers fast.
- `MAX_REPOS_PER_RUN` (default 400) caps how many repos a run fully processes.
- `RUN_TIME_BUDGET_SECONDS` (default 2700) stops the slow commit-stats phase
  before the task timeout (partial completion is safe; nothing is corrupted).
- The durable Postgres cache is the CHECKPOINT: repos processed on a prior run
  are skipped, so the next scheduled (or re-triggered) run RESUMES.
- A freshness SLO metric (`FRESHNESS_SLO_HOURS`, default 48) is logged every
  run so staleness is observable.

### Operator action

A run that does not drain the whole backlog finishes with status `partial` and
prints `Deferred to next run: N`. To catch up faster, simply re-trigger the
"Manual Ingestion Run" workflow (or wait for the weekly cron); each run drains
the next batch until `Deferred to next run: 0`. The production RUN itself is
operator-gated (`workflow_dispatch`) — this fix does not auto-run it.

Tuning: raise `MAX_REPOS_PER_RUN` once a run is observed to comfortably finish a
full batch in budget; set it to `0` to disable the cap (legacy whole-corpus
behaviour). `timeoutSeconds` in `deploy/job.yaml` was bumped 3600 -> 5400 as
defense-in-depth only; it is NOT the mechanism that keeps runs in budget.

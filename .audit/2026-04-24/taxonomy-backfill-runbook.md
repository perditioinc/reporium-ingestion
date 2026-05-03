# Runbook: Taxonomy backfill for no-tag forks

**Purpose.** Close the 184-fork tag gap tracked in
[perditioinc/reporium#240](https://github.com/perditioinc/reporium/issues/240)
by running `scripts/backfill_no_tag_forks.py` against production Cloud SQL.
Zero AI spend; deterministic tagger only.

This runbook assumes the script at `scripts/backfill_no_tag_forks.py` has
merged to `dev` and been pulled into whatever environment has Cloud SQL
reachability.

---

## Pre-flight

1. You have a checkout of `reporium-ingestion` with the merged backfill
   script.
2. You can reach Cloud SQL (either via the Auth Proxy or by running from
   a VM/Cloud Run Job with VPC access).
3. Secrets accessible (env vars or Secret Manager):
   - `DATABASE_URL` or Secret Manager `reporium-db-url`
   - `GH_TOKEN` or Secret Manager `gh-token` (or `gh auth token` working)
4. `pip install -r requirements.txt` complete in the execution env. The
   script only needs `psycopg2`, `urllib` (stdlib), and the repo's own
   `ingestion.enrichment` package.

## Step 1 — Snapshot the "before" floor

Record the current no-tag-fork count so the post-run diff is provable.

```sql
-- Run against prod Cloud SQL (read-only)
SELECT COUNT(*) AS no_tag_forks
FROM (
  SELECT r.id
  FROM repos r
  LEFT JOIN repo_tags rt ON rt.repo_id = r.id
  WHERE r.is_fork = TRUE AND r.forked_from IS NOT NULL
  GROUP BY r.id
  HAVING COUNT(rt.tag) = 0
) t;
```

**Expected**: ~184 per the 2026-04-19 audit. Any dramatic drift (say, <50
or >250) means someone else already ran a backfill or a bulk import
changed the corpus — investigate before continuing.

## Step 2 — Dry run

```bash
export DATABASE_URL='postgresql://.../reporium'
export GH_TOKEN="$(gh auth token)"

python scripts/backfill_no_tag_forks.py --dry-run --limit 10
```

**Check**:
- `Found N no-tag forks in DB` where N is close to Step 1's number.
- Per-line `DRY` output shows `tags=<n>` with `n > 0` for at least ~80%
  of the sampled 10.
- `upstream_topics` is non-zero for most rows (proves the GitHub fetch is
  working and the upstream topic inheritance theory holds).
- `readme_bytes` is non-trivial (>200) for most rows.
- No stack traces, no SQL execution.

If fewer than 5 of the 10 sampled repos pick up tags, STOP — upstream
REST access may be rate-limited, the token may be scoped wrong, or the
repos' upstream READMEs may be empty. Investigate before going wider.

## Step 3 — Bounded write

```bash
python scripts/backfill_no_tag_forks.py --limit 20
```

This writes 20 rows for real. Expected wall-clock ~30s (0.3s sleep + ~2
API calls per repo).

**Check**:
- Exit code 0.
- Summary line `No-tag forks remaining after run: ~164` (assuming 20 hit
  and got tags).
- Spot-check one row manually:
  ```sql
  SELECT r.name, COUNT(rt.tag) AS tag_count, COUNT(DISTINCT rc.category_id) AS cat_count
  FROM repos r
  LEFT JOIN repo_tags rt ON rt.repo_id = r.id
  LEFT JOIN repo_categories rc ON rc.repo_id = r.id
  WHERE r.name = '<one of the names logged by the script>'
  GROUP BY r.name;
  ```
  Expect `tag_count >= 2` and `cat_count >= 1`.

## Step 4 — Full run

```bash
python scripts/backfill_no_tag_forks.py
```

Expected wall-clock ~3-5 minutes for ~180 rows at 0.3s sleep.

**Check**:
- Summary totals: `processed` = remaining candidates, `errors` ideally 0
  (a handful of 404s on deleted/private upstreams is acceptable and gets
  logged as `ERR` per row).
- `No-tag forks remaining after run`: ideally < 10. Anything above 20
  warrants investigation (upstream repos with genuinely empty READMEs
  and no topics are the expected residual).

## Step 5 — Refresh the aggregated taxonomy surface

The backfill writes to `repo_tags`, `repo_categories`, and `repo_taxonomy`
directly. The `taxonomy_values` aggregate used by the `/taxonomy/*`
endpoints is rebuilt by the `rebuild_taxonomy` helper in `reporium-api`
(see `reporium#251` for the pending API-side fix).

Trigger a rebuild via the admin endpoint:

```bash
curl -X POST "$REPORIUM_API_URL/admin/taxonomy/rebuild" \
  -H "X-Admin-Key: $INGEST_API_KEY"
```

Or let the next nightly Pub/Sub `repo-ingested` event fire it naturally.
For the 184-row backfill specifically, triggering it explicitly gives
immediate UI feedback.

## Step 6 — Invalidate library cache

The `/library/full` cache must drop so the frontend reflects the new
tags:

```bash
# If there's an admin cache-bust endpoint:
curl -X POST "$REPORIUM_API_URL/admin/cache/invalidate" \
  -H "X-Admin-Key: $INGEST_API_KEY" \
  -d '{"keys":["library:full*","repos:list:*","stats:overview"]}'
```

Or just wait for the ingest pipeline's next run (it invalidates these
keys via `_upsert_repo`).

## Validation checklist

After Step 6, run through each of these:

- [ ] **Floor dropped below 5%** of the no-tag-fork corpus (< 10 rows
  out of 184). Criterion from reporium#240 acceptance.
- [ ] **No new errors in API logs** for taxonomy endpoints.
- [ ] **Library page renders tag chips** for a sample of 3 previously
  no-tag forks (pick 3 names from the script log, visit
  `/repos/<owner>/<name>` on reporium, confirm non-empty tag badges).
- [ ] **`/taxonomy/dimensions`** now lists `tag` and `category`
  dimensions with non-zero `repo_count` (assumes reporium#251 API fix
  is in place; if not, this item stays pending and is tracked by
  reporium#251 separately).
- [ ] **Nightly ingest still green** after the backfill — confirm the
  next Cloud Run Job execution for `reporium-ingestion-nightly`
  reports exit code 0 and does not re-flip these repos to zero tags.
- [ ] **Run-history entry** recorded in
  `/admin/runs` (backfill writes tags but does not create a run log;
  the operator may add a manual `manual_backfill` entry if desired).

## Rollback

Because the script replaces `repo_tags` / `repo_categories` rows for
matched repos only, rollback is per-repo:

```sql
-- Only the rows this script wrote are tagged with assigned_by='backfill_no_tag_forks'
DELETE FROM repo_taxonomy
WHERE assigned_by = 'backfill_no_tag_forks';

-- repo_tags / repo_categories don't carry an origin marker. To revert a
-- specific repo you'd need to repopulate from a pre-backfill snapshot.
-- Recommendation: snapshot `repo_tags` and `repo_categories` before
-- Step 3 if rollback is a realistic concern.
```

A full rollback after a few minutes of operation is cheap — the tags the
script wrote were deterministic, so re-running it produces the same
output. If a reviewer objects to specific tags, fix the tagger in
`ingestion/enrichment/tagger.py`, re-run the backfill, and the next pass
converges.

## Known residuals and follow-ups

- Forks whose upstream repos have been deleted or privatized will 404
  and stay no-tag. Acceptable.
- Forks whose upstream README is genuinely thin (<200 chars) may still
  not accumulate rich tags — meta-only tags (language + "Forked" + time
  bucket) will still land, so `tag_count >= 1` should be achieved.
- Wiring this script into the nightly Cloud Run Job as an idempotent
  sweep is the logical next step — **not in this lane**; track as a
  separate ticket.
- The reporium#251 API fix (`taxonomy_values` for `tag` + `category`
  dims) is separate and API-owned.

## Nightly Graph Build — operator checklist: from "wrapper failure" to real root cause

**Audience:** on-call / platform operator triaging a red `Nightly Graph Build` workflow run.
**Goal:** in under 5 minutes, get from `gcloud.run.jobs.execute` exit code 1 to the actual container error message.
**Scope of this checklist:** triage only. Fix steps for the two specific known failure modes are at the bottom.

### What the workflow already does for you (after PR #66, `4c5f2f3` on `main`)

On the failure path the workflow runs three diagnostic blocks before giving up:

1. `gcloud run jobs executions list --limit=3` — shows last three execution names + completion conditions.
2. `gcloud run jobs executions describe <latest>` — full execution YAML (image SHA, env wiring, SA, Cloud SQL annotation, status conditions). Always works under the current CI SA.
3. `gcloud logging read ...` — the container's `textPayload` lines. **Currently fails with `PERMISSION_DENIED`** because `reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com` lacks `roles/logging.viewer`. Until that grant lands, fall back to step 2 below.

### Step 1 — read the GitHub Actions log first

```sh
# Replace 24927546067 with the failing run id (most recent failure: 2026-04-25 09:14 UTC, exec reporium-graph-build-2jskf)
gh run view <run-id> -R perditioinc/reporium-ingestion --log-failed | less
# Note: --log-failed currently only returns the wrapper step. To see the diagnostics step's
# `PERMISSION_DENIED` line and the execution YAML, use the full log instead:
gh run view <run-id> -R perditioinc/reporium-ingestion --log | less
```

What to look for:

- **`Cloud Run Job completed successfully`** — workflow body succeeded; the failure is downstream (snapshot rebuild HTTP call or graph endpoint verification). Skip to step 4.
- **`ERROR: (gcloud.run.jobs.execute) The execution failed.`** — the Cloud Run Job itself failed; continue to step 2.
- **`PERMISSION_DENIED` on `gcloud logging read`** — expected today; do step 2 by hand.

### Step 2 — read the container logs out-of-band

The execution name is in the workflow log under `Recent reporium-graph-build executions` and again under `Execution detail: <name>`. Take the latest, e.g. `reporium-graph-build-2jskf` (2026-04-25 09:14 UTC) or `reporium-graph-build-s5bkz` (2026-04-24 09:58 UTC), then run from a workstation authenticated as a human user (not the CI SA):

```sh
gcloud logging read \
  'resource.type=cloud_run_job
   AND resource.labels.job_name=reporium-graph-build
   AND labels."run.googleapis.com/execution_name"=reporium-graph-build-2jskf' \
  --project=perditio-platform \
  --limit=200 \
  --format='value(timestamp,severity,textPayload)' \
  --order=asc
```

Or open the Cloud Console URL the workflow YAML prints in the `logUri` field:
`https://console.cloud.google.com/run/jobs/executions/details/us-central1/<execution-name>/logs?project=perditio-platform`

Search the log for the first ERROR line — that's the real root cause.

### Step 3 — classify the container error

| Symptom in the container log | Almost certainly | Owner |
| --- | --- | --- |
| `psycopg2.OperationalError: ... password authentication failed for user "postgres"` | Stale `reporium-db-url` secret value (post-password-rotation drift). | Ops / Secret Manager. See "Fix A" below. |
| `psycopg2.OperationalError: ... could not connect to server: Connection refused` on `/cloudsql/.../.s.PGSQL.5432` | Cloud SQL annotation missing or sidecar not provisioned. | Ops / Cloud Run Job spec. |
| `psycopg2.OperationalError: ... timeout` | Cloud SQL instance overloaded or VPC egress misconfigured. | Ops / Cloud SQL + VPC. |
| Python traceback rooted in `scripts/build_knowledge_graph.py` (not psycopg2) | Application bug — graph builder regression. | Ingestion app lane. File a KAN. |
| OOMKilled, exit 137 | Resource limits insufficient (current: 1 CPU / 512Mi). | Ops / Cloud Run Job spec. |

### Step 4 — if Cloud Run Job succeeded but workflow still red

The follow-on steps in `nightly_graph_build.yml` call `reporium-api`:

- `POST /admin/graph/rebuild-snapshot` (with `X-Admin-Key`)
- `GET /graph/edges?limit=100`

Failures here mean the graph rebuilt, but the API surface is broken. That's `reporium-api` lane, not this one. Hand off with the HTTP code + response body that the workflow already echoed.

### Fix A — rotate `reporium-db-url` (today's known root cause)

```sh
# 1. Read the working DSN from the sibling secret used by enrichment:
gcloud secrets versions access latest \
  --secret=reporium-db-url-async --project=perditio-platform

# 2. Add it as a new version on the broken secret (job consumes :latest):
printf '%s' '<DSN-from-step-1>' | \
  gcloud secrets versions add reporium-db-url \
    --data-file=- --project=perditio-platform

# 3. Smoke test before tomorrow's 08:30 UTC scheduled run:
gcloud run jobs execute reporium-graph-build \
  --project=perditio-platform --region=us-central1 --wait
```

If the smoke test goes green, also re-run the failed scheduled GitHub run with `gh run rerun <run-id>` to clear the red.

### Fix B — grant log-read to CI (one-time; unblocks Step 1 going forward)

```sh
gcloud projects add-iam-policy-binding perditio-platform \
  --member='serviceAccount:reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com' \
  --role='roles/logging.viewer'
```

Verify on next failure that `gcloud logging read` no longer prints `PERMISSION_DENIED` in the workflow log. No code change required — PR #66 already runs the read.

### What NOT to do

- Do **not** add `|| true` everywhere in the workflow to make the red disappear — that loses signal.
- Do **not** widen `permissions:` in the workflow file thinking it affects GCP IAM — `permissions:` only controls the GitHub-issued `GITHUB_TOKEN`, not the federated GCP SA.
- Do **not** rotate or delete the GCP SA key for `reporium-ingestion-ci@` while debugging — its `credentials_json` lives in the `GCP_SA_KEY` GitHub secret and is consumed at the start of the workflow.
- Do **not** patch `ingest_run_manager.py` from this lane to "handle" auth failures — the right durable fix is migrating off psycopg2/password to Cloud SQL Auth Proxy + IAM auth. Separate ticket.

### Reference

- Workflow file (deployed): `.github/workflows/nightly_graph_build.yml` on `main` (note: file is **not present on `dev`** — inverted GitFlow).
- Diagnostics PR: https://github.com/perditioinc/reporium-ingestion/pull/66 (`4c5f2f3`).
- Today's RCA: [`.audit/2026-04-25/graph-build-root-cause-jira.md`](graph-build-root-cause-jira.md).
- Prior PM RCA: [`.audit/2026-04-24/graph-build-root-cause-jira.md`](../2026-04-24/graph-build-root-cause-jira.md).

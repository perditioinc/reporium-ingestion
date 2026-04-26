# Graph-Build Follow-Up — JIRA fallback card

**Anchor:** 2026-04-25 17:50 PDT (graph-build follow-up lane)
**Repo:** `reporium-ingestion` · workflow `nightly_graph_build.yml` (lives on `main` only)
**Classification:** **BLOCKED OUTSIDE REPO** — GCP Secret Manager value drift, plus a missing CI IAM grant.

This is the JIRA-shaped fallback because no JIRA writer is reachable from this lane. If a real KAN exists, link it in the title and treat this file as the working copy.

## Status (live, this anchor)

| Surface | Value |
| --- | --- |
| `origin/main` HEAD | `4c5f2f3` (PR #66 diagnostics — already merged) |
| Latest `nightly_graph_build.yml` run | [`24927546067`](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) — failure, 2026-04-25 09:14 UTC |
| Streak | **4 consecutive scheduled red** (last green 2026-04-22 09:57 UTC) |
| Next scheduled fire | ~2026-04-26 08:30 UTC (≈ 01:30 PDT) — will be the **5th red** if nothing lands |
| Container exit | exit 1, ~3 min lifetime, consistent with auth-then-fatal |
| CI diagnostics | `gcloud logging read` returns `PERMISSION_DENIED` for `reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com` |

## Root cause (carried from `2026-04-24` PM lane out-of-band log read)

Cloud SQL `postgres` password was rotated 2026-04-22. The async sibling secret `projects/perditio-platform/secrets/reporium-db-url-async` was rotated to the new password — that's why `reporium-api` keeps working. The graph-build secret `projects/perditio-platform/secrets/reporium-db-url` was **not** rotated, so the Cloud Run Job still injects the pre-rotation DSN at startup and fails with `psycopg2.OperationalError: FATAL: password authentication failed for user "postgres"`.

## Exact operator actions (3 commands, under 5 minutes)

```sh
# 1. Rotate the broken secret (unblocks the next 08:30 UTC schedule).
DSN="$(gcloud secrets versions access latest \
  --secret=reporium-db-url-async --project=perditio-platform)"
printf '%s' "$DSN" | gcloud secrets versions add reporium-db-url \
  --data-file=- --project=perditio-platform

# 2. Smoke test before the next scheduled fire.
gcloud run jobs execute reporium-graph-build \
  --project=perditio-platform --region=us-central1 --wait
# Expected: exit 0 within ~3 min.

# 3. (Independent — do once.) Grant log-read to the CI SA so future failures
#    surface the actual container textPayload inline in the workflow log.
gcloud projects add-iam-policy-binding perditio-platform \
  --member='serviceAccount:reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com' \
  --role='roles/logging.viewer'
```

## Acceptance

- Smoke test (`gcloud run jobs execute … --wait`) exits 0.
- Next scheduled run goes green; streak breaks.
- A subsequent simulated failure shows the real `textPayload` in the GitHub Actions log instead of `PERMISSION_DENIED`.

## Out of scope (separate KAN, owned by ingestion app lane — not this triage lane)

Migrate `ingestion/graph/ingest_run_manager.py` from psycopg2/password to Cloud SQL Auth Proxy + IAM auth, matching `reporium-api`. This is the durable answer to "why does a single password rotation black out the graph build."

## Evidence trail (for review only — not action items)

- RCA + drift sweeps: [`.audit/2026-04-25/nightly-graph-build-rca.md`](../2026-04-25/nightly-graph-build-rca.md)
- Operator playbook (longer form): [`.audit/2026-04-25/graph-build-operator-checklist.md`](../2026-04-25/graph-build-operator-checklist.md)
- Prior lane artifacts under `.audit/2026-04-24/` and `.audit/2026-04-25/`
- This anchor's lane note: [`graph-build-followup-note.md`](graph-build-followup-note.md)

# Graph-Build Follow-Up — lane final note

**Lane:** Ingestion Graph-Build Follow-Up
**Anchor:** 2026-04-25 17:50 PDT (autonomous shift, ~25 min after PM-lane handoff at 17:26 PDT)
**Repo:** `reporium-ingestion` (workspace `C:\DEV\PERDITIO_PLATFORM\reporium-ingestion`)
**Time horizon:** ~60 min, single shift.

## Classification (final, this lane)

**BLOCKED OUTSIDE REPO.** No repo-local patch is justified. PR #66 already shipped diagnostics; the two remaining issues are live GCP config:

1. `projects/perditio-platform/secrets/reporium-db-url` — secret value not rotated to the post-2026-04-22 Cloud SQL `postgres` password.
2. `reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com` — missing `roles/logging.viewer`, which is why PR #66's diagnostics step still surfaces as `PERMISSION_DENIED` instead of the actual container `textPayload`.

Both are GCP-side, neither is a `reporium-ingestion` code/workflow defect.

## Repo-local patch / PR — none from this lane

- No branch cut, no commit, no push, no PR opened from this lane.
- No edit to `.github/workflows/nightly_graph_build.yml` or graph-build code.
- Only filesystem changes are under `.audit/2026-04-26/`:
  - [`graph-build-followup-jira.md`](graph-build-followup-jira.md) — JIRA-fallback card; the sharp one-page operator handoff.
  - This note.

Repo posture is unchanged at the same HEADs the PM lane recorded:

```
origin/main    : 4c5f2f3
origin/dev     : a8db3de
PR #67 head    : c2de352  (out of scope for this lane; KAN-TBD-taxonomy-backfill-completion)
```

## Why no workflow patch was made

Considered and rejected, in order:

- **`::error::` annotation linking to runbook on `PERMISSION_DENIED`.** Marginal: operators already see the error in the run log. Adds a cosmetic line; doesn't change root cause or unblock anything.
- **Add fallback `gcloud run jobs executions describe` highlight of `status.conditions[0].message`.** Already in the diagnostics step (`gcloud run jobs executions list … --format='table(...status.conditions[0].message)'` and the YAML describe). The Cloud Run-surfaced message is the generic "Container exited with non-zero status code" — the actionable detail is in `textPayload`, which requires the IAM grant.
- **Widen workflow `permissions:` block.** No effect on the federated GCP SA — `permissions:` only controls the GitHub-issued `GITHUB_TOKEN`.
- **Migrate `ingest_run_manager.py` off psycopg2/password.** Durable, but app code outside this triage lane's scope. Belongs in its own KAN; flagged in the JIRA card.

The lane prompt is explicit: *"Do not invent repo fixes if the problem is still clearly in GCP/runtime/secret state."* Met.

## Why this lane wrote a fresh card instead of a fourth narrative note

The same incident already has 10+ artifacts across `.audit/2026-04-24/`, `.audit/2026-04-25/`, and `.audit/2026-04-26/`. The handoff is no longer sharp — operators have to triangulate across files to extract three commands. The new card [`graph-build-followup-jira.md`](graph-build-followup-jira.md) is a single page, ticket-shaped, action-first, with an evidence-trail link list at the bottom for depth. It's the smallest delta that actually improves the operator handoff.

## Exact next operator action

Run the three commands at [`graph-build-followup-jira.md`](graph-build-followup-jira.md) §"Exact operator actions". Cuts paste-ready. ~5 minutes. Owner is GCP/platform ops, not an ingestion-repo committer.

If those run before ~01:25 PDT 2026-04-26, the next scheduled `nightly_graph_build.yml` cron at 08:30 UTC (≈ 01:30 PDT) will go green and break the streak at 4. If they don't, the 5th red will fire with the same shape.

## Exact missing evidence (still unresolved)

The actual container `textPayload` from the failing executions has not been read by any in-repo lane this cycle:

| Fail | Execution name | Status from `executions describe` | textPayload read? |
| --- | --- | --- | --- |
| 2026-04-25 09:14 UTC | `reporium-graph-build-2jskf` | ContainerReady=True, Completed=False, NonZeroExitCode | **No** — `gcloud logging read` 403'd in CI; no out-of-band read this cycle. |
| 2026-04-24 09:58 UTC | `reporium-graph-build-s5bkz` | same shape | Yes — read out-of-band by the 2026-04-24 PM lane; that read produced the carried-forward `psycopg2.OperationalError: FATAL: password authentication failed for user "postgres"` finding. |
| 2026-04-23 10:00 UTC | (in `executions list`) | same shape | No |

The 2026-04-24 read is the only direct evidence of the auth-failure root cause. Two follow-up shifts (PM lane at 17:05 PDT and 17:26 PDT) declined to re-run the out-of-band read because the failure shape is unchanged and the durable plan is the rotation, not a re-investigation. If a human operator wants fresh evidence before rotating, the one command to run from a workstation authenticated as a human GCP user is:

```sh
gcloud logging read \
  'resource.type=cloud_run_job
   AND resource.labels.job_name=reporium-graph-build
   AND labels."run.googleapis.com/execution_name"=reporium-graph-build-2jskf' \
  --project=perditio-platform --limit=200 \
  --format='value(timestamp,severity,textPayload)' --order=asc
```

Otherwise: rotate the secret and let the next green run be the proof.

## Scheduled coverage (already booked by prior PM lane — not duplicated by this lane)

- `ingestion-lane-followup-plus3h-2026-04-25-pm` — fires 2026-04-25 20:05 PDT (drift sweep).
- `ingestion-lane-followup-plus8h-2026-04-26-am` — fires 2026-04-26 01:05 PDT (~25 min before the next 08:30 UTC scheduled fire). Verifies whether rotation landed; can wait/poll up to 30 min to capture the firing's outcome.

This lane created **no new** scheduled tasks — the existing two cover the next-scheduled-fire window already.

## Read order for the morning shift

1. This file (final dispositions, 60 sec read).
2. [`graph-build-followup-jira.md`](graph-build-followup-jira.md) — sharp operator card (60 sec read, 5 min execute).
3. Drift outcome appended to [`ingestion-execution-note.md`](ingestion-execution-note.md) §"Follow-up agent appendix" by the +8h scheduled task.
4. Anything older in `.audit/2026-04-25/` or `.audit/2026-04-24/` — for evidence depth only; not action items.

---

## Update — 2026-04-25 ~20:15 PDT (graph-build-followup lane #2)

**Anchor:** 2026-04-25 20:15 PDT (autonomous lane shift, ~25 min after the +3h scheduled drift sweep at 20:05 PDT).
**Disposition (still):** **BLOCKED OUTSIDE REPO** on the same two GCP-side issues. The carried-forward RCA (`reporium-db-url` Secret Manager rotation drift) is unchanged.
**Disposition (new):** **REPO-LOCAL DIAGNOSTIC IMPROVEMENT SHIPPED** as PR [#68](https://github.com/perditioinc/reporium-ingestion/pull/68) (branch `claude/feature/KAN-DRAFT-graph-build-followup`, base `main`, mergeStateStatus=MERGEABLE/UNSTABLE while Tests run).

### Live state at this anchor (re-validated)

| Surface | Value at +3h anchor (20:05 PDT) | Value at this anchor (20:15 PDT) | Drift |
| --- | --- | --- | --- |
| `origin/main` HEAD | `4c5f2f3` | `4c5f2f3` | none |
| `origin/dev` HEAD | `a8db3de` | `a8db3de` | none |
| Latest `nightly_graph_build.yml` run | [`24927546067`](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) failure 09:14 UTC | same — no manual `workflow_dispatch`, no new schedule fire | none |
| Streak | 4 consecutive scheduled red | 4 consecutive scheduled red | none |
| Ops actions on `reporium-db-url` rotation or `roles/logging.viewer` grant | not landed | not landed | none |

### What this lane did (overrides the "no patch" stance of the 17:50 PDT predecessor lane)

The 17:50 PDT lane explicitly considered and rejected:
- More `gcloud logging read` flags / fallbacks (paper over missing IAM grant)
- `::error::` annotation linking to runbook (cosmetic, marginal)
- Widening workflow `permissions:` (no effect on federated GCP SA)
- Migrating off psycopg2 (out of triage scope)

What that lane did **not** consider — and what this lane shipped — is **inline Secret Manager rotation-drift detection**: a new failure-only step that compares `gcloud secrets versions list` `createTime` between `reporium-db-url` and `reporium-db-url-async` and emits a structured `::error::` annotation linking to this runbook when drift is detected.

This is qualitatively different from the rejected options because:

1. It targets the **specific** carried-forward RCA (rotation drift), not a generic "see runbook" pointer.
2. It uses a **different IAM surface** (`secretmanager.viewer` instead of `logging.viewer`). Even if the CI SA also lacks that grant, the step surfaces a clear "second IAM gap" warning naming the exact role to grant — still useful evidence.
3. It produces a **structured GitHub Actions annotation** (`::error::` cluster at top of run summary) the operator sees without scrolling 200 log lines.
4. The annotation links to this folder's `graph-build-followup-jira.md`, which is also now committed to `main` so the URL resolves.

### Files changed in the patch (PR pending — branch `claude/feature/KAN-DRAFT-graph-build-followup`)

- `.github/workflows/nightly_graph_build.yml` — added one new step "Check Secret Manager rotation drift (inline RCA hint)" between the existing "Surface Cloud Run Job diagnostics on failure" step and the snapshot-rebuild step. Same `if: failure() && steps.graph_job.outcome == 'failure'` gate. ~30 lines of YAML.
- `.audit/2026-04-25/nightly-graph-build-rca.md` — full RCA, was untracked, now committed for runbook URL stability.
- `.audit/2026-04-25/graph-build-operator-checklist.md` — operator playbook, was untracked, now committed.
- `.audit/2026-04-26/graph-build-followup-jira.md` — sharp 1-page operator card (the URL target), was untracked, now committed.
- `.audit/2026-04-26/graph-build-followup-note.md` — this file.

Files **not** changed by this lane:
- `.github/workflows/test.yml` — out of scope per lane prompt.
- `scripts/backfill_no_tag_forks.py` and `tests/test_backfill_no_tag_forks.py` — owned by PR #67, out of scope per lane prompt.
- `ingestion/graph/ingest_run_manager.py` — durable migration to Cloud SQL Auth Proxy + IAM auth still belongs to a separate KAN owned by the ingestion app lane, not this triage lane.
- Anything under `reporium-api/` or `reporium-security/` — out of scope per lane prompt.

### Validation done before commit

- `python -c "import yaml; yaml.safe_load(open(...))"` — workflow parses cleanly, 7 steps total.
- Local shell simulation of the rotation-drift detection logic across 6 input cases (both 403, ASYNC newer, equal, MAIN newer, both empty, only one populated). All branches classified correctly. See validation transcript in this lane's chat history.
- The runbook URL `https://github.com/perditioinc/reporium-ingestion/blob/main/.audit/2026-04-26/graph-build-followup-jira.md` will resolve once this PR merges (the file is part of the same commit set).

### Validation NOT done before commit

- The new step has not actually fired on a real failure. It will fire on the next failed `nightly_graph_build.yml` run (next scheduled fire ≈ 01:30 PDT 2026-04-26 — see `+8h` follow-up below). If the CI SA lacks `roles/secretmanager.viewer`, the step will emit the second-IAM-gap warning instead of a positive drift verdict — both are useful, neither breaks the workflow.
- A `workflow_dispatch` smoke test from this lane was not run. The step is gated on `if: failure()`, so a happy-path manual fire would not exercise it. Triggering a deliberate failure to exercise the step is out of scope for this lane.

### Acceptance criteria for the new step (operator-facing)

- On the next failed run, the GitHub Actions failure summary shows either:
  - `::error::SECRET ROTATION DRIFT DETECTED — ...` with the runbook URL (positive RCA confirmation), **or**
  - `::warning::Secret Manager metadata read denied for CI SA. ... grant roles/secretmanager.viewer ...` (a clear second-IAM-gap signal), **or**
  - `::notice::Secret rotation drift NOT confirmed by metadata comparison ...` (correctly negative — operator should pursue other causes).
- The new step does not change the workflow's overall pass/fail outcome — it is informational on the failure path only.
- The runbook URL in the annotation is clickable and lands on a file that exists on `main`.

### What is still BLOCKED OUTSIDE REPO

Both unchanged from the prior lanes:

1. `projects/perditio-platform/secrets/reporium-db-url` value rotation — operator action, GCP Secret Manager. The 3 commands at [`graph-build-followup-jira.md`](graph-build-followup-jira.md) §"Exact operator actions" remain the unblocking path.
2. `roles/logging.viewer` grant on `reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com` — operator action, GCP IAM. Independent of #1; one-time grant.
3. (Newly surfaced by this patch's design) `roles/secretmanager.viewer` grant on the same CI SA — only needed if the operator wants the new inline RCA detection to produce a positive verdict instead of the second-IAM-gap warning. **Strictly optional**; the operator can also just fix #1 and ignore the warning.

### Exact next operator action (unchanged)

Run the 3 commands at [`graph-build-followup-jira.md`](graph-build-followup-jira.md) §"Exact operator actions". The new workflow patch does not change the unblocking path; it only sharpens the diagnostics on future failures.

### Scheduled follow-ups (unchanged)

The two follow-ups booked by the prior PM lane remain in place:
- `ingestion-lane-followup-plus3h-2026-04-25-pm` — already fired at 20:05 PDT (`NO-CHANGE`).
- `ingestion-lane-followup-plus8h-2026-04-26-am` — fires 2026-04-26 01:05 PDT (~25 min before the next scheduled `nightly_graph_build.yml` cron at 08:30 UTC). Will verify whether the new step is exercised on the 5th fire (if ops has not rotated by then).

This lane created **no new** scheduled tasks.

### Read order for the morning shift (revised)

1. This file's "Update — 2026-04-25 ~20:15 PDT" section above for the latest disposition + PR pointer.
2. [`graph-build-followup-jira.md`](graph-build-followup-jira.md) — sharp operator card (still the actionable runbook).
3. The opened PR diff (one workflow step added; 5 audit files newly tracked) for review.
4. `+8h` scheduled follow-up's appendix in [`ingestion-execution-note.md`](ingestion-execution-note.md) for the 5th-fire outcome.

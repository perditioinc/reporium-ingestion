# Nightly Graph Build — RCA & ops handoff (2026-04-25)

**Lane:** Ingestion lane (`reporium-ingestion`)
**Re-verified:** 2026-04-25 04:35 PDT (this lane)
**Disposition:** **BLOCKED OUTSIDE REPO.** No in-repo workflow patch is justified.
**Streak:** 4 consecutive scheduled red runs (last green: 2026-04-22 09:57 UTC).

This file consolidates and re-verifies the prior lanes' RCA. Earlier artifacts on the same incident (read first if context is missing):

- [`.audit/2026-04-24/graph-build-root-cause-jira.md`](../2026-04-24/graph-build-root-cause-jira.md) — primary RCA
- [`.audit/2026-04-24/nightly-graph-build-root-cause.md`](../2026-04-24/nightly-graph-build-root-cause.md) — evidence dossier
- [`.audit/2026-04-24/graph-build-operator-checklist.md`](../2026-04-24/graph-build-operator-checklist.md) — first-cut operator playbook
- [`.audit/2026-04-25/graph-build-root-cause-jira.md`](graph-build-root-cause-jira.md) — 02:18 PDT JIRA draft (this incident)
- [`.audit/2026-04-25/graph-build-operator-checklist.md`](graph-build-operator-checklist.md) — operator commands
- [`.audit/2026-04-25/ingestion-triage-gate-jira.md`](ingestion-triage-gate-jira.md) — 03:07 PDT triage gate

## Run history

| Date (UTC) | Run id | Conclusion | Notes |
| --- | --- | --- | --- |
| 2026-04-25 09:14 | [24927546067](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) | ❌ failure | Container exit 1, execution `reporium-graph-build-2jskf`. Diagnostics step ran; `gcloud logging read` 403'd on CI SA (no log-viewer binding). |
| 2026-04-24 09:58 | [24883677740](https://github.com/perditioinc/reporium-ingestion/actions/runs/24883677740) | ❌ failure | Same shape, execution `reporium-graph-build-s5bkz`. |
| 2026-04-23 10:00 | [24829092448](https://github.com/perditioinc/reporium-ingestion/actions/runs/24829092448) | ❌ failure | First fail of the streak. |
| 2026-04-22 09:57 | [24772115281](https://github.com/perditioinc/reporium-ingestion/actions/runs/24772115281) | ✅ success | Last green. |
| 2026-04-21 09:56 | [24716058615](https://github.com/perditioinc/reporium-ingestion/actions/runs/24716058615) | ✅ success | |

## What is constant across all 4 failures (from `gcloud run jobs executions describe`)

```
image                : us-central1-docker.pkg.dev/perditio-platform/cloud-run-source-deploy/
                       reporium-graph-build@sha256:c243472a8b7841ef6cd47d0a6a1e7a247368eafd83d6abfb7bd4f750bb6a0b4c
serviceAccountName   : reporium-api@perditio-platform.iam.gserviceaccount.com
DATABASE_URL         : secretKeyRef name=reporium-db-url, key=latest
cloudsql-instances   : perditio-platform:us-central1:reporium-db
vpc-access-egress    : private-ranges-only
maxRetries           : 0
status               : ResourcesAvailable=True, ContainerReady=True, Started=True,
                       Completed=False (NonZeroExitCode, "The container exited with an error")
```

No image rebuild, no env change, no SA change, no Secret Manager rotation since 2026-04-22. The wiring is identical between the last green and the first red.

## Root cause (carried forward; not re-verified by this lane)

**Primary container error (PM lane out-of-band log read, 2026-04-24):**

```
psycopg2.OperationalError: FATAL: password authentication failed for user "postgres"
```

The Cloud SQL `postgres` user password was rotated 2026-04-22 (per `reporium-platform/.audit/2026-04-22/password-rotation-runbook.md`). The sibling secret `projects/perditio-platform/secrets/reporium-db-url-async` was rotated to the new password — that's why `reporium-api` and the async path keep working. **The graph-build secret `projects/perditio-platform/secrets/reporium-db-url` was not rotated**, so the Cloud Run Job still injects the pre-rotation DSN at startup.

This lane could not re-verify the container error string from CI — the diagnostics step in PR #66 (`4c5f2f3`) wires `gcloud logging read … textPayload`, but the CI SA `reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com` lacks `roles/logging.viewer` on `perditio-platform`, so the read returns `PERMISSION_DENIED`. The PM finding is accepted as current because nothing about the secret, image, or environment has changed since.

## Why no in-repo patch from this lane

Prior lanes already explored and rejected every in-repo change:

- More `gcloud logging read` flags or fallbacks would just paper over the missing IAM grant.
- Widening the workflow's `permissions:` block has no effect on the GCP federated SA.
- Migrating `ingestion/graph/ingest_run_manager.py` off psycopg2 to Cloud SQL Auth Proxy + IAM auth (matching the path `reporium-api` already took) is the durable fix, but it's application code outside this lane's scope and belongs in its own KAN.
- The workflow `nightly_graph_build.yml` lives on `main` only (inverted GitFlow); it is absent from `dev`. PR #66 already shipped diagnostics. There is no smaller patch to write.

## Smallest next actions (operator handoff — GCP-side, in order)

These are reproduced from `.audit/2026-04-24/graph-build-operator-checklist.md` and `.audit/2026-04-25/graph-build-operator-checklist.md`. Each step is one command and verifiable on its own. None is a `reporium-ingestion` repo change.

### 1. Rotate the secret (unblocks tomorrow's 08:30 UTC scheduled run)

```sh
# Read the current good password from the working sibling secret:
gcloud secrets versions access latest \
  --secret=reporium-db-url-async --project=perditio-platform

# Write the same DSN as a new version on the broken secret:
printf '%s' '<DSN-from-above>' | gcloud secrets versions add reporium-db-url \
  --data-file=- --project=perditio-platform

# Smoke test the job manually before the next scheduled fire:
gcloud run jobs execute reporium-graph-build \
  --project=perditio-platform --region=us-central1 --wait
```

Expected: smoke test exits 0; next scheduled fire (~2026-04-26 08:30 UTC) goes green.

### 2. Grant `roles/logging.viewer` to the CI SA (so future failures surface inline)

```sh
gcloud projects add-iam-policy-binding perditio-platform \
  --member='serviceAccount:reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com' \
  --role='roles/logging.viewer'
```

No workflow change required — PR #66's diagnostics step (`gcloud logging read … textPayload`) starts working on the next failure (whenever that is).

### 3. File a new KAN ticket for the durable migration

Migrate `ingestion/graph/ingest_run_manager.py` from psycopg2/password to Cloud SQL Auth Proxy + IAM auth, matching `reporium-api`. Owner: ingestion app lane (not this triage lane). This is the durable answer to "why does a single password rotation black out the graph build."

## Acceptance criteria (when ops actions land)

- `gcloud run jobs execute reporium-graph-build --wait` returns exit 0 from a fresh manual smoke.
- The next scheduled run (cron `30 8 * * *` UTC ≈ 08:30 UTC) is green.
- A subsequent simulated failure surfaces the container's `textPayload` lines in the GitHub Actions log instead of `PERMISSION_DENIED`.

## Stop condition

Lane contract: *"If the blocker is live GCP/IAM/runtime, classify it that way and stop. Do not invent repo changes to compensate for missing operational access."* Both remaining issues — Secret Manager rotation and Cloud Logging read access — are live GCP config. **No branch merge, no PR, no workflow patch from this lane.**

## Re-check schedule

This lane has scheduled itself to re-check at:

- **+2h (≈06:35 PDT)** — has a new commit / PR / workflow run / Secret Manager activity appeared?
- **+6h (≈10:35 PDT)** — same checks; if a new run hasn't fired, no action.
- **+9h (≈13:35 PDT)** — final pre-handoff sweep before the next 08:30 UTC scheduled fire (~2026-04-26 01:30 PDT).

If any check shows ops actions landed, this RCA is updated and the [`ingestion-lane-status.md`](ingestion-lane-status.md) status flips. See [`ingestion-scheduled-followups.md`](ingestion-scheduled-followups.md) for the handoff version of the same schedule.

## Drift sweep — T+12.5h (2026-04-25 17:05 PDT)

**Decision: `NO-CHANGE`.** Same blocker, no ops actions landed, no new evidence.

Re-verified live state (this lane, autonomous shift starting 17:05 PDT):

| Surface | Value at T+0 (04:35 PDT) | Value at T+12.5h (17:05 PDT) | Drift |
| --- | --- | --- | --- |
| `origin/main` HEAD | `4c5f2f3` | `4c5f2f3` | none |
| `origin/dev` HEAD | `a8db3de` | `a8db3de` | none |
| PR #67 HEAD | `c2de352` | `c2de352` | none |
| PR #67 state | draft, mergeStateStatus=CLEAN, Tests SUCCESS | draft, mergeStateStatus=CLEAN, Tests SUCCESS | none |
| PR #67 last update | 2026-04-25 11:41 UTC (this lane's hygiene comment) | 2026-04-25 11:41 UTC | none |
| Latest `nightly_graph_build.yml` run | [`24927546067`](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) failure (09:14 UTC) | same run, no manual `workflow_dispatch` since | none |
| Streak | 4 consecutive scheduled red | 4 consecutive scheduled red (next fire ~01:30 PDT 2026-04-26 will be 5th if unblocked) | none |
| `gh run view 24927546067 --log` diagnostics step | container exit 1; `gcloud logging read` → `PERMISSION_DENIED` for `reporium-ingestion-ci@…` | re-pulled this shift; identical (`ERROR: (gcloud.logging.read) PERMISSION_DENIED: Permission denied for all log views. This command is authenticated as reporium-ingestion-ci@perditio-platform.iam.gserviceaccount.com`) | none |

Per the anti-pattern rule in [`ingestion-scheduled-followups.md`](ingestion-scheduled-followups.md) §"Anti-pattern: do not re-investigate the same root cause": failure shape is unchanged (container exit 1 inside ~3 minutes of starting, consistent with auth-then-fatal); root cause assignment from the morning lane stands; the operator handoff in §"Smallest next actions" remains the only path forward.

## Pre-fire window (next scheduled fire ≈ 01:30 PDT 2026-04-26, 8.4h from this T+12.5h sweep)

If ops has not rotated `projects/perditio-platform/secrets/reporium-db-url` by ~01:25 PDT 2026-04-26, the 5th scheduled fire will fail with the same shape. The +8h follow-up (scheduled below) is timed to land at ~01:05 PDT (≈25 minutes pre-fire) so it can confirm whether the rotation has happened in time.

### Scheduled follow-ups for the next window (this lane, T+12.5h)

| Slot | Local fire | UTC fire | Task id | Purpose |
| --- | --- | --- | --- | --- |
| **+3h** | 2026-04-25 20:05 PDT | 2026-04-26 03:05 UTC | `ingestion-lane-followup-plus3h-2026-04-25-pm` | Mid-evening drift sweep. Catches any out-of-band Secret Manager rotation, manual `workflow_dispatch` smoke test, or new PR #67 movement. |
| **+8h** | 2026-04-26 01:05 PDT | 2026-04-26 08:05 UTC | `ingestion-lane-followup-plus8h-2026-04-26-am` | Pre-fire window. ~25 min before scheduled `nightly_graph_build.yml` cron at 08:30 UTC. Verifies rotation status before the 5th scheduled fire and after-the-fact, the next firing's outcome (the task prompt instructs the agent to wait/poll if necessary). |

## Drift sweep — T+12.85h (2026-04-25 17:26 PDT, post-PM-handoff)

**Decision: `NO-CHANGE`.** Same blocker, no ops actions landed, no new evidence. ~21 minutes after the 17:05 PDT PM handoff.

| Surface | Value at T+12.5h (17:05 PDT) | Value at T+12.85h (17:26 PDT) | Drift |
| --- | --- | --- | --- |
| PR #67 HEAD | `c2de352` | `c2de352` | none |
| PR #67 state | draft, mergeable=MERGEABLE, Tests SUCCESS | draft, mergeable=MERGEABLE, Tests SUCCESS | none |
| PR #67 last update | `2026-04-26T00:08:12Z` (PM-lane drift comment) | `2026-04-26T00:08:12Z` (same comment, no new activity) | none |
| Latest `nightly_graph_build.yml` run | [`24927546067`](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) failure (09:14 UTC) | same run; no manual `workflow_dispatch`, no new schedule fire | none |
| Streak | 4 consecutive scheduled red | 4 consecutive scheduled red | none |
| `gh run list --limit 5` newest 5 | Tests ✅ / Nightly ❌ / Nightly ❌ / Tests ✅ / push-main Tests ✅ | identical IDs and order | none |

**Triage / follow-up lane (this slot) action:** wrote the prompt-named [`../2026-04-26/ingestion-daytime-note.md`](../2026-04-26/ingestion-daytime-note.md) as a thin re-anchor pointer to the canonical PM artifacts; appended this drift row. Did not post a second PR #67 comment within 21 minutes of the PM-lane drift comment (would be noise). Did not re-investigate root cause (anti-pattern rule). Did not patch the repo (stop condition still met). The two scheduled follow-ups (`+3h` 20:05 PDT and `+8h` 01:05 PDT) remain the agreed evidence-gathering plan.

## Drift sweep — T+15.5h (2026-04-25 20:05 PDT, scheduled `+3h` follow-up)

**Decision: `NO-CHANGE`.** Same blocker, no ops actions landed, no new evidence. Fired by scheduled task `ingestion-lane-followup-plus3h-2026-04-25-pm`.

| Surface | Value at T+12.85h (17:26 PDT) | Value at T+15.5h (20:05 PDT) | Drift |
| --- | --- | --- | --- |
| `origin/main` HEAD | `4c5f2f3` | `4c5f2f3` | none |
| `origin/dev` HEAD | `a8db3de` | `a8db3de` | none |
| PR #67 HEAD | `c2de352` | `c2de352` | none |
| PR #67 state | draft, mergeable=MERGEABLE, Tests SUCCESS | draft, mergeStateStatus=CLEAN, Tests SUCCESS | none |
| PR #67 last update / latest comment | `2026-04-26T00:08:12Z` (PM-lane drift comment) | `2026-04-26T02:47:13Z` (sibling **PR #67 Readiness Lane** verdict comment) | +1 comment, no merge-gate movement |
| PR #67 comment count | 3 | 4 | +1 |
| Latest `nightly_graph_build.yml` run | [`24927546067`](https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067) failure (09:14 UTC) | same run; no manual `workflow_dispatch`, no new schedule fire (expected — next cron ~08:30 UTC ≈ 01:30 PDT 04-26, ≈5.4h after this slot) | none |
| Streak | 4 consecutive scheduled red | 4 consecutive scheduled red | none |
| Recent PRs | `#67` open, `#66`/`#65`/`#64`/`#63` merged | identical | none |

**The new PR #67 comment ([`issuecomment` 2026-04-26T02:47:13Z](https://github.com/perditioinc/reporium-ingestion/pull/67#issuecomment))** was posted by an autonomous **PR #67 Readiness Lane** at 19:44 PDT (~21 min before this slot's fire). Verdict: **KEEP DRAFT** — same disposition as AM / PM lanes. Re-validated `c2de352` against `dev` `a8db3de`, ran the local CI gate (`pytest …` → 38 passed), confirmed the three "minor observations" remain non-blocking, and reasserted both pre-promote gates (KAN-id rename + operator dry-run). No new defects, no patch, no rebase, no push. Effectively a third re-affirmation of the same triage stance. Sibling lane's full note: [`.audit/2026-04-26/pr-67-readiness-note.md`](../2026-04-26/pr-67-readiness-note.md). **Does not** alter the merge gate; the two operator gates still hold.

**`workflow_dispatch` watch:** none. The prompt explicitly noted the next *scheduled* fire is ≈01:30 PDT 2026-04-26 (8.4h post-T+12.5h, ≈5.4h post this slot), so the absence of a new run between 17:05 PDT and 20:05 PDT is expected; only an out-of-band manual run from ops would have surfaced here, and none did. Therefore no new container log to inspect, no new failure shape, and the `gcloud logging read` IAM gap is moot until either (a) ops manually fires a smoke test or (b) the next cron lands at 08:30 UTC.

**Triage / follow-up lane (this slot) action:** appended this drift stanza; appended the run-log row in [`ingestion-scheduled-followups.md`](ingestion-scheduled-followups.md); filled the **+3h appendix** in [`../2026-04-26/ingestion-execution-note.md`](../2026-04-26/ingestion-execution-note.md). **Did not** post a third lane drift comment on PR #67 (would be the third routine NO-CHANGE comment in ~4 hours; per prompt rule "Do NOT post a new PR comment unless something material changed"). **Did not** re-investigate root cause. **Did not** patch the repo. The remaining `+8h` follow-up (`ingestion-lane-followup-plus8h-2026-04-26-am`, fires 01:05 PDT 2026-04-26) is timed ~25 min before the next scheduled `nightly_graph_build.yml` cron and remains the next evidence-gathering slot.

## Links

- Latest fail (4th in streak): https://github.com/perditioinc/reporium-ingestion/actions/runs/24927546067
- Cloud Console execution detail (latest): https://console.cloud.google.com/run/jobs/executions/details/us-central1/reporium-graph-build-2jskf/logs?project=perditio-platform
- Diagnostics PR (already merged to `main`): https://github.com/perditioinc/reporium-ingestion/pull/66 (`4c5f2f3`)
- Working sibling secret: `projects/perditio-platform/secrets/reporium-db-url-async`
- Failing secret: `projects/perditio-platform/secrets/reporium-db-url`
- Cloud SQL instance: `perditio-platform:us-central1:reporium-db`
- Password rotation runbook: `reporium-platform/.audit/2026-04-22/password-rotation-runbook.md` (sibling repo)

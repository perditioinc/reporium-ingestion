# Phase 0 gate verification — 2026-04-30

**Observer:** Claude (automated, observation-only lane)  
**Checked-at:** 2026-04-30  
**Scope:** `reporium-evals` Sprint 1 nightly harness readiness + `reporium-ingestion` precision-changing graph PRs since 2026-04-27  
**No production code modified.**

---

## Verdict

> **Phase 0 gate NOT YET SATISFIED — no graph-quality PRs shipped**

The Sprint 1 nightly Ask-eval harness is not live on the default branch of
`reporium-evals`. No precision-changing graph code has merged to `reporium-ingestion`
main since 2026-04-27. This is a **safe state**: the prerequisite gate is simply
not yet closed. Do not ship §5.3 / §5.4 / §5.5 PRs from the remediation plan
until Phase 0 is confirmed satisfied.

---

## Remediation plan location

The file `reporium-ingestion/.audit/2026-04-27/graph-quality-remediation-plan.md`
does **not exist** on `main` or on any branch of this repository (GitHub code
search: 0 results). The `.audit/` directory on `main` contains only `2026-04-25/`
and `2026-04-26/`. This note is therefore written as a fresh file at
`.audit/2026-04-30/phase0-gate-check.md` per the gate-check protocol.

---

## 1. reporium-evals — Sprint 1 harness state

### 1a. Workflow file

| Property | Value |
|---|---|
| File | `.github/workflows/golden-ask-evals.yml` |
| Exists on `main`? | **NO** — `main` is at seed commit `3ec9be9`, has no `.github/workflows/` directory |
| Exists on branch | `claude/feature/ask-eval-harness-sprint0` (HEAD `757b942`) |
| Schedule | `cron: "30 9 * * *"` (09:30 UTC daily) |
| Manual trigger | `workflow_dispatch` with optional `reason` input |
| Currently enabled (fires on schedule)? | **NO** — GitHub scheduled workflows only run from the repository default branch; the workflow is not on `main` |

### 1b. Sprint branch commit history

| SHA | Message |
|---|---|
| `757b942` | `docs(evals): record Sprint 1 baseline verification` |
| `dc31289` | `feat: golden ask eval Sprint 1 workflow + reporting layer` ← workflow added here |
| `57becf5` | `feat: initial eval harness + 50-question golden set (Sprint 0)` |
| `3ec9be9` | `chore: seed main branch` ← **main HEAD** |

### 1c. Open PRs in reporium-evals

| PR | State | From → To | Notes |
|---|---|---|---|
| #1 | Closed (not merged) | `claude/feature/ask-eval-harness-sprint0` → `main` | Sprint 0 baseline; operator chose not to merge |
| #2 | Open | `feat/eval-verify-and-extend` → `claude/feature/ask-eval-harness-sprint0` | Sprint 1 smoke run + 5 boundary cases; NOT targeting main |
| #3 | Open | `claude/feature/KAN-CI-WIREUP-evals` → `main` | Adds PR-trigger `pr.yml` CI only; does **not** include `golden-ask-evals.yml` |

**The golden eval workflow has never been merged to main. No path to merge is currently open.**

### 1d. Workflow runs

No scheduled runs can have fired: GitHub does not execute scheduled workflows
that exist only on non-default branches. No `workflow_dispatch` runs are
visible from available tooling. **Zero confirmed runs.**

### 1e. Safety properties (read from YAML at `dc31289`)

| Check | Result |
|---|---|
| Live API calls gated on `RUN_GOLDEN_EVAL=1`? | PASS — set only inside the `if: steps.gate.outputs.should_run == 'true'` step, which fires only when both secrets are non-empty |
| `REPORIUM_API_URL` from `secrets.*`? | PASS — `${{ secrets.REPORIUM_API_URL }}` |
| `REPORIUM_APP_TOKEN` from `secrets.*`? | PASS — `${{ secrets.REPORIUM_APP_TOKEN }}` |
| Token echoed anywhere? | PASS — gate step uses `[[ -z "${VAR:-}" ]]` (no echo); no `set -x` |
| Artifacts defined? | PASS — `summary.json`, `summary.md`, `pytest.log` uploaded under `golden-ask-evals-{run_id}` with `retention-days: 30` |

---

## 2. reporium-ingestion — precision-changing graph code since 2026-04-27

### 2a. Commits to main since 2026-04-27

GitHub API (`list_commits --since 2026-04-27 --sha main`): **empty — zero commits.**

Last commit on main: `70aa62b` — `ci(graph-build): inline Secret Manager
rotation-drift detection on failure (#68)`, authored 2026-04-25 20:29 PDT
(2026-04-26 03:29 UTC). CI/infra only; does not touch graph algorithm or
threshold logic.

### 2b. Key graph files — last main-branch commits

| File | Last commit on main | Date | Classification |
|---|---|---|---|
| `scripts/build_knowledge_graph.py` | `8b02988` (#54 Cloud Run Job) | 2026-04-16 | CI/infra migration; algorithm unchanged |
| `scripts/build_knowledge_graph.py` | `adbfce2` (Wave 3 atomic rebuild) | 2026-04-10 | Algorithm change (pre-plan) |
| `ingestion/graph/atomic_swap.py` | `adbfce2` (Wave 3) | 2026-04-10 | Infrastructure only |
| `ingestion/graph/ingest_run_manager.py` | `adbfce2` (Wave 3) | 2026-04-10 | Infrastructure only |
| `ingestion/graph_snapshot.py` | `3c96a1f` (KAN-121) | 2026-04-21 | Removed per-type edge cap (pre-plan) |

All graph-relevant changes predate 2026-04-27. None were merged after the plan date.

### 2c. Open PRs touching graph logic

| PR | Title | Target branch | Verdict |
|---|---|---|---|
| #67 | `feat(backfill): no-tag fork tag-recovery via upstream README + topics` | `dev` (not `main`) | New backfill script; uses existing deterministic tagger; no graph edge algorithm change. NOT precision-changing. |
| #69 | `docs(runbook): document post-#67 primary_category_column reconcile step` | `main` | Documentation only (RUNBOOK.md). NOT precision-changing. |

PR #67 does not modify `build_knowledge_graph.py`, `atomic_swap.py`,
`ingest_run_manager.py`, `graph_snapshot.py`, or any Alembic migration touching
`repo_edges`. It targets `dev`, not `main`.

### 2d. Alembic migrations — repo_edges columns / edge types since 2026-04-27

**None.** No commits to main after 2026-04-26; no open PRs targeting main with
migration changes affecting `repo_edges`.

---

## 3. Summary and recommendations

| Gate criterion | Status |
|---|---|
| Workflow exists in `reporium-evals` | Partial — exists on feature branch, not on `main` |
| Workflow scheduled and firing nightly | NOT MET — cannot fire from non-default branch |
| Latest run green with artifacts | NOT MET — no runs have executed |
| Safety properties satisfied | PASS (would be ready once merged to main) |
| No precision-changing graph PRs shipped before gate | PASS — zero such merges since 2026-04-27 |

**Recommended actions:**

1. **Merge the Sprint 1 harness to main** in `reporium-evals`:
   - Merge PR #2 (`feat/eval-verify-and-extend` → `claude/feature/ask-eval-harness-sprint0`) first (smoke-run fixes + boundary cases).
   - Then open a PR from `claude/feature/ask-eval-harness-sprint0` → `main` and merge it. This is what PR #1 tried but did not complete.
2. **Configure repository secrets** `REPORIUM_API_URL` and `REPORIUM_APP_TOKEN` in `reporium-evals` settings so the nightly schedule actually hits the live API.
3. **Confirm one successful nightly run** produces `summary.json`, `summary.md`, and `pytest.log` artifacts before declaring Phase 0 satisfied.
4. **Do not merge §5.3 / §5.4 / §5.5 remediation-plan PRs** (precision-changing graph work) until the above three steps are complete and a live run is confirmed.
5. **Freeze is not required** — the current main state is clean. But hold any graph-quality PRs until the eval harness is live.

---

*observation-only; no production graph-build code modified*

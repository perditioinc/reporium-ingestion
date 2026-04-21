# Cloud Run Job setup: `reporium-enrichment`

One-time infra setup for the nightly enrichment job. Run these commands in
order from a shell authenticated against `perditio-platform`. Everything here
is explicit gcloud — no Config Connector, no Terraform — because the live
project does not have a reconciler picking up `cloudscheduler.cnrm.cloud.google.com`
resources (verified 2026-04-21: only `reporium-api-healthcheck` exists in
Cloud Scheduler, and no CNRM controller is installed).

## Prerequisites

```bash
export PROJECT_ID=perditio-platform
export REGION=us-central1
gcloud config set project "$PROJECT_ID"
```

## 1. Create least-privilege service accounts

Runtime SA (runs the job, reads secrets, connects to Cloud SQL):

```bash
gcloud iam service-accounts create reporium-enrichment \
  --display-name="Reporium Enrichment (Cloud Run Job runtime)" \
  --description="Runs nightly enrichment. DB + Secret Manager access only."
```

Scheduler SA (triggers the job; holds only `run.invoker`):

```bash
gcloud iam service-accounts create reporium-enrichment-scheduler \
  --display-name="Reporium Enrichment (Cloud Scheduler invoker)" \
  --description="Holds run.invoker on reporium-enrichment. No runtime access."
```

Rationale: not reusing `reporium-ingestion-ci@` (mixes CI deploy power with
runtime power) or `reporium-api@` (broader runtime role than this job needs).
New SAs are free; permission soup is not.

## 2. Grant IAM to the runtime SA

Cloud SQL Client (required to open the `/cloudsql/...` socket):

```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:reporium-enrichment@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"
```

Secret Manager accessor — **only** the three secrets this job needs
(no project-wide accessor):

```bash
for SECRET in reporium-db-url-async anthropic-api-key github-token; do
  gcloud secrets add-iam-policy-binding "$SECRET" \
    --member="serviceAccount:reporium-enrichment@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
done
```

## 3. Build the container image

From repo root:

```bash
gcloud builds submit \
  --config cloudbuild-enrichment.yaml \
  --substitutions=_IMAGE_TAG=latest
```

## 4. Deploy the Cloud Run Job

Substitute placeholders and apply:

```bash
sed "s/PROJECT_ID/${PROJECT_ID}/g; s/REGION/${REGION}/g" deploy/enrichment-job.yaml \
  | gcloud run jobs replace - --region="$REGION"
```

`run jobs replace` is idempotent — safe to re-run on manifest edits.

## 5. Manual verification run (MANDATORY before step 6)

Do **not** create the Cloud Scheduler trigger until this succeeds end-to-end:

```bash
gcloud run jobs execute reporium-enrichment --region="$REGION" --wait
```

Expected outcome:
- Exit code 0
- Logs show `Enriching N new repos` or `No repos need enrichment`
- A row count in `repos` where `readme_summary IS NOT NULL` increases
  (or stays equal if there was nothing to enrich)

If it fails, check:
- `gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=reporium-enrichment' --limit=50 --format=value\(textPayload\)`
- The legacy GitHub Actions cron is still running nightly until this is
  proven, so there is no production outage window during verification.

## 6. Grant the scheduler SA invoker on the job

```bash
gcloud run jobs add-iam-policy-binding reporium-enrichment \
  --region="$REGION" \
  --member="serviceAccount:reporium-enrichment-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

## 7. Create the Cloud Scheduler trigger

Matches the legacy cron slot (07:00 UTC — after reporium-db sync, before
graph-build at 08:30 UTC):

```bash
gcloud scheduler jobs create http reporium-enrichment-nightly \
  --location="$REGION" \
  --schedule="0 7 * * *" \
  --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/reporium-enrichment:run" \
  --http-method=POST \
  --oauth-service-account-email="reporium-enrichment-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
```

## 8. Disable the legacy GitHub Actions cron (PR follow-up)

After ONE successful scheduled run (verify at 07:00 UTC the day after step 7),
open a follow-up PR to:

- Re-apply `if: false` on the `legacy-enrich` job in
  `.github/workflows/nightly_enrichment.yml`
- Remove the `schedule:` block from that workflow
- Delete this file after one stable week

Leaving the legacy path enabled during the verification window is
deliberate — the scripts are idempotent (`WHERE readme_summary IS NULL`),
so running both on 07:00 UTC is harmless but gives us a safety net.

## Teardown (if the migration is abandoned)

```bash
gcloud scheduler jobs delete reporium-enrichment-nightly --location="$REGION"
gcloud run jobs delete reporium-enrichment --region="$REGION"
gcloud iam service-accounts delete reporium-enrichment@${PROJECT_ID}.iam.gserviceaccount.com
gcloud iam service-accounts delete reporium-enrichment-scheduler@${PROJECT_ID}.iam.gserviceaccount.com
```

# Cloud Run Deployment Guide

This document describes how to deploy `reporium-ingestion` as a private Cloud Run job/service in GCP.
It assumes the source code stays private and execution remains operator-controlled.

## 1. Prerequisites

Before deploying, confirm the following already exist:

- GCP project with billing enabled
- Artifact Registry repository for container images
- Cloud Run Jobs and Cloud Scheduler APIs enabled
- Service account for runtime execution with:
  - Secret Manager Secret Accessor
  - Cloud Run Job Runner / appropriate job execution permissions
  - Pub/Sub Publisher if `PUBSUB_REPO_INGESTED_TOPIC` is used
- Secret Manager entries already configured for:
  - `GH_TOKEN`
  - `REPORIUM_API_KEY`
  - `INGEST_API_KEY`
  - `ANTHROPIC_API_KEY`
- Network access from Cloud Run to `reporium-api`

Recommended project default:

```bash
gcloud config set project perditio-platform
```

## 2. Build and Push the Container

If a `cloudbuild.yaml` is added later, it should build the ingestion image and push it to Artifact Registry.
Until then, the manual flow is:

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev

docker build -t us-central1-docker.pkg.dev/perditio-platform/reporium/reporium-ingestion:latest .

docker push us-central1-docker.pkg.dev/perditio-platform/reporium/reporium-ingestion:latest
```

If you prefer Cloud Build, the equivalent build target is:

```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/perditio-platform/reporium/reporium-ingestion:latest
```

## 3. Deploy to Cloud Run Jobs

Use Cloud Run Jobs for scheduled or manual ingestion runs.

```bash
gcloud run jobs deploy reporium-ingestion \
  --image us-central1-docker.pkg.dev/perditio-platform/reporium/reporium-ingestion:latest \
  --region us-central1 \
  --service-account reporium-ingestion@perditio-platform.iam.gserviceaccount.com \
  --set-env-vars REPORIUM_API_URL=https://reporium-api-573778300586.us-central1.run.app,GH_USERNAME=perditioinc,ENRICHMENT_MODEL=claude-sonnet-4-20250514,EMBEDDING_MODEL=all-MiniLM-L6-v2,MIN_RATE_LIMIT_BUFFER=100,MAX_CONCURRENCY=2,DEFAULT_RUN_MODE=quick,PUBSUB_REPO_INGESTED_TOPIC=projects/perditio-platform/topics/repo-ingested,GRAPH_SNAPSHOT_BUCKET=perditio-platform-bucket,GRAPH_SNAPSHOT_OBJECT=reporium/graph/knowledge-graph.json \
  --set-secrets GH_TOKEN=GH_TOKEN:latest,REPORIUM_API_KEY=REPORIUM_API_KEY:latest,INGEST_API_KEY=INGEST_API_KEY:latest,ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest
```

If you also use local cache or database-backed execution, add:

- `DATABASE_URL`
- `CACHE_DB_PATH`
- `REQUEST_DELAY_MS`

## 4. Trigger a Manual Run

Execute the Cloud Run job directly when you need an operator-triggered ingestion pass:

```bash
gcloud run jobs execute reporium-ingestion --region us-central1 --wait
```

To override the run mode for a manual execution, pass command arguments supported by the ingestion CLI:

```bash
gcloud run jobs execute reporium-ingestion \
  --region us-central1 \
  --args="-m","ingestion","run","--mode","full" \
  --wait
```

Use `quick` for incremental runs, `weekly` for heavier maintenance, and `full` only when you intend to refresh the full corpus.

## 5. Cloud Scheduler Setup

Recommended schedules:

- `5am UTC` daily quick mode
- `Sunday 5am UTC` weekly/full maintenance cadence as needed

Example daily quick-mode trigger:

```bash
gcloud scheduler jobs create http reporium-ingestion-quick \
  --location us-central1 \
  --schedule "0 5 * * *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/perditio-platform/jobs/reporium-ingestion:run" \
  --http-method POST \
  --oauth-service-account-email scheduler@perditio-platform.iam.gserviceaccount.com
```

Example weekly Sunday full-mode trigger using a separate job definition:

```bash
gcloud scheduler jobs create http reporium-ingestion-full \
  --location us-central1 \
  --schedule "0 5 * * 0" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/perditio-platform/jobs/reporium-ingestion-full:run" \
  --http-method POST \
  --oauth-service-account-email scheduler@perditio-platform.iam.gserviceaccount.com
```

If you keep one job and vary arguments per schedule, create separate Cloud Run Jobs or update the job spec before execution.

## 6. Monitoring and Alerting

Logs should stay structured so Cloud Logging can filter by severity and run metadata.

Useful Cloud Logging filter:

```text
resource.type="cloud_run_job"
resource.labels.job_name="reporium-ingestion"
severity>=ERROR
```

Recommended alert:

- alert when `severity>=ERROR` appears for `reporium-ingestion`
- alert when the daily quick job does not execute within its expected window
- alert when Pub/Sub publish failures or GitHub rate-limit exhaustion appear repeatedly

Operational checks after deployment:

- confirm `POST /admin/runs` receives completed run records
- confirm the `repo-ingested` Pub/Sub event is published when configured
- confirm the downstream API refresh path updates taxonomy and portfolio intelligence
- confirm `gs://perditio-platform-bucket/reporium/graph/knowledge-graph.json` is updated after graph publication

## 7. Nightly Scheduling

This repository now includes two operator-editable manifests:

- `deploy/job.yaml`
- `deploy/scheduler.yaml`

Before applying them, replace the literal placeholders:

- `PROJECT_ID`
- `REGION`

Apply the Cloud Run Job manifest first so the scheduler has a target:

```bash
gcloud run jobs replace deploy/job.yaml --region REGION
```

Then apply or translate the scheduler manifest into your preferred deployment path:

```bash
kubectl apply -f deploy/scheduler.yaml
```

If you do not use Config Connector, treat `deploy/scheduler.yaml` as the source-of-truth shape for creating the equivalent Cloud Scheduler job in GCP.

Required IAM before enabling the scheduler:

- the runtime service account must be able to read the referenced Secret Manager secrets
- the scheduler service account `ingestion-scheduler@PROJECT_ID.iam.gserviceaccount.com` must be allowed to invoke Cloud Run Jobs
- the deployment operator must be able to administer Cloud Run Jobs and Cloud Scheduler in the target project

After both manifests are applied:

- run the job once manually to verify the container image and secrets
- verify Cloud Scheduler can obtain an OIDC token for the configured service account
- confirm a successful scheduled run writes run history and emits the downstream refresh event

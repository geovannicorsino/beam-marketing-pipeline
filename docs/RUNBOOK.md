# Runbook

End-to-end guide to set up, run locally, and deploy to Dataflow from a clean machine.

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Python | 3.12+ | [python.org](https://python.org) |
| Docker Desktop | any | [docker.com](https://docker.com) |
| Google Cloud SDK | any | [cloud.google.com/sdk](https://cloud.google.com/sdk) |

---

## 1. GCP setup (one-time)

### Authenticate

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project corsino-marketing-labs
```

### Enable APIs

```bash
gcloud services enable \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  containerregistry.googleapis.com
```

### Create GCS buckets

```bash
# Raw data and dead-letter output
gcloud storage buckets create gs://corsino-marketing-datalake --location=us-central1

# Dataflow temp and staging
gcloud storage buckets create gs://corsino-marketing-dataflow --location=us-central1
```

### Create BigQuery dataset

```bash
bq mk --location=US corsino-marketing-labs:marketing_analytics_silver
```

### Create dead-letter external table (query bad records from BigQuery)

```bash
bq mk --external_table_definition=@JSON \
  corsino-marketing-labs:marketing_analytics_silver.dead_letter \
  << 'EOF'
{
  "sourceFormat": "NEWLINE_DELIMITED_JSON",
  "sourceUris": ["gs://corsino-marketing-datalake/dead-letter/date=*/*.json"],
  "autodetect": true
}
EOF
```

### Upload fixtures to GCS (required for integration tests and local runs)

```bash
gsutil cp data/fixtures/ga4.json \
  gs://corsino-marketing-datalake/raw/ga4/sessions/account_id=corsino-marketing-labs/date=2026-04-11/ga4.json

gsutil cp data/fixtures/adobe_analytics.parquet \
  gs://corsino-marketing-datalake/raw/adobe/sessions/account_id=corsino-marketing-labs/date=2026-04-11/adobe_analytics.parquet

gsutil cp data/fixtures/crm_2026-04-10.csv \
  gs://corsino-marketing-datalake/crm/files/data.csv
```

---

## 2. Local setup

```bash
# Clone and enter the repo
git clone <repo-url>
cd beam-marketing-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows

# Install dependencies (including dev tools)
pip install -e ".[dev]"
```

---

## 3. Running locally (DirectRunner)

Reads from GCS, writes to BigQuery and GCS dead-letter. No Dataflow workers involved.

```bash
python pipeline/main.py \
  --bucket=corsino-marketing-datalake \
  --project=corsino-marketing-labs \
  --date=2026-04-11 \
  --temp_location=gs://corsino-marketing-dataflow/tmp
```

Or use the VS Code launch configuration: **Run Pipeline (local)**.

---

## 4. Running tests

```bash
# Unit tests only (fast, no GCP)
pytest tests/unit/

# Integration tests (reads from GCS — requires GCP auth)
pytest tests/integration/

# Full suite with coverage
pytest --cov=pipeline --cov-report=term-missing
```

---

## 5. Deploying to Dataflow

The SDK container image must use the **same Python minor version** as the submission environment. This project uses Python 3.13 and `apache/beam_python3.13_sdk:2.72.0`.

The Dockerfile must **not** define a custom `ENTRYPOINT` — Dataflow overrides it to start the worker harness.

### Create the Artifact Registry repository (one-time)

```bash
gcloud artifacts repositories create beam-marketing-pipeline \
  --repository-format=docker \
  --location=us-central1 \
  --project=corsino-marketing-labs
```

### Build and push the SDK container image

```bash
# Configure Docker auth for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build
docker build -t us-central1-docker.pkg.dev/corsino-marketing-labs/beam-marketing-pipeline/pipeline:latest .

# Push
docker push us-central1-docker.pkg.dev/corsino-marketing-labs/beam-marketing-pipeline/pipeline:latest
```

### Submit the Dataflow job

```bash
python pipeline/main.py \
  --runner=DataflowRunner \
  --project=corsino-marketing-labs \
  --region=us-central1 \
  --bucket=corsino-marketing-datalake \
  --date=2026-04-11 \
  --temp_location=gs://corsino-marketing-dataflow/tmp \
  --sdk_container_image=us-central1-docker.pkg.dev/corsino-marketing-labs/beam-marketing-pipeline/pipeline:latest \
  --no_use_public_ips
```

Monitor the job at: https://console.cloud.google.com/dataflow/jobs

---

## 6. Querying results

### Enriched leads

```sql
SELECT *
FROM `corsino-marketing-labs.marketing_analytics_silver.leads_enriched`
WHERE processing_date = '2026-04-11'
ORDER BY lead_classification, lead_score DESC;
```

### Dead-letter records

```sql
SELECT reason, source_system, raw_record, error_timestamp
FROM `corsino-marketing-labs.marketing_analytics_silver.dead_letter`
ORDER BY error_timestamp DESC;
```

---

## 7. Linting

```bash
ruff check .
```

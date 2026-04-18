# CI/CD

## Overview

Two workflows handle code quality and image publishing separately:

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Push to `main` | Lint + unit tests (automatic) |
| `publish.yml` | Manual (`workflow_dispatch`) | Build + push Docker image to Artifact Registry |

GCP authentication in GitHub Actions uses **Workload Identity Federation** — no service account keys stored anywhere.

---

## Workload Identity Federation

### Why not a service account key?

A JSON key is a long-lived credential that never expires on its own. If it leaks (logs, git history, a public gist), it's valid until manually rotated. It also needs to be stored as a GitHub secret and rotated periodically.

WIF replaces keys with short-lived OIDC tokens. The flow:

```
GitHub Actions job
  │
  │  1. GitHub issues an OIDC token signed by token.actions.githubusercontent.com
  │     The token contains: repo name, branch, workflow, actor
  │
  ▼
GCP Workload Identity Pool
  │
  │  2. GCP validates the token against the trusted issuer
  │     Checks the attribute condition: repository == "geovanni-corsino/beam-marketing-pipeline"
  │
  ▼
Service Account impersonation
  │
  │  3. GCP issues short-lived credentials (~1h) scoped to the SA
  │
  ▼
Artifact Registry push
```

The credential expires when the job ends. A leaked token from one job cannot be reused in another context.

### GCP resources created (one-time setup — already done)

```bash
# 1. Workload Identity Pool
gcloud iam workload-identity-pools create "github-actions" \
  --project=corsino-marketing-labs \
  --location=global \
  --display-name="GitHub Actions"

# 2. OIDC Provider — trusts GitHub's token issuer, scoped to this repo
gcloud iam workload-identity-pools providers create-oidc "github" \
  --project=corsino-marketing-labs \
  --location=global \
  --workload-identity-pool="github-actions" \
  --display-name="GitHub OIDC" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='geovanni-corsino/beam-marketing-pipeline'"

# 3. Service Account with Artifact Registry write permission
gcloud iam service-accounts create "github-actions-publisher" \
  --project=corsino-marketing-labs \
  --display-name="GitHub Actions — Artifact Registry publisher"

gcloud artifacts repositories add-iam-policy-binding "beam-marketing-pipeline" \
  --project=corsino-marketing-labs \
  --location=us-central1 \
  --member="serviceAccount:github-actions-publisher@corsino-marketing-labs.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

# 4. Allow the WIF pool to impersonate the SA — scoped to this repo only
PROJECT_NUMBER=$(gcloud projects describe corsino-marketing-labs --format="value(projectNumber)")

gcloud iam service-accounts add-iam-policy-binding \
  "github-actions-publisher@corsino-marketing-labs.iam.gserviceaccount.com" \
  --project=corsino-marketing-labs \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-actions/attribute.repository/geovanni-corsino/beam-marketing-pipeline"
```

### GitHub secrets required

Add these two secrets in **GitHub → repository → Settings → Secrets and variables → Actions**:

| Secret | Value |
|---|---|
| `WIF_PROVIDER` | `projects/482022309764/locations/global/workloadIdentityPools/github-actions/providers/github` |
| `WIF_SERVICE_ACCOUNT` | `github-actions-publisher@corsino-marketing-labs.iam.gserviceaccount.com` |

---

## GCP resources summary

| Resource | Name |
|---|---|
| Workload Identity Pool | `github-actions` |
| OIDC Provider | `github` |
| Service Account | `github-actions-publisher@corsino-marketing-labs.iam.gserviceaccount.com` |
| Artifact Registry repo | `us-central1-docker.pkg.dev/corsino-marketing-labs/beam-marketing-pipeline` |
| Repo scope | `geovanni-corsino/beam-marketing-pipeline` only |

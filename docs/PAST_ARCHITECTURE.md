# 📚 Past Architecture — Original Java Pipeline

> 🔍 **A professional retrospective** of a production Beam pipeline that processed multi-source marketing data for a global brand.
> 
> This document covers the original design, reasoning, lessons learned, and what I would approach differently today.

---

## 🎯 Context

A marketing client needed a **single consolidated view** of their data across three independent platforms:
- 📊 **Web Analytics** — Adobe Analytics & Google Analytics 4  
- 💰 **Paid Media** — TikTok Ads, Facebook Ads, Google Ads, DV360
- 🗂️ **CRM System** — Third-party vendor managed

The goal was to produce a clean, dashboard-ready BigQuery table that:
- ✅ Combined all three sources  
- ✅ Applied lead classification rules  
- ✅ Traced campaign attribution with real spend data

---

### 💡 My Context
This was **my first production project with Apache Beam**, built in Java alongside a team that used Dataflow as the standard delivery pattern. The depth of that experience eventually led to earning the **Google Cloud Professional Data Engineer certification**.

---

## 📐 Architecture Overview

The pipeline was structured in three distinct layers: raw ingestion, a silver quality/audit layer, and a gold output layer for dashboard consumption.

![Architecture Diagram](architecture.svg)

---

## 🔧 Raw Ingestion Layer

### 📡 API Sources — Analytics & Ad Platforms

My team had developed an **internal API ingestion framework** that standardised how we pulled data from **any third-party API** and landed it in GCS. **I was a core contributor to its architecture and development**, shaping how connectors were built and how output was standardised. Each connector ran as a **Cloud Run Function**, writing output in **Avro format**.

For this project, the framework covered two categories of sources:

**📊 Web analytics** — Google Analytics 4 and Adobe Analytics, providing session-level and behavioural data.

**💰 Paid media / ad platforms** — TikTok Ads, Facebook Ads, Google Ads, and DV360, delivering campaign performance metrics (impressions, clicks, spend, conversions). This data was **crucial for campaign attribution** — providing the full picture of spend per campaign.

#### Why Avro?

All connectors shared the same output contract: **partitioned Avro files on GCS**, keyed by date.

| Benefit | Why it mattered |
|---|---|
| 🔒 **Schema enforcement** | Every file carried its schema; no unintended drift |
| 📦 **Binary compactness** | Smaller files than text-based formats |
| ⚡ **Native ecosystem** | Dataflow reads Avro natively; BigQuery ingests directly |
| 📋 **Data cataloguing** | Typed schemas enabled easy data catalog registration |

### 📁 CRM Source — Weekly CSV Snapshot

The client's CRM vendor sent a **weekly full snapshot** of all leads as a CSV file. Rather than engineering a direct API integration, I built a flow that put the control in the hands of the data analyst:

1. 📤 Analyst uploads weekly CSV to GCS
2. ⚡ GCS trigger fires Cloud Run Function automatically  
3. ✅ Function validates records (required fields, value sets, format)
4. 💾 Output written as **partitioned Avro**, accumulating full history

Each weekly CSV snapshot contained **one year of lead data** — a moving window sent by the CRM vendor. The Cloud Run Function converted each delivery into date-partitioned Avro files and appended them to the existing history on GCS, so the cumulative dataset grew with every import.

> **Key advantage:** The analyst never touched any pipeline tooling. Uploading the file was the entire workflow — everything else was automated.

---

## 📊 Scale

The pipeline processed **high volumes** of data for a global brand:
- Consolidating leads, sessions, and ad spend metrics across all markets
- Running **once per day** on a fixed schedule  
- Running **on demand** whenever reprocessing or corrected batches were needed

---

## ⏱️ Pipeline Triggers

The Dataflow job ran under two conditions:

| Trigger | Source | Frequency |
|---|---|---|
| ⏰ **Cloud Scheduler** | Daily schedule | Every day — latest analytics data |
| 📂 **Cloud Run Function** | CSV import completion | On demand — automatic post-CRM import |

---

## 🥈 Silver Layer — Quality & Audit

Before writing to the final output, records were evaluated against taxonomy rules and cross-referencing checks. All records — valid and invalid — were written to a **BigQuery Silver table**, with dedicated columns indicating:

- ❌ Unrecognised or out-of-range taxonomy values
- 🔗 Failed cross-referencing (e.g., lead ID in CRM but not in analytics)
- 📝 Specific error messages per failure type

> This served as an **audit trail** — data quality problems were immediately visible and investigable at the source level, without losing any records or blocking the pipeline.

---

## 🥇 Gold Layer — Dashboard Output

The gold table was the final output consumed by analysts via their dashboard tool. It contained **all records**, including those with errors — no records were dropped. Error fields were carried through, allowing dashboards to filter or flag data quality issues in context without losing coverage.

Key transformations applied at this layer:

| Transformation | Purpose |
|---|---|
| 🔀 **Consolidation** | Records from all sources (GA4, Adobe, ad platforms, CRM) merged into unified schema |
| 🏷️ **Lead classification** | Each CRM lead classified by funnel stage (converted, qualified, nurturing, cold) |
| 🎯 **Campaign attribution** | CRM leads crossed with analytics & ad platforms to determine responsible campaign using first-touch rules, **contextualized with actual campaign cost** |

---

## 🤔 Design Decisions

### 🔶 Why ETL and Dataflow?

The team I joined had **Dataflow as its established standard** for data pipelines. Most prior deliveries followed the same ETL pattern, and the tooling and knowledge were already in place. 

> I was not yet familiar with ELT architectures or tools like dbt and Dataform at the time. The decision to use Dataflow was consistent with the team's delivery patterns, not an explicit architectural choice weighing alternatives.

### 🔶 Why Avro?

Primarily driven by the API ingestion framework, but **deliberately so**. Avro gave us:
- Schema rigidity not available in raw file formats
- Binary compactness for large-scale datasets
- Straightforward data cataloguing with typed schemas
- Simplified Dataflow integration

### 🔶 Why Cloud Run Functions for CRM processing?

The CRM flow needed to be **operable by a data analyst** without any engineering involvement. A Cloud Run function triggered by a GCS upload provided exactly that:

✅ Serverless — no infrastructure to manage  
✅ Event-driven — automatic on file upload  
✅ Invisible to the analyst — just upload the file  

---

## 📚 What I Learned

### From the original Java pipeline

This was my **first production project with Apache Beam**. It required building a working understanding of:

- 🎓 **Beam's distributed computation model** — PCollections, transforms, and the separation between pipeline definition and runner
- 🏃 **Dataflow's parallelism patterns** — understanding how operations like `CoGroupByKey` behave differently at scale
- 🛠️ **Operational Dataflow** — monitoring, failure handling, and reprocessing strategies

The depth of that experience directly supported earning the **Google Cloud Professional Data Engineer certification**.

### From reimplementing in Python

Rebuilding this pipeline from scratch in Python surfaced patterns and decisions that the original didn't address — either because of time constraints or because the team's framework abstracted them away:

| Pattern | Original | Python Rebuild | Why it matters |
|---|---|---|---|
| **Dead-letter routing** | Silver table with error columns | `TaggedOutput` + GCS | Makes routing intent explicit in code; keeps main output clean |
| **Observability** | Silent data quality | Beam `Metrics.counter` per DoFn | Low match rate surfaces as explicit warning vs. silent gap |
| **Runtime config** | Baked into code | `AsSingleton` side inputs | Update business rules in GCS without rebuilding container |
| **Auth** | Service account keys | Workload Identity Federation | Eliminate long-lived credentials; safer for shared CI |
| **Container startup** | Default Dataflow worker | Custom SDK container | Pre-install dependencies; remove worker startup overhead |

---

## 🚀 What I Would Do Differently Today

Looking back with more experience, the most significant limitation of this architecture was its **monolithic nature**. The Dataflow pipeline handled everything — ingestion normalisation, validation, classification, attribution, and writing — as a single job. If any step failed or needed to be rerun, the **entire pipeline had to be re-executed**.

Today I would approach this differently:

#### 1️⃣ Use BigQuery as the compute layer, not just the sink

**BigQuery's processing capacity** is more than sufficient for this volume and complexity. Pushing the transformation logic into SQL — with dbt or Dataform — would eliminate the need for a distributed processing framework entirely.

#### 2️⃣ Break the pipeline into independently rerunnable steps

With dbt or Dataform, **each model is an isolated, rerunnable unit**. If campaign attribution logic needs to change, only that model and its dependents need to rerun — not the entire pipeline from raw ingestion.

#### 3️⃣ Separate responsibilities more cleanly

- **Ingestion** — keep the Cloud Run Functions pattern for landing raw data (it works well)
- **Transformation** — dbt/Dataform models for each business logic step  
- **Orchestration** — Cloud Composer or Workflows for scheduling and dependency management

**Benefits:**
- ✅ Easier to debug
- ✅ Faster to iterate on  
- ✅ Simpler for other engineers to understand and maintain

---

## 📊 Architecture Comparison — Then vs. Now

| Dimension | Original Pipeline (Java) | Current Project (Python) |
|---|---|---|
| 🐍 **Language** | Java | Python 3.13 |
| 📦 **Beam version** | Beam 2.x (circa 2021–2022) | Beam 2.72.0 |
| 📄 **Raw format** | Avro (all sources) | JSON (GA4), Parquet (Adobe), CSV (CRM) |
| 🔧 **Ingestion** | Shared internal framework | Per-source readers built in-pipeline |
| ⏰ **Trigger** | Cloud Scheduler + GCS event | Manual / direct invocation |
| 📥 **Side inputs** | Not used | `AsSingleton` for runtime rule injection |
| 🗂️ **Dead-letter handling** | Silver BigQuery table | GCS dead-letter with tagged outputs |
| 📤 **Output** | Silver + Gold BigQuery tables | Single `leads_enriched` BigQuery table |
| 🔐 **CI/CD auth** | Service account keys | Workload Identity Federation |
| ✅ **Tests** | Limited | 71 tests (unit + integration) |
| 🎯 **Primary goal** | Client delivery | Learning + portfolio |

> **Note:** The current project deliberately reads heterogeneous formats (JSON, Parquet, CSV) rather than normalising to Avro upfront — the intent is to practice handling real-world format diversity with Apache Beam, which the original pipeline abstracted away through the ingestion framework.

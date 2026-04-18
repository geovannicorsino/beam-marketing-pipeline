# beam-marketing-pipeline — Context

## What this project is

A batch data pipeline built with Apache Beam (Python) that ingests marketing data from Google Analytics 4, Adobe Analytics, and a CRM system, joins them, classifies leads, and delivers enriched data to BigQuery and GCS.

This is a personal practice project. A few years ago the same pipeline was built in Java with Apache Beam. This is a reimplementation in Python, used to solidify Apache Beam fundamentals and refine things that were not in the original — specifically lead classification, dead-letter sink, and Beam metrics.

The focus is not on questioning the architecture or whether it's the optimal solution. It's on practicing Apache Beam patterns in Python and building a clean, testable, production-grade pipeline from scratch.

## Goals

- Reimplement a known pipeline architecture in Python instead of Java
- Practice Apache Beam patterns: multi-source ingest, CoGroupByKey, Flatten, tagged outputs, DoFn metrics, side inputs (AsSingleton)
- Add what was missing in the original: lead classification rules from GCS side input, dead-letter sink, Beam counters, Workload Identity Federation
- Serve as a portfolio piece demonstrating production-grade Beam development

## What we are building

A pipeline that:

1. Reads raw session data from GA4 (JSON), Adobe Analytics (Parquet), and leads from a CRM (CSV)
2. Normalizes all three sources into a unified `TableRecord` schema
3. Enriches CRM records with `campaign_name` from analytics data via `CoGroupByKey`
4. Deduplicates analytics records (GA4 + Adobe) by `analytics_user_id`
5. Flattens analytics and enriched CRM records into a single PCollection
6. Classifies leads into: `CONVERTED`, `QUALIFIED`, `NURTURING`, `COLD`
7. Writes the final records to BigQuery (`leads_enriched`)
8. Routes invalid records to a dead-letter sink on GCS

## GCP project

| Resource        | Value                                       |
| --------------- | ------------------------------------------- |
| Project ID      | `corsino-marketing-labs`                    |
| BigQuery table  | `marketing_analytics_silver.leads_enriched` |
| GCS dead-letter | `dead-letter/date={date}/{date}.json`       |

## Implementation status

### Done

- `TableRecord` — unified output schema
- Source: GA4 (`ReadFromText` + `json.loads`)
- Source: Adobe Analytics (`ReadFromParquet`)
- Source: CRM (`ReadFromText` + `csv.DictReader`)
- Normalize: `NormalizeGA4Fn`, `NormalizeAdobeFn` → `TableRecord`
- Normalize: `NormalizeCRMFn` → `dict`
- Transform: `JoinAnalyticsCRMFn` (`CoGroupByKey`)
- `MarketingPipelineOptions` (`--bucket`, `--project_id`, `--date`)
- Fixtures: `ga4.json`, `adobe_analytics.parquet`, `crm_2026-04-10.csv`
- Unit tests: 41 passing, 72% coverage

## Key design decisions

- **Single schema** — `TableRecord` is the only data model; fields not available from a given source default to empty/zero
- **CRM as primary entity** — the join enriches CRM records with analytics data, not the other way around
- **Full CRM load** — CRM file is a full weekly snapshot (`crm/files/data.csv`), no date partition
- **Dead-letter over discard** — invalid records are written to GCS for investigation and reprocessing
- **Classification rules as GCS side input** — `config/lead_classification_rules.json` stored in GCS, read via `ReadFromText` and passed as `AsSingleton` to `ClassifyLeadFn`; updatable without rebuilding the image
- **Account list from GCS** — `config/accounts.json` loaded before the pipeline via `load_json_config()`; drives which accounts are ingested in each run
- **Flatten** — analytics and enriched CRM records are merged into one PCollection for the final sinks

## Tech stack

- Python 3.13
- Apache Beam 2.72.0 (DirectRunner for dev, DataflowRunner for prod)
- Google Cloud Storage — raw input + silver + dead-letter output
- BigQuery — final enriched output
- Ruff — linting
- Pytest — unit + integration testing

For full architecture and pipeline flow see `ARCHITECTURE.md`.

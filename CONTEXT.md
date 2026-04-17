# beam-marketing-pipeline — Context

## What this project is

A batch data pipeline built with Apache Beam (Python) that ingests marketing data from Google Analytics 4, Adobe Analytics, and a CRM system, joins them, classifies leads, and delivers enriched data to BigQuery and GCS.

This is a personal practice project. A few years ago the same pipeline was built in Java with Apache Beam, working alongside a senior data engineer. This is a reimplementation in Python, used to solidify Apache Beam fundamentals and refine things that were not done in the original — specifically lead classification, dead-letter sink, and Beam metrics.

The focus is not on questioning the architecture or whether it's the optimal solution. It's on practicing Apache Beam patterns in Python and building a clean, testable, production-grade pipeline from scratch.

## Goals

- Reimplement a known pipeline architecture in Python instead of Java
- Practice Apache Beam patterns: multi-source ingest, CoGroupByKey, Flatten, DoFn metrics
- Add what was missing in the original: lead classification rules, dead-letter sink, Beam counters
- Serve as a portfolio piece demonstrating production-grade Beam development

## What we are building

A pipeline that:

1. Reads raw session data from GA4 (JSON), Adobe Analytics (Parquet), and leads from a CRM (CSV)
2. Normalizes all three sources into a unified `TableRecord` schema
3. Enriches CRM records with `campaign_name` from analytics data via `CoGroupByKey`
4. Deduplicates analytics records (GA4 + Adobe) by `analytics_user_id`
5. Flattens analytics and enriched CRM records into a single PCollection
6. Classifies leads into: `CONVERTED`, `QUALIFIED`, `NURTURING`, `COLD`
7. Writes the final records to GCS (Parquet) and BigQuery
8. Routes invalid records to a dead-letter sink on GCS

## GCP project

| Resource        | Value                                              |
| --------------- | -------------------------------------------------- |
| Project ID      | `corsino-marketing-labs`                           |
| BigQuery table  | `marketing_analytics_silver.leads_enriched`        |
| GCS silver      | `silver/beam-analytics/date={date}/{date}.parquet` |
| GCS dead-letter | `dead-letter/date={date}/{date}.json`              |

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

### To do

- [x] `ClassifyLeadFn` + `config/lead_classification_rules.json` + tests
- [ ] `DeduplicateAnalyticsFn` + tests
- [ ] Dead-letter sink (`pipeline/sinks/dead_letter.py`) + `NormalizeCRMFn` integration
- [ ] Parquet sink → GCS silver (`pipeline/sinks/parquet.py`)
- [ ] BigQuery sink → `leads_enriched` (`pipeline/sinks/bigquery.py`)
- [ ] Beam metrics (`pipeline/utils/metrics.py`) + `log_metrics()` with match rate alert
- [ ] Wire everything in `main.py` (sinks, dead-letter, metrics)
- [ ] Integration test (`tests/integration/test_pipeline_e2e.py`)
- [ ] Dockerfile
- [ ] GitHub Actions CI (`ruff check` + `pytest`)

## Key design decisions

- **Single schema** — `TableRecord` is the only data model; fields not available from a given source default to empty/zero
- **CRM as primary entity** — the join enriches CRM records with analytics data, not the other way around
- **Full CRM load** — CRM file is a full weekly snapshot (`crm/files/data.csv`), no date partition
- **Dead-letter over discard** — invalid records are written to GCS for investigation and reprocessing
- **Classification rules in JSON** — `config/lead_classification_rules.json` keeps business rules out of code
- **Flatten** — analytics and enriched CRM records are merged into one PCollection for the final sinks

## Tech stack

- Python 3.13
- Apache Beam 2.72.0 (DirectRunner for dev, DataflowRunner for prod)
- Google Cloud Storage — raw input + silver + dead-letter output
- BigQuery — final enriched output
- Ruff — linting
- Pytest — unit + integration testing

For full architecture and pipeline flow see `ARCHITECTURE.md`.

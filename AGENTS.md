# AGENTS.md

Guidelines for AI agents working on this codebase. Read alongside `CONTEXT.md` and `ARCHITECTURE.md`.

---

## Project in one paragraph

Apache Beam pipeline (Python 3.13, Beam 2.72.0) that ingests GA4 JSON, Adobe Analytics Parquet, and CRM CSV from GCS, joins them via `CoGroupByKey`, classifies leads, and writes enriched records to BigQuery (`leads_enriched`) and invalid records to a GCS dead-letter sink. Runs on Google Cloud Dataflow in production. The pipeline was rebuilt from a prior Java implementation to practice advanced Beam patterns: `AsSingleton` side inputs, `TaggedOutput` dead-letter routing, Beam metrics counters, Workload Identity Federation, and custom SDK containers.

---

## Before making any change

```bash
# Verify the baseline passes
ruff check .
pytest tests/unit/
```

Integration tests require GCS auth and uploaded fixtures — run only when needed:

```bash
pytest tests/integration/
```

---

## Module map

| Module | Role |
|---|---|
| `pipeline/main.py` | Entry point — wires all phases; do not put business logic here |
| `pipeline/options.py` | `MarketingPipelineOptions` — `--bucket`, `--date`, `--rules_path`, `--accounts_path` |
| `pipeline/ingest.py` | `build_analytics(p, bucket, account_ids, date)`, `build_crm(p, bucket)` — read + normalize + dedup |
| `pipeline/schemas/table_record.py` | `TableRecord` — the only data model; all PCollections share this type after normalization |
| `pipeline/sources/` | Raw readers returning `PCollection[dict]`; GA4 uses `ReadFromText + json.loads`, Adobe uses `ReadFromParquet`, CRM uses `ReadFromText + csv.DictReader` |
| `pipeline/normalize/` | `NormalizeGA4Fn`, `NormalizeAdobeFn` → `TableRecord`; `NormalizeCRMFn` → `dict` with dead-letter tagged outputs |
| `pipeline/transforms/join.py` | `JoinAnalyticsCRMFn` — `CoGroupByKey` + first-touch attribution (picks `campaign_name` from the analytics record with the earliest date) |
| `pipeline/transforms/classification.py` | `ClassifyLeadFn` — receives `rules` as `AsSingleton` side input; only runs on `source_system == "crm"` records |
| `pipeline/transforms/dedup_crm.py` | `DeduplicateCRMFn` — deduplicates by `crm_id`; routes duplicates to dead-letter |
| `pipeline/sinks/bigquery.py` | `write_leads_enriched` — partition decorator `${YYYYMMDD}`, WRITE_TRUNCATE per partition |
| `pipeline/sinks/dead_letter.py` | `write_dead_letter` — GCS NDJSON at `dead-letter/date={date}/{date}.json` |
| `pipeline/sinks/write.py` | `write_pipeline_output` — combines both sinks; call from `main.py` only |
| `pipeline/utils/config.py` | `load_json_config(path)` — supports both local paths and `gs://` URIs |
| `pipeline/utils/metrics.py` | `log_metrics(result)` — reads Beam counters after job completion; warns if match rate < 70% |

---

## Data flow

```
accounts.json (GCS) ──► load_json_config ──► account_ids list

GA4 JSON     ──► NormalizeGA4Fn ──►┐
                                   ├──► FlattenAnalytics ──► CoGroupByKey ──► JoinAnalyticsCRMFn ──► ClassifyLeadFn ──► write_leads_enriched (BQ)
Adobe Parquet──► NormalizeAdobeFn ─┘                              ▲
                                                                  │
CRM CSV ──► NormalizeCRMFn ──► DeduplicateCRMFn ─────────────────┘
                   │                   │
                   └───────────────────┴──► write_dead_letter (GCS)

rules.json (GCS) ──AsSingleton──► ClassifyLeadFn
```

---

## Critical conventions

### Single schema
`TableRecord` is the only data model. Every PCollection after normalization is `PCollection[TableRecord]`. Fields not available from a given source default to `""`, `0`, or `None`. Do not introduce a second dataclass.

### CRM is the primary join entity
`JoinAnalyticsCRMFn` enriches CRM records with analytics data — not the other way around. A CRM record with no analytics match still flows through with `campaign_name=None`. An analytics record with no CRM match is not emitted as a CRM output (it still flows as an analytics record through `FlattenAll`).

### Dead-letter over discard
Never silently drop a bad record. Route it to `write_dead_letter` using `TaggedOutput`. The dead-letter JSON includes a `reason` field explaining why the record was rejected.

### Classification is CRM-only
`ClassifyLeadFn.process()` returns analytics records unchanged (`source_system != "crm"`). Classification only applies to records where `source_system == "crm"`.

### Side input for classification rules
Rules are loaded from GCS via `ReadFromText` → `combiners.ToList()` → `beam.Map(json.loads)` and passed to `ClassifyLeadFn` as `AsSingleton`. Do not load rules inside `setup()` or `__init__` — the side input pattern allows rule updates without rebuilding the container image.

### `setup()` for DoFn state
If a DoFn needs to initialize heavy objects (e.g., compiled regex, a lookup table not from a side input), use `setup()` — not `__init__`. `__init__` runs at pipeline construction time on the submitter machine; `setup()` runs on each worker before processing begins.

### Beam metrics placement
Counters are defined as class-level attributes using `Metrics.counter(namespace, name)` and incremented inside `process()`. Do not define them inside `process()` or `setup()` — that creates a new counter object on every call.

```python
class MyFn(beam.DoFn):
    records_processed = Metrics.counter("my_namespace", "records_processed")  # class level

    def process(self, element):
        self.records_processed.inc()
        yield element
```

---

## Beam patterns used in this codebase

### TaggedOutput (dead-letter routing)
```python
def process(self, element):
    if is_invalid(element):
        yield beam.pvalue.TaggedOutput("dead_letter", build_dead_letter_record(element))
        return
    yield normalized_record
```
Call site: `beam.ParDo(MyFn()).with_outputs("dead_letter", main="valid")`

### AsSingleton side input
```python
# In pipeline/main.py
rules = (p | "ReadRules" >> ReadFromText(rules_path)
           | "CollectRules" >> combiners.ToList()
           | "ParseRules" >> beam.Map(lambda lines: json.loads("".join(lines))))

# In the transform graph
| "ClassifyLead" >> beam.ParDo(ClassifyLeadFn(), rules=AsSingleton(rules))

# In ClassifyLeadFn
def process(self, element: TableRecord, rules: dict):
    ...
```

### CoGroupByKey join
```python
result = (
    {"analytics": analytics_keyed, "crm": crm_keyed}
    | beam.CoGroupByKey()
    | beam.ParDo(JoinAnalyticsCRMFn())
)
```
Each element in `JoinAnalyticsCRMFn.process()` is `(key, {"analytics": [...], "crm": [...]})`.

### Unique transform labels
When applying the same DoFn multiple times (e.g., one per account_id), suffix the label with the account_id:
```python
| f"NormalizeGA4_{account_id}" >> beam.ParDo(NormalizeGA4Fn())
```

---

## Adding a new source

1. Add a reader to `pipeline/sources/` returning `PCollection[dict]`
2. Add a normalizer DoFn to `pipeline/normalize/` returning `TableRecord`; use `TaggedOutput` for invalid records
3. Add a counter (`records_processed`) to the normalizer
4. Wire into `pipeline/ingest.py`
5. Add unit tests under `tests/unit/normalize/` and `tests/unit/sources/` as appropriate
6. Update `ARCHITECTURE.md` Phase 1/2 and the project structure table

---

## Adding or changing a classification rule

1. Edit `config/lead_classification_rules.json`
2. Upload to GCS: `gsutil cp config/lead_classification_rules.json gs://corsino-marketing-datalake/config/lead_classification_rules.json`
3. No pipeline rebuild needed — the side input loads the file at job start

---

## GCP resources

| Resource | Value |
|---|---|
| Project | `corsino-marketing-labs` |
| GCS data bucket | `corsino-marketing-datalake` |
| GCS temp/staging | `corsino-marketing-dataflow` |
| BQ table | `corsino-marketing-labs.marketing_analytics_silver.leads_enriched` |
| GCS dead-letter | `gs://corsino-marketing-datalake/dead-letter/date={date}/{date}.json` |
| Artifact Registry | `us-central1-docker.pkg.dev/corsino-marketing-labs/beam-marketing-pipeline/pipeline:latest` |

---

## Fixture data (for tests and local runs)

| File | Records | Notes |
|---|---|---|
| `data/fixtures/ga4.json` | 10 | NDJSON, one record per line |
| `data/fixtures/adobe_analytics.parquet` | 10 | Snappy-compressed |
| `data/fixtures/crm_2026-04-10.csv` | 15 | 12 valid, 3 dead-letter (2 missing user_id + 1 invalid score) |

Expected integration test output: 32 total enriched records, 3 dead-letter, 8 CRM matched, 4 no-match.

---

## What not to do

- **Do not add a second output schema** — `TableRecord` is the contract between all pipeline stages
- **Do not use `WRITE_APPEND` for `leads_enriched`** — the sink uses partition decorator + `WRITE_TRUNCATE` to make daily reruns idempotent
- **Do not put GCS or BQ logic inside a DoFn** — sources and sinks are isolated from transforms; DoFns must be pure (no I/O)
- **Do not load classification rules in `setup()`** — they come from the `AsSingleton` side input; loading in `setup()` would require a path parameter and break the runtime-update pattern
- **Do not define Beam counters inside `process()`** — define them at class level (see Beam metrics section above)
- **Do not use `WRITE_APPEND` on the dead-letter sink** — `WriteToText` overwrites the file on each run for the same date; this is intentional (reruns replace the previous dead-letter output for that partition)

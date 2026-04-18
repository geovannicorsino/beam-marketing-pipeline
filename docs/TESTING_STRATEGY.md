# Testing Strategy

## Unit tests vs. integration tests

### The core difference

A **unit test** isolates a single piece of logic — one DoFn, one function — and feeds it controlled input via `beam.Create`. It never reads from files or calls external services. If a unit test fails, you know exactly which component broke.

An **integration test** runs the entire pipeline from source to the last transform, using real fixture files and the full transform graph. It proves that all the pieces work together correctly and that the pipeline produces the exact output you expect for a known input.

Neither replaces the other. Unit tests give fast feedback during development; the integration test guards against regressions in the pipeline as a whole.

---

### How unit tests work here

Each DoFn gets its own test file under `tests/unit/`. The pattern is always:

```python
with TestPipeline() as p:
    result = (
        p
        | beam.Create([input_record])
        | beam.ParDo(MyDoFn()).with_outputs("tag", main="valid")
    )
    assert_that(result.valid, equal_to([expected_output]))
    assert_that(result["tag"], equal_to([]))
```

`beam.Create` injects data directly into the pipeline without any I/O. `assert_that` is a Beam-native assertion that is evaluated when the pipeline finishes — if the expectation fails, `TestPipeline` raises a descriptive error.

For tests that need to inspect **Beam metrics counters** after the run, the `with TestPipeline()` block cannot be used because it returns the PCollection, not the `PipelineResult`. Use the explicit form:

```python
p = TestPipeline()
p | beam.Create([...]) | beam.ParDo(MyFn())
result = p.run()
result.wait_until_finish()

counters = {
    f"{c.key.metric.namespace}/{c.key.metric.name}": c.committed
    for c in result.metrics().query()["counters"]
}
assert counters["namespace/name"] == expected_value
```

Note: Beam only emits a counter entry if it was incremented at least once. Use `.get("key", 0)` for counters that may never fire.

---

### How the integration test works

`tests/integration/test_pipeline_e2e.py` re-wires the complete transform graph — the same stages as `main.py` — but replaces the GCP sinks with `assert_that` assertions on the output PCollections. No BigQuery or GCS access is required.

```
read_ga4 ──► NormalizeGA4Fn ──►┐
                                Flatten ──► CoGroupByKey ──► JoinAnalyticsCRMFn ──► ClassifyLeadFn ──► assert_that
read_adobe ─► NormalizeAdobeFn ►┘                ▲
                                                  │
read_crm ──► NormalizeCRMFn ──► DeduplicateCRMFn ─┘
                    │                    │
                    └────────────────────┴──► assert_that (dead-letter)
```

The test is **deterministic**: the fixture files are static, `DirectRunner` is single-threaded, and every transform is pure (no I/O, no randomness). Given the same input, the output is always the same. This makes the integration test a reliable regression guard — if a classification rule, join logic, or schema field changes unexpectedly, the test fails immediately with a clear diff.

Each test function builds its own independent pipeline instance via `_build_pipeline(p)`. Tests do not share state.

---

### Running the tests

```bash
# Fast feedback during development
pytest tests/unit/

# Full pipeline validation
pytest tests/integration/

# Everything
pytest

# With coverage report
pytest --cov=pipeline --cov-report=term-missing
```

---

## Coverage

Coverage measured across the full test suite (unit + integration). `main.py` and `options.py` are excluded from the percentage because they contain pipeline wiring and CLI argument parsing that require a live GCP run to exercise.

| Module | Statements | Covered | Coverage |
|---|---|---|---|
| `pipeline/normalize/analytics.py` | 19 | 19 | 100% |
| `pipeline/normalize/crm.py` | 25 | 25 | 100% |
| `pipeline/schemas/table_record.py` | 21 | 21 | 100% |
| `pipeline/sources/adobe.py` | 7 | 7 | 100% |
| `pipeline/sources/crm.py` | 13 | 13 | 100% |
| `pipeline/sources/ga4.py` | 8 | 8 | 100% |
| `pipeline/transforms/classification.py` | 35 | 35 | 100% |
| `pipeline/transforms/dedup_crm.py` | 14 | 14 | 100% |
| `pipeline/transforms/join.py` | 19 | 19 | 100% |
| `pipeline/utils/metrics.py` | 16 | 16 | 100% |
| `pipeline/sinks/dead_letter.py` | 5 | 4 | 80% |
| `pipeline/sinks/bigquery.py` | — | — | not measured* |
| `pipeline/main.py` | — | — | not measured* |
| `pipeline/options.py` | — | — | not measured* |
| **Total (measured modules)** | **182** | **181** | **~99%** |

\* These modules require a live GCP connection (`WriteToBigQuery`, `DataflowRunner`) and are intentionally excluded from automated test coverage.

---

## Test inventory

**71 tests total — 62 unit, 9 integration**

| File | Tests | What it covers |
|---|---|---|
| `unit/normalize/test_analytics.py` | 12 | `NormalizeGA4Fn`, `NormalizeAdobeFn` — field mapping, date parsing, null handling |
| `unit/normalize/test_crm.py` | 10 | `NormalizeCRMFn` — valid path, dead-letter routing, reason field, JSON encoding |
| `unit/transforms/test_join.py` | 9 | `JoinAnalyticsCRMFn` — match, no-match, first-touch attribution, edge cases |
| `unit/transforms/test_dedup_crm.py` | 6 | `DeduplicateCRMFn` — single record, duplicate routing, three-way dedup |
| `unit/transforms/test_classification.py` | 15 | `ClassifyLeadFn` — all four tiers, pass-through for non-CRM, score threshold |
| `unit/sinks/test_dead_letter.py` | 4 | Dead-letter JSON serialization and schema |
| `unit/utils/test_metrics.py` | 5 | `log_metrics` — counter extraction, match rate warning |
| `integration/test_pipeline_e2e.py` | 9 | Full pipeline — record counts, dead-letter routing, classification distribution, campaign attribution, Beam metrics |

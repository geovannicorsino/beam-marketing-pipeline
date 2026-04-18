"""
End-to-end integration test for the marketing pipeline.

Runs the complete pipeline with DirectRunner against real GCS fixtures uploaded to
gs://corsino-marketing-datalake. Sinks are replaced by assert_that assertions on
the output PCollections — no BigQuery writes occur during tests.

Expected counts (derived from data/fixtures/):
  GA4   : 10 records → 10 normalized analytics TableRecords
  Adobe : 10 records → 10 normalized analytics TableRecords
  CRM   : 15 rows   → 12 valid, 3 dead-letter
            (2 missing analytics_user_id + 1 invalid lead_score)
  Dedup : all crm_ids are unique  → 0 additional dead-letter
  Join  : 8 crm_matched, 4 crm_no_match  → 12 CRM TableRecords
  Final : 20 analytics + 12 CRM = 32 total enriched records

Classification breakdown (CRM records only, threshold=70):
  QUALIFIED : 1  (adobe_user_001 — form_completed, score=70)
  NURTURING : 4  (adobe_user_002, ga4_user_008, ga4_user_009, ga4_user_013)
  COLD      : 7  (ga4_user_001/002/003/004/005/010/012)
  CONVERTED : 0
"""

import apache_beam as beam
from apache_beam import combiners
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.normalize.analytics import NormalizeAdobeFn, NormalizeGA4Fn
from pipeline.normalize.crm import DEAD_LETTER_TAG, NormalizeCRMFn
from pipeline.sources.adobe import read_adobe
from pipeline.sources.crm import read_crm
from pipeline.sources.ga4 import read_ga4
from pipeline.transforms.classification import ClassifyLeadFn
from pipeline.transforms.dedup_crm import DeduplicateCRMFn
from pipeline.transforms.join import JoinAnalyticsCRMFn

# ── Expected values ───────────────────────────────────────────────────────────
EXPECTED_TOTAL = 32
EXPECTED_DEAD_LETTER = 3
EXPECTED_CRM_ENRICHED = 12
EXPECTED_QUALIFIED = 1
EXPECTED_NURTURING = 4
EXPECTED_COLD = 7
EXPECTED_CONVERTED = 0
EXPECTED_CRM_MATCHED = 8
EXPECTED_CRM_NO_MATCH = 4
EXPECTED_GA4_PROCESSED = 10
EXPECTED_ADOBE_PROCESSED = 10
EXPECTED_CRM_PROCESSED = 12
EXPECTED_CRM_DISCARDED = 3
EXPECTED_WITH_CAMPAIGN = 7
# ga4_user_003 is matched (has analytics) but both GA4 and Adobe have campaign_name=null,
# so first-touch attribution yields None. 4 no-match + 1 matched-but-null = 5 total.
EXPECTED_NO_CAMPAIGN = 5


# ── Pipeline builder ──────────────────────────────────────────────────────────

BUCKET = "corsino-marketing-datalake"
ACCOUNT_ID = "corsino-marketing-labs"
DATE = "2026-04-11"


def _build_pipeline(p):
    """Wire all transforms onto pipeline p. Returns (all_records, all_dead_letter, crm_enriched)."""
    ga4 = (
        read_ga4(p, bucket=BUCKET, report="sessions", account_id=ACCOUNT_ID, date=DATE)
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
    )
    adobe = (
        read_adobe(p, bucket=BUCKET, report="sessions", account_id=ACCOUNT_ID, date=DATE)
        | "NormalizeAdobe" >> beam.ParDo(NormalizeAdobeFn())
    )
    analytics = (ga4, adobe) | "FlattenAnalytics" >> beam.Flatten()

    crm_output = (
        read_crm(p, bucket=BUCKET, account_id=ACCOUNT_ID, date=DATE)
        | "NormalizeCRM" >> beam.ParDo(NormalizeCRMFn()).with_outputs(
            DEAD_LETTER_TAG, main="valid"
        )
    )
    dedup_output = (
        crm_output.valid
        | "KeyCRMById" >> beam.WithKeys(lambda r: r["crm_id"])
        | "GroupByCRMId" >> beam.GroupByKey()
        | "DeduplicateCRM" >> beam.ParDo(DeduplicateCRMFn()).with_outputs(
            DEAD_LETTER_TAG, main="valid"
        )
    )
    crm_enriched = (
        {
            "analytics": analytics | "KeyAnalytics" >> beam.WithKeys(
                lambda r: r.analytics_user_id
            ),
            "crm": dedup_output.valid | "KeyCRM" >> beam.WithKeys(
                lambda r: r["analytics_user_id"]
            ),
        }
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "JoinAnalyticsCRM" >> beam.ParDo(JoinAnalyticsCRMFn())
        | "ClassifyLead" >> beam.ParDo(
            ClassifyLeadFn("config/lead_classification_rules.json")
        )
    )
    all_records = (analytics, crm_enriched) | "FlattenAll" >> beam.Flatten()
    all_dead_letter = (
        (crm_output[DEAD_LETTER_TAG], dedup_output[DEAD_LETTER_TAG])
        | "FlattenDeadLetter" >> beam.Flatten()
    )
    return all_records, all_dead_letter, crm_enriched


def _count(pcoll, label: str):
    return pcoll | f"Count{label}" >> combiners.Count.Globally()


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_total_record_count():
    """Pipeline produces the expected total of analytics + CRM enriched records."""
    with TestPipeline() as p:
        all_records, _, _ = _build_pipeline(p)
        assert_that(_count(all_records, "Total"), equal_to([EXPECTED_TOTAL]))


def test_dead_letter_count():
    """Invalid CRM records are routed to dead-letter and not silently dropped."""
    with TestPipeline() as p:
        _, all_dead_letter, _ = _build_pipeline(p)
        assert_that(_count(all_dead_letter, "DL"), equal_to([EXPECTED_DEAD_LETTER]))


def test_dead_letter_reasons():
    """Dead-letter records carry the correct reason field."""
    with TestPipeline() as p:
        _, all_dead_letter, _ = _build_pipeline(p)

        missing = all_dead_letter | "FilterMissing" >> beam.Filter(
            lambda r: r["reason"] == "missing_analytics_user_id"
        )
        invalid = all_dead_letter | "FilterInvalid" >> beam.Filter(
            lambda r: r["reason"] == "invalid_lead_score"
        )

        assert_that(_count(missing, "Missing"), equal_to([2]), label="missing_count")
        assert_that(_count(invalid, "Invalid"), equal_to([1]), label="invalid_count")


def test_dead_letter_schema():
    """Every dead-letter record has the required fields."""
    with TestPipeline() as p:
        _, all_dead_letter, _ = _build_pipeline(p)

        required_keys = {"reason", "source_system", "raw_record", "error_timestamp"}
        malformed = all_dead_letter | beam.Filter(
            lambda r: not required_keys.issubset(r.keys())
        )
        assert_that(_count(malformed, "Malformed"), equal_to([0]))


def test_classification_distribution():
    """Lead classification matches expected counts for each tier."""
    with TestPipeline() as p:
        _, _, crm_enriched = _build_pipeline(p)

        for classification, expected in [
            ("QUALIFIED", EXPECTED_QUALIFIED),
            ("NURTURING", EXPECTED_NURTURING),
            ("COLD", EXPECTED_COLD),
            ("CONVERTED", EXPECTED_CONVERTED),
        ]:
            filtered = (
                crm_enriched
                | f"Filter{classification}" >> beam.Filter(
                    lambda r, cls=classification: r.lead_classification == cls
                )
                | f"Count{classification}" >> combiners.Count.Globally()
            )
            assert_that(filtered, equal_to([expected]), label=f"count_{classification.lower()}")


def test_crm_records_have_crm_source_system():
    """All records produced by the join carry source_system='crm'."""
    with TestPipeline() as p:
        _, _, crm_enriched = _build_pipeline(p)

        non_crm = crm_enriched | beam.Filter(lambda r: r.source_system != "crm")
        assert_that(_count(non_crm, "NonCRM"), equal_to([0]))


def test_matched_crm_records_have_campaign_name():
    """CRM records with an analytics match receive campaign_name from first-touch attribution."""
    with TestPipeline() as p:
        _, _, crm_enriched = _build_pipeline(p)

        with_campaign = crm_enriched | beam.Filter(lambda r: r.campaign_name is not None)
        assert_that(_count(with_campaign, "WithCampaign"), equal_to([EXPECTED_WITH_CAMPAIGN]))


def test_unmatched_crm_records_have_no_campaign_name():
    """CRM records with no analytics match still flow through with campaign_name=None."""
    with TestPipeline() as p:
        _, _, crm_enriched = _build_pipeline(p)

        no_campaign = crm_enriched | beam.Filter(lambda r: r.campaign_name is None)
        assert_that(
            _count(no_campaign, "NoCampaign"),
            equal_to([EXPECTED_NO_CAMPAIGN]),
        )


def test_beam_metrics():
    """Beam counters report correct values for all namespaces after a full run."""
    p = TestPipeline()
    all_records, all_dead_letter, crm_enriched = _build_pipeline(p)

    # Force all branches to execute by asserting on each output.
    assert_that(_count(all_records, "TotalM"), equal_to([EXPECTED_TOTAL]), label="total_m")
    assert_that(
        _count(all_dead_letter, "DLM"), equal_to([EXPECTED_DEAD_LETTER]), label="dl_m"
    )
    assert_that(
        _count(crm_enriched, "CRMM"), equal_to([EXPECTED_CRM_ENRICHED]), label="crm_m"
    )

    result = p.run()
    result.wait_until_finish()

    counters = {
        f"{c.key.metric.namespace}/{c.key.metric.name}": c.committed
        for c in result.metrics().query()["counters"]
    }

    assert counters.get("ga4/records_processed") == EXPECTED_GA4_PROCESSED
    assert counters.get("adobe/records_processed") == EXPECTED_ADOBE_PROCESSED
    assert counters.get("crm/records_processed") == EXPECTED_CRM_PROCESSED
    assert counters.get("crm/records_discarded") == EXPECTED_CRM_DISCARDED
    assert counters.get("join/crm_matched") == EXPECTED_CRM_MATCHED
    assert counters.get("join/crm_no_match") == EXPECTED_CRM_NO_MATCH
    assert counters.get("classify/qualified") == EXPECTED_QUALIFIED
    assert counters.get("classify/nurturing") == EXPECTED_NURTURING
    assert counters.get("classify/cold") == EXPECTED_COLD
    assert counters.get("classify/converted", 0) == EXPECTED_CONVERTED

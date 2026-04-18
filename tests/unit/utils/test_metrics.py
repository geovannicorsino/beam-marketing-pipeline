import logging

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipeline.normalize.analytics import NormalizeGA4Fn
from pipeline.normalize.crm import NormalizeCRMFn, DEAD_LETTER_TAG
from pipeline.transforms.join import JoinAnalyticsCRMFn
from pipeline.utils.metrics import log_metrics


GA4_RECORD = {
    "custom_dimension_user_id": "ga4_user_001",
    "property_id": "123",
    "property_name": "Site",
    "date": "2026-04-10",
    "source_medium": "google / cpc",
    "campaign_name": "spring_sale",
    "sessions": 2,
}

CRM_VALID = {
    "analytics_user_id": "ga4_user_001",
    "crm_id": "crm-001",
    "status": "lead",
    "date": "2026-03-15",
    "lead_score": "80",
    "product_interest": "enterprise_plan",
}

CRM_INVALID = {
    "analytics_user_id": "",
    "crm_id": "crm-bad",
    "status": "lead",
    "date": "2026-03-15",
    "lead_score": "50",
    "product_interest": None,
}


def _query(pipeline_result, namespace, name):
    for c in pipeline_result.metrics().query()["counters"]:
        if c.key.metric.namespace == namespace and c.key.metric.name == name:
            return c.committed
    return 0


def test_ga4_records_processed_counter():
    p = TestPipeline()
    p | beam.Create([GA4_RECORD, GA4_RECORD]) | beam.ParDo(NormalizeGA4Fn())
    result = p.run()
    result.wait_until_finish()
    assert _query(result, "ga4", "records_processed") == 2


def test_crm_records_processed_counter():
    p = TestPipeline()
    (
        p
        | beam.Create([CRM_VALID])
        | beam.ParDo(NormalizeCRMFn()).with_outputs(DEAD_LETTER_TAG, main="valid")
    )
    result = p.run()
    result.wait_until_finish()
    assert _query(result, "crm", "records_processed") == 1


def test_crm_records_discarded_counter():
    p = TestPipeline()
    (
        p
        | beam.Create([CRM_INVALID])
        | beam.ParDo(NormalizeCRMFn()).with_outputs(DEAD_LETTER_TAG, main="valid")
    )
    result = p.run()
    result.wait_until_finish()
    assert _query(result, "crm", "records_discarded") == 1


def test_log_metrics_returns_dict_with_counters():
    p = TestPipeline()
    p | beam.Create([GA4_RECORD]) | beam.ParDo(NormalizeGA4Fn())
    result = p.run()
    result.wait_until_finish()
    metrics = log_metrics(result)
    assert isinstance(metrics, dict)
    assert "ga4/records_processed" in metrics


def test_log_metrics_warns_on_low_match_rate(caplog):
    crm_no_match = {**CRM_VALID, "analytics_user_id": "unmatched_user"}

    p = TestPipeline()
    analytics = (
        p
        | "CreateAnalytics" >> beam.Create([])
        | "KeyAnalytics" >> beam.WithKeys(lambda r: r.analytics_user_id)
    )
    crm = (
        p
        | "CreateCRM" >> beam.Create([crm_no_match])
        | "KeyCRM" >> beam.WithKeys(lambda r: r["analytics_user_id"])
    )
    (
        {"analytics": analytics, "crm": crm}
        | beam.CoGroupByKey()
        | beam.ParDo(JoinAnalyticsCRMFn())
    )
    result = p.run()
    result.wait_until_finish()

    with caplog.at_level(logging.WARNING, logger="pipeline.utils.metrics"):
        log_metrics(result)

    assert any("match rate" in msg.lower() for msg in caplog.messages)

import json

import pytest

from pipeline.schemas.table_record import TableRecord
from pipeline.transforms.classification import ClassifyLeadFn


@pytest.fixture(scope="session")
def rules() -> dict:
    with open("config/lead_classification_rules.json") as f:
        return json.load(f)


@pytest.fixture
def fn() -> ClassifyLeadFn:
    return ClassifyLeadFn()


def _crm_record(**kwargs) -> TableRecord:
    defaults = {
        "analytics_user_id": "u001",
        "source_system": "crm",
        "status": "page_visited",
        "lead_score": 50,
    }
    return TableRecord(**{**defaults, **kwargs})


# ── Pass-through (non-CRM records) ───────────────────────────────────────────

def test_ga4_record_passes_through_unchanged(fn, rules):
    record = TableRecord(analytics_user_id="u001", source_system="ga4", sessions=3)
    result = list(fn.process(record, rules=rules))
    assert len(result) == 1
    assert result[0].lead_classification == ""


def test_adobe_record_passes_through_unchanged(fn, rules):
    record = TableRecord(analytics_user_id="u001", source_system="adobe", sessions=1)
    result = list(fn.process(record, rules=rules))
    assert len(result) == 1
    assert result[0].lead_classification == ""


# ── CONVERTED ─────────────────────────────────────────────────────────────────

def test_converted_status_converted(fn, rules):
    result = list(fn.process(_crm_record(status="converted"), rules=rules))
    assert result[0].lead_classification == "CONVERTED"


def test_converted_status_purchased(fn, rules):
    result = list(fn.process(_crm_record(status="purchased"), rules=rules))
    assert result[0].lead_classification == "CONVERTED"


def test_converted_status_subscribed(fn, rules):
    result = list(fn.process(_crm_record(status="subscribed"), rules=rules))
    assert result[0].lead_classification == "CONVERTED"


# ── QUALIFIED ─────────────────────────────────────────────────────────────────

def test_qualified_status_and_score_at_threshold(fn, rules):
    result = list(fn.process(_crm_record(status="demo_requested", lead_score=70), rules=rules))
    assert result[0].lead_classification == "QUALIFIED"


def test_qualified_status_and_score_above_threshold(fn, rules):
    result = list(fn.process(_crm_record(status="cart_abandoned", lead_score=95), rules=rules))
    assert result[0].lead_classification == "QUALIFIED"


def test_qualified_status_but_score_below_threshold_becomes_cold(fn, rules):
    result = list(fn.process(_crm_record(status="form_completed", lead_score=40), rules=rules))
    assert result[0].lead_classification == "COLD"


# ── NURTURING ─────────────────────────────────────────────────────────────────

def test_nurturing_status_email_opened(fn, rules):
    result = list(fn.process(_crm_record(status="email_opened"), rules=rules))
    assert result[0].lead_classification == "NURTURING"


def test_nurturing_status_page_visited(fn, rules):
    result = list(fn.process(_crm_record(status="page_visited"), rules=rules))
    assert result[0].lead_classification == "NURTURING"


def test_nurturing_status_retargeted(fn, rules):
    result = list(fn.process(_crm_record(status="retargeted"), rules=rules))
    assert result[0].lead_classification == "NURTURING"


# ── COLD ──────────────────────────────────────────────────────────────────────

def test_unknown_status_becomes_cold(fn, rules):
    result = list(fn.process(_crm_record(status="unsubscribed"), rules=rules))
    assert result[0].lead_classification == "COLD"


def test_empty_status_becomes_cold(fn, rules):
    result = list(fn.process(_crm_record(status=""), rules=rules))
    assert result[0].lead_classification == "COLD"


# ── Output integrity ──────────────────────────────────────────────────────────

def test_classified_record_is_table_record(fn, rules):
    result = list(fn.process(_crm_record(status="converted"), rules=rules))
    assert isinstance(result[0], TableRecord)


def test_classification_does_not_mutate_other_fields(fn, rules):
    record = _crm_record(
        status="purchased",
        crm_id="crm-001",
        lead_score=85,
        product_interest="enterprise_plan",
        campaign_name="spring_sale_2026",
    )
    result = list(fn.process(record, rules=rules))[0]
    assert result.crm_id == "crm-001"
    assert result.lead_score == 85
    assert result.product_interest == "enterprise_plan"
    assert result.campaign_name == "spring_sale_2026"
    assert result.source_system == "crm"

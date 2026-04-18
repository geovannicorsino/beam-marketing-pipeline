import pytest

from pipeline.schemas.table_record import TableRecord
from pipeline.transforms.classification import ClassifyLeadFn


@pytest.fixture
def rules_path() -> str:
    return "config/lead_classification_rules.json"


@pytest.fixture
def fn(rules_path: str) -> ClassifyLeadFn:
    classify = ClassifyLeadFn(rules_path)
    classify.setup()
    return classify


def _crm_record(**kwargs) -> TableRecord:
    defaults = {
        "analytics_user_id": "u001",
        "source_system": "crm",
        "status": "page_visited",
        "lead_score": 50,
    }
    return TableRecord(**{**defaults, **kwargs})


# ── Pass-through (non-CRM records) ───────────────────────────────────────────

def test_ga4_record_passes_through_unchanged(fn):
    record = TableRecord(analytics_user_id="u001", source_system="ga4", sessions=3)
    result = list(fn.process(record))
    assert len(result) == 1
    assert result[0].lead_classification == ""


def test_adobe_record_passes_through_unchanged(fn):
    record = TableRecord(analytics_user_id="u001", source_system="adobe", sessions=1)
    result = list(fn.process(record))
    assert len(result) == 1
    assert result[0].lead_classification == ""


# ── CONVERTED ─────────────────────────────────────────────────────────────────

def test_converted_status_converted(fn):
    record = _crm_record(status="converted")
    result = list(fn.process(record))
    assert result[0].lead_classification == "CONVERTED"


def test_converted_status_purchased(fn):
    record = _crm_record(status="purchased")
    result = list(fn.process(record))
    assert result[0].lead_classification == "CONVERTED"


def test_converted_status_subscribed(fn):
    record = _crm_record(status="subscribed")
    result = list(fn.process(record))
    assert result[0].lead_classification == "CONVERTED"


# ── QUALIFIED ─────────────────────────────────────────────────────────────────

def test_qualified_status_and_score_at_threshold(fn):
    record = _crm_record(status="demo_requested", lead_score=70)
    result = list(fn.process(record))
    assert result[0].lead_classification == "QUALIFIED"


def test_qualified_status_and_score_above_threshold(fn):
    record = _crm_record(status="cart_abandoned", lead_score=95)
    result = list(fn.process(record))
    assert result[0].lead_classification == "QUALIFIED"


def test_qualified_status_but_score_below_threshold_becomes_cold(fn):
    record = _crm_record(status="form_completed", lead_score=40)
    result = list(fn.process(record))
    assert result[0].lead_classification == "COLD"


# ── NURTURING ─────────────────────────────────────────────────────────────────

def test_nurturing_status_email_opened(fn):
    record = _crm_record(status="email_opened")
    result = list(fn.process(record))
    assert result[0].lead_classification == "NURTURING"


def test_nurturing_status_page_visited(fn):
    record = _crm_record(status="page_visited")
    result = list(fn.process(record))
    assert result[0].lead_classification == "NURTURING"


def test_nurturing_status_retargeted(fn):
    record = _crm_record(status="retargeted")
    result = list(fn.process(record))
    assert result[0].lead_classification == "NURTURING"


# ── COLD ──────────────────────────────────────────────────────────────────────

def test_unknown_status_becomes_cold(fn):
    record = _crm_record(status="unsubscribed")
    result = list(fn.process(record))
    assert result[0].lead_classification == "COLD"


def test_empty_status_becomes_cold(fn):
    record = _crm_record(status="")
    result = list(fn.process(record))
    assert result[0].lead_classification == "COLD"


# ── Output integrity ──────────────────────────────────────────────────────────

def test_classified_record_is_table_record(fn):
    record = _crm_record(status="converted")
    result = list(fn.process(record))
    assert isinstance(result[0], TableRecord)


def test_classification_does_not_mutate_other_fields(fn):
    record = _crm_record(
        status="purchased",
        crm_id="crm-001",
        lead_score=85,
        product_interest="enterprise_plan",
        campaign_name="spring_sale_2026",
    )
    result = list(fn.process(record))[0]
    assert result.crm_id == "crm-001"
    assert result.lead_score == 85
    assert result.product_interest == "enterprise_plan"
    assert result.campaign_name == "spring_sale_2026"
    assert result.source_system == "crm"

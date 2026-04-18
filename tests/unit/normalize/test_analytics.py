from pipeline.normalize.analytics import NormalizeAdobeFn, NormalizeGA4Fn
from pipeline.schemas.table_record import TableRecord

# ── GA4 ──────────────────────────────────────────────────────────────────────

GA4_VALID = {
    "property_id": "123456789",
    "property_name": "Site Corporativo",
    "date": "2026-04-10",
    "custom_dimension_user_id": "ga4_user_001",
    "source_medium": "google / cpc",
    "campaign_name": "spring_sale_2026",
    "sessions": 3,
}


def test_normalize_ga4_returns_table_record():
    fn = NormalizeGA4Fn()
    result = list(fn.process(GA4_VALID))
    assert len(result) == 1
    assert isinstance(result[0], TableRecord)


def test_normalize_ga4_maps_fields_correctly():
    fn = NormalizeGA4Fn()
    record = list(fn.process(GA4_VALID))[0]
    assert record.analytics_user_id == "ga4_user_001"
    assert record.date == "2026-04-10"
    assert record.account_id == "123456789"
    assert record.account_name == "Site Corporativo"
    assert record.source_medium == "google / cpc"
    assert record.campaign_name == "spring_sale_2026"
    assert record.sessions == 3
    assert record.source_system == "ga4"


def test_normalize_ga4_null_user_id_becomes_empty_string():
    fn = NormalizeGA4Fn()
    record = list(fn.process({**GA4_VALID, "custom_dimension_user_id": None}))[0]
    assert record.analytics_user_id == ""


def test_normalize_ga4_null_campaign_is_none():
    fn = NormalizeGA4Fn()
    record = list(fn.process({**GA4_VALID, "campaign_name": None}))[0]
    assert record.campaign_name is None


def test_normalize_ga4_source_system_is_ga4():
    fn = NormalizeGA4Fn()
    record = list(fn.process(GA4_VALID))[0]
    assert record.source_system == "ga4"


def test_normalize_ga4_crm_fields_are_default():
    fn = NormalizeGA4Fn()
    record = list(fn.process(GA4_VALID))[0]
    assert record.crm_id == ""
    assert record.status == ""
    assert record.lead_score == 0


# ── Adobe ─────────────────────────────────────────────────────────────────────

ADOBE_VALID = {
    "report_suite_id": "mycompany.prod",
    "account_name": "My Company",
    "date": "Apr 10, 2026",
    "custom_user_id": "adobe_user_001",
    "marketing_channel": "Paid Search",
    "campaign": "spring_sale_2026",
    "visits": 2,
}


def test_normalize_adobe_returns_table_record():
    fn = NormalizeAdobeFn()
    result = list(fn.process(ADOBE_VALID))
    assert len(result) == 1
    assert isinstance(result[0], TableRecord)


def test_normalize_adobe_maps_fields_correctly():
    fn = NormalizeAdobeFn()
    record = list(fn.process(ADOBE_VALID))[0]
    assert record.analytics_user_id == "adobe_user_001"
    assert record.date == "2026-04-10"
    assert record.account_id == "mycompany.prod"
    assert record.account_name == "My Company"
    assert record.source_medium == "Paid Search"
    assert record.campaign_name == "spring_sale_2026"
    assert record.sessions == 2
    assert record.source_system == "adobe"


def test_normalize_adobe_date_parsed_to_iso():
    fn = NormalizeAdobeFn()
    record = list(fn.process(ADOBE_VALID))[0]
    assert record.date == "2026-04-10"


def test_normalize_adobe_invalid_date_becomes_empty_string():
    fn = NormalizeAdobeFn()
    record = list(fn.process({**ADOBE_VALID, "date": "invalid-date"}))[0]
    assert record.date == ""


def test_normalize_adobe_null_user_id_becomes_empty_string():
    fn = NormalizeAdobeFn()
    record = list(fn.process({**ADOBE_VALID, "custom_user_id": None}))[0]
    assert record.analytics_user_id == ""


def test_normalize_adobe_source_system_is_adobe():
    fn = NormalizeAdobeFn()
    record = list(fn.process(ADOBE_VALID))[0]
    assert record.source_system == "adobe"

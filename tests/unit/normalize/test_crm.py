from pipeline.normalize.crm import NormalizeCRMFn


CRM_VALID = {
    "analytics_user_id": "ga4_user_001",
    "crm_id": "crm-001",
    "status": "lead",
    "date": "2026-03-15",
    "lead_score": "95",
    "last_interaction_days": "5",
    "product_interest": "enterprise_plan",
}


def test_normalize_crm_valid_record_yields_dict():
    fn = NormalizeCRMFn()
    result = list(fn.process(CRM_VALID))
    assert len(result) == 1
    assert isinstance(result[0], dict)


def test_normalize_crm_maps_fields_correctly():
    fn = NormalizeCRMFn()
    record = list(fn.process(CRM_VALID))[0]
    assert record["analytics_user_id"] == "ga4_user_001"
    assert record["crm_id"] == "crm-001"
    assert record["status"] == "lead"
    assert record["date"] == "2026-03-15"
    assert record["lead_score"] == 95
    assert record["product_interest"] == "enterprise_plan"


def test_normalize_crm_lead_score_coerced_to_int():
    fn = NormalizeCRMFn()
    record = list(fn.process(CRM_VALID))[0]
    assert isinstance(record["lead_score"], int)


def test_normalize_crm_empty_user_id_discarded():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": ""}))
    assert result == []


def test_normalize_crm_whitespace_user_id_discarded():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": "   "}))
    assert result == []


def test_normalize_crm_invalid_lead_score_discarded():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "lead_score": "abc"}))
    assert result == []


def test_normalize_crm_empty_product_interest_becomes_none():
    fn = NormalizeCRMFn()
    record = list(fn.process({**CRM_VALID, "product_interest": ""}))[0]
    assert record["product_interest"] is None

import json

from apache_beam import TaggedOutput

from pipeline.normalize.crm import DEAD_LETTER_TAG, NormalizeCRMFn


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


def test_normalize_crm_empty_product_interest_becomes_none():
    fn = NormalizeCRMFn()
    record = list(fn.process({**CRM_VALID, "product_interest": ""}))[0]
    assert record["product_interest"] is None


def test_normalize_crm_empty_user_id_goes_to_dead_letter():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": ""}))
    assert len(result) == 1
    assert isinstance(result[0], TaggedOutput)
    assert result[0].tag == DEAD_LETTER_TAG


def test_normalize_crm_empty_user_id_dead_letter_reason():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": ""}))
    assert result[0].value["reason"] == "missing_analytics_user_id"
    assert result[0].value["source_system"] == "crm"


def test_normalize_crm_empty_user_id_dead_letter_raw_record_is_json():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": ""}))
    raw = json.loads(result[0].value["raw_record"])
    assert raw["crm_id"] == "crm-001"


def test_normalize_crm_whitespace_user_id_goes_to_dead_letter():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "analytics_user_id": "   "}))
    assert isinstance(result[0], TaggedOutput)
    assert result[0].tag == DEAD_LETTER_TAG


def test_normalize_crm_invalid_lead_score_goes_to_dead_letter():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "lead_score": "abc"}))
    assert len(result) == 1
    assert isinstance(result[0], TaggedOutput)
    assert result[0].tag == DEAD_LETTER_TAG


def test_normalize_crm_invalid_lead_score_dead_letter_reason():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "lead_score": "abc"}))
    assert result[0].value["reason"] == "invalid_lead_score"
    assert result[0].value["source_system"] == "crm"


def test_normalize_crm_invalid_lead_score_dead_letter_raw_record_is_json():
    fn = NormalizeCRMFn()
    result = list(fn.process({**CRM_VALID, "lead_score": "abc"}))
    raw = json.loads(result[0].value["raw_record"])
    assert raw["lead_score"] == "abc"

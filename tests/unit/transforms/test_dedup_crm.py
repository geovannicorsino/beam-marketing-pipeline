from apache_beam import TaggedOutput

from pipeline.normalize.crm import DEAD_LETTER_TAG
from pipeline.transforms.dedup_crm import DeduplicateCRMFn


RECORD_A = {
    "analytics_user_id": "ga4_user_001",
    "crm_id": "crm-001",
    "status": "lead",
    "date": "2026-03-15",
    "lead_score": 95,
    "product_interest": "enterprise_plan",
}

RECORD_B = {**RECORD_A, "status": "converted", "lead_score": 80}


def _make_element(records):
    return ("crm-001", iter(records))


def test_dedup_single_record_yields_it():
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A])))
    assert len(result) == 1
    assert result[0] == RECORD_A


def test_dedup_single_record_no_dead_letter():
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A])))
    assert not any(isinstance(r, TaggedOutput) for r in result)


def test_dedup_duplicate_keeps_first():
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A, RECORD_B])))
    valid = [r for r in result if not isinstance(r, TaggedOutput)]
    assert len(valid) == 1
    assert valid[0] == RECORD_A


def test_dedup_duplicate_routes_second_to_dead_letter():
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A, RECORD_B])))
    dead = [r for r in result if isinstance(r, TaggedOutput)]
    assert len(dead) == 1
    assert dead[0].tag == DEAD_LETTER_TAG
    assert dead[0].value["reason"] == "duplicate_crm_id"
    assert dead[0].value["source_system"] == "crm"


def test_dedup_duplicate_dead_letter_has_error_timestamp():
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A, RECORD_B])))
    dead = [r for r in result if isinstance(r, TaggedOutput)]
    assert "error_timestamp" in dead[0].value


def test_dedup_three_records_keeps_first_dead_letters_two():
    record_c = {**RECORD_A, "lead_score": 50}
    fn = DeduplicateCRMFn()
    result = list(fn.process(_make_element([RECORD_A, RECORD_B, record_c])))
    valid = [r for r in result if not isinstance(r, TaggedOutput)]
    dead = [r for r in result if isinstance(r, TaggedOutput)]
    assert len(valid) == 1
    assert len(dead) == 2

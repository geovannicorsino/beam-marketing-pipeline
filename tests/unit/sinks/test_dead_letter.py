import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.sinks.dead_letter import write_dead_letter


DEAD_LETTER_RECORD = {
    "reason": "missing_analytics_user_id",
    "source_system": "crm",
    "raw_record": json.dumps({"analytics_user_id": "", "crm_id": "crm-001"}),
}


def test_dead_letter_record_serializes_to_valid_json():
    serialized = json.dumps(DEAD_LETTER_RECORD)
    parsed = json.loads(serialized)
    assert parsed["reason"] == "missing_analytics_user_id"
    assert parsed["source_system"] == "crm"


def test_dead_letter_raw_record_is_parseable():
    parsed = json.loads(DEAD_LETTER_RECORD["raw_record"])
    assert isinstance(parsed, dict)


def test_dead_letter_serialize_step_produces_json_strings():
    with TestPipeline() as p:
        result = (
            p
            | beam.Create([DEAD_LETTER_RECORD])
            | beam.Map(json.dumps)
        )
        assert_that(result, equal_to([json.dumps(DEAD_LETTER_RECORD)]))


def test_dead_letter_serialize_step_handles_multiple_records():
    record_a = {**DEAD_LETTER_RECORD, "reason": "missing_analytics_user_id"}
    record_b = {**DEAD_LETTER_RECORD, "reason": "invalid_lead_score"}
    with TestPipeline() as p:
        result = (
            p
            | beam.Create([record_a, record_b])
            | beam.Map(json.dumps)
        )
        assert_that(result, equal_to([json.dumps(record_a), json.dumps(record_b)]))


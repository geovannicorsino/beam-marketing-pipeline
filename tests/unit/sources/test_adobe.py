import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline.sources.adobe import _read_from_path


def test_read_adobe_returns_ten_records(adobe_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, adobe_fixture)
            | beam.combiners.Count.Globally()
        )
        assert_that(result, equal_to([10]))


def test_read_adobe_returns_dicts(adobe_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, adobe_fixture)
            | beam.Map(lambda r: isinstance(r, dict))
        )
        assert_that(result, equal_to([True] * 10))


def test_read_adobe_record_has_expected_fields(adobe_fixture):
    expected_keys = {
        "report_suite_id", "account_name", "date",
        "custom_user_id", "marketing_channel", "campaign", "visits",
    }
    with TestPipeline() as p:
        result = (
            _read_from_path(p, adobe_fixture)
            | beam.Map(lambda r: set(r.keys()) == expected_keys)
        )
        assert_that(result, equal_to([True] * 10))


def test_read_adobe_known_record(adobe_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, adobe_fixture)
            | beam.Filter(lambda r: r.get("custom_user_id") == "adobe_user_001")
            | beam.Map(lambda r: r["marketing_channel"])
        )
        assert_that(result, equal_to(["Paid Search"]))


def test_read_adobe_visits_is_integer(adobe_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, adobe_fixture)
            | beam.Map(lambda r: isinstance(r["visits"], int))
        )
        assert_that(result, equal_to([True] * 10))

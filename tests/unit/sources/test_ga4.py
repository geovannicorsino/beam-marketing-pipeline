import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline.sources.ga4 import _read_from_path


def test_read_ga4_returns_ten_records(ga4_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, ga4_fixture)
            | beam.combiners.Count.Globally()
        )
        assert_that(result, equal_to([10]))


def test_read_ga4_returns_dicts(ga4_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, ga4_fixture)
            | beam.Map(lambda r: isinstance(r, dict))
        )
        assert_that(result, equal_to([True] * 10))


def test_read_ga4_record_has_expected_fields(ga4_fixture):
    expected_keys = {
        "property_id", "property_name", "date",
        "custom_dimension_user_id", "source_medium",
        "campaign_name", "sessions",
    }
    with TestPipeline() as p:
        result = (
            _read_from_path(p, ga4_fixture)
            | beam.Map(lambda r: set(r.keys()) == expected_keys)
        )
        assert_that(result, equal_to([True] * 10))


def test_read_ga4_known_record(ga4_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, ga4_fixture)
            | beam.Filter(lambda r: r.get("custom_dimension_user_id") == "ga4_user_001")
            | beam.Map(lambda r: r["source_medium"])
        )
        assert_that(result, equal_to(["google / cpc"]))


def test_read_ga4_null_campaign_allowed(ga4_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, ga4_fixture)
            | beam.Filter(lambda r: r.get("campaign_name") is None)
            | beam.combiners.Count.Globally()
        )
        assert_that(result, equal_to([2]))

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline.sources.crm import _parse_csv_line, _read_from_path


def test_parse_csv_line_returns_dict():
    line = "ga4_user_001,crm-001,lead,2026-03-15,95,5,enterprise_plan"
    result = _parse_csv_line(line)
    assert isinstance(result, dict)


def test_parse_csv_line_all_columns():
    line = "ga4_user_001,crm-001,lead,2026-03-15,95,5,enterprise_plan"
    result = _parse_csv_line(line)
    assert result["analytics_user_id"] == "ga4_user_001"
    assert result["crm_id"] == "crm-001"
    assert result["status"] == "lead"
    assert result["date"] == "2026-03-15"
    assert result["lead_score"] == "95"
    assert result["last_interaction_days"] == "5"
    assert result["product_interest"] == "enterprise_plan"


def test_parse_csv_line_empty_user_id():
    line = ",crm-014,page_visited,2026-03-15,10,30,starter_plan"
    result = _parse_csv_line(line)
    assert result["analytics_user_id"] == ""


def test_parse_csv_line_empty_product_interest():
    line = "ga4_user_010,crm-010,churned,2025-10-01,5,200,"
    result = _parse_csv_line(line)
    assert result["product_interest"] == ""


def test_read_crm_returns_dicts(crm_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, crm_fixture)
            | beam.Map(lambda r: isinstance(r, dict))
        )
        assert_that(result, equal_to([True] * 15))


def test_read_crm_skips_header(crm_fixture):
    with TestPipeline() as p:
        result = (
            _read_from_path(p, crm_fixture)
            | beam.Filter(lambda r: r.get("analytics_user_id") == "analytics_user_id")
        )
        assert_that(result, equal_to([]))

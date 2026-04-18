from pipeline.schemas.table_record import TableRecord
from pipeline.transforms.join import JoinAnalyticsCRMFn

ANALYTICS_RECORD = TableRecord(
    analytics_user_id="ga4_user_001",
    date="2026-04-10",
    account_id="123456789",
    account_name="Site Corporativo",
    source_medium="google / cpc",
    campaign_name="spring_sale_2026",
    sessions=3,
    source_system="ga4",
)

CRM_RECORD = {
    "analytics_user_id": "ga4_user_001",
    "crm_id": "crm-001",
    "status": "lead",
    "date": "2026-03-15",
    "lead_score": 95,
    "product_interest": "enterprise_plan",
}


def _make_grouped(analytics=None, crm=None):
    return ("ga4_user_001", {
        "analytics": analytics or [],
        "crm": crm or [],
    })


def test_join_with_both_sides_returns_table_record():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[ANALYTICS_RECORD], crm=[CRM_RECORD])
    result = list(fn.process(element))
    assert len(result) == 1
    assert isinstance(result[0], TableRecord)


def test_join_campaign_name_from_analytics():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[ANALYTICS_RECORD], crm=[CRM_RECORD])
    result = list(fn.process(element))
    assert result[0].campaign_name == "spring_sale_2026"


def test_join_crm_fields_populated():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[ANALYTICS_RECORD], crm=[CRM_RECORD])
    record = list(fn.process(element))[0]
    assert record.crm_id == "crm-001"
    assert record.status == "lead"
    assert record.lead_score == 95
    assert record.product_interest == "enterprise_plan"


def test_join_without_analytics_match_campaign_is_none():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[], crm=[CRM_RECORD])
    result = list(fn.process(element))
    assert len(result) == 1
    assert isinstance(result[0], TableRecord)
    assert result[0].campaign_name is None


def test_join_without_analytics_match_record_is_emitted():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[], crm=[CRM_RECORD])
    record = list(fn.process(element))[0]
    assert record.crm_id == "crm-001"
    assert record.source_system == "crm"


def test_join_no_crm_yields_nothing():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[ANALYTICS_RECORD], crm=[])
    result = list(fn.process(element))
    assert result == []


def test_join_source_system_is_crm():
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[ANALYTICS_RECORD], crm=[CRM_RECORD])
    record = list(fn.process(element))[0]
    assert record.source_system == "crm"


def test_join_first_touch_attribution_picks_oldest_date():
    older = TableRecord(
        analytics_user_id="ga4_user_001",
        date="2026-01-01",
        campaign_name="first_campaign",
        source_system="ga4",
    )
    newer = TableRecord(
        analytics_user_id="ga4_user_001",
        date="2026-04-10",
        campaign_name="last_campaign",
        source_system="adobe",
    )
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[newer, older], crm=[CRM_RECORD])
    record = list(fn.process(element))[0]
    assert record.campaign_name == "first_campaign"


def test_join_analytics_without_campaign_name_is_skipped_in_attribution():
    no_campaign = TableRecord(
        analytics_user_id="ga4_user_001",
        date="2026-01-01",
        campaign_name=None,
        source_system="ga4",
    )
    with_campaign = TableRecord(
        analytics_user_id="ga4_user_001",
        date="2026-04-10",
        campaign_name="spring_sale_2026",
        source_system="adobe",
    )
    fn = JoinAnalyticsCRMFn()
    element = _make_grouped(analytics=[no_campaign, with_campaign], crm=[CRM_RECORD])
    record = list(fn.process(element))[0]
    assert record.campaign_name == "spring_sale_2026"

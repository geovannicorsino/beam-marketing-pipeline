import apache_beam as beam
from datetime import date
from pipeline.schemas.analytics import AnalyticsRecord


class NormalizeGA4Fn(beam.DoFn):
    def process(self, element: dict):

        yield AnalyticsRecord(
            date=date.fromisoformat(element["date"]),
            source_system="ga4",
            account_id=element["property_id"],
            account_name=element["property_name"],
            analytics_user_id=element["custom_dimension_user_id"],
            source_medium=element["source_medium"],
            campaign_name=element.get("campaign_name"),
            sessions=element.get("sessions", 0),
        )

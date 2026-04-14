import apache_beam as beam
from pipeline.schemas.table_record import TableRecord


class NormalizeGA4Fn(beam.DoFn):
    def process(self, element: dict):
        yield TableRecord(
            analytics_user_id=element.get("custom_dimension_user_id") or "",
            date=element.get("date", ""),
            account_id=element["property_id"],
            account_name=element["property_name"],
            source_medium=element.get("source_medium", ""),
            campaign_name=element.get("campaign_name"),
            sessions=element.get("sessions", 0),
            source_system="ga4",
        )

import apache_beam as beam
from datetime import datetime
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


class NormalizeAdobeFn(beam.DoFn):
    def process(self, element: dict):
        raw_date = element.get("date", "")
        try:
            parsed_date = datetime.strptime(raw_date, "%b %d, %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            parsed_date = ""

        yield TableRecord(
            analytics_user_id=element.get("custom_user_id") or "",
            date=parsed_date,
            account_id=element.get("report_suite_id", ""),
            account_name=element.get("account_name", ""),
            source_medium=element.get("marketing_channel", ""),
            campaign_name=element.get("campaign"),
            sessions=element.get("visits", 0),
            source_system="adobe",
        )

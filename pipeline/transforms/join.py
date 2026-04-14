import apache_beam as beam
from pipeline.schemas.table_record import TableRecord


class JoinAnalyticsCRMFn(beam.DoFn):
    def process(self, element):
        analytics_user_id, grouped = element

        crm_records = list(grouped["crm"])
        analytics_records = list(grouped["analytics"])

        if not crm_records:
            return

        crm = crm_records[0]
        campaign_name = analytics_records[0].campaign_name if analytics_records else None

        yield TableRecord(
            analytics_user_id=analytics_user_id,
            crm_id=crm["crm_id"],
            status=crm["status"],
            date=crm["date"],
            lead_score=crm["lead_score"],
            product_interest=crm["product_interest"],
            campaign_name=campaign_name,
            source_system="crm",
        )

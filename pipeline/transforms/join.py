import apache_beam as beam
from apache_beam.metrics import Metrics

from pipeline.schemas.table_record import TableRecord


class JoinAnalyticsCRMFn(beam.DoFn):
    crm_matched  = Metrics.counter("join", "crm_matched")
    crm_no_match = Metrics.counter("join", "crm_no_match")

    def process(self, element):
        analytics_user_id, grouped = element

        crm_records = list(grouped["crm"])
        analytics_records = list(grouped["analytics"])

        if not crm_records:
            return

        crm = crm_records[0]

        # First-touch attribution: pick campaign_name from the oldest analytics record
        first_touch = min(
            (r for r in analytics_records if r.date and r.campaign_name),
            key=lambda r: r.date,
            default=None,
        )
        campaign_name = first_touch.campaign_name if first_touch else None

        if analytics_records:
            self.crm_matched.inc()
        else:
            self.crm_no_match.inc()

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

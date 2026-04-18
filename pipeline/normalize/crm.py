import json
from datetime import UTC, datetime

import apache_beam as beam
from apache_beam import TaggedOutput
from apache_beam.metrics import Metrics

DEAD_LETTER_TAG = "dead_letter"


def _dead_letter(reason: str, element: dict) -> dict:
    return {
        "reason": reason,
        "source_system": "crm",
        "raw_record": json.dumps(element),
        "error_timestamp": datetime.now(UTC).isoformat(),
    }


class NormalizeCRMFn(beam.DoFn):
    records_processed = Metrics.counter("crm", "records_processed")
    records_discarded = Metrics.counter("crm", "records_discarded")

    def process(self, element: dict):
        analytics_user_id = element.get("analytics_user_id", "").strip()

        if not analytics_user_id:
            self.records_discarded.inc()
            yield TaggedOutput(DEAD_LETTER_TAG, _dead_letter("missing_analytics_user_id", element))
            return

        try:
            lead_score = int(element.get("lead_score") or 0)
        except ValueError:
            self.records_discarded.inc()
            yield TaggedOutput(DEAD_LETTER_TAG, _dead_letter("invalid_lead_score", element))
            return

        self.records_processed.inc()
        yield {
            "analytics_user_id": analytics_user_id,
            "crm_id": element.get("crm_id", ""),
            "status": element.get("status", ""),
            "date": element.get("date", ""),
            "lead_score": lead_score,
            "product_interest": element.get("product_interest") or None,
        }

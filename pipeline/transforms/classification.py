from __future__ import annotations

import apache_beam as beam
from apache_beam.metrics import Metrics

from pipeline.schemas.table_record import TableRecord


class ClassifyLeadFn(beam.DoFn):
    converted = Metrics.counter("classify", "converted")
    qualified = Metrics.counter("classify", "qualified")
    nurturing = Metrics.counter("classify", "nurturing")
    cold      = Metrics.counter("classify", "cold")

    def process(self, element: TableRecord, rules: dict):
        if element.source_system != "crm":
            yield element
            return

        status = element.status
        score = element.lead_score

        if status in rules["converted_statuses"]:
            classification = "CONVERTED"
            self.converted.inc()
        elif (
            status in rules["qualified_statuses"]
            and score >= rules["qualified_lead_score_threshold"]
        ):
            classification = "QUALIFIED"
            self.qualified.inc()
        elif status in rules["nurturing_statuses"]:
            classification = "NURTURING"
            self.nurturing.inc()
        else:
            classification = "COLD"
            self.cold.inc()

        yield TableRecord(**{**element.__dict__, "lead_classification": classification})

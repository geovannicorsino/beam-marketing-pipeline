from __future__ import annotations

import json

import apache_beam as beam
from apache_beam.metrics import Metrics

from pipeline.schemas.table_record import TableRecord


class ClassifyLeadFn(beam.DoFn):
    converted  = Metrics.counter("classify", "converted")
    qualified  = Metrics.counter("classify", "qualified")
    nurturing  = Metrics.counter("classify", "nurturing")
    cold       = Metrics.counter("classify", "cold")

    def __init__(self, rules_path: str) -> None:
        self.rules_path = rules_path
        self.rules: dict | None = None

    def setup(self) -> None:
        with open(self.rules_path) as f:
            self.rules = json.load(f)

    def process(self, element: TableRecord):
        if element.source_system != "crm":
            yield element
            return

        rules = self.rules
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

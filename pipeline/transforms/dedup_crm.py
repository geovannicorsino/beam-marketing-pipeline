import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import TaggedOutput

from pipeline.normalize.crm import DEAD_LETTER_TAG


def _dead_letter_duplicate(crm_id: str, record: dict) -> dict:
    return {
        "reason": "duplicate_crm_id",
        "source_system": "crm",
        "raw_record": json.dumps(record),
        "error_timestamp": datetime.now(timezone.utc).isoformat(),
    }


class DeduplicateCRMFn(beam.DoFn):
    def process(self, element):
        crm_id, records = element
        records = list(records)

        yield records[0]

        for duplicate in records[1:]:
            yield TaggedOutput(DEAD_LETTER_TAG, _dead_letter_duplicate(crm_id, duplicate))

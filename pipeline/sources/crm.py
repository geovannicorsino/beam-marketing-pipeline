import csv
import io

import apache_beam as beam
from apache_beam.io import ReadFromText

COLUMNS = [
    "analytics_user_id", "crm_id", "status", "date",
    "lead_score", "last_interaction_days", "product_interest",
]


def _parse_csv_line(line: str) -> dict:
    reader = csv.DictReader(io.StringIO(line), fieldnames=COLUMNS)
    return next(reader)


def _read_from_path(pipeline, pattern: str):
    return (
        pipeline
        | "ReadCRMRaw" >> ReadFromText(pattern, skip_header_lines=1)
        | "ParseCRMCSV" >> beam.Map(_parse_csv_line)
    )


def read_crm(pipeline, bucket: str):
    pattern = f"gs://{bucket}/crm/files/data.csv"
    return _read_from_path(pipeline, pattern)

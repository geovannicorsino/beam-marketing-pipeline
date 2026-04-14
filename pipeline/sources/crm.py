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


def read_crm(pipeline, bucket: str, account_id: str, date: str):
    pattern = f"data/fixtures/crm_2026-04-10.csv"
    # pattern = f"gs://{bucket}/raw/crm/leads/account_id={account_id}/date={date}/*.csv"
    return (
        pipeline
        | "ReadCRMRaw" >> ReadFromText(pattern, skip_header_lines=1)
        | "ParseCRMCSV" >> beam.Map(_parse_csv_line)
    )

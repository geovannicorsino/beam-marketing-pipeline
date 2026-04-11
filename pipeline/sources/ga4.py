import json
import apache_beam as beam
from apache_beam.io import ReadFromText


def read_ga4(pipeline, bucket: str, report: str, account_id: str, date: str):
    # pattern = f"gs://{bucket}/raw/ga4/{report}/account_id={account_id}/date={date}/*.json"
    pattern = "data/fixtures/ga4.json"
    return (
        pipeline
        | "ReadGA4Raw" >> ReadFromText(pattern)
        | "ParseGA4JSON" >> beam.Map(json.loads)
    )

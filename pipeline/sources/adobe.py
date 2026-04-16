import apache_beam as beam
from apache_beam.io import ReadFromParquet


def _read_from_path(pipeline, pattern: str):
    return (
        pipeline
        | "ReadAdobeRaw" >> ReadFromParquet(pattern, as_rows=False)
    )


def read_adobe(pipeline, bucket: str, report: str, account_id: str, date: str):
    # pattern = f"gs://{bucket}/raw/adobe/{report}/account_id={account_id}/date={date}/*.parquet"
    pattern = "data/fixtures/adobe_analytics.parquet"
    return _read_from_path(pipeline, pattern)

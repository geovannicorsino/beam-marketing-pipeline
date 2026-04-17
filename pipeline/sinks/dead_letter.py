import json

import apache_beam as beam
from apache_beam.io import WriteToText


def write_dead_letter(pcollection, bucket: str, run_date: str):
    output_path = f"gs://{bucket}/dead-letter/date={run_date}/{run_date}"
    return (
        pcollection
        | "SerializeDeadLetter" >> beam.Map(json.dumps)
        | "WriteDeadLetter" >> WriteToText(output_path, file_name_suffix=".json")
    )

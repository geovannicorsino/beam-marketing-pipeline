import json

import apache_beam as beam
from apache_beam.io import WriteToText


def write_dead_letter(pcollection, output_path: str):
    return (
        pcollection
        | "SerializeDeadLetter" >> beam.Map(json.dumps)
        | "WriteDeadLetter" >> WriteToText(output_path, file_name_suffix=".json")
    )

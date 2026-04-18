import json
from urllib.parse import urlparse


def load_json_config(path: str) -> dict:
    """Load a JSON config file from a local path or GCS URI (gs://)."""
    if path.startswith("gs://"):
        from google.cloud import storage

        parsed = urlparse(path)
        client = storage.Client()
        blob = client.bucket(parsed.netloc).blob(parsed.path.lstrip("/"))
        return json.loads(blob.download_as_text())

    with open(path) as f:
        return json.load(f)

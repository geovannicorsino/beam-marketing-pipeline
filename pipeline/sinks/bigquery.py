import dataclasses

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

LEADS_ENRICHED_SCHEMA = {
    "fields": [
        {"name": "analytics_user_id",   "type": "STRING",  "mode": "REQUIRED"},
        {"name": "date",                "type": "STRING",  "mode": "NULLABLE"},
        {"name": "crm_id",              "type": "STRING",  "mode": "NULLABLE"},
        {"name": "status",              "type": "STRING",  "mode": "NULLABLE"},
        {"name": "lead_score",          "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "product_interest",    "type": "STRING",  "mode": "NULLABLE"},
        {"name": "campaign_name",       "type": "STRING",  "mode": "NULLABLE"},
        {"name": "account_id",          "type": "STRING",  "mode": "NULLABLE"},
        {"name": "account_name",        "type": "STRING",  "mode": "NULLABLE"},
        {"name": "source_medium",       "type": "STRING",  "mode": "NULLABLE"},
        {"name": "sessions",            "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "conversions",         "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_revenue",       "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "source_system",       "type": "STRING",  "mode": "NULLABLE"},
        {"name": "lead_classification", "type": "STRING",  "mode": "NULLABLE"},
        {"name": "processing_date",     "type": "DATE",    "mode": "NULLABLE"},
        {"name": "pipeline_run_id",     "type": "STRING",  "mode": "NULLABLE"},
    ]
}


def write_leads_enriched(
    pcollection,
    project: str,
    run_date: str,
    dataset: str = "marketing_analytics_silver",
):
    # Partition decorator $YYYYMMDD — WRITE_TRUNCATE truncates only that partition,
    # preserving all other dates in history.
    partition = run_date.replace("-", "")
    table = f"{project}:{dataset}.leads_enriched${partition}"
    return (
        pcollection
        | "ToLeadsDict" >> beam.Map(
            lambda r: {**dataclasses.asdict(r), "processing_date": run_date}
        )
        | "WriteLeadsToBQ" >> WriteToBigQuery(
            table,
            schema=LEADS_ENRICHED_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {"type": "DAY", "field": "processing_date"},
            },
        )
    )

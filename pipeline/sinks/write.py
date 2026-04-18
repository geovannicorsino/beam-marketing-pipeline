from pipeline.sinks.bigquery import write_leads_enriched
from pipeline.sinks.dead_letter import write_dead_letter


def write_pipeline_output(
    all_records,
    all_dead_letter,
    project: str,
    run_date: str,
    bucket: str,
):
    """Write enriched records to BigQuery and dead-letter records to GCS."""
    write_leads_enriched(all_records, project=project, run_date=run_date)
    write_dead_letter(
        all_dead_letter,
        output_path=f"gs://{bucket}/dead-letter/date={run_date}/{run_date}",
    )

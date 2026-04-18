import json

import apache_beam as beam
from apache_beam import combiners
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions
from apache_beam.pvalue import AsSingleton

from pipeline.ingest import build_analytics, build_crm
from pipeline.options import MarketingPipelineOptions
from pipeline.sinks.write import write_pipeline_output
from pipeline.transforms.classification import ClassifyLeadFn
from pipeline.transforms.join import JoinAnalyticsCRMFn
from pipeline.utils.config import load_json_config
from pipeline.utils.metrics import log_metrics

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)
project = options.view_as(GoogleCloudOptions).project
runner = options.view_as(StandardOptions).runner or "DirectRunner"

accounts_path = known.accounts_path or f"gs://{known.bucket}/config/accounts.json"
rules_path = known.rules_path or f"gs://{known.bucket}/config/lead_classification_rules.json"

accounts = load_json_config(accounts_path)
account_ids = accounts["account_ids"]

with beam.Pipeline(options=options) as p:
    rules = (
        p
        | "ReadRules" >> ReadFromText(rules_path)
        | "CollectRules" >> combiners.ToList()
        | "ParseRules" >> beam.Map(lambda lines: json.loads("".join(lines)))
    )

    analytics = build_analytics(p, bucket=known.bucket, account_ids=account_ids, date=known.date)
    crm_valid, crm_dead_letter = build_crm(p, bucket=known.bucket)

    crm_enriched = (
        {
            "analytics": analytics | "KeyAnalytics" >> beam.WithKeys(
                lambda r: r.analytics_user_id
            ),
            "crm": crm_valid | "KeyCRM" >> beam.WithKeys(
                lambda r: r["analytics_user_id"]
            ),
        }
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "JoinAnalyticsCRM" >> beam.ParDo(JoinAnalyticsCRMFn())
        | "ClassifyLead" >> beam.ParDo(ClassifyLeadFn(), rules=AsSingleton(rules))
    )

    all_records = (analytics, crm_enriched) | "FlattenAll" >> beam.Flatten()

    write_pipeline_output(
        all_records=all_records,
        all_dead_letter=crm_dead_letter,
        project=project,
        run_date=known.date,
        bucket=known.bucket,
    )

log_metrics(p.result)

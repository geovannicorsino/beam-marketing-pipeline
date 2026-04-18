import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions

from pipeline.normalize.analytics import NormalizeAdobeFn, NormalizeGA4Fn
from pipeline.normalize.crm import DEAD_LETTER_TAG, NormalizeCRMFn
from pipeline.options import MarketingPipelineOptions
from pipeline.sinks.bigquery import write_leads_enriched
from pipeline.sinks.dead_letter import write_dead_letter
from pipeline.sources.adobe import read_adobe
from pipeline.sources.crm import read_crm
from pipeline.sources.ga4 import read_ga4
from pipeline.transforms.classification import ClassifyLeadFn
from pipeline.transforms.dedup_crm import DeduplicateCRMFn
from pipeline.transforms.join import JoinAnalyticsCRMFn
from pipeline.utils.metrics import log_metrics

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)
project = options.view_as(GoogleCloudOptions).project
runner = options.view_as(StandardOptions).runner or "DirectRunner"


with beam.Pipeline(options=options) as p:
    ga4 = (
        read_ga4(p, bucket=known.bucket, report="sessions",
                 account_id=project, date=known.date)
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
    )

    adobe = (
        read_adobe(p, bucket=known.bucket, report="sessions",
                   account_id=project, date=known.date)
        | "NormalizeAdobe" >> beam.ParDo(NormalizeAdobeFn())
    )

    analytics = (ga4, adobe) | "FlattenAnalytics" >> beam.Flatten()

    crm_output = (
        read_crm(p, bucket=known.bucket,
                 account_id=project, date=known.date)
        | "NormalizeCRM" >> beam.ParDo(NormalizeCRMFn()).with_outputs(
            DEAD_LETTER_TAG, main="valid"
        )
    )

    dedup_output = (
        crm_output.valid
        | "KeyCRMById" >> beam.WithKeys(lambda r: r["crm_id"])
        | "GroupByCRMId" >> beam.GroupByKey()
        | "DeduplicateCRM" >> beam.ParDo(DeduplicateCRMFn()).with_outputs(
            DEAD_LETTER_TAG, main="valid"
        )
    )

    crm_enriched = (
        {
            "analytics": analytics | "KeyAnalytics" >> beam.WithKeys(lambda r: r.analytics_user_id),
            "crm": dedup_output.valid | "KeyCRM" >> beam.WithKeys(lambda r: r["analytics_user_id"]),
        }
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "JoinAnalyticsCRM" >> beam.ParDo(JoinAnalyticsCRMFn())
        | "ClassifyLead" >> beam.ParDo(ClassifyLeadFn("config/lead_classification_rules.json"))
    )

    all_records = (analytics, crm_enriched) | "FlattenAll" >> beam.Flatten()

    all_dead_letter = (
        (crm_output[DEAD_LETTER_TAG], dedup_output[DEAD_LETTER_TAG])
        | "FlattenDeadLetter" >> beam.Flatten()
    )

    write_leads_enriched(
        all_records, project=project, run_date=known.date)
    write_dead_letter(
        all_dead_letter, output_path=f"gs://{known.bucket}/dead-letter/date={known.date}/{known.date}")

log_metrics(p.result)

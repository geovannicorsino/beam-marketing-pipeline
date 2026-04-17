import apache_beam as beam
from pipeline.normalize.analytics import NormalizeAdobeFn, NormalizeGA4Fn
from pipeline.normalize.crm import DEAD_LETTER_TAG, NormalizeCRMFn
from pipeline.options import MarketingPipelineOptions
from pipeline.sinks.dead_letter import write_dead_letter
from pipeline.sources.adobe import read_adobe
from pipeline.sources.crm import read_crm
from pipeline.sources.ga4 import read_ga4
from pipeline.transforms.classification import ClassifyLeadFn
from pipeline.transforms.dedup_crm import DeduplicateCRMFn
from pipeline.transforms.join import JoinAnalyticsCRMFn

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)

with beam.Pipeline(options=options) as p:
    ga4 = (
        read_ga4(p, bucket=known.bucket, report="sessions",
                 account_id=known.project_id, date=known.date)
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
    )

    adobe = (
        read_adobe(p, bucket=known.bucket, report="sessions",
                   account_id=known.project_id, date=known.date)
        | "NormalizeAdobe" >> beam.ParDo(NormalizeAdobeFn())
    )

    analytics = (ga4, adobe) | "FlattenAnalytics" >> beam.Flatten()

    crm_output = (
        read_crm(p, bucket=known.bucket,
                 account_id=known.project_id, date=known.date)
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

    all_records | "PrintRecords" >> beam.Map(print)
    write_dead_letter(all_dead_letter, bucket=known.bucket, run_date=known.date)

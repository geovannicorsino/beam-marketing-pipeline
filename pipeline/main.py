import apache_beam as beam
from pipeline.options import MarketingPipelineOptions
from pipeline.sources.ga4 import read_ga4
from pipeline.sources.crm import read_crm
from pipeline.normalize.analytics import NormalizeGA4Fn
from pipeline.normalize.crm import NormalizeCRMFn
from pipeline.transforms.join import JoinAnalyticsCRMFn

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)

with beam.Pipeline(options=options) as p:
    ga4 = (
        read_ga4(p, bucket=known.bucket, report="sessions",
                 account_id=known.project_id, date=known.date)
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
    )

    crm = (
        read_crm(p, bucket=known.bucket,
                 account_id=known.project_id, date=known.date)
        | "NormalizeCRM" >> beam.ParDo(NormalizeCRMFn())
    )

    crm_enriched = (
        {"analytics": ga4 | "KeyGA4" >> beam.WithKeys(lambda r: r.analytics_user_id),
         "crm": crm | "KeyCRM" >> beam.WithKeys(lambda r: r["analytics_user_id"])}
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "JoinAnalyticsCRM" >> beam.ParDo(JoinAnalyticsCRMFn())
    )

    all_records = (ga4, crm_enriched) | "Flatten" >> beam.Flatten()

    all_records | "PrintRecords" >> beam.Map(print)

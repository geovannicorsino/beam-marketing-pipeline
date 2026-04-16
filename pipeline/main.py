import apache_beam as beam
from pipeline.options import MarketingPipelineOptions
from pipeline.sources.ga4 import read_ga4
from pipeline.sources.adobe import read_adobe
from pipeline.sources.crm import read_crm
from pipeline.normalize.analytics import NormalizeGA4Fn, NormalizeAdobeFn
from pipeline.normalize.crm import NormalizeCRMFn
from pipeline.transforms.join import JoinAnalyticsCRMFn

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)

with beam.Pipeline(options=options) as p:
    ga4 = (
        read_ga4(p, bucket=known.bucket, report="sessions", account_id=known.project_id, date=known.date)
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
    )

    adobe = (
        read_adobe(p, bucket=known.bucket, report="sessions", account_id=known.project_id, date=known.date)
        | "NormalizeAdobe" >> beam.ParDo(NormalizeAdobeFn())
    )

    analytics = (ga4, adobe) | "FlattenAnalytics" >> beam.Flatten()

    crm = (
        read_crm(p, bucket=known.bucket, account_id=known.project_id, date=known.date)
        | "NormalizeCRM" >> beam.ParDo(NormalizeCRMFn())
    )

    crm_enriched = (
        {"analytics": analytics | "KeyAnalytics" >> beam.WithKeys(lambda r: r.analytics_user_id),
         "crm": crm | "KeyCRM" >> beam.WithKeys(lambda r: r["analytics_user_id"])}
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "JoinAnalyticsCRM" >> beam.ParDo(JoinAnalyticsCRMFn())
    )

    all_records = (analytics, crm_enriched) | "FlattenAll" >> beam.Flatten()

    all_records | "PrintRecords" >> beam.Map(print)

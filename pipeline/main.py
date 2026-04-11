import apache_beam as beam
from pipeline.options import MarketingPipelineOptions
from pipeline.sources.ga4 import read_ga4
from pipeline.normalize.analytics import NormalizeGA4Fn

options = MarketingPipelineOptions()
known = options.view_as(MarketingPipelineOptions)

with beam.Pipeline(options=options) as p:
    records = (
        read_ga4(
            p,
            bucket=known.bucket,
            report="sessions",
            account_id=known.project_id,
            date=known.date,
        )
        | "NormalizeGA4" >> beam.ParDo(NormalizeGA4Fn())
        | "PrintRecords" >> beam.Map(print)
    )

from apache_beam.options.pipeline_options import PipelineOptions


class MarketingPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--bucket",
            required=True,
            help="GCS bucket do cliente",
        )
        parser.add_argument(
            "--project_id",
            required=True,
            help="GCP project ID",
        )
        parser.add_argument(
            "--date",
            required=True,
            help="Data de processamento yyyy-mm-dd",
        )

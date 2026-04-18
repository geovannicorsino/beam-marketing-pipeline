from apache_beam.options.pipeline_options import PipelineOptions


class MarketingPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--bucket",
            required=True,
            help="GCS bucket for raw inputs and dead-letter output.",
        )
        parser.add_argument(
            "--date",
            required=True,
            help="Processing date in yyyy-mm-dd format.",
        )
        parser.add_argument(
            "--rules_path",
            default=None,
            help="GCS or local path to lead_classification_rules.json.",
        )
        parser.add_argument(
            "--accounts_path",
            default=None,
            help="GCS or local path to accounts.json.",
        )

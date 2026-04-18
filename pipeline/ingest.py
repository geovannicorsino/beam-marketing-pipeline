import apache_beam as beam

from pipeline.normalize.analytics import NormalizeAdobeFn, NormalizeGA4Fn
from pipeline.normalize.crm import DEAD_LETTER_TAG, NormalizeCRMFn
from pipeline.sources.adobe import read_adobe
from pipeline.sources.crm import read_crm
from pipeline.sources.ga4 import read_ga4
from pipeline.transforms.dedup_crm import DeduplicateCRMFn


def build_analytics(p, bucket: str, account_ids: list[str], date: str):
    """Read and normalize GA4 + Adobe for every account into a single PCollection."""
    reads = []
    for account_id in account_ids:
        ga4 = (
            read_ga4(p, bucket=bucket, report="sessions", account_id=account_id, date=date)
            | f"NormalizeGA4_{account_id}" >> beam.ParDo(NormalizeGA4Fn())
        )
        adobe = (
            read_adobe(p, bucket=bucket, report="sessions", account_id=account_id, date=date)
            | f"NormalizeAdobe_{account_id}" >> beam.ParDo(NormalizeAdobeFn())
        )
        reads.extend([ga4, adobe])
    return tuple(reads) | "FlattenAnalytics" >> beam.Flatten()


def build_crm(p, bucket: str):
    """Read, normalize, and deduplicate CRM records.

    Returns:
        crm_valid:       PCollection[dict] — clean, deduped CRM records
        crm_dead_letter: PCollection[dict] — invalid / duplicate records
    """
    crm_output = (
        read_crm(p, bucket=bucket)
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
    crm_dead_letter = (
        (crm_output[DEAD_LETTER_TAG], dedup_output[DEAD_LETTER_TAG])
        | "FlattenCRMDeadLetter" >> beam.Flatten()
    )
    return dedup_output.valid, crm_dead_letter

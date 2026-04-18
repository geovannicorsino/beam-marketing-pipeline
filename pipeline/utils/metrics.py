import logging

logger = logging.getLogger(__name__)

MATCH_RATE_THRESHOLD = 0.70


def log_metrics(result) -> dict:
    query = result.metrics().query()

    counters = {
        f"{c.key.metric.namespace}/{c.key.metric.name}": c.committed
        for c in query["counters"]
    }

    for name, value in sorted(counters.items()):
        logger.info("metric %s = %d", name, value)

    matched  = counters.get("join/crm_matched", 0)
    no_match = counters.get("join/crm_no_match", 0)
    total    = matched + no_match

    if total > 0:
        match_rate = matched / total
        if match_rate < MATCH_RATE_THRESHOLD:
            logger.warning(
                "Low CRM match rate: %.1f%% (%d matched / %d total) — threshold is %.0f%%",
                match_rate * 100,
                matched,
                total,
                MATCH_RATE_THRESHOLD * 100,
            )

    return counters

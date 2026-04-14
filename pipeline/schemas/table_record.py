from __future__ import annotations
from dataclasses import dataclass


@dataclass
class TableRecord:
    analytics_user_id: str
    date: str = ""

    # CRM
    crm_id: str = ""
    status: str = ""
    lead_score: int = 0
    product_interest: str | None = None

    # GA4
    campaign_name: str | None = None
    account_id: str = ""
    account_name: str = ""
    source_medium: str = ""
    sessions: int = 0
    conversions: int = 0
    total_revenue: float = 0.0

    # metadados do pipeline
    source_system: str = ""
    lead_classification: str = ""
    processing_date: str = ""
    pipeline_run_id: str = ""

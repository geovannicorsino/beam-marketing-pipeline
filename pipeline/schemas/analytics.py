from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class AnalyticsRecord:
    date: date
    source_system: str
    account_id: str
    account_name: str
    analytics_user_id: str
    source_medium: str
    campaign_name: str | None
    sessions: int = 0
    conversions: int = 0
    total_revenue: float = 0.0

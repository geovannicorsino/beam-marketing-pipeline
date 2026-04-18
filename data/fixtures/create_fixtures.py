"""
Generates data/fixtures/adobe_analytics.parquet.
Run once before executing tests:
    python data/fixtures/create_fixtures.py
"""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT = Path(__file__).parent / "adobe_analytics.parquet"

data = {
    "report_suite_id": [
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
        "mycompany.prod",
    ],
    "account_name": [
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
        "My Company",
    ],
    "date": [
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
        "Apr 10, 2026",
    ],
    "custom_user_id": [
        "adobe_user_001",
        "adobe_user_002",
        "adobe_user_003",
        "adobe_user_004",
        "adobe_user_005",
        "adobe_user_006",
        "adobe_user_007",
        "adobe_user_008",
        # ga4_user_003 also appears in GA4 — tests cross-source first-touch attribution
        "ga4_user_003",
        # null custom_user_id — analytics_user_id becomes ""
        None,
    ],
    "marketing_channel": [
        "Paid Search",
        "Email",
        "Natural Search",
        "Paid Search",
        "Display",
        "Direct",
        "Social Networks",
        "Natural Search",
        "Natural Search",
        "Paid Search",
    ],
    "campaign": [
        "spring_sale_2026",
        "april_nurture",
        None,
        "retargeting_q2",
        "display_awareness",
        None,
        None,
        "brand_search",
        None,
        "spring_sale_2026",
    ],
    "visits": [2, 1, 3, 1, 2, 1, 4, 1, 2, 1],
}

schema = pa.schema([
    pa.field("report_suite_id", pa.string()),
    pa.field("account_name", pa.string()),
    pa.field("date", pa.string()),
    pa.field("custom_user_id", pa.string()),
    pa.field("marketing_channel", pa.string()),
    pa.field("campaign", pa.string()),
    pa.field("visits", pa.int64()),
])

table = pa.Table.from_pydict(data, schema=schema)
pq.write_table(table, OUTPUT)
print(f"Created: {OUTPUT} ({len(table)} records)")

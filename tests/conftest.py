from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    return Path(__file__).parent.parent / "data" / "fixtures"


@pytest.fixture(scope="session")
def ga4_fixture(fixture_dir) -> str:
    return str(fixture_dir / "ga4.json")


@pytest.fixture(scope="session")
def adobe_fixture(fixture_dir) -> str:
    return str(fixture_dir / "adobe_analytics.parquet")


@pytest.fixture(scope="session")
def crm_fixture(fixture_dir) -> str:
    return str(fixture_dir / "crm_2026-04-10.csv")

from datetime import date

import pytest

from veloparc_gen.config import load_config
from veloparc_gen.pipeline import build_world


def small_config(countries=("PT",), start=date(2026, 9, 1), end=date(2026, 9, 3), **kwargs):
    return load_config(start=start, end=end, countries=list(countries), **kwargs)


@pytest.fixture(scope="session")
def clean_world():
    """1 country, 3 days, anomalies off (the acceptance case for R1-R16)."""
    return build_world(small_config(anomalies_enabled=False))

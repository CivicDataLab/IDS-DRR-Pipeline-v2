"""Shared fixtures.

The parity tests run against seeded reference data, which is not committed.
``pytest.importorskip``-style skipping keeps the suite green on a fresh
clone; run ``python scripts/seed_data.py`` to enable them.
"""

import pandas as pd
import pytest

from pipeline.sources import staging_layout
from pipeline.state_config import load_state_config

STATE = "himachal_pradesh"

SEED_HINT = "seed data not present — run `python scripts/seed_data.py` to enable parity tests"


def golden_dir():
    return staging_layout.reference_dir(STATE) / "golden"


@pytest.fixture(scope="session")
def state() -> str:
    return STATE


@pytest.fixture(scope="session")
def state_cfg() -> dict:
    return load_state_config(STATE)


@pytest.fixture(scope="session")
def golden_master() -> pd.DataFrame:
    path = golden_dir() / "MASTER_VARIABLES.csv"
    if not path.exists():
        pytest.skip(SEED_HINT)
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def golden_factor():
    def _load(name: str) -> pd.DataFrame:
        path = golden_dir() / f"factor_scores_l1_{name}.csv"
        if not path.exists():
            pytest.skip(SEED_HINT)
        return pd.read_csv(path)

    return _load


@pytest.fixture(scope="session")
def golden_risk_scores() -> pd.DataFrame:
    path = golden_dir() / "risk_score_final_district.csv"
    if not path.exists():
        pytest.skip(SEED_HINT)
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def staged_variables_present() -> bool:
    path = staging_layout.variables_dir(STATE, "imd", "mean_rain")
    if not path.exists() or not any(path.glob("*.csv")):
        pytest.skip(SEED_HINT)
    return True

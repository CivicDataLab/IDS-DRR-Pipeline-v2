"""Canonical staging-area layout for the multi-state pipeline.

Every extraction source writes per-variable monthly CSVs to the same
contract, mirroring the flood-data-ecosystem repos' convention::

    staging/variables/{state}/{source}/{variable}/{variable}_{YYYY_MM}.csv

Each CSV has an ``object_id`` column (admin-boundary join key) plus one or
more value columns. Annual variables use ``{variable}_{YYYY}.csv``.
Derived layers live next to it::

    staging/master/{state}/MASTER_VARIABLES.csv
    staging/factors/{state}/factor_scores_l1_{factor}.csv
    staging/outputs/{state}/risk_score.csv, risk_score_final_district.csv

``inbox/{state}/{source}/`` (repo root) is the drop folder for manual
sources; validated files are transformed into the contract above and moved
to ``inbox/{state}/{source}/processed/``.
"""

from pathlib import Path

import pandas as pd

from pipeline.state_config import REPO_ROOT

STAGING_ROOT = REPO_ROOT / "staging"
INBOX_ROOT = REPO_ROOT / "inbox"
REFERENCE_ROOT = REPO_ROOT / "data" / "reference"


def month_key_to_timeperiod(month_key: str) -> str:
    """Dagster month partition key ("2026-06-01") -> ecosystem timeperiod ("2026_06")."""
    return month_key[:7].replace("-", "_")


def timeperiod_to_year_month(timeperiod: str) -> tuple[int, int]:
    """Ecosystem timeperiod ("2026_06") -> (2026, 6)."""
    year, month = timeperiod.split("_")
    return int(year), int(month)


def variables_dir(state: str, source: str, variable: str) -> Path:
    return STAGING_ROOT / "variables" / state / source / variable


def variable_csv_path(state: str, source: str, variable: str, timeperiod: str) -> Path:
    """Path of one variable CSV; ``timeperiod`` is ``YYYY_MM`` or ``YYYY`` (annual)."""
    return variables_dir(state, source, variable) / f"{variable}_{timeperiod}.csv"


def master_csv_path(state: str) -> Path:
    return STAGING_ROOT / "master" / state / "MASTER_VARIABLES.csv"


def factors_dir(state: str) -> Path:
    return STAGING_ROOT / "factors" / state


def factor_csv_path(state: str, factor: str) -> Path:
    return factors_dir(state) / f"factor_scores_l1_{factor}.csv"


def outputs_dir(state: str) -> Path:
    return STAGING_ROOT / "outputs" / state


def inbox_dir(state: str, source: str) -> Path:
    return INBOX_ROOT / state / source


def reference_dir(state: str) -> Path:
    return REFERENCE_ROOT / state


def shared_dir(source: str) -> Path:
    """State-independent staging (e.g. national IMD grids)."""
    return STAGING_ROOT / "shared" / source


def write_variable_csv(
    df: pd.DataFrame,
    state: str,
    source: str,
    variable: str,
    timeperiod: str,
    value_columns: list[str] | None = None,
) -> Path:
    """Validate and write one variable CSV to the staging contract.

    The frame must carry ``object_id``; ``value_columns`` restricts which
    value columns are written (all non-key columns when omitted).
    """
    if "object_id" not in df.columns:
        raise ValueError(
            f"{source}/{variable}: dataframe has no 'object_id' column "
            f"(columns: {list(df.columns)})"
        )
    if value_columns is not None:
        missing = [c for c in value_columns if c not in df.columns]
        if missing:
            raise ValueError(f"{source}/{variable}: missing value columns {missing}")
        df = df[["object_id", *value_columns]]
    if len(df.columns) < 2:
        raise ValueError(f"{source}/{variable}: no value columns to write")

    path = variable_csv_path(state, source, variable, timeperiod)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path

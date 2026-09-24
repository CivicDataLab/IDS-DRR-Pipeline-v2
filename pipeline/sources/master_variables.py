"""Build a state's MASTER_VARIABLES panel from the staging variables contract.

Port of ``Sources/master.py`` + ``Sources/master2.py`` from the
flood-data-ecosystem repositories, driven by the ``master:`` section of the
state YAML instead of hardcoded paths:

1. A tehsil x month skeleton is built from the admin-boundary GeoJSON and
   the configured start month.
2. Monthly variables are merged on ``object_id`` x ``timeperiod`` (variables
   marked ``level: district`` are merged on the district prefix of
   ``object_id`` instead).
3. Annual variables are merged on ``object_id`` x year; one-time variables
   (from ``data/reference/<state>/variables/``) on ``object_id`` alone.
4. Configured imputations run (per-object mean for rain/runoff, per-district
   mean/median for Antyodaya variables), then remaining gaps fill with 0.
"""

import json
import logging
import re

import pandas as pd

from pipeline.sources import staging_layout
from pipeline.state_config import boundaries_path

logger = logging.getLogger(__name__)

MONTHLY_FILE_RE = re.compile(r"_(\d{4}_\d{2})\.csv$")
ANNUAL_FILE_RE = re.compile(r"_(\d{4})\.csv$")

# Skeleton columns taken from the boundary GeoJSON properties
# (master2.py: TEHSIL, object_id, dtcode11, tehsil_area, District).
SKELETON_PROPERTIES = {
    "TEHSIL": "TEHSIL",
    "object_id": "object_id",
    "dtcode11": "dtcode11",
    "tehsil_area": "tehsil_area",
    "District": "district",
}


def _load_skeleton(state: str) -> pd.DataFrame:
    path = boundaries_path(state)
    if path is None or not path.exists():
        raise FileNotFoundError(
            f"No admin boundary GeoJSON for {state} "
            f"(expected at {path}; run scripts/seed_data.py)"
        )
    with open(path) as f:
        geojson = json.load(f)
    rows = [feat.get("properties", {}) for feat in geojson.get("features", [])]
    df = pd.DataFrame(rows)
    missing = [c for c in SKELETON_PROPERTIES if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing boundary properties {missing}")
    df = df[list(SKELETON_PROPERTIES)].rename(columns=SKELETON_PROPERTIES)
    return df


def _read_variable_folder(
    state: str, source: str, variable: str, cadence: str = "monthly"
) -> pd.DataFrame | None:
    """Concatenate one variable's staged CSVs, tagging each with timeperiod."""
    folder = staging_layout.variables_dir(state, source, variable)
    if not folder.exists():
        return None
    pattern = MONTHLY_FILE_RE if cadence == "monthly" else ANNUAL_FILE_RE
    frames = []
    for csv in sorted(folder.glob("*.csv")):
        match = pattern.search(csv.name)
        if not match:
            continue
        df = pd.read_csv(csv)
        df["timeperiod"] = match.group(1)
        frames.append(df)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def build_master(state: str, cfg: dict, end_month: str) -> tuple[pd.DataFrame, dict]:
    """Build the MASTER_VARIABLES panel for ``state`` up to ``end_month``.

    ``cfg`` is the state YAML dict; ``end_month`` is a month partition key
    ("2026-06-01") or timeperiod ("2026_06").

    Returns ``(master_df, coverage)`` where coverage maps each configured
    variable to the number of timeperiods found in staging.
    """
    master_cfg = cfg.get("master", {})
    start_month = str(master_cfg.get("start_month", "2021-04")).replace("_", "-")
    end_month = end_month.replace("_", "-")[:7]

    date_range = pd.date_range(start=f"{start_month}-01", end=f"{end_month}-01", freq="MS")
    formatted_dates = [date.strftime("%Y_%m") for date in date_range]
    if not formatted_dates:
        raise ValueError(f"empty month range {start_month} .. {end_month}")

    skeleton = _load_skeleton(state)
    dfs = []
    for year_month in formatted_dates:
        df = skeleton.copy()
        df["timeperiod"] = year_month
        dfs.append(df)
    master_df = pd.concat(dfs).reset_index(drop=True)
    master_df["district_obj"] = master_df["object_id"].str.rsplit("-", n=1).str[0]

    coverage: dict[str, int] = {}

    # --- Monthly variables -------------------------------------------------
    for entry in master_cfg.get("monthly_variables", []):
        source, variable = entry["source"], entry["variable"]
        level = entry.get("level", "object")
        variable_df = _read_variable_folder(state, source, variable)
        if variable_df is None:
            logger.warning("%s: no staged data for %s/%s — skipping", state, source, variable)
            coverage[f"{source}/{variable}"] = 0
            continue
        coverage[f"{source}/{variable}"] = variable_df["timeperiod"].nunique()

        if level == "district":
            variable_df = variable_df.rename(columns={"object_id": "district_obj"})
            variable_df = variable_df.drop_duplicates(subset=["district_obj", "timeperiod"])
            value_cols = [c for c in variable_df.columns if c not in ("district_obj", "timeperiod")]
            master_df = master_df.merge(
                variable_df[["district_obj", "timeperiod", *value_cols]],
                on=["district_obj", "timeperiod"],
                how="left",
            )
        else:
            variable_df = variable_df.drop_duplicates(subset=["object_id", "timeperiod"])
            master_df = master_df.merge(
                variable_df,
                on=["object_id", "timeperiod"],
                how="left",
            )

    # --- Annual variables --------------------------------------------------
    master_df["year"] = master_df["timeperiod"].str[:4].astype(int)
    for entry in master_cfg.get("annual_variables", []):
        source, variable = entry["source"], entry["variable"]
        variable_df = _read_variable_folder(state, source, variable, cadence="annual")
        if variable_df is None:
            logger.warning("%s: no staged data for %s/%s — skipping", state, source, variable)
            coverage[f"{source}/{variable}"] = 0
            continue
        coverage[f"{source}/{variable}"] = variable_df["timeperiod"].nunique()
        variable_df = variable_df.rename(columns={"timeperiod": "year"})
        variable_df["year"] = variable_df["year"].astype(int)
        variable_df = variable_df.drop_duplicates(subset=["object_id", "year"])
        master_df = master_df.merge(variable_df, on=["object_id", "year"], how="left")

    # --- One-time variables ------------------------------------------------
    reference_variables = staging_layout.reference_dir(state) / "variables"
    for name in master_cfg.get("onetime_variables", []):
        path = reference_variables / f"{name}.csv"
        if not path.exists():
            logger.warning("%s: one-time variable file missing: %s", state, path)
            coverage[f"onetime/{name}"] = 0
            continue
        coverage[f"onetime/{name}"] = 1
        variable_df = pd.read_csv(path)
        variable_df = variable_df.drop(columns=["timeperiod", "year"], errors="ignore")
        variable_df = variable_df.loc[:, ~variable_df.columns.str.startswith("Unnamed")]
        # Drop columns that already exist so merges never suffix _x/_y
        overlap = [c for c in variable_df.columns if c in master_df.columns and c != "object_id"]
        variable_df = variable_df.drop(columns=overlap)
        variable_df = variable_df.drop_duplicates(subset=["object_id"])
        master_df = master_df.merge(variable_df, on="object_id", how="left")

    # --- Cleanup + imputation (master2.py order) ---------------------------
    master_df = master_df.drop(columns=master_cfg.get("drop_columns", []), errors="ignore")

    imputation = master_cfg.get("imputation", {})

    def _impute(columns: list[str], group_col: str, how: str) -> None:
        for column in columns:
            if column not in master_df.columns:
                continue
            fill = master_df.groupby(group_col)[column].transform(how)
            master_df[column] = master_df[column].fillna(fill)

    _impute(imputation.get("object_mean", []), "object_id", "mean")
    _impute(imputation.get("district_mean", []), "district", "mean")
    _impute(imputation.get("district_median", []), "district", "median")

    master_df = master_df.drop(columns=["district_obj"])
    if imputation.get("default", "zero") == "zero":
        numeric = master_df.select_dtypes("number").columns
        master_df[numeric] = master_df[numeric].fillna(0)
    master_df["year"] = ""

    expected_rows = len(skeleton) * len(formatted_dates)
    if len(master_df) != expected_rows:
        raise ValueError(
            f"{state}: master panel has {len(master_df)} rows, expected "
            f"{expected_rows} ({len(skeleton)} boundaries x {len(formatted_dates)} months) "
            "— a merge introduced duplicates"
        )

    return master_df, coverage


def write_master(state: str, master_df: pd.DataFrame) -> str:
    path = staging_layout.master_csv_path(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    master_df.to_csv(path, index=False)
    return str(path)

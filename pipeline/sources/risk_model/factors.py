"""Factor computations for the flood risk score.

Port of the IDS-DRR-Himachal-Pradesh-Risk-Score-Model scripts
(``hazard_AHP.py``, ``exposure.py``, ``vulnerability.py``,
``govtresponse.py``, ``topis_riskscore_district.py``) as pure functions
parameterised by the state's ``risk_model`` YAML block.

Each ``compute_*`` takes the MASTER_VARIABLES panel and returns only its
factor columns keyed on ``object_id`` x ``timeperiod``; callers merge them
onto the panel to produce ``factor_scores_l1_*.csv``.
"""

import logging

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from pipeline.sources.risk_model import dea
from pipeline.sources.risk_model.binning import assign_bin_with_handling
from pipeline.sources.risk_model.topsis import Topsis

logger = logging.getLogger(__name__)

DAMAGE_THRESHOLD = 0.0001


def get_financial_year(timeperiod: str) -> str:
    """Indian financial year ("2023_07" -> "2023-2024")."""
    year, month = timeperiod.split("_")
    if int(month) >= 4:
        return f"{int(year)}-{int(year) + 1}"
    return f"{int(year) - 1}-{int(year)}"


def _require(df: pd.DataFrame, columns: list[str], factor: str) -> list[str]:
    """Keep the configured columns that exist; warn about the rest."""
    present = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning("%s: skipping variables absent from master panel: %s", factor, missing)
    if not present:
        raise ValueError(f"{factor}: none of the configured variables are in the master panel")
    return present


def _sanitise(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Replace non-finite values (0/0 from per-capita division) with 0."""
    df[columns] = df[columns].replace([np.inf, -np.inf], np.nan).fillna(0)
    return df


def compute_hazard(master_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """AHP-weighted flood hazard level, binned 1-5 per month."""
    hazard_cfg = cfg.get("risk_model", {}).get("hazard", {})
    weights = dict(hazard_cfg.get("weights", {}))
    hazard_vars = _require(master_df, list(weights), "hazard")
    weights = {k: v for k, v in weights.items() if k in hazard_vars}
    total = sum(weights.values())
    weights = {k: v / total for k, v in weights.items()}
    invert = [c for c in hazard_cfg.get("invert", []) if c in hazard_vars]

    hazard_df = master_df[hazard_vars + ["timeperiod", "object_id"]].copy()
    hazard_df = _sanitise(hazard_df, hazard_vars)

    months = []
    for month in hazard_df.timeperiod.unique():
        month_df = hazard_df[hazard_df.timeperiod == month].copy()
        month_df[hazard_vars] = MinMaxScaler().fit_transform(month_df[hazard_vars])

        for column in invert:
            month_df[column] = 1 - month_df[column]

        month_df["flood_hazard_level"] = sum(month_df[var] * weights[var] for var in hazard_vars)
        month_df["flood-hazard"] = pd.cut(
            month_df["flood_hazard_level"],
            bins=np.linspace(0, 1, 6),
            labels=[1, 2, 3, 4, 5],
            include_lowest=True,
        )
        months.append(month_df)

    hazard = pd.concat(months)
    return hazard[["timeperiod", "object_id", "flood-hazard"]]


def compute_exposure(master_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Population/household exposure, quintile-binned 1-5 per month."""
    exposure_cfg = cfg.get("risk_model", {}).get("exposure", {})
    exposure_vars = _require(master_df, exposure_cfg.get("vars", []), "exposure")
    raw_var = exposure_cfg.get("raw_var", exposure_vars[0])

    exposure_df = master_df[exposure_vars + ["timeperiod", "object_id"]].copy()
    exposure_df = _sanitise(exposure_df, exposure_vars)

    months = []
    for month in exposure_df.timeperiod.unique():
        month_df = exposure_df[exposure_df.timeperiod == month].copy()
        month_df[exposure_vars] = MinMaxScaler().fit_transform(month_df[exposure_vars])
        month_df["exposure_raw"] = master_df.loc[month_df.index, raw_var]
        month_df["exposure"] = pd.qcut(
            month_df["exposure_raw"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
        ).astype(int)
        months.append(month_df)

    exposure = pd.concat(months)
    return exposure[["timeperiod", "object_id", "exposure"]]


def compute_government_response(master_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Government response from financial-year cumulative spend, binned 5-1.

    Higher cumulative response gets a *lower* score, since the composite
    risk score treats all four factors as "more is more risk".
    """
    response_cfg = cfg.get("risk_model", {}).get("government_response", {})
    response_vars = _require(master_df, response_cfg.get("vars", []), "government_response")

    response_df = master_df[response_vars + ["timeperiod", "object_id"]].copy()
    response_df = _sanitise(response_df, response_vars)
    response_df["financial_year"] = response_df["timeperiod"].apply(get_financial_year)
    for var in response_vars:
        response_df[var] = response_df.groupby(["object_id", "financial_year"])[var].cumsum()

    months = []
    for month in response_df.timeperiod.unique():
        month_df = response_df[response_df.timeperiod == month].copy()
        month_df[response_vars] = MinMaxScaler().fit_transform(month_df[response_vars])
        month_df["sum"] = month_df[response_vars].sum(axis=1)

        mean, std = month_df["sum"].mean(), month_df["sum"].std()
        conditions = [
            month_df["sum"] <= mean,
            (month_df["sum"] > mean) & (month_df["sum"] <= mean + std),
            (month_df["sum"] > mean + std) & (month_df["sum"] <= mean + 2 * std),
            (month_df["sum"] > mean + 2 * std) & (month_df["sum"] <= mean + 3 * std),
            month_df["sum"] > mean + 3 * std,
        ]
        month_df["government-response"] = np.select(conditions, [5, 4, 3, 2, 1])
        months.append(month_df)

    response = pd.concat(months)
    return response[["timeperiod", "object_id", "government-response"]]


def compute_vulnerability(master_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """DEA efficiency of coping capacity, Jenks-binned into vulnerability 1-5.

    Inputs are coping-capacity indicators, outputs are (inverted) loss and
    damage variables, so a DMU that suffers little damage for its capacity
    scores as efficient — and therefore less vulnerable.
    """
    vuln_cfg = cfg.get("risk_model", {}).get("vulnerability", {})
    input_vars = _require(master_df, vuln_cfg.get("input_vars", []), "vulnerability inputs")
    damage_vars = _require(master_df, vuln_cfg.get("damage_vars", []), "vulnerability damages")
    neg_inputs = [c for c in vuln_cfg.get("neg_inputs", []) if c in input_vars]
    invert_inputs = [c for c in vuln_cfg.get("invert_inputs", []) if c in input_vars]

    working = master_df.copy()
    for variable, denominator in (vuln_cfg.get("per_capita") or {}).items():
        if variable not in working.columns or denominator not in working.columns:
            logger.warning(
                "vulnerability: skipping per-capita %s/%s (column missing)",
                variable,
                denominator,
            )
            continue
        working[variable] = working[variable] / working[denominator]
    working = _sanitise(working, input_vars + damage_vars)

    vulnerability_df = working[input_vars + damage_vars + ["timeperiod", "object_id"]].copy()

    scaler = MinMaxScaler()
    vulnerability_df["landd_score"] = (
        scaler.fit_transform(vulnerability_df[damage_vars]).sum(axis=1) + 1
    )

    # The loss-and-damage weighting is applied to the whole panel once per
    # month iteration in the upstream model, so a tehsil's damage values
    # compound as later months are processed. Reproduced here to keep
    # scores comparable with the published model; set
    # `compound_damage_weights: false` in the state YAML to apply the
    # weighting a single time instead.
    compound = vuln_cfg.get("compound_damage_weights", True)
    weighted_columns = damage_vars + neg_inputs

    def apply_custom_weights(df: pd.DataFrame) -> pd.DataFrame:
        multiplier = np.power(df["landd_score"], 2)
        significant = (df[damage_vars] > DAMAGE_THRESHOLD).any(axis=1)
        df.loc[significant, weighted_columns] = df.loc[significant, weighted_columns].mul(
            multiplier[significant], axis=0
        )
        return df

    if not compound:
        vulnerability_df = apply_custom_weights(vulnerability_df)

    months = []
    for month in vulnerability_df.timeperiod.unique():
        if compound:
            vulnerability_df = apply_custom_weights(vulnerability_df)

        month_df = vulnerability_df[vulnerability_df.timeperiod == month].copy()
        month_df = month_df.set_index("object_id")

        model_vars = input_vars + damage_vars
        month_df[model_vars] = MinMaxScaler().fit_transform(month_df[model_vars])

        for column in invert_inputs:
            month_df[column] = 1 - month_df[column]
        # More damage means a worse output, so outputs are inverted too.
        month_df[damage_vars] = 1 - month_df[damage_vars]
        month_df[model_vars] = np.round(month_df[model_vars], 8)

        X = month_df[input_vars].T.to_dict("list")
        y = month_df[damage_vars].T.to_dict("list")
        efficiency_df = dea.CRS(list(month_df.index), X, y, orientation="input", dual=False)

        month_df = month_df.reset_index().merge(
            efficiency_df, left_on="object_id", right_on="DMU", how="left"
        )
        month_df["efficiency"] = np.round(month_df["efficiency"].astype(float), 6)

        try:
            month_df["vulnerability"] = assign_bin_with_handling(
                month_df["efficiency"], n_classes=5
            )
        except ValueError as exc:
            logger.warning("vulnerability: skipping %s — binning failed: %s", month, exc)
            continue

        months.append(month_df)

    vulnerability = pd.concat(months)
    return vulnerability[["timeperiod", "object_id", "efficiency", "vulnerability", "landd_score"]]


# Financial-year cumulative columns added to the tehsil-level output.
CUMULATIVE_VARS = [
    "total_tender_awarded_value",
    "Repair and Restoration_tenders_awarded_value",
    "LWSS_tenders_awarded_value",
    "NDRF_tenders_awarded_value",
    "SDMF_tenders_awarded_value",
    "WSS_tenders_awarded_value",
    "Preparedness Measures_tenders_awarded_value",
    "Immediate Measures_tenders_awarded_value",
    "Others_tenders_awarded_value",
    "relief_and_mitigation_sanction_value",
]

FACTOR_COLUMNS = ["flood-hazard", "exposure", "vulnerability", "government-response"]

# District aggregation: how each indicator rolls up from tehsils.
DISTRICT_AGGREGATIONS = {
    "sum": [
        "total-tender-awarded-value",
        "repair-and-restoration-tenders-awarded-value",
        "lwss-tenders-awarded-value",
        "ndrf-tenders-awarded-value",
        "sdmf-tenders-awarded-value",
        "wss-tenders-awarded-value",
        "preparedness-measures-tenders-awarded-value",
        "immediate-measures-tenders-awarded-value",
        "others-tenders-awarded-value",
        "relief-and-mitigation-sanction-value",
        "total-livestock-loss",
        "schools-damaged",
        "person-dead",
        "person-major-injury",
        "structure-lost",
        "health-centres-lost",
        "roadlength",
        "sum-population",
        "inundation-intensity-sum",
        "total-hhd",
        "sum-aged-population",
        "schools-count",
        "road-length",
        "rail-length",
        "sum-rain",
        "sum-young-population",
        "net-sown-area-in-hac",
        "road-count",
        "rail-count",
        "sum-runoff",
    ],
    "mean": [
        "block-nosanitation-hhds-pct",
        "drainage-density",
        "inundation-pct",
        "inundation-intensity-mean-nonzero",
        "inundation-intensity-mean",
        "avg-electricity",
        "block-piped-hhds-pct",
        "mean-sex-ratio",
        "mean-rain",
        "elevation-mean",
        "slope-mean",
        "avg-tele",
        "distance-from-river-mean",
        "mean-daily-runoff",
        "nviall-comp",
        "sviall-comp",
        "pviall-comp",
        "hviall-comp",
        "fviall-comp",
        "cviall-comp",
        "topsis-score",
    ],
    "max": ["max-rain", "peak-runoff"],
}

DISTRICT_ROUNDING = {
    0: [
        "total-tender-awarded-value",
        "repair-and-restoration-tenders-awarded-value",
        "lwss-tenders-awarded-value",
        "ndrf-tenders-awarded-value",
        "sdmf-tenders-awarded-value",
        "wss-tenders-awarded-value",
        "preparedness-measures-tenders-awarded-value",
        "immediate-measures-tenders-awarded-value",
        "others-tenders-awarded-value",
        "relief-and-mitigation-sanction-value",
        "net-sown-area-in-hac",
        "sum-aged-population",
        "sum-young-population",
        "sum-population",
        "rail-length",
        "road-length",
        "elevation-mean",
        "slope-mean",
        "total-hhd",
    ],
    1: ["avg-tele", "avg-electricity"],
    2: [
        "mean-sex-ratio",
        "inundation-intensity-mean-nonzero",
        "block-piped-hhds-pct",
        "block-nosanitation-hhds-pct",
        "inundation-intensity-sum",
        "max-rain",
        "mean-rain",
        "sum-rain",
        "mean-daily-runoff",
        "sum-runoff",
        "peak-runoff",
        "nviall-comp",
        "sviall-comp",
        "pviall-comp",
        "hviall-comp",
        "fviall-comp",
        "cviall-comp",
    ],
}

OUTPUT_RENAMES = {
    "nviall-comp": "natural-vulnerability-index",
    "sviall-comp": "social-vulnerability-index",
    "pviall-comp": "physical-vulnerability-index",
    "hviall-comp": "human-vulnerability-index",
    "fviall-comp": "financial-vulnerability-index",
    "cviall-comp": "composite-vulnerability-index",
    "block-piped-hhds-pct": "tehsil-piped-hhds-pct",
    "block-nosanitation-hhds-pct": "tehsil-nosanitation-hhds-pct",
    "preparedness-measures-tenders-awarded-value": ("restoration-measures-tenders-awarded-value"),
    "mean-sexratio": "sexratio",
}

INFRASTRUCTURE_DAMAGE_PARTS = ["structure-lost", "health-centres-lost", "schools-damaged"]

# Bookkeeping columns carried in from source CSVs; not part of the
# published table.
OUTPUT_DROP_COLUMNS = [
    "objectid",
    "object-id-new",
    "timeperiod-datetime",
    "year",
    "unnamed:-0",
    "shape-leng",
    "shape-area",
    "count",
]


def _to_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.lower().replace("_", "-").replace(" ", "-") for col in df.columns]
    return df.loc[:, ~df.columns.duplicated()]


def compute_risk_scores(
    master_df: pd.DataFrame,
    factor_dfs: dict[str, pd.DataFrame],
    cfg: dict,
) -> pd.DataFrame:
    """Combine the four factors into tehsil-level TOPSIS risk scores.

    ``factor_dfs`` maps factor name -> the factor's columns keyed on
    ``object_id`` x ``timeperiod``.
    """
    weights_cfg = cfg.get("risk_model", {}).get("topsis_weights", {})

    merged = master_df.copy()
    for factor_df in factor_dfs.values():
        merged = merged.merge(factor_df, on=["object_id", "timeperiod"], how="inner")

    missing = [c for c in FACTOR_COLUMNS if c not in merged.columns]
    if missing:
        raise ValueError(f"risk score: factor columns missing after merge: {missing}")

    merged["financial_year"] = merged["timeperiod"].apply(get_financial_year)
    merged = merged.sort_values(by=["object_id", "financial_year", "timeperiod"])

    for var in CUMULATIVE_VARS:
        if var not in merged.columns:
            logger.warning("risk score: no cumulative column for %s", var)
            continue
        merged[f"{var}_fy_cumsum"] = merged.groupby(["object_id", "financial_year"])[var].cumsum()

    weights = [weights_cfg.get(factor, 1) for factor in FACTOR_COLUMNS]
    months = []
    for month in merged.timeperiod.unique():
        month_df = merged[merged.timeperiod == month].copy()

        topsis = Topsis(
            np.array(month_df[FACTOR_COLUMNS].values),
            weights,
            [True, True, True, True],
        )
        topsis.calc()
        month_df["TOPSIS_Score"] = topsis.worst_similarity
        month_df = month_df.sort_values(by="TOPSIS_Score", ascending=False)
        month_df["risk-score"] = pd.cut(
            month_df["TOPSIS_Score"], bins=5, precision=0, labels=[1, 2, 3, 4, 5]
        )
        months.append(month_df)

    tehsil = _to_output_columns(pd.concat(months))
    tehsil["risk-score"] = tehsil["risk-score"].astype(int)
    return tehsil


def build_district_output(
    tehsil: pd.DataFrame, district_map: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Tehsil rows plus their district rollup, rounded and renamed.

    This is the published ``risk_score_final_district`` table: tehsil-level
    and district-level rows in one frame.
    """
    final = tehsil
    if district_map is not None:
        final = pd.concat([tehsil, _aggregate_districts(tehsil, district_map)], ignore_index=True)

    final = _apply_rounding(final)
    final = final.drop(columns=OUTPUT_DROP_COLUMNS, errors="ignore")
    final = final.rename(columns=OUTPUT_RENAMES)
    final["financial-year"] = final["timeperiod"].apply(get_financial_year)

    damage_parts = [c for c in INFRASTRUCTURE_DAMAGE_PARTS if c in final.columns]
    if damage_parts:
        final["total-infrastructure-damage"] = final[damage_parts].sum(axis=1)

    return final


def _aggregate_districts(tehsil: pd.DataFrame, district_map: pd.DataFrame) -> pd.DataFrame:
    """Roll tehsil rows up to district rows, re-binning each factor 1-5."""
    district_map = district_map.rename(columns=lambda c: c.strip().lower())
    district_map = district_map[[c for c in district_map.columns if not c.startswith("unnamed")]]

    labels = [1, 2, 3, 4, 5]
    frames = []
    for column in [*FACTOR_COLUMNS, "risk-score"]:
        if column not in tehsil.columns:
            continue
        summed = tehsil.groupby(["district", "timeperiod"])[column].sum().reset_index()
        summed[column] = pd.cut(summed[column], bins=5, precision=0, labels=labels)
        frames.append(summed.set_index(["district", "timeperiod"])[column])

    rules = {}
    for how, columns in DISTRICT_AGGREGATIONS.items():
        for column in columns:
            if column in tehsil.columns:
                rules[column] = how
    indicators = tehsil.groupby(["district", "timeperiod"]).agg(rules).reset_index()
    frames.append(indicators.set_index(["district", "timeperiod"]))

    district = pd.concat(frames, axis=1).reset_index()
    return district.merge(district_map, on="district", how="left")


def _apply_rounding(df: pd.DataFrame) -> pd.DataFrame:
    for decimals, columns in DISTRICT_ROUNDING.items():
        for column in columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").round(decimals)
    return df

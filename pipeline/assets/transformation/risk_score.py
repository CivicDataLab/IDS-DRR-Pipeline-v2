"""
Government response factor and the composite risk score.

The composite score runs TOPSIS over the four factor scores (hazard,
exposure, vulnerability, government response) for each month.
"""

import dagster as dg
import pandas as pd

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import staging_layout
from pipeline.sources.risk_model import factors
from pipeline.state_config import load_state_config

# Factor CSV stem -> the column that CSV contributes to the composite score.
FACTOR_SOURCES = {
    "flood-hazard": ["flood-hazard"],
    "exposure": ["exposure"],
    "vulnerability": ["vulnerability", "efficiency", "landd_score"],
    "government-response": ["government-response"],
}


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["master_variables"],
    automation_condition=dg.AutomationCondition.eager(),
    description="Government response factor from tender and relief spending",
)
def government_response_factor(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Calculate the government response factor for a state."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    cfg = load_state_config(state)
    master = pd.read_csv(staging_layout.master_csv_path(state))
    context.log.info(f"Computing government response factor for {state} ({len(master)} rows)")

    response = factors.compute_government_response(master, cfg)
    scored = master.merge(response, on=["object_id", "timeperiod"], how="left")

    path = staging_layout.factor_csv_path(state, "government-response")
    path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(path, index=False)

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "path": str(path),
            "rows": len(scored),
            "distribution": str(
                response["government-response"].value_counts().sort_index().to_dict()
            ),
        }
    )


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=[
        "hazard_factor",
        "vulnerability_factor",
        "exposure_factor",
        "government_response_factor",
    ],
    automation_condition=dg.AutomationCondition.eager(),
    description="Composite TOPSIS risk score across all four factors",
)
def composite_risk_score(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Combine the four factor scores into the tehsil-level risk score."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    cfg = load_state_config(state)
    master = pd.read_csv(staging_layout.master_csv_path(state))

    factor_dfs = {}
    for factor, columns in FACTOR_SOURCES.items():
        path = staging_layout.factor_csv_path(state, factor)
        if not path.exists():
            raise FileNotFoundError(f"missing factor scores for {state}: {path}")
        df = pd.read_csv(path)
        present = [c for c in columns if c in df.columns]
        factor_dfs[factor] = df[["object_id", "timeperiod", *present]]

    context.log.info(f"Running TOPSIS for {state} over {len(factor_dfs)} factors")
    tehsil = factors.compute_risk_scores(master, factor_dfs, cfg)

    path = staging_layout.outputs_dir(state) / "risk_score.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    tehsil.to_csv(path, index=False)

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "path": str(path),
            "rows": len(tehsil),
            "distribution": str(tehsil["risk-score"].value_counts().sort_index().to_dict()),
        }
    )

"""
Risk model output assets.

Final stage of the pipeline: district-level aggregation of the composite
risk score and the monthly report summary.
"""

import dagster as dg
import pandas as pd

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import staging_layout
from pipeline.sources.risk_model import factors
from pipeline.state_config import load_state_config, resolve_path


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["composite_risk_score"],
    automation_condition=dg.AutomationCondition.eager(),
    description="Published risk score table with tehsil and district rows",
)
def risk_score_output(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Aggregate tehsil-level TOPSIS results to district level."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    cfg = load_state_config(state)
    tehsil = pd.read_csv(staging_layout.outputs_dir(state) / "risk_score.csv")

    district_map = None
    district_map_path = cfg.get("risk_model", {}).get("district_map")
    if district_map_path:
        path = resolve_path(district_map_path)
        if path.exists():
            district_map = pd.read_csv(path)
        else:
            context.log.warning(
                f"No district map at {path}; publishing tehsil rows only"
            )

    context.log.info(f"Building published risk table for {state} - {month}")
    final = factors.build_district_output(tehsil, district_map)

    path = staging_layout.outputs_dir(state) / "risk_score_final_district.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(path, index=False)

    district_rows = len(final) - len(tehsil)
    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "path": str(path),
            "rows": len(final),
            "tehsil_rows": len(tehsil),
            "district_rows": district_rows,
        }
    )


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["risk_score_output"],
    automation_condition=dg.AutomationCondition.eager(),
    description="Monthly risk report summary for a state",
)
def monthly_risk_report(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Summarise the partition month's risk scores."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    timeperiod = staging_layout.month_key_to_timeperiod(month)
    final = pd.read_csv(staging_layout.outputs_dir(state) / "risk_score_final_district.csv")
    current = final[final["timeperiod"] == timeperiod]

    if current.empty:
        context.log.warning(f"No risk rows for {state} {timeperiod}")
        return dg.MaterializeResult(
            metadata={"state": state, "month": month, "status": "no_data"}
        )

    # District rollup rows share the table but carry no tehsil name.
    tehsils = current[current["tehsil"].notna()] if "tehsil" in current else current
    high_risk = tehsils[tehsils["risk-score"] >= 4]
    top = (
        tehsils.nlargest(5, "topsis-score")["tehsil"].tolist()
        if "topsis-score" in tehsils
        else []
    )

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "timeperiod": timeperiod,
            "rows": len(current),
            "tehsils": len(tehsils),
            "high_risk_tehsils": len(high_risk),
            "mean_risk_score": float(tehsils["risk-score"].mean()),
            "highest_risk": ", ".join(str(t) for t in top) or "n/a",
        }
    )

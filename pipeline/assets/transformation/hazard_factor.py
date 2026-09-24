"""
Hazard Factor calculation.

AHP-weighted combination of rainfall, runoff, elevation and distance from
river, binned into hazard levels 1-5 per month.
"""

import dagster as dg
import pandas as pd

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import staging_layout
from pipeline.sources.risk_model import factors
from pipeline.state_config import load_state_config


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["master_variables"],
    automation_condition=dg.AutomationCondition.eager(),
    description="Flood hazard factor (AHP-weighted, binned 1-5)",
)
def hazard_factor(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Calculate the hazard factor for every tehsil-month of a state."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    cfg = load_state_config(state)
    master = pd.read_csv(staging_layout.master_csv_path(state))
    context.log.info(f"Computing hazard factor for {state} ({len(master)} rows)")

    hazard = factors.compute_hazard(master, cfg)
    scored = master.merge(hazard, on=["object_id", "timeperiod"], how="left")

    path = staging_layout.factor_csv_path(state, "flood-hazard")
    path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(path, index=False)

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "path": str(path),
            "rows": len(scored),
            "distribution": str(hazard["flood-hazard"].value_counts().sort_index().to_dict()),
        }
    )

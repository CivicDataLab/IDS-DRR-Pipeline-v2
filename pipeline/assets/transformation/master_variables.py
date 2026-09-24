"""
MASTER_VARIABLES panel assembly.

Merges every staged source variable into the tehsil x month panel that the
risk model consumes. Tolerant of missing optional upstreams — absent
sources are reported as coverage metadata and imputed downstream.
"""

import dagster as dg

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import master_variables as master_src
from pipeline.state_config import load_state_config

UPSTREAM_ASSETS = [
    "imd_monthly_rain_data",
    "raw_satellite_data",
    "sentinel_indices",
    "ffs_river_levels",
    "nrsc_runoff",
    "worldpop_annual_data",
    "raw_procurement_data",
]


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=UPSTREAM_ASSETS,
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "Tehsil x month panel of every source variable "
        "(staging/master/{state}/MASTER_VARIABLES.csv)"
    ),
)
def master_variables(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Rebuild the state's master panel up to the partition month."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    cfg = load_state_config(state)
    context.log.info(f"Building master panel for {state} through {month}")

    panel, coverage = master_src.build_master(state, cfg, month)
    path = master_src.write_master(state, panel)

    populated = sum(1 for count in coverage.values() if count)
    empty = sorted(name for name, count in coverage.items() if not count)
    if empty:
        context.log.warning(f"{len(empty)} variables have no staged data: {empty}")

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "month": month,
            "path": path,
            "rows": len(panel),
            "columns": len(panel.columns),
            "variables_with_data": populated,
            "variables_total": len(coverage),
            "coverage_pct": round(100 * populated / max(len(coverage), 1), 1),
            "variables_missing": ", ".join(empty) or "none",
        }
    )

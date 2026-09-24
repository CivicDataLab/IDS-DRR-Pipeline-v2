"""
Demographic data extraction assets.

WorldPop population statistics refresh once a year. The asset is
partitioned by state-month like everything else, but only does work in the
configured refresh month; other months confirm the current year's figures
are already staged.
"""

from datetime import datetime

import dagster as dg

from pipeline.assets.extraction.hydrology import boundary_object_ids
from pipeline.partitions import monthly_state_partitions
from pipeline.sources import manual_inbox, staging_layout
from pipeline.sources.worldpop_scraper import POPULATION_VARIABLES
from pipeline.state_config import load_state_config


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="Annual WorldPop population statistics per admin boundary",
)
def worldpop_annual_data(context: dg.AssetExecutionContext) -> dict:
    """Stage the year's WorldPop variables for a state."""
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    config = load_state_config(state)
    worldpop_cfg = config.get("data_sources", {}).get("demographics", {}).get("worldpop", {})
    if not worldpop_cfg.get("enabled"):
        context.log.info(f"WorldPop not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    year = target_date.year
    refresh_month = worldpop_cfg.get("refresh_month", 1)

    staged = [
        variable
        for variable in POPULATION_VARIABLES
        if staging_layout.variable_csv_path(state, "worldpop", variable, str(year)).exists()
    ]

    if target_date.month != refresh_month:
        context.log.info(
            f"{state} {month_str}: not the WorldPop refresh month "
            f"({refresh_month}); {len(staged)} variables already staged for {year}"
        )
        return {
            "state": state,
            "month": month_str,
            "year": year,
            "variables": staged,
            "status": "not_refresh_month",
        }

    pending = manual_inbox.scan_inbox(state, "worldpop")
    if not pending:
        status = "already_staged" if staged else "awaiting_upload"
        context.log.info(f"No pending WorldPop files for {state} ({status})")
        return {
            "state": state,
            "month": month_str,
            "year": year,
            "variables": staged,
            "status": status,
        }

    boundary_ids = boundary_object_ids(state)
    promoted, failed = [], []
    for path in pending:
        try:
            promoted.append(str(manual_inbox.promote(state, "worldpop", path, boundary_ids)))
        except ValueError as exc:
            context.log.error(f"Rejected {path.name}: {exc}")
            failed.append(f"{path.name}: {exc}")

    if failed:
        raise ValueError(
            f"{len(failed)} WorldPop file(s) failed validation and remain in the inbox: "
            + "; ".join(failed)
        )

    context.log.info(f"Promoted {len(promoted)} WorldPop file(s) for {state}")
    return {
        "state": state,
        "month": month_str,
        "year": year,
        "promoted": promoted,
        "status": "extracted",
    }

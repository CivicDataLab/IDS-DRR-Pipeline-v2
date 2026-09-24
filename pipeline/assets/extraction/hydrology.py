"""
Hydrology data extraction assets.

NRSC runoff arrives as manually prepared CSVs dropped into
``inbox/{state}/nrsc/``; this asset validates and promotes them into the
staging variables contract.
"""

import dagster as dg

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import manual_inbox, staging_layout
from pipeline.state_config import boundaries_path, load_state_config


def boundary_object_ids(state: str) -> set[str]:
    """Valid ``object_id`` values for a state, for inbox join validation."""
    import json

    path = boundaries_path(state)
    if path is None or not path.exists():
        return set()
    with open(path) as f:
        geojson = json.load(f)
    return {
        str(feature.get("properties", {}).get("object_id"))
        for feature in geojson.get("features", [])
        if feature.get("properties", {}).get("object_id") is not None
    }


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="NRSC runoff variables promoted from the manual inbox",
)
def nrsc_runoff(context: dg.AssetExecutionContext) -> dict:
    """Promote any pending NRSC runoff CSVs for a state."""
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    config = load_state_config(state)
    nrsc_cfg = config.get("data_sources", {}).get("satellite", {}).get("nrsc_runoff", {})
    if not nrsc_cfg.get("enabled"):
        context.log.info(f"NRSC runoff not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    timeperiod = staging_layout.month_key_to_timeperiod(month_str)
    pending = manual_inbox.scan_inbox(state, "nrsc")

    if not pending:
        existing = staging_layout.variable_csv_path(state, "nrsc", "runoff", timeperiod)
        status = "already_staged" if existing.exists() else "awaiting_upload"
        context.log.info(f"No pending NRSC files for {state} ({status})")
        return {"state": state, "month": month_str, "status": status}

    boundary_ids = boundary_object_ids(state)
    promoted, failed = [], []
    for path in pending:
        try:
            promoted.append(str(manual_inbox.promote(state, "nrsc", path, boundary_ids)))
        except ValueError as exc:
            context.log.error(f"Rejected {path.name}: {exc}")
            failed.append(f"{path.name}: {exc}")

    if failed:
        raise ValueError(
            f"{len(failed)} NRSC file(s) failed validation and remain in the inbox: "
            + "; ".join(failed)
        )

    context.log.info(f"Promoted {len(promoted)} NRSC file(s) for {state}")
    return {
        "state": state,
        "month": month_str,
        "promoted": promoted,
        "status": "extracted",
    }

"""
Procurement data extraction assets.

State tender portals sit behind captchas, so flood-related tender exports
are prepared by hand and dropped into ``inbox/{state}/tenders/`` as
``<variable>_YYYY_MM.csv``. This asset validates and promotes them.
Automating the scrape is tracked as a follow-up (see docs/follow_ups.md).
"""

import dagster as dg

from pipeline.assets.extraction.hydrology import boundary_object_ids
from pipeline.partitions import monthly_state_partitions
from pipeline.sources import manual_inbox, staging_layout
from pipeline.state_config import load_state_config


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="Government tender data promoted from the manual inbox",
)
def raw_procurement_data(context: dg.AssetExecutionContext) -> dict:
    """Promote any pending tender CSVs for a state-month."""
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    config = load_state_config(state)
    tenders_cfg = config.get("data_sources", {}).get("procurement", {}).get("tenders", {})
    if not tenders_cfg.get("enabled"):
        context.log.info(f"Tenders not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    if tenders_cfg.get("mode") != "manual":
        raise NotImplementedError(
            f"{state}: automated tender scraping is not implemented; "
            "set data_sources.procurement.tenders.mode to 'manual'"
        )

    timeperiod = staging_layout.month_key_to_timeperiod(month_str)
    pending = manual_inbox.scan_inbox(state, "tenders")

    if not pending:
        staged = list(
            staging_layout.variables_dir(state, "tenders", "total_tender_awarded_value").glob(
                f"*_{timeperiod}.csv"
            )
        )
        status = "already_staged" if staged else "awaiting_upload"
        context.log.info(f"No pending tender files for {state} ({status})")
        return {
            "state": state,
            "month": month_str,
            "portal_url": tenders_cfg.get("portal_url"),
            "status": status,
        }

    boundary_ids = boundary_object_ids(state)
    promoted, failed = [], []
    for path in pending:
        try:
            promoted.append(str(manual_inbox.promote(state, "tenders", path, boundary_ids)))
        except ValueError as exc:
            context.log.error(f"Rejected {path.name}: {exc}")
            failed.append(f"{path.name}: {exc}")

    if failed:
        raise ValueError(
            f"{len(failed)} tender file(s) failed validation and remain in the inbox: "
            + "; ".join(failed)
        )

    context.log.info(f"Promoted {len(promoted)} tender file(s) for {state}")
    return {
        "state": state,
        "month": month_str,
        "portal_url": tenders_cfg.get("portal_url"),
        "promoted": promoted,
        "tenders_collected": len(promoted),
        "status": "extracted",
    }

"""
Dagster entry point for the IDS-DRR Flood Risk Pipeline.

This module defines the Dagster Definitions object that registers all assets,
resources, schedules, and sensors for the multi-state flood risk pipeline.
"""

import json
import logging

import dagster as dg

from flood_risk_pipeline.assets.extraction import satellite, weather, procurement
from flood_risk_pipeline.assets.transformation import hazard_factor, vulnerability_factor, risk_score
from flood_risk_pipeline.assets.outputs import risk_model
from flood_risk_pipeline.sources.apis import api_resources
from flood_risk_pipeline.sources.storage import storage_resources
from flood_risk_pipeline.partitions import (
    state_partitions,
    monthly_state_partitions,
    daily_partitions,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Collect all assets
# ---------------------------------------------------------------------------

extraction_assets = dg.load_assets_from_modules(
    [satellite, weather, procurement],
    group_name="extraction",
)

transformation_assets = dg.load_assets_from_modules(
    [hazard_factor, vulnerability_factor, risk_score],
    group_name="transformation",
)

output_assets = dg.load_assets_from_modules(
    [risk_model],
    group_name="outputs",
)

all_assets = [*extraction_assets, *transformation_assets, *output_assets]


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

daily_collection_schedule = dg.ScheduleDefinition(
    name="daily_collection",
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    target=dg.AssetSelection.groups("extraction").required_multi_asset_neighbors(),
    description="Daily collection of weather and satellite data",
)

monthly_aggregation_schedule = dg.ScheduleDefinition(
    name="monthly_aggregation",
    cron_schedule="0 2 1 * *",  # Run at 2 AM on 1st of each month
    target=dg.AssetSelection.groups("transformation", "outputs"),
    description="Monthly aggregation and risk model execution",
)


# ---------------------------------------------------------------------------
# Sensors
# ---------------------------------------------------------------------------

# States to monitor for new flood dates on the Bhuvan portal.
_BHUVAN_STATES: dict[str, str] = {
    "as": "assam",
    "br": "bihar",
    "od": "odisha",
    "hp": "himachal_pradesh",
    "up": "uttar_pradesh",
}


@dg.sensor(
    name="bhuvan_new_dates_sensor",
    target=dg.AssetSelection.keys("bhuvan_flood_maps"),
    minimum_interval_seconds=3600 * 6,  # Check every 6 hours
    description="Polls Bhuvan portal for new flood observation dates",
)
def bhuvan_new_dates_sensor(context: dg.SensorEvaluationContext):
    """Detect new flood dates and trigger materialisation of bhuvan_flood_maps.

    The sensor stores a JSON cursor mapping each state to a list of
    previously-seen date strings.  When new dates appear, it emits
    ``RunRequest`` objects targeting the corresponding state-month partitions.
    """
    from flood_risk_pipeline.sources.bhuvan import discover_flood_dates

    cursor: dict[str, list[str]] = json.loads(context.cursor or "{}")
    new_cursor = dict(cursor)
    run_requests: list[dg.RunRequest] = []

    for code, state_id in _BHUVAN_STATES.items():
        try:
            dates = discover_flood_dates(code)
        except Exception as exc:
            logger.warning("Date discovery failed for %s: %s", state_id, exc)
            continue

        known = set(cursor.get(state_id, []))
        new_dates = [d for d in dates if d.date_string not in known]

        if new_dates:
            # Group by month to create one run per state-month
            months_seen: set[str] = set()
            for d in new_dates:
                month_key = f"{d.year}-{d.month:02d}-01"
                months_seen.add(month_key)

            for month_key in sorted(months_seen):
                run_requests.append(
                    dg.RunRequest(
                        run_key=f"{state_id}_{month_key}",
                        partition_key=dg.MultiPartitionKey(
                            {"state": state_id, "month": month_key}
                        ),
                    )
                )

            new_cursor[state_id] = list(
                known | {d.date_string for d in new_dates}
            )

    context.update_cursor(json.dumps(new_cursor))
    return run_requests


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

all_resources = {
    **api_resources,
    **storage_resources,
}


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = dg.Definitions(
    assets=all_assets,
    resources=all_resources,
    schedules=[
        daily_collection_schedule,
        monthly_aggregation_schedule,
    ],
    sensors=[
        bhuvan_new_dates_sensor,
    ],
)

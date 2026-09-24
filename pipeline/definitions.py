"""
Dagster entry point for the IDS-DRR Flood Risk Pipeline.

This module defines the Dagster Definitions object that registers all assets,
resources, schedules, and sensors for the multi-state flood risk pipeline.
"""

import json
import logging

import dagster as dg
import importlib
from dateutil.relativedelta import relativedelta
import pipeline


# from pipeline.assets.extraction import satellite, weather, procurement
# from pipeline.assets.transformation import hazard_factor, vulnerability_factor, risk_score
# from pipeline.assets.outputs import risk_model
# from pipeline.assets import extraction, transformation, outputs

satellite = importlib.import_module('pipeline.assets.extraction.satellite')
weather = importlib.import_module('pipeline.assets.extraction.weather')
procurement = importlib.import_module('pipeline.assets.extraction.procurement')

hazard_factor = importlib.import_module('pipeline.assets.transformation.hazard_factor')
vulnerability_factor = importlib.import_module('pipeline.assets.transformation.vulnerability_factor')
risk_score = importlib.import_module('pipeline.assets.transformation.risk_score')

risk_model = importlib.import_module('pipeline.assets.outputs.risk_model')

from pipeline.sources.apis import api_resources
# from pipeline.sources.storage import storage_resources
from pipeline.partitions import (
    state_partitions,
    monthly_state_partitions,
    daily_partitions,
)

logger = logging.getLogger(__name__)


# Collect all assets

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



# States for IMD monthly schedule
_IMD_STATES = [
    "himachal_pradesh",
    "assam",
    "odisha",
    "bihar",
    "uttar_pradesh",
]


@dg.schedule(
    name="imd_monthly_rain_schedule",
    cron_schedule="0 2 2 * *",  # 2nd of every month, 2 AM
    target=dg.AssetSelection.keys("imd_monthly_rain_data"),
)
def imd_monthly_rain_schedule(context: dg.ScheduleEvaluationContext):
    """Extract last month's IMD rain data for all 5 states."""
    last_month = context.scheduled_execution_time - relativedelta(months=1)
    month_key = last_month.strftime("%Y-%m-01")

    return [
        dg.RunRequest(
            run_key=f"imd_rain_{state}_{month_key}",
            partition_key=dg.MultiPartitionKey({"state": state, "month": month_key}),
        )
        for state in _IMD_STATES
    ]


# Schedules
daily_collection_schedule = dg.ScheduleDefinition(
    name="daily_collection",
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    target=dg.AssetSelection.keys("gcn250_rainfall_data"),
    description="Daily collection of weather and satellite data",
)

monthly_extraction_schedule = dg.ScheduleDefinition(
    name="monthly_extraction",
    cron_schedule="0 2 1 * *",  # Run at 2 AM on 1st of each month
    target=dg.AssetSelection.keys(
        "bhuvan_flood_maps",
        "raw_satellite_data",
        "raw_weather_data",
        "raw_procurement_data",
        "raw_budget_data",
    ),
    description="Monthly extraction of satellite, weather, and procurement data",
)

monthly_aggregation_schedule = dg.ScheduleDefinition(
    name="monthly_aggregation",
    cron_schedule="0 2 1 * *",  # Run at 2 AM on 1st of each month
    target=dg.AssetSelection.groups("transformation") | dg.AssetSelection.keys("risk_score_output"),
    description="Monthly aggregation and risk model execution",
)


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
    minimum_interval_seconds=864000,  # Check every 10 days (10 * 24 * 3600)
    description="Polls Bhuvan portal for new flood observation dates",
)

def bhuvan_new_dates_sensor(context: dg.SensorEvaluationContext):
    """Detect new flood dates and trigger materialisation of bhuvan_flood_maps.

    The sensor stores a JSON cursor mapping each state to a list of
    previously-seen date strings.  When new dates appear, it emits
    ``RunRequest`` objects targeting the corresponding state-month partitions.
    """
    from pipeline.sources.bhuvan import discover_flood_dates

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


#Jobs
assam_extraction_job = dg.define_asset_job(
    name="assam_extraction_job",
    selection=dg.AssetSelection.keys(
        "bhuvan_flood_maps",
        "raw_weather_data",
    ).upstream(),
    partitions_def=monthly_state_partitions,
    description="Extract flood risk data for Assam state",
)


Odisha_extraction_job = dg.define_asset_job(
    name="odisha_extraction_job",
    selection=dg.AssetSelection.keys(
        "bhuvan_flood_maps",
        "raw_weather_data",
    ).upstream(),
    partitions_def=monthly_state_partitions,
    description="Extract flood risk data for Odisha state",
)


hp_extraction_job = dg.define_asset_job(
    name="hp_extraction_job",
    selection=dg.AssetSelection.keys(
        "bhuvan_flood_maps",
        "raw_weather_data",
    ).upstream(),
    partitions_def=monthly_state_partitions,
    description="Extract flood risk data for Himachal Pradesh state",
)


# Resources
all_resources = {
    **api_resources,
}


# Definitions

defs = dg.Definitions(
    assets=all_assets,
    resources=all_resources,
    schedules=[
        daily_collection_schedule,
        monthly_extraction_schedule,
        monthly_aggregation_schedule,
        imd_monthly_rain_schedule,
    ],
    sensors=[
        bhuvan_new_dates_sensor,
    ],
    jobs=[
        assam_extraction_job, Odisha_extraction_job, hp_extraction_job
    ],
)

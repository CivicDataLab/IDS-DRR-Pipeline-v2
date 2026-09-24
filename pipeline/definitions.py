"""
Dagster entry point for the IDS-DRR Flood Risk Pipeline.

This module defines the Dagster Definitions object that registers all assets,
resources, schedules, and sensors for the multi-state flood risk pipeline.

Orchestration model:

- ``monthly_state_etl_schedule`` fires on the 1st of each month and requests
  last month's partition of every extraction asset, once per enabled state.
- Everything downstream (master_variables -> factors -> outputs) carries
  ``AutomationCondition.eager()`` and re-derives whenever an upstream
  materialises — necessary because manual drop-in sources land at
  unpredictable times.
- ``bhuvan_new_dates_sensor`` polls the Bhuvan portal for new flood dates.
- ``manual_inbox_sensor`` watches ``inbox/{state}/{source}/`` drop folders.
"""

import importlib
import json
import logging

import dagster as dg
from dateutil.relativedelta import relativedelta

from pipeline.sources import manual_inbox
from pipeline.sources.apis import api_resources
from pipeline.state_config import enabled_states

satellite = importlib.import_module("pipeline.assets.extraction.satellite")
weather = importlib.import_module("pipeline.assets.extraction.weather")
procurement = importlib.import_module("pipeline.assets.extraction.procurement")
hydrology = importlib.import_module("pipeline.assets.extraction.hydrology")
demographic = importlib.import_module("pipeline.assets.extraction.demographic")

master_variables = importlib.import_module(
    "pipeline.assets.transformation.master_variables"
)
hazard_factor = importlib.import_module("pipeline.assets.transformation.hazard_factor")
vulnerability_factor = importlib.import_module(
    "pipeline.assets.transformation.vulnerability_factor"
)
risk_score = importlib.import_module("pipeline.assets.transformation.risk_score")

risk_model = importlib.import_module("pipeline.assets.outputs.risk_model")

logger = logging.getLogger(__name__)


# Collect all assets

extraction_assets = dg.load_assets_from_modules(
    [satellite, weather, procurement, hydrology, demographic],
    group_name="extraction",
)

transformation_assets = dg.load_assets_from_modules(
    [master_variables, hazard_factor, vulnerability_factor, risk_score],
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

# Monthly extraction assets kicked off together on the 1st. Downstream
# transformation/output assets are NOT scheduled: they follow automatically
# via their eager automation conditions once extraction lands.
_ETL_ASSET_KEYS = [
    "imd_monthly_rain_data",
    "ffs_river_levels",
    "bhuvan_flood_maps",
    "raw_satellite_data",
    "sentinel_indices",
    "nrsc_runoff",
    "worldpop_annual_data",
    "raw_procurement_data",
]


@dg.schedule(
    name="monthly_state_etl_schedule",
    cron_schedule="0 2 1 * *",  # 1st of every month, 2 AM
    target=dg.AssetSelection.assets(*_ETL_ASSET_KEYS),
)
def monthly_state_etl_schedule(context: dg.ScheduleEvaluationContext):
    """Request last month's extraction partition for every enabled state."""
    last_month = context.scheduled_execution_time - relativedelta(months=1)
    month_key = last_month.strftime("%Y-%m-01")

    return [
        dg.RunRequest(
            run_key=f"etl_{state}_{month_key}",
            partition_key=dg.MultiPartitionKey({"state": state, "month": month_key}),
        )
        for state in enabled_states()
    ]


# ---------------------------------------------------------------------------
# Sensors
# ---------------------------------------------------------------------------

# Evaluates the eager() automation conditions on downstream assets.
automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300,
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
    target=dg.AssetSelection.assets("bhuvan_flood_maps"),
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


# Drop-in sources: inbox source folder -> asset that promotes its files.
_INBOX_ASSET_BY_SOURCE = {
    "tenders": "raw_procurement_data",
    "nrsc": "nrsc_runoff",
    "worldpop": "worldpop_annual_data",
}


@dg.sensor(
    name="manual_inbox_sensor",
    target=dg.AssetSelection.assets(*sorted(set(_INBOX_ASSET_BY_SOURCE.values()))),
    minimum_interval_seconds=3600,  # hourly
    description="Watches inbox/{state}/{source}/ for manually dropped CSVs",
)
def manual_inbox_sensor(context: dg.SensorEvaluationContext):
    """Request the matching asset partition for each new inbox file.

    The cursor is a JSON snapshot of pending file names and mtimes per
    state/source; a run is requested only when the snapshot changes.
    """
    cursor: dict = json.loads(context.cursor or "{}")
    new_cursor: dict = {}
    run_requests: list[dg.RunRequest] = []

    for state in enabled_states():
        pending = manual_inbox.scan_all_inboxes(state)
        if not pending:
            continue
        new_cursor[state] = json.loads(manual_inbox.inbox_cursor(pending))

        for source, files in pending.items():
            asset_name = _INBOX_ASSET_BY_SOURCE.get(source)
            if asset_name is None:
                continue

            seen = cursor.get(state, {}).get(source, {})
            months: set[str] = set()
            for path in files:
                if str(path.stat().st_mtime) == str(seen.get(path.name)):
                    continue  # unchanged since last evaluation
                try:
                    _, timeperiod = manual_inbox.parse_inbox_filename(source, path.name)
                except ValueError as exc:
                    logger.warning("Ignoring inbox file %s: %s", path, exc)
                    continue
                year, month = timeperiod.split("_")[0], timeperiod.split("_")[-1]
                if len(timeperiod) == 4:  # annual file -> refresh via January run
                    months.add(f"{year}-01-01")
                else:
                    months.add(f"{year}-{month}-01")

            for month_key in sorted(months):
                run_requests.append(
                    dg.RunRequest(
                        run_key=f"inbox_{state}_{source}_{month_key}_{context.cursor or ''}",
                        partition_key=dg.MultiPartitionKey(
                            {"state": state, "month": month_key}
                        ),
                        asset_selection=[dg.AssetKey(asset_name)],
                    )
                )

    context.update_cursor(json.dumps(new_cursor, sort_keys=True))
    return run_requests


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

# Ad-hoc backfill job: materialise all extraction assets for chosen
# state-month partitions from the Dagster UI or CLI.
state_etl_job = dg.define_asset_job(
    name="state_etl_job",
    selection=dg.AssetSelection.assets(*_ETL_ASSET_KEYS),
    description="Extract all sources for one state-month partition",
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
        monthly_state_etl_schedule,
    ],
    sensors=[
        automation_sensor,
        bhuvan_new_dates_sensor,
        manual_inbox_sensor,
    ],
    jobs=[
        state_etl_job,
    ],
)

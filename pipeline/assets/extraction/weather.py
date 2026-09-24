"""
Weather data extraction assets.

- IMD: national rainfall grids resampled to 0.01 degrees, then zonal
  statistics per admin boundary.
"""

import calendar
from datetime import datetime
from pathlib import Path

import dagster as dg
import geopandas as gpd

from pipeline.partitions import monthly_state_partitions
from pipeline.sources import imd as imd_src
from pipeline.sources import staging_layout
from pipeline.state_config import boundaries_path, load_state_config


@dg.asset(
    partitions_def=monthly_state_partitions,
    description=(
        "Monthly IMD rainfall data: download national grids, resample to 0.01°, "
        "and compute zonal statistics per subdistrict for the given state."
    ),
)
def imd_monthly_rain_data(context: dg.AssetExecutionContext) -> dict:
    keys = context.partition_key.keys_by_dimension
    state = keys["state"]
    month_str = keys["month"]  # e.g. "2026-01-01"

    config = load_state_config(state)
    if not config.get("data_sources", {}).get("weather", {}).get("imd", {}).get("enabled"):
        context.log.info(f"IMD not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    year, month = target_date.year, target_date.month
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    # --- Download (skip if TIF already exists from another state run) ---
    month_tif = Path(imd_src.TIFF_DATA_FOLDER) / f"{year}_{month:02d}_resampled.tif"
    if not month_tif.exists():
        context.log.info(f"Downloading IMD rain data for {year}-{month:02d}")
        imd_src.download_data(year, start_date, end_date)
        imd_src.parse_and_format_data(year, start_date, end_date)
    else:
        context.log.info(f"Reusing existing TIF for {year}-{month:02d}")

    # --- State-specific zonal statistics ---
    admin_path = boundaries_path(state)
    if not admin_path or not admin_path.exists():
        context.log.warning(f"No admin boundaries for {state}, skipping zonal stats")
        return {"state": state, "month": month_str, "status": "no_admin_boundary"}

    join_field = config.get("boundaries", {}).get("join_field", "object_id")
    processed = imd_src.retrieve_subdistrict_data(
        year,
        gpd.read_file(admin_path),
        months=[f"{month:02d}"],
        state=state,
        join_field=join_field,
    )

    if not processed:
        context.log.warning(f"No IMD raster available yet for {year}-{month:02d}")
        return {"state": state, "month": month_str, "status": "no_raster"}

    _, staged = processed[0]
    return {
        "state": state,
        "month": month_str,
        "year": year,
        "month_num": month,
        "variables": staged,
        "status": "extracted",
    }


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="Monthly river levels from the Flood Forecasting System",
)
def ffs_river_levels(context: dg.AssetExecutionContext) -> dict:
    """Fetch and aggregate FFS station readings for a state-month."""
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    config = load_state_config(state)
    ffs_cfg = config.get("data_sources", {}).get("weather", {}).get("ffs", {})
    if not ffs_cfg.get("enabled"):
        context.log.info(f"FFS not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    from pipeline.sources import ffs as ffs_src

    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    if not ffs_src.stations_path(state).exists():
        context.log.warning(
            f"No FFS station geometries for {state} at {ffs_src.stations_path(state)}"
        )
        return {"state": state, "month": month_str, "status": "no_stations"}

    try:
        result = ffs_src.extract_month(state, target_date.year, target_date.month)
    except Exception as exc:
        context.log.error(f"FFS extraction failed for {state} {month_str}: {exc}")
        return {"state": state, "month": month_str, "status": "failed", "error": str(exc)}

    context.log.info(
        f"FFS {state} {month_str}: {result['status']} " f"({result.get('stations', 0)} stations)"
    )
    return {"state": state, "month": month_str, **result}


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["imd_monthly_rain_data", "ffs_river_levels"],
    description="Aggregated weather variables staged for a state-month",
)
def raw_weather_data(context: dg.AssetExecutionContext) -> dict:
    """Report which weather variables landed in staging for this partition."""
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]
    timeperiod = staging_layout.month_key_to_timeperiod(month_str)

    staged = []
    for source, variables in (
        ("imd", imd_src.RAIN_VARIABLES.values()),
        ("ffs", ["riverlevel_mean", "riverlevel_min", "riverlevel_max"]),
    ):
        for variable in variables:
            if staging_layout.variable_csv_path(state, source, variable, timeperiod).exists():
                staged.append(f"{source}/{variable}")

    context.log.info(f"raw_weather_data {state} {month_str}: {len(staged)} variables staged")
    return {
        "state": state,
        "month": month_str,
        "variables": staged,
        "status": "aggregated" if staged else "no_data",
    }

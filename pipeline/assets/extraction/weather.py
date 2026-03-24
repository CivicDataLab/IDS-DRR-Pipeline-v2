import calendar
import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import geopandas as gpd
import yaml

from flood_risk_pipeline.partitions import monthly_state_partitions
from flood_risk_pipeline.sources import imd as imd_src


def _load_state_config(state: str) -> dict:
    """Load state-specific configuration from YAML."""
    config_path = Path(__file__).parent.parent.parent / "config" / "states" / f"{state}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


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

    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    year, month = target_date.year, target_date.month
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    # --- Download (skip if TIF already exists from another state run) ---
    month_tif = Path(imd_src.TIFF_DATA_FOLDER) / f"{year}_{month:02d}.tif"
    if not month_tif.exists():
        context.log.info(f"Downloading IMD rain data for {year}-{month:02d}")
        imd_src.download_data(year, start_date, end_date)
        imd_src.parse_and_format_data(year, start_date, end_date)
    else:
        context.log.info(f"Reusing existing TIF for {year}-{month:02d}")

    # --- State-specific zonal statistics ---
    config = _load_state_config(state)
    bhuvan_cfg = config.get("data_sources", {}).get("satellite", {}).get("bhuvan_config", {})
    admin_shp = bhuvan_cfg.get("admin_boundary_shapefile")

    if not admin_shp or not os.path.exists(admin_shp):
        context.log.warning(f"No admin boundary shapefile for {state}, skipping zonal stats")
        return {"state": state, "month": month_str, "status": "no_admin_boundary"}

    admin_gdf = gpd.read_file(admin_shp)
    imd_src.retrieve_subdistrict_data(year, admin_gdf)

    csv_path = Path(imd_src.CSV_DATA_FOLDER) / f"{year}_{month:02d}.csv"
    return {
        "state": state,
        "month": month_str,
        "year": year,
        "month_num": month,
        "csv_path": str(csv_path),
        "status": "extracted",
    }


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["imd_monthly_rain_data"],
    description="Aggregated IMD rainfall data per state-month",
)
def raw_weather_data(context: dg.AssetExecutionContext) -> dict:
    keys = context.partition_key.keys_by_dimension
    state = keys["state"]
    month_str = keys["month"]
    context.log.info(f"raw_weather_data: state={state}, month={month_str}")
    return {"state": state, "month": month_str, "status": "aggregated"}

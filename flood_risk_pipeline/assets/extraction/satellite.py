"""
Satellite data extraction assets.

Handles extraction from:
- GCN250: Google Earth Engine rainfall-runoff data
- Bhuvan: ISRO flood maps via tile-based WMS download (no GDAL CLI)
"""

from datetime import datetime
from pathlib import Path
import os

import dagster as dg
import yaml

from flood_risk_pipeline.partitions import (
    STATE_SOURCES,
    daily_partitions,
    monthly_state_partitions,
)
from flood_risk_pipeline.sources.bhuvan import BhuvanStateConfig
from flood_risk_pipeline.sources import standalone_bhuvan


def _load_state_config(state: str) -> dict:
    """Load state-specific YAML configuration."""
    config_path = (
        Path(__file__).resolve().parent.parent.parent
        / "config"
        / "states"
        / f"{state}.yaml"
    )
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


# ---------------------------------------------------------------------------
# GCN250 (placeholder — unchanged)
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    description="Raw GCN250 rainfall-runoff data from Google Earth Engine",
)
def gcn250_rainfall_data(context: dg.AssetExecutionContext) -> dict:
    """Extract GCN250 rainfall-runoff data for a given date."""
    partition_date = context.partition_key
    context.log.info(f"Extracting GCN250 data for {partition_date}")
    # TODO: Implement actual GEE extraction
    return {
        "date": partition_date,
        "source": "gcn250",
        "status": "extracted",
        "records": 0,
    }



# ---------------------------------------------------------------------------
# Bhuvan flood maps — full tile-based pipeline


@dg.asset(
    partitions_def=monthly_state_partitions,
    description=(
        "Bhuvan flood maps: tile download, stitching, watermark removal, "
        "and GeoTIFF creation for all available dates in a state-month."
    ),
    required_resource_keys={"staging"},
)
def bhuvan_flood_maps(context: dg.AssetExecutionContext) -> dict:
    """Download and process all Bhuvan flood maps for a state-month.

    For each flood observation date within the partition month:

    1. Downloads 256x256 WMS tiles in parallel.
    2. Stitches tiles into a full-state mosaic.
    3. Removes the Bhuvan watermark.
    4. Creates a binary inundation GeoTIFF (1 = inundated, 0 = not).
    """
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    config = _load_state_config(state)
    bhuvan_enabled = (
        config.get("data_sources", {}).get("satellite", {}).get("bhuvan", False)
    )
    if not bhuvan_enabled:
        context.log.info(f"Bhuvan not enabled for {state}, skipping")
        return {"state": state, "month": month_str, "status": "skipped"}

    bhuvan_cfg = config.get("data_sources", {}).get("satellite", {}).get("bhuvan_config")
    if not bhuvan_cfg:
        context.log.warning(f"Bhuvan enabled but no bhuvan_config for {state}")
        return {"state": state, "month": month_str, "status": "missing_config"}

    staging = context.resources.staging

    # Parse target year/month from partition key (format "2024-06-01")
    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    target_year = target_date.year
    target_month = target_date.month

    # Load standalone bhuvan config
    bhuvan_config = standalone_bhuvan.load_config()
    state_actual_name, state_cfg = standalone_bhuvan.get_state_config(state, bhuvan_config)

    if not state_cfg:
        context.log.error(f"State '{state}' not found in standalone_bhuvan configuration")
        return {"state": state, "month": month_str, "status": "config_not_found"}

    # Fetch dates from Bhuvan
    try:
        context.log.info(f"Fetching dates for {state} from Bhuvan...")
        all_dates = standalone_bhuvan.fetch_dates_from_bhuvan(state_actual_name, bhuvan_config)
    except Exception as exc:
        context.log.error(f"Date discovery failed for {state}: {exc}")
        return {"state": state, "month": month_str, "status": "discovery_failed"}

    # Filter dates to the target month
    # Date format is: YYYY_DD_MM or YYYY_DD_MM_HH
    month_dates = []
    for date_str in all_dates:
        parts = date_str.split('_')
        if len(parts) >= 3:
            date_year = int(parts[0])
            date_month = int(parts[2])
            if date_year == target_year and date_month == target_month:
                month_dates.append(date_str)

    context.log.info(
        f"Found {len(month_dates)} flood dates for {state} "
        f"in {target_year}-{target_month:02d}"
    )
    if not month_dates:
        return {
            "state": state,
            "month": month_str,
            "status": "no_data",
            "daily_tiffs": [],
        }

    # Set up output directory
    output_dir = staging.get_monthly_path(state, month_str) / "bhuvan"
    os.makedirs(output_dir, exist_ok=True)

    # Setup directories for standalone_bhuvan
    paths = standalone_bhuvan.setup_directories(str(output_dir))

    # Process each date
    daily_tiffs: list[str] = []

    for date_string in month_dates:
        context.log.info(f"Processing {state} — {date_string}")
        try:
            success = standalone_bhuvan.process_date(
                date_string,
                state_actual_name,
                state_cfg,
                str(output_dir),
                paths
            )
            if success:
                # Find the created TIFF
                tiff_path = os.path.join(paths['tiffs'], f'{date_string}.tif')
                if os.path.exists(tiff_path):
                    daily_tiffs.append(tiff_path)
                    context.log.info(f"Successfully processed {date_string}")
            else:
                context.log.warning(f"Failed to process {date_string}")
        except Exception as exc:
            context.log.warning(f"Failed to process {date_string}: {exc}")

    return {
        "state": state,
        "month": month_str,
        "status": "extracted",
        "daily_tiffs": daily_tiffs,
        "dates_processed": len(daily_tiffs),
        "dates_available": len(month_dates),
    }


# Aggregated satellite data — monthly composite + zonal statistics



@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["gcn250_rainfall_data", "bhuvan_flood_maps"],
    description=(
        "Aggregated satellite data: monthly inundation raster and "
        "zonal statistics per admin boundary."
    ),
    required_resource_keys={"staging"},
)
def raw_satellite_data(context: dg.AssetExecutionContext) -> dict:
    """
    Aggregate daily flood maps and compute zonal statistics.
    1. Sums daily inundation rasters into a monthly composite GeoTIFF.
    2. Computes per-admin-boundary statistics (inundation %, intensity).
    3. Saves results as CSV.
    """
    keys = context.partition_key.keys_by_dimension
    state, month_str = keys["state"], keys["month"]

    sources = STATE_SOURCES.get(state, [])
    has_bhuvan = "bhuvan" in sources

    if not has_bhuvan:
        context.log.info(f"Bhuvan not in STATE_SOURCES for {state}")
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": False,
            "status": "no_bhuvan",
        }
        
    config = _load_state_config(state)
    bhuvan_cfg_dict = (
        config.get("data_sources", {}).get("satellite", {}).get("bhuvan_config")
    )
    if not bhuvan_cfg_dict:
        context.log.warning(f"No bhuvan_config in YAML for {state}")
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": True,
            "status": "missing_config",
        }

    state_cfg = BhuvanStateConfig.from_yaml_dict(config)
    staging = context.resources.staging

    # Locate daily GeoTIFFs produced by bhuvan_flood_maps
    # The standalone_bhuvan saves tiffs in the 'tiffs' subdirectory
    monthly_dir = staging.get_monthly_path(state, month_str) / "bhuvan"
    tiffs_dir = monthly_dir / "tiffs"

    daily_tiffs = sorted(Path(tiffs_dir).glob("*.tif")) if tiffs_dir.exists() else []

    if not daily_tiffs:
        context.log.info(f"No daily GeoTIFFs for {state} {month_str}")
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": True,
            "status": "no_daily_data",
        }

    # Parse target year/month from partition key
    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    target_year = target_date.year
    target_month = target_date.month

    # Setup paths for zonal stats computation
    paths = {
        'tiffs': str(tiffs_dir),
        'stitched_monthly': str(monthly_dir / "stitched_monthly"),
        'csv': str(monthly_dir / "csv")
    }

    for path_dir in [paths['stitched_monthly'], paths['csv']]:
        os.makedirs(path_dir, exist_ok=True)

    # Get admin shapefile path
    admin_path = state_cfg.admin_boundary_path

    if admin_path and os.path.exists(admin_path):
        try:
            context.log.info(f"Computing monthly zonal statistics for {state} {month_str}")

            # Use standalone_bhuvan's compute_monthly_zonal_stats function
            csv_path = standalone_bhuvan.compute_monthly_zonal_stats(
                str(tiffs_dir),
                paths,
                admin_path,
                str(target_year),
                f"{target_month:02d}"
            )

            if csv_path:
                # Find the monthly stitched raster
                monthly_raster_path = os.path.join(
                    paths['stitched_monthly'],
                    f"stitched_{target_year}_{target_month:02d}.tif"
                )

                context.log.info(
                    f"Monthly raster saved: {monthly_raster_path} "
                    f"(aggregated {len(daily_tiffs)} daily maps)"
                )

                return {
                    "state": state,
                    "month": month_str,
                    "bhuvan_available": True,
                    "daily_maps_count": len(daily_tiffs),
                    "monthly_raster": monthly_raster_path,
                    "zonal_stats_csv": csv_path,
                    "status": "aggregated",
                }
            else:
                context.log.warning(f"Zonal stats computation returned None for {state} {month_str}")
                return {
                    "state": state,
                    "month": month_str,
                    "bhuvan_available": True,
                    "daily_maps_count": len(daily_tiffs),
                    "status": "aggregated_zonal_failed",
                }
        except Exception as exc:
            context.log.error(f"Zonal stats failed: {exc}")
            return {
                "state": state,
                "month": month_str,
                "bhuvan_available": True,
                "daily_maps_count": len(daily_tiffs),
                "status": "aggregated_zonal_failed",
                "error": str(exc),
            }
    else:
        context.log.warning(
            f"No admin_boundary_shapefile for {state} or file doesn't exist — skipping zonal stats"
        )
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": True,
            "daily_maps_count": len(daily_tiffs),
            "status": "aggregated_no_zonal",
        }

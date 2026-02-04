"""
Satellite data extraction assets.

Handles extraction from:
- GCN250: Google Earth Engine rainfall-runoff data
- Bhuvan: ISRO flood maps via tile-based WMS download (no GDAL CLI)
"""

from datetime import datetime
from pathlib import Path

import dagster as dg
import yaml

from flood_risk_pipeline.partitions import (
    STATE_SOURCES,
    daily_partitions,
    monthly_state_partitions,
)
from flood_risk_pipeline.sources.bhuvan import BhuvanStateConfig


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
    group_name="extraction",
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
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=monthly_state_partitions,
    group_name="extraction",
    description=(
        "Bhuvan flood maps: tile download, stitching, watermark removal, "
        "and GeoTIFF creation for all available dates in a state-month."
    ),
    required_resource_keys={"bhuvan_wms", "staging"},
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

    state_cfg = BhuvanStateConfig.from_yaml_dict(config)
    bhuvan = context.resources.bhuvan_wms
    staging = context.resources.staging

    # Parse target year/month from partition key (format "2024-06-01")
    target_date = datetime.strptime(month_str, "%Y-%m-%d")
    target_year = target_date.year
    target_month = target_date.month

    # Discover all available flood dates for this state
    try:
        all_dates = bhuvan.discover_dates(state_cfg.bhuvan_code)
    except Exception as exc:
        context.log.error(f"Date discovery failed for {state}: {exc}")
        return {"state": state, "month": month_str, "status": "discovery_failed"}

    # Filter to dates within the target month
    month_dates = [
        d
        for d in all_dates
        if d.year == target_year and d.month == target_month
    ]

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

    # Download and process each date
    output_dir = staging.get_monthly_path(state, month_str) / "bhuvan" / "daily"
    daily_tiffs: list[str] = []

    for flood_date in month_dates:
        context.log.info(
            f"Processing {state} — {flood_date.date_string}"
        )
        try:
            tiff_path = bhuvan.download_flood_map(
                state_cfg,
                flood_date.date_string,
                output_dir,
            )
            daily_tiffs.append(str(tiff_path))
        except Exception as exc:
            context.log.warning(
                f"Failed to process {flood_date.date_string}: {exc}"
            )

    return {
        "state": state,
        "month": month_str,
        "status": "extracted",
        "daily_tiffs": daily_tiffs,
        "dates_processed": len(daily_tiffs),
        "dates_available": len(month_dates),
    }


# ---------------------------------------------------------------------------
# Aggregated satellite data — monthly composite + zonal statistics
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=monthly_state_partitions,
    group_name="extraction",
    deps=[gcn250_rainfall_data, bhuvan_flood_maps],
    description=(
        "Aggregated satellite data: monthly inundation raster and "
        "zonal statistics per admin boundary."
    ),
    required_resource_keys={"bhuvan_wms", "staging"},
)
def raw_satellite_data(context: dg.AssetExecutionContext) -> dict:
    """Aggregate daily flood maps and compute zonal statistics.

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
    bhuvan = context.resources.bhuvan_wms
    staging = context.resources.staging

    # Locate daily GeoTIFFs produced by bhuvan_flood_maps
    daily_dir = (
        staging.get_monthly_path(state, month_str) / "bhuvan" / "daily"
    )
    daily_tiffs = sorted(daily_dir.glob("*.tif")) if daily_dir.exists() else []

    if not daily_tiffs:
        context.log.info(f"No daily GeoTIFFs for {state} {month_str}")
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": True,
            "status": "no_daily_data",
        }

    # Monthly aggregation
    monthly_dir = staging.get_monthly_path(state, month_str) / "bhuvan"
    monthly_path = monthly_dir / f"monthly_{state}_{month_str}.tif"
    raster_data, raster_meta = bhuvan.aggregate_month(daily_tiffs, monthly_path)

    context.log.info(
        f"Monthly raster saved: {monthly_path} "
        f"(aggregated {len(daily_tiffs)} daily maps)"
    )

    # Zonal statistics (only if shapefile path configured)
    admin_path = state_cfg.admin_boundary_path
    if admin_path:
        try:
            zonal_df = bhuvan.compute_stats(
                raster_data, raster_meta, admin_path
            )
            csv_path = (
                monthly_dir / f"zonal_stats_{state}_{month_str}.csv"
            )
            zonal_df.to_csv(str(csv_path), index=False)
            context.log.info(
                f"Zonal stats: {len(zonal_df)} admin units → {csv_path}"
            )
            return {
                "state": state,
                "month": month_str,
                "bhuvan_available": True,
                "daily_maps_count": len(daily_tiffs),
                "monthly_raster": str(monthly_path),
                "zonal_stats_csv": str(csv_path),
                "admin_units": len(zonal_df),
                "status": "aggregated",
            }
        except Exception as exc:
            context.log.error(f"Zonal stats failed: {exc}")
            return {
                "state": state,
                "month": month_str,
                "bhuvan_available": True,
                "daily_maps_count": len(daily_tiffs),
                "monthly_raster": str(monthly_path),
                "status": "aggregated_zonal_failed",
                "error": str(exc),
            }
    else:
        context.log.warning(
            f"No admin_boundary_shapefile for {state} — skipping zonal stats"
        )
        return {
            "state": state,
            "month": month_str,
            "bhuvan_available": True,
            "daily_maps_count": len(daily_tiffs),
            "monthly_raster": str(monthly_path),
            "status": "aggregated_no_zonal",
        }

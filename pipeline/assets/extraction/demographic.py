import logging
import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import yaml

from flood_risk_pipeline.sources import process_geojson, WorldPopDataFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
WORLDPOP_DIR = str(BASE_DIR / "sources" / "data" / "worldpop")
CONFIG_DIR = BASE_DIR / "config" / "states"

from flood_risk_pipeline.partitions import (
    STATE_SOURCES,
    daily_partitions,
    monthly_state_partitions,
)

def load_state_config(state):
    config_path = CONFIG_DIR / f"{state}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}

@dg.asset(partitions_def=monthly_state_partitions, description = "Demographic data of states from worldpop")

def district_geojsons(context: dg.AssetExecutionContext):
    keys = context.partition_key.keys_by_dimension

    state, month = keys["state"], keys["month"]

    context.log.info(f"Extracting budget data for {state} - {month}")
    context.log.info(f"Preparing geojson for {state}")

    config = load_state_config(state)

    shapefile_path = config["data_sources"]["satellite"]["bhuvan_config"]["admin_boundary_shapefile"]

    output_dir = os.path.join(WORLDPOP_DIR, state)

    os.makedirs(output_dir, exist_ok=True)

    geojson_files = [
        f for f in os.listdir(output_dir)
        if f.endswith(".geojson")
    ]

    if geojson_files:
        context.log.info(f"Geojsons already exist for {state}")
        return output_dir

    context.log.info(f"Generating district geojsons for {state}")

    process_geojson(
        input_shapefile=shapefile_path,
        output_dir=output_dir
    )

    return output_dir

@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["district_geojsons"],
    description="Demographic data of states from worldpop",
)

def worldpop_data(context: dg.AssetExecutionContext):
    keys = context.partition_key.keys_by_dimension

    state = keys["state"]
    month_str = keys["month"]
    year = datetime.strptime(month_str, "%Y-%m-%d").year

    config = load_state_config(state)
    geojson_dir_value = (
        config.get("data_sources", {})
        .get("demographics", {})
        .get("geojson_dir")
    )
    if geojson_dir_value:
        geojson_dir = Path(geojson_dir_value)
    else:
        geojson_dir = Path(WORLDPOP_DIR) / state
        context.log.warning(
            f"No geojson_dir configured for {state}; defaulting to {geojson_dir}"
        )

    fetcher = WorldPopDataFetcher(
        year=year,
        api_key=None,   # or "YOUR_KEY"
        simplify_tolerance=0.01,
        truncate_precision=3,
        async_threshold=1500,
        output_dir=None,  # let the class use/create .../agesexstructure/2017
    )

    files = list(sorted(geojson_dir.glob("*.geojson")))
    logger.info(f"Found {len(files)} geojson files in {geojson_dir}")

    for gj in files:
        district = gj.stem
        # This is the same naming pattern used in _save_pyramid_data
        expected_csv = fetcher.output_dir / f"{district}_agesexpyramid_{year}.csv"

        if expected_csv.exists():
            logger.info(f"CSV already exists for {district}: {expected_csv} (will OVERWRITE)")
        else:
            logger.info(f"No existing CSV for {district}. A new file will be created at: {expected_csv}")

        logger.info(f"Fetching for: {gj.name}")
        success = fetcher.fetch_worldpop_data(str(gj), dataset="wpgpas", year=year)
        if not success:
            logger.error(f"Failed for {gj.name}")






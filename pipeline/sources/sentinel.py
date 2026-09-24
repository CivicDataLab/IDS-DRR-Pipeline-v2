"""Sentinel-2 vegetation and built-up indices via Google Earth Engine.

Port of ``Sources/SENTINEL/scripts/sentinel.py``. The upstream script
selected the boundary join field with a per-state if-switch; here it comes
from ``boundaries.join_field`` in the state YAML, so a new state needs no
code change.

Earth Engine credentials are read from the service-account JSON named by
``data_sources.satellite.sentinel.gee_credentials_env``. When that variable
is unset the caller gets ``status: no_credentials`` rather than an
exception, so the pipeline degrades gracefully in CI.
"""

import json
import logging
import os
import tempfile

import geopandas as gpd
import numpy as np
import pandas as pd

from pipeline.sources import staging_layout
from pipeline.state_config import boundaries_path

logger = logging.getLogger(__name__)

COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
MAX_CLOUD_PCT = 20
SCALE_METRES = 250

INDICES = {
    "ndvi": ("B8", "B4"),
    "ndbi": ("B11", "B8"),
}


def mask_s2_clouds(image):
    """Mask cloud and cirrus pixels using the Sentinel-2 QA60 band."""
    qa = image.select("QA60")
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
    return image.updateMask(mask).divide(10000)


def initialize_earth_engine(cfg: dict) -> bool:
    """Initialise the Earth Engine client; False when no credentials exist."""
    sentinel_cfg = cfg.get("data_sources", {}).get("satellite", {}).get("sentinel", {})
    env_var = sentinel_cfg.get("gee_credentials_env", "GEE_SERVICE_ACCOUNT_JSON")
    raw = os.environ.get(env_var)
    if not raw:
        logger.warning("Earth Engine credentials not found in $%s", env_var)
        return False

    import ee

    credentials_json = raw
    if not raw.lstrip().startswith("{"):
        with open(raw) as f:
            credentials_json = f.read()
    service_account = json.loads(credentials_json)["client_email"]

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        tmp.write(credentials_json)
        key_path = tmp.name
    try:
        ee.Initialize(ee.ServiceAccountCredentials(service_account, key_path))
    finally:
        os.unlink(key_path)
    return True


def compute_monthly_indices(state: str, cfg: dict, year: int, month: int) -> dict:
    """Compute per-boundary NDVI/NDBI means for one state-month.

    Zonal means are computed inside Earth Engine with ``reduceRegions``,
    which avoids downloading rasters entirely.
    """
    if not initialize_earth_engine(cfg):
        return {"status": "no_credentials", "variables": []}

    import ee

    join_field = cfg.get("boundaries", {}).get("join_field", "object_id")
    boundaries = gpd.read_file(boundaries_path(state)).to_crs("EPSG:4326")

    start = f"{year}-{month:02d}-01"
    end = (pd.Timestamp(start) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")

    features = [
        ee.Feature(
            ee.Geometry(row.geometry.__geo_interface__),
            {join_field: getattr(row, join_field)},
        )
        for row in boundaries.itertuples()
        if row.geometry is not None
    ]
    zones = ee.FeatureCollection(features)

    collection = (
        ee.ImageCollection(COLLECTION)
        .filter(ee.Filter.date(start, end))
        .filter(ee.Filter.bounds(zones.geometry()))
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", MAX_CLOUD_PCT))
    )
    if collection.size().getInfo() == 0:
        logger.warning("No Sentinel-2 scenes for %s %s-%02d", state, year, month)
        return {"status": "no_imagery", "variables": []}

    median = collection.map(mask_s2_clouds).median()
    timeperiod = f"{year}_{month:02d}"

    written = []
    for index, (band_a, band_b) in INDICES.items():
        image = median.normalizedDifference([band_a, band_b]).rename(index)
        stats = image.reduceRegions(
            collection=zones, reducer=ee.Reducer.mean(), scale=SCALE_METRES
        ).getInfo()

        rows = [
            {
                "object_id": feature["properties"].get(join_field),
                f"mean_{index}": feature["properties"].get("mean", np.nan),
            }
            for feature in stats["features"]
        ]
        df = pd.DataFrame(rows).dropna(subset=["object_id"])
        path = staging_layout.write_variable_csv(
            df,
            state,
            "sentinel",
            f"mean_{index}",
            timeperiod,
            value_columns=[f"mean_{index}"],
        )
        written.append(str(path))

    return {"status": "extracted", "variables": written, "boundaries": len(boundaries)}

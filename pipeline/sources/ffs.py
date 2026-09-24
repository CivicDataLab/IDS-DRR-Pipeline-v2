"""Flood Forecasting System (ffs.india-water.gov.in) river level readings.

Port of ``Sources/FFS/scripts/{scraper,transformer}.py`` from the
flood-data-ecosystem repositories, parameterised by state and month
instead of a hardcoded multi-year range.

Station geometries are read from ``data/reference/<state>/ffs/state_stations.geojson``
(a spatial join of the FFS station list against the state boundary, as
produced by the upstream ``stations.py``); readings are aggregated to the
admin boundary containing each station.
"""

import logging
from urllib.parse import quote

import geopandas as gpd
import pandas as pd
import requests

from pipeline.sources import staging_layout
from pipeline.state_config import boundaries_path

logger = logging.getLogger(__name__)

FFS_API = "https://ffs.india-water.gov.in/iam/api/new-entry-data/specification/sorted"
RIVER_LEVEL_VARIABLES = ["riverlevel_mean", "riverlevel_min", "riverlevel_max"]


def stations_path(state: str):
    return staging_layout.reference_dir(state) / "ffs" / "state_stations.geojson"


def _query(station_code: str, start_date: str, end_date: str) -> str:
    """Build the FFS specification query for one station and date range."""
    sort = '{"sortOrderDtos":[{"sortDirection":"ASC","field":"id.dataTime"}]}'
    specification = (
        '{"where":{"where":{"where":{"expression":{"valueIsRelationField":false,'
        '"fieldName":"id.stationCode","operator":"eq","value":"' + station_code + '"}},'
        '"and":{"expression":{"valueIsRelationField":false,"fieldName":"id.datatypeCode",'
        '"operator":"eq","value":"HHS"}}},'
        '"and":{"expression":{"valueIsRelationField":false,"fieldName":"dataValue",'
        '"operator":"null","value":"false"}}},'
        '"and":{"expression":{"valueIsRelationField":false,"fieldName":"id.dataTime",'
        '"operator":"btn","value":"'
        + start_date
        + "T00:00:00.000,"
        + end_date
        + 'T23:59:59.999"}}}'
    )
    return f"{FFS_API}?sort-criteria={quote(sort)}&specification={quote(specification)}"


def fetch_station_readings(
    station_code: str, start_date: str, end_date: str, timeout: int = 60
) -> pd.DataFrame:
    """Fetch one station's hourly readings; empty frame when none exist."""
    response = requests.get(
        _query(station_code, start_date, end_date), verify=False, timeout=timeout
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return pd.DataFrame()

    df = pd.DataFrame(payload)
    if "id" not in df.columns:
        return pd.DataFrame()

    df["Date"] = df.id.apply(lambda x: x["dataTime"].split("T")[0])
    df["Time"] = df.id.apply(lambda x: x["dataTime"].split("T")[1])
    return df[["stationCode", "Date", "Time", "dataValue", "datatypeCode"]]


def fetch_month(state: str, year: int, month: int) -> pd.DataFrame:
    """Fetch every configured station's readings for one month."""
    path = stations_path(state)
    if not path.exists():
        raise FileNotFoundError(
            f"No FFS station geometries for {state} at {path}; "
            "run scripts/seed_data.py or add the file"
        )
    stations = gpd.read_file(path)

    start_date = f"{year}-{month:02d}-01"
    end_date = (pd.Timestamp(start_date) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    frames = []
    for station_code in stations.stationCode:
        try:
            readings = fetch_station_readings(station_code, start_date, end_date)
        except Exception as exc:
            logger.warning("FFS fetch failed for station %s: %s", station_code, exc)
            continue
        if not readings.empty:
            frames.append(readings)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def aggregate_to_boundaries(state: str, readings: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station readings to the admin boundary containing each station."""
    stations = gpd.read_file(stations_path(state))
    boundaries = gpd.read_file(boundaries_path(state))

    joined = gpd.sjoin(
        stations[["stationCode", "geometry"]].to_crs("EPSG:4326"),
        boundaries[["object_id", "geometry"]].to_crs("EPSG:4326"),
        how="inner",
    )

    merged = readings.merge(joined[["stationCode", "object_id"]], on="stationCode")
    aggregated = merged.groupby("object_id")["dataValue"].agg(["mean", "min", "max"]).reset_index()
    aggregated.columns = ["object_id", *RIVER_LEVEL_VARIABLES]
    return aggregated


def extract_month(state: str, year: int, month: int) -> dict:
    """Fetch, aggregate and stage one month of river levels for a state."""
    readings = fetch_month(state, year, month)
    if readings.empty:
        return {"status": "no_data", "stations": 0, "boundaries": 0}

    aggregated = aggregate_to_boundaries(state, readings)
    timeperiod = f"{year}_{month:02d}"
    paths = [
        str(
            staging_layout.write_variable_csv(
                aggregated, state, "ffs", variable, timeperiod, value_columns=[variable]
            )
        )
        for variable in RIVER_LEVEL_VARIABLES
    ]
    return {
        "status": "extracted",
        "stations": readings.stationCode.nunique(),
        "boundaries": len(aggregated),
        "paths": paths,
    }

"""WorldPop annual population statistics per admin boundary.

Port of ``Sources/WORLDPOP/scraper/.../worldpop_data_fetcher.py`` plus
``scripts/zonalstats.py``, with the hardcoded year and absolute paths
removed. Two routes are supported:

* ``zonal_stats_from_raster`` — preferred when a WorldPop GeoTIFF has been
  downloaded to ``data/reference/<state>/worldpop/``.
* ``WorldPopDataFetcher`` — the stats API, for age/sex pyramids where no
  raster is available. Large geometries are simplified and, if the API
  still rejects them, recursively split.
"""

import csv
import json
import logging
import time
from pathlib import Path

import requests
from shapely.geometry import box, mapping, shape

logger = logging.getLogger(__name__)

WORLDPOP_API = "https://api.worldpop.org/v1"
POPULATION_VARIABLES = [
    "sum_population",
    "mean_sex_ratio",
    "sum_aged_population",
    "sum_young_population",
]


def zonal_stats_from_raster(
    state: str, year: int, raster_path: Path, variable: str, statistic: str = "sum"
):
    """Aggregate a WorldPop raster to admin boundaries and stage the result."""
    import geopandas as gpd
    import pandas as pd
    import rasterio
    import rasterstats

    from pipeline.sources import staging_layout
    from pipeline.state_config import boundaries_path

    boundaries = gpd.read_file(boundaries_path(state))
    raster = rasterio.open(raster_path)

    stats = rasterstats.zonal_stats(
        boundaries.to_crs(raster.crs),
        raster.read(1),
        affine=raster.transform,
        stats=[statistic],
        nodata=raster.nodata,
        geojson_out=True,
    )
    df = pd.concat([pd.DataFrame([feature["properties"]]) for feature in stats]).reset_index(
        drop=True
    )
    df = df.rename(columns={statistic: variable})

    return staging_layout.write_variable_csv(
        df, state, "worldpop", variable, str(year), value_columns=[variable]
    )


class WorldPopDataFetcher:
    """Client for the WorldPop statistics API."""

    def __init__(
        self,
        output_dir: Path,
        year: int,
        base_url: str = WORLDPOP_API,
        simplify_tolerance: float = 0.14,
        poll_attempts: int = 10,
        poll_interval: float = 2.0,
    ):
        self.base_url = base_url
        self.year = year
        self.simplify_tolerance = simplify_tolerance
        self.poll_attempts = poll_attempts
        self.poll_interval = poll_interval
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def simplify_geometry(self, geojson, tolerance=None):
        tolerance = self.simplify_tolerance if tolerance is None else tolerance
        feature = geojson["features"][0]
        geometry = shape(feature["geometry"])
        feature["geometry"] = mapping(geometry.simplify(tolerance))
        return geojson

    def truncate_coordinates(self, geojson, precision=3):
        def _truncate(value):
            return round(float(value), precision)

        feature = geojson["features"][0]
        coords = feature["geometry"]["coordinates"]
        geometry_type = feature["geometry"]["type"]

        if geometry_type == "Polygon":
            feature["geometry"]["coordinates"] = [
                [[_truncate(v) for v in point] for point in ring] for ring in coords
            ]
        elif geometry_type == "MultiPolygon":
            feature["geometry"]["coordinates"] = [
                [[[_truncate(v) for v in point] for point in ring] for ring in polygon]
                for polygon in coords
            ]
        return geojson

    def split_geometry(self, geojson, n_splits=4):
        """Split a geometry into an n x n grid of intersecting pieces."""
        geometry = shape(geojson["features"][0]["geometry"])
        minx, miny, maxx, maxy = geometry.bounds
        dx = (maxx - minx) / n_splits
        dy = (maxy - miny) / n_splits

        pieces = []
        for i in range(n_splits):
            for j in range(n_splits):
                cell = box(minx + i * dx, miny + j * dy, minx + (i + 1) * dx, miny + (j + 1) * dy)
                piece = geometry.intersection(cell)
                if not piece.is_empty and piece.area > 0:
                    pieces.append(piece)
        return pieces

    def _geometry_to_geojson(self, geometry, original_geojson):
        new_geojson = json.loads(json.dumps(original_geojson))
        new_geojson["features"][0]["geometry"] = mapping(geometry)
        return new_geojson

    def fetch_worldpop_data(self, geojson_path, dataset="wpgpas", year=None) -> bool:
        year = year or self.year
        district = Path(geojson_path).stem
        try:
            with open(geojson_path) as f:
                geojson = json.load(f)

            geojson = self.simplify_geometry(geojson)
            pyramid = self._make_api_call(geojson, dataset, year, district)

            if pyramid == "PAYLOAD_TOO_LARGE":
                logger.warning("Splitting %s into pieces...", district)
                pyramid = self._fetch_split(geojson, year, district, dataset=dataset)

            if not pyramid or pyramid == "PAYLOAD_TOO_LARGE":
                return False

            self._save_pyramid_data(pyramid, district, year)
        except Exception as exc:
            logger.error("Error processing %s: %s", district, exc)
            return False
        return True

    def _fetch_split(self, geojson, year, district, dataset="wpgpas", n_splits=4, depth=0):
        if depth > 2:
            logger.error("Max split depth reached for %s", district)
            return None

        aggregated: dict = {}
        for index, piece in enumerate(self.split_geometry(geojson, n_splits=n_splits)):
            piece_geojson = self._geometry_to_geojson(piece, geojson)
            piece_geojson = self.simplify_geometry(piece_geojson, tolerance=0.08)
            piece_geojson = self.truncate_coordinates(piece_geojson, precision=2)

            piece_name = f"{district}_part{index}_d{depth}"
            result = self._make_api_call(piece_geojson, dataset, year, piece_name)

            if result == "PAYLOAD_TOO_LARGE":
                result = self._fetch_split(
                    piece_geojson, year, piece_name, dataset=dataset, n_splits=2, depth=depth + 1
                )
            if not result or result == "PAYLOAD_TOO_LARGE":
                continue

            for row in result["data"]["agesexpyramid"]:
                key = (row["class"], row["age"])
                entry = aggregated.setdefault(
                    key, {"class": row["class"], "age": row["age"], "male": 0, "female": 0}
                )
                entry["male"] += row["male"]
                entry["female"] += row["female"]

        return {"data": {"agesexpyramid": list(aggregated.values())}} if aggregated else None

    def _make_api_call(self, geojson, dataset, year, district):
        try:
            response = requests.get(
                f"{self.base_url}/services/stats",
                params={
                    "dataset": dataset,
                    "year": year,
                    "geojson": json.dumps(geojson),
                    "runasync": "false",
                },
                timeout=120,
            )
            response.raise_for_status()
            task_id = response.json()["taskid"]

            for _ in range(self.poll_attempts):
                time.sleep(self.poll_interval)
                status = requests.get(f"{self.base_url}/tasks/{task_id}", timeout=60)
                status.raise_for_status()
                result = status.json()

                if result.get("status") == "finished":
                    return result
                if result.get("status") == "failed":
                    logger.error("WorldPop task failed for %s", district)
                    return None

            logger.warning("WorldPop task for %s did not finish in time", district)
            return None
        except requests.RequestException as exc:
            if "413" in str(exc):
                return "PAYLOAD_TOO_LARGE"
            logger.error("WorldPop request failed for %s: %s", district, exc)
            return None

    def _save_pyramid_data(self, data, district, year):
        output_file = self.output_dir / f"{district}_agesexpyramid_{year}.csv"
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["class", "age", "male", "female"])
            writer.writeheader()
            for row in data["data"]["agesexpyramid"]:
                writer.writerow(row)
        return output_file

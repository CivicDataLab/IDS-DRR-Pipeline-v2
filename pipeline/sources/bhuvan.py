
from __future__ import annotations

import io
import logging
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import requests
from PIL import Image

logger = logging.getLogger(__name__)


TILE_SIZE = 256
TILE_DELTA = 0.0439453125  # degrees per tile (~5 km at equator)
WMS_BASE_URL = "https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms"
DISASTER_PORTAL_URL = (
    "https://bhuvan-app1.nrsc.gov.in/disaster/disaster.php?id=flood"
)

# Pillow safety limit for large mosaics (Assam ~900M pixels)
Image.MAX_IMAGE_PIXELS = 933_120_000

# Map lowercase WMS state codes to state names used on the disaster portal.
STATE_CODE_TO_NAME: dict[str, str] = {
    "as": "assam",
    "br": "bihar",
    "od": "odisha",
    "hp": "himachal pradesh",
    "up": "uttar pradesh",
    "ap": "andhra pradesh",
    "dl": "delhi",
    "hr": "haryana",
    "ka": "karnataka",
    "kl": "kerala",
    "mh": "maharashtra",
    "mn": "manipur",
    "ml": "meghalaya",
    "pb": "punjab",
    "tn": "tamil nadu",
    "ts": "telangana",
    "tr": "tripura",
    "wb": "west bengal",
}




@dataclass
class BhuvanBBox:
    """Bounding box for a state, aligned to the WMS tile grid."""

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @property
    def num_tiles_x(self) -> int:
        return math.ceil((self.max_lon - self.min_lon) / TILE_DELTA)

    @property
    def num_tiles_y(self) -> int:
        return math.ceil((self.max_lat - self.min_lat) / TILE_DELTA)

    @property
    def total_tiles(self) -> int:
        return self.num_tiles_x * self.num_tiles_y

    def tile_bboxes(self) -> list[tuple[float, float, float, float]]:
   

        tiles: list[tuple[float, float, float, float]] = []
        lon = self.min_lon
        for _ in range(self.num_tiles_x):
            lat = self.min_lat
            for _ in range(self.num_tiles_y):
                tiles.append((lon, lat, lon + TILE_DELTA, lat + TILE_DELTA))
                lat += TILE_DELTA
            lon += TILE_DELTA
        return tiles


@dataclass
class BhuvanStateConfig:
    """Per-state configuration for Bhuvan data extraction."""

    state_id: str
    state_code: str  # uppercase, e.g. "AS"
    bhuvan_code: str  # lowercase WMS code, e.g. "as"
    bbox: BhuvanBBox
    admin_boundary_path: Optional[str] = None
    layer_prefix: str = "flood"

    @classmethod
    def from_yaml_dict(cls, config: dict) -> "BhuvanStateConfig":
        """Build from a state YAML config dictionary."""
        state_id = config["state_id"]
        state_code = config["state_code"]
        bhuvan_cfg = (
            config.get("data_sources", {})
            .get("satellite", {})
            .get("bhuvan_config", {})
        )

        bbox_list = bhuvan_cfg.get("bbox", [])
        if len(bbox_list) != 4:
            raise ValueError(
                f"State {state_id} missing valid bhuvan bbox config "
                f"(need 4 values, got {len(bbox_list)})"
            )

        return cls(
            state_id=state_id,
            state_code=state_code,
            bhuvan_code=bhuvan_cfg.get("wms_code", state_code.lower()),
            bbox=BhuvanBBox(*bbox_list),
            admin_boundary_path=bhuvan_cfg.get("admin_boundary_shapefile"),
            layer_prefix=bhuvan_cfg.get("layer_prefix", "flood"),
        )


@dataclass
class BhuvanFloodDate:
    """A single flood observation date scraped from the Bhuvan portal."""

    raw_text: str
    date_string: str  # WMS-ready, e.g. "2023_07_07_18"
    year: int
    month: int
    day: int
    hour: Optional[int] = None


_DATE_PATTERN = re.compile(
    r"(\d{1,2}(?:-\d{1,2})?/\d{2}/\d{4}(?:\s*(?:Hr\s*\d{1,2}|\d{1,2}\s*Hr))?)"
)


def _parse_bhuvan_date(raw: str) -> Optional[BhuvanFloodDate]:
    """Parse a single date string from the portal.

    Handles:
    - ``DD/MM/YYYY``
    - ``DD/MM/YYYY Hr HH`` or ``DD/MM/YYYY HHHr``
    - ``DD-DD/MM/YYYY`` (date range; uses the *last* day)
    """
    try:
        text = raw.strip()
        hour: Optional[int] = None

        # Extract hour if present  (e.g. "Hr 18", "18Hr", "Hr18")
        hr_match = re.search(r"(?:Hr\s*(\d{1,2})|(\d{1,2})\s*Hr)", text)
        if hr_match:
            hour = int(hr_match.group(1) or hr_match.group(2))
            text = text[: hr_match.start()].strip()

        # Handle date-range "DD-DD/MM/YYYY" — use the end day
        range_match = re.match(r"(\d{1,2})-(\d{1,2})/(\d{2})/(\d{4})", text)
        if range_match:
            day = int(range_match.group(2))
            month = int(range_match.group(3))
            year = int(range_match.group(4))
        else:
            parts = text.split("/")
            if len(parts) != 3:
                return None
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])

        if hour is not None:
            date_string = f"{year}_{day:02d}_{month:02d}_{hour:02d}"
        else:
            date_string = f"{year}_{day:02d}_{month:02d}"

        return BhuvanFloodDate(
            raw_text=raw,
            date_string=date_string,
            year=year,
            month=month,
            day=day,
            hour=hour,
        )
    except (ValueError, IndexError):
        logger.warning("Could not parse Bhuvan date: %s", raw)
        return None


def discover_flood_dates(
    state_code: str,
    *,
    portal_url: str = DISASTER_PORTAL_URL,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> list[BhuvanFloodDate]:

    from bs4 import BeautifulSoup

    state_name = STATE_CODE_TO_NAME.get(state_code.lower())
    if not state_name:
        raise ValueError(f"Unknown Bhuvan state code: {state_code!r}")

    http = session or requests.Session()
    resp = http.get(portal_url, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    page_text = soup.get_text(separator="\n")

    dates: list[BhuvanFloodDate] = []
    in_target_state = False

    for line in page_text.splitlines():
        stripped = line.strip().lower()
        if not stripped:
            continue

        # Detect state headers (e.g. "Assam" or "ASSAM-2024")
        if any(sn in stripped for sn in STATE_CODE_TO_NAME.values()):
            in_target_state = state_name in stripped
            continue

        if in_target_state:
            for match in _DATE_PATTERN.findall(line):
                parsed = _parse_bhuvan_date(match)
                if parsed:
                    dates.append(parsed)

    # Deduplicate by date_string
    seen: set[str] = set()
    unique: list[BhuvanFloodDate] = []
    for d in dates:
        if d.date_string not in seen:
            seen.add(d.date_string)
            unique.append(d)

    unique.sort(key=lambda d: (d.year, d.month, d.day, d.hour or 0))
    return unique

#tite download
def build_wms_layer_name(
    state_code: str,
    date_string: str,
    prefix: str = "flood",
) -> str:
    """Build the WMS layer name for a state and date.

    >>> build_wms_layer_name("as", "2023_07_07_18")
    'flood:as_2023_07_07_18'
    """
    return f"{prefix}:{state_code.lower()}_{date_string}"


def download_tile(
    bbox: tuple[float, float, float, float],
    layer_name: str,
    *,
    base_url: str = WMS_BASE_URL,
    timeout: int = 30,
    max_retries: int = 3,
    session: Optional[requests.Session] = None,
) -> np.ndarray:
    """Download a single 256x256 WMS tile and return as a numpy array.

    Returns an array of shape ``(256, 256, C)`` where *C* is 3 (RGB) or
    4 (RGBA) depending on the server response.
    """
    http = session or requests.Session()
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

    url = (
        f"{base_url}?LAYERS={layer_name}"
        f"&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1"
        f"&REQUEST=GetMap&STYLES=&FORMAT=image%2Fpng"
        f"&SRS=EPSG%3A4326&BBOX={bbox_str}"
        f"&WIDTH={TILE_SIZE}&HEIGHT={TILE_SIZE}"
    )

    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = http.get(url, timeout=timeout)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content))
            return np.asarray(img).copy()
        except Exception as exc:
            last_err = exc
            logger.warning(
                "Tile download attempt %d/%d failed (bbox=%s): %s",
                attempt,
                max_retries,
                bbox_str,
                exc,
            )

    raise RuntimeError(
        f"Failed to download tile after {max_retries} attempts: {last_err}"
    )


def download_all_tiles(
    state_bbox: BhuvanBBox,
    layer_name: str,
    *,
    base_url: str = WMS_BASE_URL,
    max_workers: int = 8,
    timeout: int = 30,
    max_retries: int = 3,
) -> list[list[np.ndarray]]:
    """Download all tiles for a state bounding box in parallel.

    Returns a 2-D list indexed as ``tiles[col][row]`` where *col* runs
    west-to-east and *row* runs south-to-north.
    """
    tile_bboxes = state_bbox.tile_bboxes()
    n_x = state_bbox.num_tiles_x
    n_y = state_bbox.num_tiles_y

    logger.info(
        "Downloading %d tiles (%d cols x %d rows) for layer %s",
        state_bbox.total_tiles,
        n_x,
        n_y,
        layer_name,
    )

    # Pre-allocate grid
    tiles: list[list[Optional[np.ndarray]]] = [
        [None] * n_y for _ in range(n_x)
    ]

    session = requests.Session()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_idx = {}
        for idx, bbox in enumerate(tile_bboxes):
            col = idx // n_y
            row = idx % n_y
            future = pool.submit(
                download_tile,
                bbox,
                layer_name,
                base_url=base_url,
                timeout=timeout,
                max_retries=max_retries,
                session=session,
            )
            future_to_idx[future] = (col, row)

        done = 0
        for future in as_completed(future_to_idx):
            col, row = future_to_idx[future]
            tiles[col][row] = future.result()
            done += 1
            if done % 500 == 0:
                logger.info(
                    "Downloaded %d / %d tiles", done, state_bbox.total_tiles
                )

    return tiles  # type: ignore[return-value]


#stitching
def remove_watermark_from_tile(tile: np.ndarray) -> np.ndarray:
    """Remove the Bhuvan watermark from a single tile array.

    Watermark pixels are grayscale (R == G == B).  These are set to 255
    so they become "not inundated" in the binary conversion step.
    """
    if tile.ndim == 3 and tile.shape[2] >= 3:
        mask = (tile[:, :, 0] == tile[:, :, 1]) & (
            tile[:, :, 1] == tile[:, :, 2]
        )
        tile[:, :, :3][mask] = 255
    return tile


def stitch_tiles(
    tiles: list[list[np.ndarray]],
    *,
    remove_watermarks: bool = True,
) -> np.ndarray:
    """Stitch a 2-D grid of tiles into a single binary inundation array.

    Args:
        tiles: ``tiles[col][row]`` — col runs W→E, row runs S→N.
        remove_watermarks: Apply watermark removal per tile.

    Returns:
        ``uint8`` array of shape ``(height, width)`` where
        ``1`` = inundated and ``0`` = not inundated.
    """
    n_cols = len(tiles)
    n_rows = len(tiles[0])

    processed_cols: list[np.ndarray] = []
    for col_idx in range(n_cols):
        col_strips: list[np.ndarray] = []
        # Reverse rows so the northernmost tile is at the top of the image
        for row_idx in reversed(range(n_rows)):
            tile = tiles[col_idx][row_idx]
            if remove_watermarks:
                tile = remove_watermark_from_tile(tile)

            # Convert to single-channel grayscale
            if tile.ndim == 3:
                gray = np.mean(tile[:, :, :3], axis=2).astype(np.uint8)
            else:
                gray = tile.astype(np.uint8)

            col_strips.append(gray)

        processed_cols.append(np.vstack(col_strips))

    full_image = np.hstack(processed_cols)

    # Binary inundation mask: pixel < 255 → inundated (1), else 0
    inundation = np.zeros_like(full_image, dtype=np.uint8)
    inundation[full_image < 255] = 1

    return inundation


#geotiff

def create_geotiff(
    data: np.ndarray,
    bbox: BhuvanBBox,
    output_path: Path,
    crs: str = "EPSG:4326",
) -> Path:
    """Write a 2-D numpy array as a single-band GeoTIFF via *rasterio*.

    Args:
        data: ``(height, width)`` uint8 array.
        bbox: Geographic bounding box for georeferencing.
        output_path: Destination file path.
        crs: Coordinate reference system identifier.

    Returns:
        *output_path* for convenience.
    """
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    height, width = data.shape
    transform = from_bounds(
        bbox.min_lon,
        bbox.min_lat,
        bbox.max_lon,
        bbox.max_lat,
        width,
        height,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(
        str(output_path),
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="uint8",
        crs=CRS.from_string(crs),
        transform=transform,
        compress="deflate",
    ) as dst:
        dst.write(data, 1)

    logger.info("Created GeoTIFF: %s (%d x %d)", output_path, width, height)
    return output_path



def aggregate_monthly_rasters(
    tiff_paths: Sequence[Path],
) -> tuple[np.ndarray, dict]:
    """Sum daily inundation rasters into a monthly cumulative raster.

    Args:
        tiff_paths: Paths to single-band daily GeoTIFFs.

    Returns:
        ``(summed_array, raster_meta)`` — meta from the first file,
        updated with ``dtype=int16`` and ``nodata=-1``.
    """
    import rasterio

    if not tiff_paths:
        raise ValueError("No GeoTIFF paths provided for aggregation")

    with rasterio.open(str(tiff_paths[0])) as src:
        cumulative = src.read(1).astype(np.int16)
        meta = src.meta.copy()

    for path in tiff_paths[1:]:
        with rasterio.open(str(path)) as src:
            cumulative += src.read(1).astype(np.int16)

    meta.update(dtype="int16", compress="deflate", nodata=-1)
    return cumulative, meta


def save_monthly_raster(
    data: np.ndarray,
    meta: dict,
    output_path: Path,
) -> Path:
    """Write the aggregated monthly raster to a GeoTIFF."""
    import rasterio

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(str(output_path), "w", **meta) as dst:
        dst.write(data, 1)

    logger.info("Saved monthly raster: %s", output_path)
    return output_path


#transformer

def compute_zonal_statistics(
    raster_data: np.ndarray,
    raster_meta: dict,
    admin_boundaries_path: str,
    *,
    id_column: str = "object_id",
) -> pd.DataFrame:
    """Compute inundation zonal statistics per admin boundary.

    Args:
        raster_data: Monthly aggregated raster (int16, summed daily values).
        raster_meta: Rasterio metadata dict (must include ``transform``,
            ``crs``, ``nodata``).
        admin_boundaries_path: Path to a shapefile with admin polygons.
        id_column: Column name for the admin-unit identifier.

    Returns:
        DataFrame with columns:

        - ``object_id``
        - ``count_bhuvan_pixels``
        - ``count_inundated_pixels``
        - ``inundation_pct``
        - ``inundation_intensity_mean``
        - ``inundation_intensity_mean_nonzero``
        - ``inundation_intensity_sum``
    """
    import geopandas as gpd
    import rasterstats

    gdf = gpd.read_file(admin_boundaries_path)
    raster_crs = raster_meta.get("crs")
    if raster_crs:
        gdf = gdf.to_crs(raster_crs)

    transform = raster_meta["transform"]
    nodata = raster_meta.get("nodata", -1)

    # --- Inundation count ---
    def _count_nonzero(x: np.ma.MaskedArray) -> int:
        return int(np.count_nonzero(x.compressed()))

    count_results = rasterstats.zonal_stats(
        gdf,
        raster_data,
        affine=transform,
        stats=["count"],
        nodata=nodata,
        add_stats={"count_nonzero": _count_nonzero},
        geojson_out=True,
    )
    count_dfs = [pd.DataFrame([f["properties"]]) for f in count_results]
    zonal_df = pd.concat(count_dfs, ignore_index=True)
    zonal_df["inundation_pct"] = (
        zonal_df["count_nonzero"] / zonal_df["count"]
    )

    # --- Intensity ---
    max_val = raster_data.max()
    if max_val > 0:
        intensity = raster_data.astype(np.float32) / float(max_val)
    else:
        intensity = np.zeros_like(raster_data, dtype=np.float32)

    def _nonzero_mean(x: np.ma.MaskedArray) -> float:
        vals = x.compressed()
        nz = vals[vals != 0]
        return float(np.mean(nz)) if len(nz) > 0 else 0.0

    intensity_results = rasterstats.zonal_stats(
        gdf,
        intensity,
        affine=transform,
        stats=["mean", "sum"],
        nodata=nodata,
        add_stats={"intensity_mean_nonzero": _nonzero_mean},
        geojson_out=True,
    )
    int_dfs = [pd.DataFrame([f["properties"]]) for f in intensity_results]
    intensity_df = pd.concat(int_dfs, ignore_index=True)
    intensity_df.rename(
        columns={"mean": "intensity_mean", "sum": "intensity_sum"},
        inplace=True,
    )

    result = pd.merge(
        zonal_df,
        intensity_df[
            [id_column, "intensity_mean", "intensity_mean_nonzero", "intensity_sum"]
        ],
        on=id_column,
    )

    result = result[
        [
            id_column,
            "count",
            "count_nonzero",
            "inundation_pct",
            "intensity_mean",
            "intensity_mean_nonzero",
            "intensity_sum",
        ]
    ]
    result.columns = [
        "object_id",
        "count_bhuvan_pixels",
        "count_inundated_pixels",
        "inundation_pct",
        "inundation_intensity_mean",
        "inundation_intensity_mean_nonzero",
        "inundation_intensity_sum",
    ]

    return result



__all__ = [
    "TILE_DELTA",
    "TILE_SIZE",
    "WMS_BASE_URL",
    "DISASTER_PORTAL_URL",
    "BhuvanBBox",
    "BhuvanFloodDate",
    "BhuvanStateConfig",
    "aggregate_monthly_rasters",
    "build_wms_layer_name",
    "compute_zonal_statistics",
    "create_geotiff",
    "discover_flood_dates",
    "download_all_tiles",
    "download_tile",
    "remove_watermark_from_tile",
    "save_monthly_raster",
    "stitch_tiles",
]

"""
Helpers for fetching flood layers from the DRIMS/Bhuvan WMS service.

Exposes a small helper to download a flood layer image for a given bounding box
and date. The function is intentionally lightweight so it can be reused inside
assets or ad-hoc scripts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import requests
from requests import Session

logger = logging.getLogger(__name__)

# Default WMS endpoint for DRIMS/Bhuvan flood layers
DEFAULT_WMS_URL = "https://bhuvan-vec2.nrsc.gov.in/bhuvan/wms"
DEFAULT_FLOOD_LAYER = "FLOOD_RASTER_CURRENT"


@dataclass
class BoundingBox:
    """Simple container for geographic bounding boxes."""

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @classmethod
    def from_sequence(cls, bbox: Iterable[float]) -> "BoundingBox":
        """Parse and validate a 4-value bounding box sequence."""
        coords = list(bbox)
        if len(coords) != 4:
            raise ValueError(
                "Bounding box must contain four values: min_lon, min_lat, max_lon, max_lat"
            )

        min_lon, min_lat, max_lon, max_lat = coords
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError(
                "Invalid bounding box coordinates; ensure min < max for both lat and lon"
            )

        return cls(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)

    def to_wms_param(self) -> str:
        """Return WMS-ready bbox string."""
        return f"{self.min_lon},{self.min_lat},{self.max_lon},{self.max_lat}"


def fetch_flood_layer(
    bbox: Sequence[float],
    date: str,
    *,
    layer_name: str = DEFAULT_FLOOD_LAYER,
    base_url: str = DEFAULT_WMS_URL,
    image_format: str = "image/png",
    width: int = 1024,
    height: int = 1024,
    srs: str = "EPSG:4326",
    timeout: int = 30,
    output_path: Optional[str] = None,
    session: Optional[Session] = None,
) -> bytes | str:
    """
    Fetch a flood layer image from DRIMS/Bhuvan for a bounding box and date.

    Args:
        bbox: Iterable with four values (min_lon, min_lat, max_lon, max_lat).
        date: ISO date string used for the WMS `time` parameter.
        layer_name: Target WMS layer to request (defaults to `DEFAULT_FLOOD_LAYER`).
        base_url: WMS endpoint URL.
        image_format: MIME type for the returned image.
        width: Pixel width of the requested map.
        height: Pixel height of the requested map.
        srs: Spatial reference system (CRS/SRS) identifier.
        timeout: Request timeout in seconds.
        output_path: If provided, writes the image to this path and returns the path.
        session: Optional `requests.Session` to reuse connections.

    Returns:
        Raw image bytes, or the output file path if `output_path` is provided.

    Raises:
        RuntimeError: When the WMS request fails or returns no content.
        ValueError: For invalid bounding box inputs.
    """

    bbox_obj = BoundingBox.from_sequence(bbox)
    params = {
        "service": "WMS",
        "request": "GetMap",
        "version": "1.3.0",
        "layers": layer_name,
        "styles": "",
        "format": image_format,
        "transparent": "true",
        "bbox": bbox_obj.to_wms_param(),
        "crs": srs,
        "width": width,
        "height": height,
        "time": date,
    }

    http = session or requests.Session()
    logger.debug("Requesting flood layer %s for bbox %s on %s", layer_name, params["bbox"], date)
    response = http.get(base_url, params=params, timeout=timeout)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        snippet = response.text[:200]
        raise RuntimeError(f"Flood layer request failed ({response.status_code}): {snippet}") from exc

    if not response.content:
        raise RuntimeError("Flood layer request succeeded but returned no content")

    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)
        logger.info("Saved flood layer %s for %s to %s", layer_name, date, path)
        return str(path)

    return response.content


__all__ = [
    "DEFAULT_FLOOD_LAYER",
    "DEFAULT_WMS_URL",
    "BoundingBox",
    "fetch_flood_layer",
]

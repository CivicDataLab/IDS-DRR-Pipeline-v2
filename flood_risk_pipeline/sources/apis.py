"""
API connection resources.

Provides configured clients for external data sources:
- Google Earth Engine
- Bhuvan WMS
- IMD API
- State tender portals
"""

import dagster as dg
from dagster import ConfigurableResource
from typing import Optional


class GEEResource(ConfigurableResource):
    """Google Earth Engine API resource."""

    project_id: str
    service_account_key_path: Optional[str] = None

    def get_client(self):
        """Initialize and return GEE client."""
        # TODO: Implement actual GEE initialization
        # import ee
        # if self.service_account_key_path:
        #     credentials = ee.ServiceAccountCredentials(None, self.service_account_key_path)
        #     ee.Initialize(credentials, project=self.project_id)
        # else:
        #     ee.Initialize(project=self.project_id)
        return None


class BhuvanWMSResource(ConfigurableResource):
    """ISRO Bhuvan WMS API resource for flood map extraction.

    Wraps the helper functions in :mod:`flood_risk_pipeline.sources.bhuvan`
    so they can be used as a Dagster resource with configurable parameters.
    """

    base_url: str = "https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms"
    portal_url: str = (
        "https://bhuvan-app1.nrsc.gov.in/disaster/disaster.php?id=flood"
    )
    timeout: int = 30
    max_workers: int = 8
    max_retries: int = 3

    def discover_dates(self, state_code: str) -> list:
        """Discover available flood dates for a state."""
        from flood_risk_pipeline.sources.bhuvan import discover_flood_dates

        return discover_flood_dates(
            state_code,
            portal_url=self.portal_url,
            timeout=self.timeout,
        )

    def download_flood_map(self, state_config, date_string: str, output_dir) -> "Path":
        """Download, stitch, and save a flood map GeoTIFF for one date.

        Args:
            state_config: A :class:`BhuvanStateConfig` instance.
            date_string: WMS date string, e.g. ``"2023_07_07_18"``.
            output_dir: Directory to write the GeoTIFF into.

        Returns:
            Path to the created GeoTIFF.
        """
        from pathlib import Path

        from flood_risk_pipeline.sources.bhuvan import (
            build_wms_layer_name,
            create_geotiff,
            download_all_tiles,
            stitch_tiles,
        )

        output_dir = Path(output_dir)
        layer_name = build_wms_layer_name(
            state_config.bhuvan_code,
            date_string,
            state_config.layer_prefix,
        )

        tiles = download_all_tiles(
            state_config.bbox,
            layer_name,
            base_url=self.base_url,
            max_workers=self.max_workers,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )

        inundation = stitch_tiles(tiles, remove_watermarks=True)

        tiff_path = output_dir / f"{state_config.state_id}_{date_string}.tif"
        create_geotiff(inundation, state_config.bbox, tiff_path)
        return tiff_path

    def aggregate_month(self, daily_tiffs: list, output_path) -> tuple:
        """Aggregate daily GeoTIFFs into a monthly composite.

        Returns:
            ``(summed_array, meta_dict)`` tuple.
        """
        from pathlib import Path

        from flood_risk_pipeline.sources.bhuvan import (
            aggregate_monthly_rasters,
            save_monthly_raster,
        )

        paths = [Path(p) for p in daily_tiffs]
        data, meta = aggregate_monthly_rasters(paths)
        save_monthly_raster(data, meta, Path(output_path))
        return data, meta

    def compute_stats(self, raster_data, raster_meta: dict, admin_boundaries_path: str):
        """Compute zonal statistics for a monthly raster."""
        from flood_risk_pipeline.sources.bhuvan import compute_zonal_statistics

        return compute_zonal_statistics(
            raster_data, raster_meta, admin_boundaries_path
        )


class IMDResource(ConfigurableResource):
    """India Meteorological Department API resource."""

    base_url: str = "https://mausam.imd.gov.in"
    api_key: Optional[str] = None

    def get_weather_data(self, state: str, date: str):
        """Fetch weather data for state and date."""
        # TODO: Implement actual IMD API request
        return None


class TenderPortalResource(ConfigurableResource):
    """Generic tender portal scraper resource."""

    request_delay: float = 1.0  # Delay between requests in seconds
    max_retries: int = 3

    def scrape_tenders(self, portal_url: str, scraper_class: str, month: str):
        """Scrape tenders from a state portal."""
        # TODO: Implement actual web scraping
        return []


# Export configured resources
api_resources = {
    "gee": GEEResource(
        project_id="flood-risk-pipeline",
    ),
    "bhuvan_wms": BhuvanWMSResource(),
    "imd": IMDResource(),
    "tender_portal": TenderPortalResource(),
}

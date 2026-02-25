"""
Procurement data extraction assets.

Handles web scraping from state tender portals:
- hptenders.gov.in (Himachal Pradesh)
- assamtenders.gov.in (Assam)
- tendersodisha.gov.in (Odisha)
- eproc2.bihar.gov.in (Bihar)
- etender.up.nic.in (Uttar Pradesh)
"""

import dagster as dg
import yaml
from pathlib import Path

from flood_risk_pipeline.partitions import monthly_state_partitions


def load_state_config(state: str) -> dict:
    """Load state-specific configuration from YAML."""
    config_path = Path(__file__).parent.parent.parent / "config" / "states" / f"{state}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="Government procurement/tender data scraped from state portals",
)
def raw_procurement_data(context: dg.AssetExecutionContext) -> dict:
    """Extract procurement data for a state-month combination."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    # Load state-specific configuration
    config = load_state_config(state)
    procurement_config = config.get("data_sources", {}).get("procurement", {})
    portal_url = procurement_config.get("portal_url", "unknown")
    scraper_class = procurement_config.get("scraper_class", "GenericScraper")

    context.log.info(f"Extracting procurement data for {state} - {month}")
    context.log.info(f"Using portal: {portal_url}")
    context.log.info(f"Scraper class: {scraper_class}")

    # TODO: Implement actual web scraping based on scraper_class
    return {
        "state": state,
        "month": month,
        "portal_url": portal_url,
        "scraper_class": scraper_class,
        "tenders_collected": 0,  # Placeholder
        "status": "extracted",
    }


@dg.asset(
    partitions_def=monthly_state_partitions,
    description="Budget allocation data for disaster response",
)
def raw_budget_data(context: dg.AssetExecutionContext) -> dict:
    """Extract budget/allocation data for disaster response."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    context.log.info(f"Extracting budget data for {state} - {month}")

    # TODO: Implement budget data extraction
    return {
        "state": state,
        "month": month,
        "source": "state_budget_portal",
        "status": "extracted",
    }

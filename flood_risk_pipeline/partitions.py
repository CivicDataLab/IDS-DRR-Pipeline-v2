"""
Partition definitions for the multi-state flood risk pipeline.

This module defines the partition schemes used across all assets:
- State partitions: Static list of 5 Indian states
- Monthly partitions: Time-based monthly partitions
- Multi-dimensional: State x Month combinations
- Daily partitions: For high-frequency data collection
- State-specific source configurations for data extraction tasks
"""

import dagster as dg


# Define 5 Indian states as static partitions
state_partitions = dg.StaticPartitionsDefinition([
    "himachal_pradesh",
    "assam",
    "odisha",
    "bihar",
    "uttar_pradesh",
])


# Daily partitions for weather/satellite data
daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-02-08")


# Monthly partitions for aggregation
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date="2026-02-08")


# Two-dimensional partitions: state x month
# This is the primary partition scheme for most assets
monthly_state_partitions = dg.MultiPartitionsDefinition({
    "state": state_partitions,
    "month": dg.MonthlyPartitionsDefinition(start_date="2026-02-08"),
})


# State-specific source configurations
# Maps each state to its available data sources
STATE_SOURCES = {
    "himachal_pradesh": ["gcn250", "bhuvan", "imd", "hptenders"],
    "assam": ["gcn250", "bhuvan", "imd", "asdma_portal"],
    "odisha": ["gcn250", "bhuvan", "imd", "osdma_portal"],
    "bihar": ["gcn250", "bhuvan", "imd", "bihar_tenders"],
    "uttar_pradesh": ["gcn250", "bhuvan", "imd", "up_tenders"],
}


def get_state_sources(state: str) -> list[str]:
    """Get the list of data sources configured for a state."""
    return STATE_SOURCES.get(state, [])

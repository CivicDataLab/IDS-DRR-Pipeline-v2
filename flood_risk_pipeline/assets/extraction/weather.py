"""
Weather data extraction assets.

Handles extraction from:
- IMD: India Meteorological Department weather data
- River gauge data
"""

import dagster as dg

from flood_risk_pipeline.partitions import daily_partitions, monthly_state_partitions


@dg.asset(
    partitions_def=daily_partitions,
    description="Daily weather data from India Meteorological Department",
)
def imd_weather_data(context: dg.AssetExecutionContext) -> dict:
    """Extract IMD weather data for a given date."""
    partition_date = context.partition_key
    context.log.info(f"Extracting IMD weather data for {partition_date}")

    # TODO: Implement actual IMD data extraction
    return {
        "date": partition_date,
        "source": "imd",
        "status": "extracted",
        "metrics": ["rainfall", "temperature", "humidity"],
    }


@dg.asset(
    partitions_def=daily_partitions,
    description="River gauge readings for flood monitoring",
)
def river_gauge_data(context: dg.AssetExecutionContext) -> dict:
    """Extract river gauge data for a given date."""
    partition_date = context.partition_key
    context.log.info(f"Extracting river gauge data for {partition_date}")

    # TODO: Implement actual river gauge data extraction
    return {
        "date": partition_date,
        "source": "cwc",  # Central Water Commission
        "status": "extracted",
        "gauges_monitored": 0,
    }


@dg.asset(
    partitions_def=monthly_state_partitions,
    deps=["imd_weather_data", "river_gauge_data"],
    description="Aggregated weather data per state-month",
)
def raw_weather_data(context: dg.AssetExecutionContext) -> dict:
    """Aggregate weather data for a state-month combination."""
    keys = context.partition_key.keys_by_dimension
    state, month = keys["state"], keys["month"]

    context.log.info(f"Aggregating weather data for {state} - {month}")

    return {
        "state": state,
        "month": month,
        "imd_available": True,
        "river_gauge_available": True,
        "status": "aggregated",
    }

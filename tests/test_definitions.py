"""Tests for Dagster definitions."""

import pytest
from dagster import materialize


def test_definitions_load():
    """Test that definitions can be loaded without errors."""
    from pipeline.definitions import defs

    assert defs is not None
    assert len(defs.get_all_asset_keys()) > 0


def test_partitions_defined():
    """Test that partitions are defined correctly."""
    from pipeline.partitions import (
        state_partitions,
        monthly_state_partitions,
        daily_partitions,
    )

    # Check state partitions
    state_keys = state_partitions.get_partition_keys()
    assert len(state_keys) == 5
    assert "himachal_pradesh" in state_keys
    assert "assam" in state_keys


def test_state_sources_configured():
    """Test that state sources are configured."""
    from pipeline.partitions import STATE_SOURCES, get_state_sources

    assert len(STATE_SOURCES) == 5

    hp_sources = get_state_sources("himachal_pradesh")
    assert "gcn250" in hp_sources
    assert "bhuvan" in hp_sources


def test_schedules_defined():
    """Test that schedules are defined."""
    from pipeline.definitions import (
        daily_collection_schedule,
        weekly_collection_schedule,
        monthly_aggregation_schedule,
    )

    assert daily_collection_schedule.cron_schedule == "0 2 * * *"
    assert weekly_collection_schedule.cron_schedule == "0 2 * * 0"
    assert monthly_aggregation_schedule.cron_schedule == "0 2 1 * *"

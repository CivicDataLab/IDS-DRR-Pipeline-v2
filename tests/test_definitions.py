"""Tests for Dagster definitions."""


def test_definitions_load():
    """Test that definitions can be loaded without errors."""
    from pipeline.definitions import defs

    assert defs is not None
    assert len(defs.resolve_all_asset_keys()) > 0


def test_partitions_defined():
    """Test that partitions are defined correctly."""
    from pipeline.partitions import state_partitions

    # Check state partitions
    state_keys = state_partitions.get_partition_keys()
    assert len(state_keys) == 5
    assert "himachal_pradesh" in state_keys
    assert "assam" in state_keys


def test_monthly_partitions_cover_panel_history():
    """The month dimension starts at the 2021-04 panel start for backfills."""
    from pipeline.partitions import monthly_state_partitions

    month_dim = next(
        d for d in monthly_state_partitions.partitions_defs if d.name == "month"
    )
    month_keys = month_dim.partitions_def.get_partition_keys()
    assert month_keys[0] == "2021-04-01"


def test_state_sources_configured():
    """Test that state sources are configured."""
    from pipeline.partitions import STATE_SOURCES, get_state_sources

    assert len(STATE_SOURCES) == 5

    hp_sources = get_state_sources("himachal_pradesh")
    assert "imd" in hp_sources
    assert "bhuvan" in hp_sources
    assert "nrsc" in hp_sources


def test_enabled_states():
    """Only Himachal Pradesh is enabled for automated runs right now."""
    from pipeline.state_config import enabled_states

    assert enabled_states() == ["himachal_pradesh"]


def test_schedules_defined():
    """Test that schedules are defined."""
    from pipeline.definitions import monthly_state_etl_schedule

    assert monthly_state_etl_schedule.cron_schedule == "0 2 1 * *"


def test_monthly_schedule_requests_enabled_states_only():
    """A schedule tick yields one run request per enabled state."""
    from datetime import datetime

    import dagster as dg

    from pipeline.definitions import monthly_state_etl_schedule

    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 9, 1, 2, 0),
    )
    requests = monthly_state_etl_schedule(context)
    # MultiPartitionKey renders dimensions sorted by name: month|state
    assert [r.partition_key for r in requests] == ["2026-08-01|himachal_pradesh"]

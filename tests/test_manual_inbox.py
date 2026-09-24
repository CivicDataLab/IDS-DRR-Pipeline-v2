"""Tests for the manual drop-folder flow and its sensor."""

import json

import pandas as pd
import pytest

from pipeline.sources import manual_inbox, staging_layout


@pytest.fixture
def inbox(tmp_path, monkeypatch):
    """Redirect the staging and inbox roots into a temp directory."""
    monkeypatch.setattr(staging_layout, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(staging_layout, "INBOX_ROOT", tmp_path / "inbox")
    path = staging_layout.inbox_dir("himachal_pradesh", "nrsc")
    path.mkdir(parents=True, exist_ok=True)
    return path


BOUNDARY_IDS = {f"02-030-{i:05d}" for i in range(2100, 2110)}


def _runoff_frame(object_ids):
    return pd.DataFrame(
        {
            "object_id": list(object_ids),
            "Mean_Daily_Runoff": 1.0,
            "Sum_Runoff": 30.0,
            "Peak_Runoff": 5.0,
        }
    )


def test_parse_filename_extracts_variable_and_timeperiod():
    assert manual_inbox.parse_inbox_filename("nrsc", "runoff_2023_07.csv") == (
        "runoff",
        "2023_07",
    )


def test_parse_filename_rejects_unexpected_variable():
    with pytest.raises(ValueError, match="not accepted"):
        manual_inbox.parse_inbox_filename("nrsc", "rainfall_2023_07.csv")


def test_parse_filename_rejects_bad_convention():
    with pytest.raises(ValueError, match="convention"):
        manual_inbox.parse_inbox_filename("nrsc", "runoff-july.csv")


def test_promote_writes_contract_and_moves_file(inbox):
    source = inbox / "runoff_2023_07.csv"
    _runoff_frame(BOUNDARY_IDS).to_csv(source, index=False)

    staged = manual_inbox.promote("himachal_pradesh", "nrsc", source, BOUNDARY_IDS)

    assert staged == staging_layout.variable_csv_path(
        "himachal_pradesh", "nrsc", "runoff", "2023_07"
    )
    written = pd.read_csv(staged)
    assert list(written.columns) == [
        "object_id",
        "Mean_Daily_Runoff",
        "Sum_Runoff",
        "Peak_Runoff",
    ]
    assert not source.exists()
    assert (inbox / "processed" / "runoff_2023_07.csv").exists()


def test_promote_rejects_missing_value_columns(inbox):
    source = inbox / "runoff_2023_07.csv"
    pd.DataFrame({"object_id": sorted(BOUNDARY_IDS), "Sum_Runoff": 1.0}).to_csv(source, index=False)

    with pytest.raises(ValueError, match="missing value columns"):
        manual_inbox.promote("himachal_pradesh", "nrsc", source, BOUNDARY_IDS)
    assert source.exists(), "a rejected file must stay in the inbox"


def test_promote_rejects_low_boundary_join_rate(inbox):
    source = inbox / "runoff_2023_07.csv"
    _runoff_frame([f"99-999-{i:05d}" for i in range(10)]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="join the"):
        manual_inbox.promote("himachal_pradesh", "nrsc", source, BOUNDARY_IDS)
    assert source.exists()


def test_scan_inbox_ignores_processed_files(inbox):
    (inbox / "processed").mkdir(exist_ok=True)
    _runoff_frame(BOUNDARY_IDS).to_csv(inbox / "processed" / "runoff_2023_06.csv", index=False)
    _runoff_frame(BOUNDARY_IDS).to_csv(inbox / "runoff_2023_07.csv", index=False)

    pending = manual_inbox.scan_inbox("himachal_pradesh", "nrsc")
    assert [p.name for p in pending] == ["runoff_2023_07.csv"]


def test_sensor_requests_partition_for_new_file(inbox):
    import dagster as dg

    from pipeline.definitions import manual_inbox_sensor

    _runoff_frame(BOUNDARY_IDS).to_csv(inbox / "runoff_2023_07.csv", index=False)

    context = dg.build_sensor_context()
    requests = manual_inbox_sensor(context)

    assert [r.partition_key for r in requests] == ["2023-07-01|himachal_pradesh"]
    assert [r.asset_selection for r in requests] == [[dg.AssetKey("nrsc_runoff")]]
    assert "runoff_2023_07.csv" in context.cursor


def test_sensor_is_quiet_when_nothing_changed(inbox):
    import dagster as dg

    from pipeline.definitions import manual_inbox_sensor

    _runoff_frame(BOUNDARY_IDS).to_csv(inbox / "runoff_2023_07.csv", index=False)

    first = dg.build_sensor_context()
    assert manual_inbox_sensor(first)

    second = dg.build_sensor_context(cursor=first.cursor)
    assert manual_inbox_sensor(second) == []


def test_inbox_cursor_is_stable(inbox):
    _runoff_frame(BOUNDARY_IDS).to_csv(inbox / "runoff_2023_07.csv", index=False)
    pending = manual_inbox.scan_all_inboxes("himachal_pradesh")

    cursor = manual_inbox.inbox_cursor(pending)
    assert manual_inbox.inbox_cursor(pending) == cursor
    assert "runoff_2023_07.csv" in json.loads(cursor)["nrsc"]

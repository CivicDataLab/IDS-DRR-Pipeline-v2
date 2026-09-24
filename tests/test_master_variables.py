"""Parity test: the rebuilt master panel against the published one.

``build_master`` reassembles MASTER_VARIABLES from the staging variables
contract. It must reproduce the panel published by the flood-data-ecosystem
repository, whose copy is seeded as a golden fixture.

Two differences are expected and asserted on explicitly:

* The published panel carries bookkeeping columns (``Shape_Leng``,
  ``objectid``, ``Unnamed: 0``, ...) that leaked in from unrestricted
  source merges. The contract writes only ``object_id`` plus value columns,
  so those never appear.
* ``TEHSIL`` in the published panel comes from the NRSC runoff CSVs and is
  zero-filled for months with no runoff data. Here it comes from the admin
  boundaries and is always the real tehsil name.
"""

import pandas as pd
import pytest

from pipeline.sources.master_variables import build_master

# Columns the published panel carries that the contract deliberately drops.
PUBLISHED_ONLY_COLUMNS = {
    "Shape_Leng",
    "Shape_Area",
    "count",
    "object_id_new",
    "timeperiod_datetime",
    "objectid",
    "Unnamed: 0",
}

KEYS = ["object_id", "timeperiod"]
TOLERANCE = 1e-6


@pytest.fixture(scope="module")
def rebuilt(state, state_cfg, staged_variables_present):
    panel, coverage = build_master(state, state_cfg, "2026-05")
    return panel, coverage


def test_every_configured_variable_has_staged_data(rebuilt):
    _, coverage = rebuilt
    empty = sorted(name for name, count in coverage.items() if not count)
    assert not empty, f"variables with no staged data: {empty}"


def test_panel_shape_matches_published(rebuilt, golden_master):
    panel, _ = rebuilt
    assert len(panel) == len(golden_master)
    assert panel["object_id"].nunique() == golden_master["object_id"].nunique()
    assert panel["timeperiod"].nunique() == golden_master["timeperiod"].nunique()


def test_panel_has_one_row_per_boundary_month(rebuilt):
    panel, _ = rebuilt
    assert not panel.duplicated(subset=KEYS).any()


def test_column_differences_are_only_the_expected_ones(rebuilt, golden_master):
    panel, _ = rebuilt
    missing = set(golden_master.columns) - set(panel.columns)
    assert missing == PUBLISHED_ONLY_COLUMNS, (
        f"unexpected columns missing from the rebuilt panel: " f"{missing - PUBLISHED_ONLY_COLUMNS}"
    )


def test_numeric_values_match_published(rebuilt, golden_master):
    panel, _ = rebuilt
    mine = panel.set_index(KEYS).sort_index()
    published = golden_master.set_index(KEYS).sort_index()
    assert mine.index.equals(published.index)

    shared = [c for c in mine.columns if c in published.columns]
    diffs = {}
    for column in shared:
        a = pd.to_numeric(mine[column], errors="coerce")
        b = pd.to_numeric(published[column], errors="coerce")
        if a.isna().all() or b.isna().all():
            continue  # non-numeric column, covered by the TEHSIL test below
        delta = (a.fillna(0) - b.fillna(0)).abs()
        mismatched = int((delta > TOLERANCE).sum())
        if mismatched:
            diffs[column] = (mismatched, float(delta.max()))

    assert not diffs, f"numeric columns diverge from the published panel: {diffs}"


def test_tehsil_names_come_from_boundaries_not_runoff(rebuilt, golden_master):
    panel, _ = rebuilt
    mine = panel.set_index(KEYS).sort_index()
    published = golden_master.set_index(KEYS).sort_index()

    differing = mine["TEHSIL"].astype(str) != published["TEHSIL"].astype(str)
    # Wherever the two disagree, the published value is the zero fill.
    assert (published.loc[differing, "TEHSIL"].astype(str) == "0").all()
    assert (mine["TEHSIL"].astype(str) != "0").all()

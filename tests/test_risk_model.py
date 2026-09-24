"""Parity test: the ported risk model against the published scores.

The four factor computations and the TOPSIS composite are run over the
published MASTER_VARIABLES panel and compared with the scores published by
the risk-score-model repository.

Hazard, exposure and government response are deterministic and must match
exactly. Vulnerability solves a linear program per tehsil-month: the
published model used Gurobi, this port uses scipy's HiGHS solver, so
degenerate LPs can settle on a different vertex of the same optimal face.
Efficiency scores are therefore compared within a tolerance, and the bins
derived from them are required to agree on the overwhelming majority of
rows.
"""

import numpy as np
import pandas as pd
import pytest

from pipeline.sources import staging_layout
from pipeline.sources.risk_model import factors

KEYS = ["object_id", "timeperiod"]

# Share of rows whose DEA efficiency must land within EFFICIENCY_TOLERANCE
# of the published value, and share of vulnerability bins that must agree.
EFFICIENCY_TOLERANCE = 1e-3
MIN_EFFICIENCY_AGREEMENT = 0.98
MIN_BIN_AGREEMENT = 0.99


def _compare(mine: pd.DataFrame, published: pd.DataFrame, column: str) -> pd.Series:
    joined = mine.merge(published, on=KEYS, suffixes=("_mine", "_published"))
    assert len(joined) == len(mine), "factor rows did not line up with the published scores"
    a = pd.to_numeric(joined[f"{column}_mine"], errors="coerce")
    b = pd.to_numeric(joined[f"{column}_published"], errors="coerce")
    return (a - b).abs()


def test_hazard_matches_published(golden_master, state_cfg, golden_factor):
    mine = factors.compute_hazard(golden_master, state_cfg)
    published = golden_factor("flood-hazard")[KEYS + ["flood-hazard"]]
    assert _compare(mine, published, "flood-hazard").max() == 0


def test_exposure_matches_published(golden_master, state_cfg, golden_factor):
    mine = factors.compute_exposure(golden_master, state_cfg)
    published = golden_factor("exposure")[KEYS + ["exposure"]]
    assert _compare(mine, published, "exposure").max() == 0


def test_government_response_matches_published(golden_master, state_cfg, golden_factor):
    mine = factors.compute_government_response(golden_master, state_cfg)
    published = golden_factor("government-response")[KEYS + ["government-response"]]
    assert _compare(mine, published, "government-response").max() == 0


@pytest.fixture(scope="module")
def vulnerability(golden_master, state_cfg):
    return factors.compute_vulnerability(golden_master, state_cfg)


def test_vulnerability_efficiency_close_to_gurobi(vulnerability, golden_factor):
    published = golden_factor("vulnerability")[KEYS + ["efficiency"]]
    delta = _compare(vulnerability, published, "efficiency")
    agreement = (delta <= EFFICIENCY_TOLERANCE).mean()
    assert agreement >= MIN_EFFICIENCY_AGREEMENT, (
        f"only {agreement:.2%} of DEA efficiency scores are within "
        f"{EFFICIENCY_TOLERANCE} of the published (Gurobi) values"
    )


def test_vulnerability_bins_agree_with_published(vulnerability, golden_factor):
    published = golden_factor("vulnerability")[KEYS + ["vulnerability"]]
    agreement = (_compare(vulnerability, published, "vulnerability") == 0).mean()
    assert (
        agreement >= MIN_BIN_AGREEMENT
    ), f"only {agreement:.2%} of vulnerability bins match the published model"


@pytest.fixture(scope="module")
def risk_scores(golden_master, state_cfg, golden_factor):
    factor_dfs = {
        "flood-hazard": golden_factor("flood-hazard")[KEYS + ["flood-hazard"]],
        "exposure": golden_factor("exposure")[KEYS + ["exposure"]],
        "vulnerability": golden_factor("vulnerability")[
            KEYS + ["vulnerability", "efficiency", "landd_score"]
        ],
        "government-response": golden_factor("government-response")[KEYS + ["government-response"]],
    }
    return factors.compute_risk_scores(golden_master, factor_dfs, state_cfg)


def test_topsis_scores_match_published(risk_scores, golden_risk_scores):
    published = golden_risk_scores[golden_risk_scores["tehsil"].notna()]
    joined = risk_scores.merge(
        published, on=["object-id", "timeperiod"], suffixes=("_mine", "_published")
    )
    assert len(joined) == len(risk_scores)

    for column in ["risk-score", "topsis-score"]:
        a = pd.to_numeric(joined[f"{column}_mine"], errors="coerce")
        b = pd.to_numeric(joined[f"{column}_published"], errors="coerce")
        assert np.allclose(a, b, atol=1e-6), f"{column} diverges from the published scores"


def test_published_tender_column_is_the_financial_year_cumulative(risk_scores, golden_risk_scores):
    """The published table's raw tender column actually holds cumulative values.

    ``govtresponse.py`` replaced the tender columns with their financial-year
    cumulative sums in place before writing its factor CSV, and the risk
    script then used that CSV as its base frame. This port keeps monthly
    values in the raw column and cumulative values in the explicit
    ``-fy-cumsum`` column, so the published raw column lines up with the
    cumulative one here.
    """
    published = golden_risk_scores[golden_risk_scores["tehsil"].notna()]
    joined = risk_scores.merge(
        published, on=["object-id", "timeperiod"], suffixes=("_mine", "_published")
    )
    cumulative = pd.to_numeric(joined["total-tender-awarded-value-fy-cumsum_mine"], errors="coerce")
    published_raw = pd.to_numeric(joined["total-tender-awarded-value_published"], errors="coerce")
    assert cumulative.corr(published_raw) > 0.9999


def test_district_rollup_adds_district_rows(risk_scores, state_cfg):
    district_map = pd.read_csv(
        staging_layout.reference_dir("himachal_pradesh") / "district_objectid.csv"
    )
    final = factors.build_district_output(risk_scores, district_map)

    assert len(final) > len(risk_scores)
    districts = final[final["tehsil"].isna()]
    expected = risk_scores["district"].nunique() * risk_scores["timeperiod"].nunique()
    assert len(districts) == expected
    assert districts["risk-score"].notna().all()

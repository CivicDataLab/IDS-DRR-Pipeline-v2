"""Risk score model: DEA, TOPSIS, binning helpers and factor computations.

Port of the IDS-DRR-Himachal-Pradesh-Risk-Score-Model scripts as pure,
state-parameterised functions (no cwd dependence, nothing runs on import).
"""

from pipeline.sources.risk_model.factors import (  # noqa: F401
    build_district_output,
    compute_exposure,
    compute_government_response,
    compute_hazard,
    compute_risk_scores,
    compute_vulnerability,
)

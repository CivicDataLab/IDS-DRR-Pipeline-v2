"""Input-oriented CRS Data Envelopment Analysis via scipy.

Reimplementation of the Gurobi-based ``DEA.CRS(orientation="input",
dual=False)`` used by the risk-score model, on ``scipy.optimize.linprog``
(HiGHS) so no commercial license is needed.

For each DMU ``r`` with inputs ``X_r`` and outputs ``Y_r``::

    max  u . Y_r
    s.t. v . X_r == 1
         u . Y_k - v . X_k <= 0   for every DMU k
         u, v >= 0

DMUs whose LP fails (infeasible/unbounded — e.g. all-zero inputs) are
skipped, matching the original's behaviour of swallowing GurobiError.
"""

import logging

import numpy as np
import pandas as pd
from scipy.optimize import linprog

logger = logging.getLogger(__name__)


def CRS(DMU, X, Y, orientation="input", dual=False) -> pd.DataFrame:
    """Efficiency scores for each DMU; columns ``DMU`` and ``efficiency``.

    ``X`` and ``Y`` map each DMU name to its list of input / output values
    (same shapes as the original ``DEA.CRS``). Only the input-oriented
    primal form is implemented.
    """
    if orientation != "input" or dual:
        raise NotImplementedError("only orientation='input', dual=False is supported")

    dmus = list(DMU)
    n_in = len(X[dmus[0]])
    n_out = len(Y[dmus[0]])

    X_mat = np.array([X[k] for k in dmus], dtype=float)  # (n_dmu, n_in)
    Y_mat = np.array([Y[k] for k in dmus], dtype=float)  # (n_dmu, n_out)

    # Decision vector z = [u (n_out), v (n_in)], all >= 0.
    # Shared constraint block: u.Y_k - v.X_k <= 0 for all k.
    A_ub = np.hstack([Y_mat, -X_mat])
    b_ub = np.zeros(len(dmus))

    names, efficiencies = [], []
    for r, (x_r, y_r) in enumerate(zip(X_mat, Y_mat)):
        c = np.concatenate([-y_r, np.zeros(n_in)])  # maximise u.Y_r
        A_eq = np.concatenate([np.zeros(n_out), x_r]).reshape(1, -1)
        result = linprog(
            c,
            A_ub=A_ub,
            b_ub=b_ub,
            A_eq=A_eq,
            b_eq=[1.0],
            bounds=[(0, None)] * (n_out + n_in),
            method="highs",
        )
        if not result.success:
            logger.warning("DEA LP failed for DMU %s: %s", dmus[r], result.message)
            continue
        names.append(dmus[r])
        efficiencies.append(-result.fun)

    df = pd.DataFrame({"DMU": names, "efficiency": efficiencies})
    return df

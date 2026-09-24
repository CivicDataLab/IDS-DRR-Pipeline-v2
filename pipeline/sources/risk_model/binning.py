"""Natural-Jenks binning with duplicate-edge handling (from vulnerability.py)."""

import jenkspy
import numpy as np
import pandas as pd


def assign_bin_with_handling(data, n_classes: int = 5):
    """Jenks-classify ``data`` into ``n_classes`` bins labelled n..1.

    Low values receive high labels (low DEA efficiency = high
    vulnerability). When duplicate bin edges appear, the number of classes
    is reduced until edges are unique — same recovery as the original.
    """
    breaks = jenkspy.jenks_breaks(data, n_classes=n_classes)

    unique_breaks = np.unique(breaks)
    if len(unique_breaks) < len(breaks):
        return assign_bin_with_handling(data, n_classes=n_classes - 1)

    return pd.cut(
        data,
        bins=unique_breaks,
        labels=list(range(n_classes, 0, -1)),
        include_lowest=True,
    )

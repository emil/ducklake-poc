"""Small correlation helpers for the analysis notebooks.

The inputs are already reduced in SQL: one value for each shot, or one value
for each time bucket. These functions do not read the lake.
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

Method = Literal["pearson", "spearman"]


def cross_correlation_matrix(
    left: pd.DataFrame, right: pd.DataFrame, method: Method = "pearson", min_pairs: int = 4
) -> pd.DataFrame:
    """Correlate each column of `left` with each column of `right`.

    Both frames have the same index (for example the shot number). Rows that
    exist in only one frame are ignored for that pair. The result has one row
    for each column of `right` and one column for each column of `left`. A pair
    with fewer than `min_pairs` shared rows gives NaN.
    """
    result = pd.DataFrame(index=right.columns, columns=left.columns, dtype=float)
    for lname in left.columns:
        for rname in right.columns:
            pair = pd.concat([left[lname], right[rname]], axis=1, join="inner").dropna()
            if (
                len(pair) < min_pairs
                or pair.iloc[:, 0].nunique() < 2
                or pair.iloc[:, 1].nunique() < 2
            ):
                continue
            if method == "spearman":
                # Spearman is Pearson on the ranks. pandas needs scipy for it. This does not.
                pair = pair.rank()
            result.loc[rname, lname] = pair.iloc[:, 0].corr(pair.iloc[:, 1])
    return result


def lagged_correlation(
    a: np.ndarray, b: np.ndarray, max_lag: int, min_pairs: int = 5
) -> pd.DataFrame:
    """Correlate `a[t]` with `b[t + lag]` for each lag in [-max_lag, max_lag].

    The two series use the same time grid. A positive lag means that `b`
    follows `a`. The result has the columns `lag` and `r`. A lag with fewer
    than `min_pairs` pairs, or with a constant series, gives NaN.
    """
    if len(a) != len(b):
        raise ValueError("a and b must have the same length")
    rows: list[tuple[int, float]] = []
    for lag in range(-max_lag, max_lag + 1):
        x = a[: len(a) - lag] if lag >= 0 else a[-lag:]
        y = b[lag:] if lag >= 0 else b[: len(b) + lag]
        if len(x) < min_pairs or np.std(x) == 0 or np.std(y) == 0:
            rows.append((lag, float("nan")))
        else:
            rows.append((lag, float(np.corrcoef(x, y)[0, 1])))
    return pd.DataFrame(rows, columns=["lag", "r"])

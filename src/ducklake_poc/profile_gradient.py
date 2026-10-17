"""Spatial profile gradient from a set of channels at one time.

The lake stores each channel as a time series. It does not store where the
channel measures. A profile gradient (dy/dR) needs one position for each
channel. In a real system the positions come from calibration metadata. This
module holds an assumed demo geometry for the `ece` stage and the pure
function that computes the gradient. The function does not read the lake.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Assumed demo geometry: major radius R in m of the 8 ECE channels. Real values depend on
# the magnetic field and the calibration. They are not in the lake.
ECE_RADIUS_M: dict[str, float] = {f"ece/ch{i:02d}": 1.60 + 0.05 * (i - 1) for i in range(1, 9)}


def profile_gradient(values: pd.DataFrame, radius_m: dict[str, float]) -> pd.DataFrame:
    """Return the profile and its gradient dy/dR for the channels in `values`.

    `values` has the columns `wave` and `y_mean` (one row for each channel).
    A channel that is not in `radius_m` is dropped. The result is sorted by
    radius and has the columns `wave`, `r_m`, `y_mean` and `grad`. The gradient
    uses central differences inside the profile and one-sided differences at
    the two ends (`numpy.gradient`). It is in y per m. A profile with fewer than
    2 channels gives NaN.
    """
    waves = [w for w in values["wave"] if w in radius_m]
    y_by_wave = dict(zip(values["wave"], values["y_mean"], strict=True))
    waves.sort(key=lambda w: radius_m[w])
    r_m = np.array([radius_m[w] for w in waves], dtype=float)
    y_mean = np.array([y_by_wave[w] for w in waves], dtype=float)
    grad = np.gradient(y_mean, r_m) if len(waves) >= 2 else np.full(len(waves), np.nan)
    return pd.DataFrame({"wave": waves, "r_m": r_m, "y_mean": y_mean, "grad": grad})

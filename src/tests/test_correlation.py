from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ducklake_poc.correlation import cross_correlation_matrix, lagged_correlation


def test_lagged_correlation_finds_the_shift() -> None:
    rng = np.random.default_rng(0)
    a = rng.normal(size=200)
    b = np.roll(a, 7)  # b follows a by 7 samples
    result = lagged_correlation(a, b, max_lag=15)
    best = result.loc[result["r"].idxmax()]
    assert best["lag"] == 7
    assert best["r"] > 0.9


def test_lagged_correlation_gives_nan_for_a_constant_series() -> None:
    result = lagged_correlation(np.ones(50), np.arange(50.0), max_lag=3)
    assert result["r"].isna().all()


def test_lagged_correlation_rejects_different_lengths() -> None:
    with pytest.raises(ValueError):
        lagged_correlation(np.ones(5), np.ones(6), max_lag=1)


def test_cross_correlation_matrix_shape_and_values() -> None:
    shots = pd.Index([1, 2, 3, 4, 5, 6], name="shot")
    left = pd.DataFrame({"CIII": [1, 2, 3, 4, 5, 6.0]}, index=shots)
    right = pd.DataFrame(
        {"up": [2, 4, 6, 8, 10, 12.0], "down": [6, 5, 4, 3, 2, 1.0], "flat": [1.0] * 6},
        index=shots,
    )
    m = cross_correlation_matrix(left, right)
    assert list(m.index) == ["up", "down", "flat"]
    assert list(m.columns) == ["CIII"]
    assert m.loc["up", "CIII"] == pytest.approx(1.0)
    assert m.loc["down", "CIII"] == pytest.approx(-1.0)
    assert np.isnan(m.loc["flat", "CIII"])


def test_cross_correlation_matrix_needs_enough_shared_shots() -> None:
    left = pd.DataFrame({"a": [1.0, 2.0, 3.0]}, index=[1, 2, 3])
    right = pd.DataFrame({"b": [1.0, 2.0, 3.0]}, index=[3, 4, 5])
    assert cross_correlation_matrix(left, right).isna().all().all()


def test_spearman_is_one_for_a_monotonic_nonlinear_relation_without_scipy() -> None:
    shots = pd.Index(range(1, 9), name="shot")
    left = pd.DataFrame({"a": np.arange(1.0, 9.0)}, index=shots)
    right = pd.DataFrame({"b": np.exp(np.arange(1.0, 9.0))}, index=shots)
    spearman = cross_correlation_matrix(left, right, method="spearman")
    pearson = cross_correlation_matrix(left, right, method="pearson")
    assert spearman.loc["b", "a"] == pytest.approx(1.0)
    assert pearson.loc["b", "a"] < 0.99


def test_spearman_handles_ties() -> None:
    shots = pd.Index(range(1, 7), name="shot")
    left = pd.DataFrame({"a": [1.0, 1.0, 2.0, 3.0, 3.0, 4.0]}, index=shots)
    right = pd.DataFrame({"b": [1.0, 2.0, 2.0, 3.0, 4.0, 4.0]}, index=shots)
    result = cross_correlation_matrix(left, right, method="spearman")
    assert 0.9 < result.loc["b", "a"] <= 1.0

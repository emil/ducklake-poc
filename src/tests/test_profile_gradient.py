"""Unit tests for the profile gradient. No services are necessary."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from ducklake_poc.profile_gradient import ECE_RADIUS_M, profile_gradient


def test_linear_profile_has_constant_gradient() -> None:
    radius = {"a": 1.0, "b": 1.5, "c": 2.0}
    values = pd.DataFrame({"wave": ["c", "a", "b"], "y_mean": [6.0, 2.0, 4.0]})
    out = profile_gradient(values, radius)
    assert list(out["wave"]) == ["a", "b", "c"]
    assert out["grad"].to_list() == pytest.approx([4.0, 4.0, 4.0])


def test_channel_without_position_is_dropped() -> None:
    values = pd.DataFrame({"wave": ["a", "b", "x"], "y_mean": [1.0, 2.0, 9.0]})
    out = profile_gradient(values, {"a": 0.0, "b": 1.0})
    assert list(out["wave"]) == ["a", "b"]


def test_fewer_than_two_channels_gives_nan() -> None:
    values = pd.DataFrame({"wave": ["a"], "y_mean": [1.0]})
    out = profile_gradient(values, {"a": 0.0})
    assert math.isnan(out["grad"].iloc[0])


def test_ece_radius_map_covers_the_ece_stage() -> None:
    from ducklake_poc.synthetic import DIAGNOSTICS

    assert set(ECE_RADIUS_M) == set(DIAGNOSTICS["ece"])

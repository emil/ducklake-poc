"""Unit tests for the shared notebook lake setup. DuckDB and the local filesystem only."""

from __future__ import annotations

from pathlib import Path

import pytest

from ducklake_poc import config
from ducklake_poc.notebook_lake import open_lake, pins_cte

STAGES = ["spectrometer", "thomson"]


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory):
    root: Path = tmp_path_factory.mktemp("demo_lake")
    return open_lake(demo_shots=[1, 2], demo_stages=STAGES, demo_root=root)


def test_demo_lake_lists_shots_and_stages(lake) -> None:
    assert lake.shots == [1, 2]
    assert lake.stages == STAGES
    assert lake.stages_of(1) == STAGES


def test_demo_lake_pins_one_version_for_each_shot_and_stage(lake) -> None:
    for shot in lake.shots:
        for stage in STAGES:
            assert isinstance(lake.pin(shot, stage), str)
    with pytest.raises(KeyError):
        lake.pin(99, "spectrometer")


def test_demo_lake_lists_waves_of_a_stage(lake) -> None:
    waves = lake.list_waves(1, "spectrometer")
    assert "spectrometer/line_CIII" in waves
    assert all(w.startswith("spectrometer/") for w in waves)


def test_cross_shot_query_with_pins_cte_counts_each_shot_once(lake) -> None:
    cte, params = pins_cte([1, 2], "spectrometer", lake.pin)
    df = lake.q(
        f"WITH {cte} SELECT w.shot, count(DISTINCT w.data_version) AS versions "
        f"FROM {config.TABLE_NAME} w JOIN pins p "
        "ON w.shot = p.shot AND w.data_version = p.data_version "
        "WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ? "
        "GROUP BY w.shot ORDER BY w.shot",
        [*params, lake.experiment, "spectrometer", 1, 2],
    )
    assert df["shot"].tolist() == [1, 2]
    assert df["versions"].tolist() == [1, 1]


def test_demo_lake_is_reused_when_the_signature_matches(tmp_path: Path) -> None:
    built: list[int] = []

    class Counter:
        def __enter__(self) -> None:
            built.append(1)

        def __exit__(self, *exc: object) -> None:
            return None

    open_lake(
        demo_shots=[1], demo_stages=["spectrometer"], demo_root=tmp_path, build_context=Counter
    )
    open_lake(
        demo_shots=[1], demo_stages=["spectrometer"], demo_root=tmp_path, build_context=Counter
    )
    assert built == [1]


def test_pin_newest_takes_the_first_row_of_each_shot_and_stage() -> None:
    from ducklake_poc.notebook_lake import pin_newest

    rows = [(1, "mirnov", "new"), (1, "mirnov", "old"), (2, "mirnov", "only")]
    assert pin_newest(rows) == {(1, "mirnov"): "new", (2, "mirnov"): "only"}


def test_pin_unique_pins_a_single_version() -> None:
    from ducklake_poc.notebook_lake import pin_unique

    rows = [(1, "mirnov", "a"), (1, "thomson", "b"), (2, "mirnov", "a")]
    pinned, ambiguous = pin_unique(rows)
    assert pinned == {(1, "mirnov"): "a", (1, "thomson"): "b", (2, "mirnov"): "a"}
    assert ambiguous == {}


def test_pin_unique_refuses_to_guess_between_versions() -> None:
    from ducklake_poc.notebook_lake import pin_unique

    with pytest.raises(ValueError, match="more than one data_version"):
        pin_unique([(1, "mirnov", "a"), (1, "mirnov", "b")])


def test_pin_unique_first_policy_pins_by_name_and_reports_the_pairs() -> None:
    from ducklake_poc.notebook_lake import pin_unique

    rows = [(1, "mirnov", "zz"), (1, "mirnov", "aa"), (2, "mirnov", "bb")]
    pinned, ambiguous = pin_unique(rows, on_ambiguous="first")
    assert pinned == {(1, "mirnov"): "aa", (2, "mirnov"): "bb"}
    assert ambiguous == {(1, "mirnov"): ["aa", "zz"]}


def test_group_versions_keeps_the_order_and_drops_duplicates() -> None:
    from ducklake_poc.notebook_lake import group_versions

    rows = [(1, "m", "b"), (1, "m", "a"), (1, "m", "b"), (2, "m", "c")]
    assert group_versions(rows) == {(1, "m"): ["b", "a"], (2, "m"): ["c"]}


def test_versions_of_puts_the_pinned_version_first(lake) -> None:
    for shot in lake.shots:
        versions = lake.versions_of(shot, "thomson")
        assert versions[0] == lake.pin(shot, "thomson")
        assert sorted(versions) == sorted(set(versions))


def test_demo_lake_exposes_connection_and_directory(lake) -> None:
    assert lake.connection is not None
    assert lake.lake_dir is not None and lake.lake_dir.name == "lake"

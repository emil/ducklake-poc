from __future__ import annotations

import pytest

from ducklake_poc.fetch_wave import check_no_overlapping_pure_row_groups

# Row shape (matches fetch_wave.COLUMNS):
# file_path, file_url, row_group_id, row_group_rows, wave_min, wave_max,
# data_version, x_min, x_max, byte_start, byte_length


def _row(file_path, rg_id, wave_min, wave_max, x_min, x_max):
    return (
        file_path,
        f"http://x/{file_path}",
        rg_id,
        100,
        wave_min,
        wave_max,
        "v1",
        x_min,
        x_max,
        0,
        1000,
    )


def test_no_overlap_across_files_is_fine() -> None:
    rows = [
        _row("f1.parquet", 0, "w", "w", 0.0, 1.0),
        _row("f2.parquet", 0, "w", "w", 1.0, 2.0),
    ]
    check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")  # should not raise


def test_adjacent_non_overlapping_boundaries_are_fine() -> None:
    """a_max == b_min (touching, not overlapping) must not trigger the
    check -- the condition is a strict '>' for a reason."""
    rows = [
        _row("f1.parquet", 0, "w", "w", 0.0, 1.0),
        _row("f2.parquet", 0, "w", "w", 1.0, 2.0),
    ]
    check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")


def test_overlapping_x_ranges_across_different_files_raises() -> None:
    rows = [
        _row("f1.parquet", 0, "w", "w", 0.0, 1.5),
        _row("f2.parquet", 0, "w", "w", 1.0, 2.0),  # overlaps f1's [0, 1.5]
    ]
    with pytest.raises(SystemExit, match="overlapping x ranges"):
        check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")


def test_overlap_within_the_same_file_does_not_raise() -> None:
    """The check specifically targets cross-FILE inconsistency (duplicate/
    partial compaction); two row groups within the same file aren't
    compared against each other by design."""
    rows = [
        _row("f1.parquet", 0, "w", "w", 0.0, 1.5),
        _row("f1.parquet", 1, "w", "w", 1.0, 2.0),
    ]
    check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")


def test_mixed_row_groups_excluded_from_check() -> None:
    """A packed/mixed row group (wave_min != wave_max) claiming a broad x
    range must not be compared against pure row groups -- that's expected
    imprecision for small packed waves, not corruption."""
    rows = [
        _row("f1.parquet", 0, "wave_a", "wave_z", 0.0, 500.0),  # mixed, broad range
        _row("f2.parquet", 0, "w", "w", 100.0, 101.0),  # pure, well within the mixed range
    ]
    check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")


def test_three_way_overlap_detected() -> None:
    rows = [
        _row("f1.parquet", 0, "w", "w", 0.0, 1.0),
        _row("f2.parquet", 0, "w", "w", 5.0, 6.0),
        _row("f3.parquet", 0, "w", "w", 0.5, 5.5),  # overlaps both neighbors
    ]
    with pytest.raises(SystemExit):
        check_no_overlapping_pure_row_groups(rows, wave="w", data_version="v1")

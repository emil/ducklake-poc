"""Unit tests for api_server.plan_response -- pure logic, synthetic rows,
no Postgres/nginx/Flask needed. This is where the gap-safety behavior
gets exercised: compact.py's guarantees make a real gap essentially
unreachable through the normal pipeline (see test_api_server.py's
whole-wave test, which now collapses to a single redirect instead of
hitting the multi-file fallback), so the defensive check itself is only
verifiable against hand-crafted rows like these.

Row shape (matches fetch_wave.COLUMNS):
file_path, file_url, row_group_id, row_group_rows, wave_min, wave_max,
data_version, x_min, x_max, byte_start, byte_length
"""

from __future__ import annotations

from ducklake_poc.api_server import plan_response


def _row(file_path, rg_id, rows, x_min, x_max, byte_start, byte_length):
    return (
        file_path,
        f"http://x/{file_path}",
        rg_id,
        rows,
        "w",
        "w",
        "v1",
        x_min,
        x_max,
        byte_start,
        byte_length,
    )


def test_single_row_group_always_collapses() -> None:
    rows = [_row("f1.parquet", 5, 1000, 10.0, 20.0, 500, 100)]
    plan = plan_response(rows)
    assert plan is not None
    assert plan.file_path == "f1.parquet"
    assert plan.byte_start == 500
    assert plan.byte_length == 100
    assert plan.row_group_ids == [5]
    assert plan.total_rows == 1000


def test_contiguous_row_groups_in_same_file_collapse() -> None:
    rows = [
        _row("f1.parquet", 2, 1000, 10.0, 20.0, 100, 50),
        _row("f1.parquet", 3, 1000, 20.0, 30.0, 150, 60),
        _row("f1.parquet", 4, 1000, 30.0, 40.0, 210, 70),
    ]
    plan = plan_response(rows)
    assert plan is not None
    assert plan.byte_start == 100
    assert plan.byte_length == 180  # (210 + 70) - 100
    assert plan.row_group_ids == [2, 3, 4]
    assert plan.total_rows == 3000
    assert plan.x_min == 10.0
    assert plan.x_max == 40.0


def test_order_of_input_rows_does_not_matter() -> None:
    """The matches come back ORDER BY row_group_id from SQL already, but
    plan_response shouldn't silently depend on that."""
    rows = [
        _row("f1.parquet", 4, 1000, 30.0, 40.0, 210, 70),
        _row("f1.parquet", 2, 1000, 10.0, 20.0, 100, 50),
        _row("f1.parquet", 3, 1000, 20.0, 30.0, 150, 60),
    ]
    plan = plan_response(rows)
    assert plan is not None
    assert plan.row_group_ids == [2, 3, 4]
    assert plan.byte_start == 100
    assert plan.byte_length == 180


def test_matches_spanning_multiple_files_refuses_to_collapse() -> None:
    rows = [
        _row("f1.parquet", 99, 1000, 10.0, 20.0, 100, 50),
        _row("f2.parquet", 0, 1000, 20.0, 30.0, 0, 60),
    ]
    assert plan_response(rows) is None


def test_gap_in_row_group_ids_refuses_to_collapse() -> None:
    """rg 2 and rg 4 match, but rg 3 (in between, same file) does NOT --
    combining would silently include rg 3's bytes, which belong to some
    other wave/packed-group entirely, not the queried one. This must
    refuse rather than guess."""
    rows = [
        _row("f1.parquet", 2, 1000, 10.0, 20.0, 100, 50),
        _row("f1.parquet", 4, 1000, 30.0, 40.0, 210, 70),
        # rg 3 deliberately absent from the match set
    ]
    assert plan_response(rows) is None


def test_gap_detection_is_order_independent() -> None:
    rows = [
        _row("f1.parquet", 4, 1000, 30.0, 40.0, 210, 70),
        _row("f1.parquet", 2, 1000, 10.0, 20.0, 100, 50),
    ]
    assert plan_response(rows) is None

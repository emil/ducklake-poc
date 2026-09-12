from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ducklake_poc import config
from ducklake_poc.manifest import build_manifest_rows


@pytest.fixture(autouse=True)
def _isolated_lake_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """file_url_for() requires paths to live under config.LAKE_DATA_DIR --
    point that at this test's tmp_path so tests stay isolated from the
    real data/lake directory instead of writing into it."""
    monkeypatch.setattr(config, "LAKE_DATA_DIR", tmp_path)


def _hive_path(tmp_path: Path, filename: str, *, experiment="exp1", shot=1, stage="mirnov") -> Path:
    """experiment/shot/stage are hive-partition keys now (see
    synthetic.py's wave_to_sample_table and manifest.py's module
    docstring) -- build_manifest_rows parses them from the directory
    path, not from the file's own data."""
    d = tmp_path / f"experiment={experiment}" / f"shot={shot}" / f"stage={stage}"
    d.mkdir(parents=True, exist_ok=True)
    return d / filename


def _write_table(path: Path, table: pa.Table, row_group_size: int | None = None) -> None:
    if row_group_size is None:
        pq.write_table(table, path)
        return
    writer = pq.ParquetWriter(path, table.schema)
    for start in range(0, table.num_rows, row_group_size):
        writer.write_table(table.slice(start, row_group_size))
    writer.close()


def _sample_table(n: int, *, wave="probe_01", version="v1"):
    """Only the physical columns -- wave, data_version, x, y. experiment/
    shot/stage are hive-path-only, so they're never part of the file's
    own data (see compact.py's PHYSICAL_COLUMNS)."""
    x = [float(i) for i in range(n)]
    return pa.table(
        {
            "wave": pa.array([wave] * n, type=pa.string()),
            "data_version": pa.array([version] * n, type=pa.string()),
            "x": pa.array(x, type=pa.float64()),
            "y": pa.array([0.0] * n, type=pa.float64()),
        }
    )


def test_build_manifest_rows_extracts_correct_stats(tmp_path: Path) -> None:
    path = _hive_path(tmp_path, "t.parquet", experiment="exp1", shot=1, stage="mirnov")
    table = _sample_table(100, wave="probe_01", version="v1")
    _write_table(path, table, row_group_size=25)

    con = duckdb.connect()
    rows = build_manifest_rows(con, path)

    assert len(rows) == 4  # 100 rows / 25 per group
    for i, row in enumerate(rows):
        (
            experiment,
            shot,
            stage,
            wave_min,
            wave_max,
            data_version,
            x_min,
            x_max,
            file_path,
            _url,
            row_group_id,
            row_group_rows,
            byte_start,
            byte_length,
        ) = row
        assert experiment == "exp1"
        assert shot == 1
        assert stage == "mirnov"
        assert wave_min == wave_max == "probe_01"
        assert data_version == "v1"
        assert row_group_id == i
        assert row_group_rows == 25
        assert x_min == float(i * 25)
        assert x_max == float(i * 25 + 24)
        assert byte_start >= 0
        assert byte_length > 0
        assert file_path == str(path.resolve().relative_to(tmp_path.resolve()))


def test_build_manifest_rows_raises_on_malformed_hive_path(tmp_path: Path) -> None:
    """experiment/shot/stage now come exclusively from the directory path
    -- a file missing one of those hive segments has no other source of
    truth for that value, so build_manifest_rows must refuse rather than
    silently producing an incomplete manifest entry."""
    bad_dir = tmp_path / "experiment=exp1" / "shot=1"  # missing stage=...
    bad_dir.mkdir(parents=True)
    path = bad_dir / "t.parquet"
    _write_table(path, _sample_table(10))

    con = duckdb.connect()
    with pytest.raises(ValueError, match="missing hive-partition"):
        build_manifest_rows(con, path)


def test_build_manifest_rows_raises_on_mixed_data_version(tmp_path: Path) -> None:
    path = _hive_path(tmp_path, "bad_version.parquet")
    t1 = _sample_table(10, version="v1")
    t2 = _sample_table(10, version="v2")
    combined = pa.concat_tables([t1, t2])
    pq.write_table(combined, path, row_group_size=20)

    con = duckdb.connect()
    with pytest.raises(ValueError, match="spans multiple"):
        build_manifest_rows(con, path)


def test_build_manifest_rows_wave_range_for_packed_row_group(tmp_path: Path) -> None:
    """Several small waves from the SAME stage+data_version packed into one
    row group is legitimate -- wave_min/wave_max should span the range
    rather than raising."""
    path = _hive_path(tmp_path, "packed.parquet")
    t1 = _sample_table(5, wave="chA")
    t2 = _sample_table(5, wave="chB")
    combined = pa.concat_tables([t1, t2])
    pq.write_table(combined, path, row_group_size=10)

    con = duckdb.connect()
    rows = build_manifest_rows(con, path)
    assert len(rows) == 1
    wave_min, wave_max = rows[0][3], rows[0][4]
    assert wave_min == "chA"
    assert wave_max == "chB"


def test_clean_relative_path_strips_hive_prefixes(tmp_path: Path) -> None:
    from ducklake_poc.manifest import clean_relative_path, relative_to_lake_dir

    path = _hive_path(tmp_path, "file.parquet", experiment="exp1", shot=1, stage="mirnov")
    path.touch()
    assert clean_relative_path(relative_to_lake_dir(path)) == "exp1/1/mirnov/file.parquet"

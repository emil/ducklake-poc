from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from ducklake_poc.parquet_encoding import (
    BYTE_STREAM_SPLIT_COLUMNS,
    DICTIONARY_COLUMNS,
    open_parquet_writer,
)


def _encodings_by_column(path: Path) -> dict[str, str]:
    con = duckdb.connect()
    rows = con.execute(
        "SELECT path_in_schema, encodings FROM parquet_metadata(?) WHERE row_group_id = 0",
        [str(path)],
    ).fetchall()
    return dict(rows)


def test_dictionary_columns_get_dictionary_encoding(tmp_path: Path) -> None:
    table = pa.table(
        {
            "experiment": pa.array(["exp1", "exp1"], type=pa.string()),
            "stage": pa.array(["mirnov", "mirnov"], type=pa.string()),
            "wave": pa.array(["mirnov/probe_01", "mirnov/probe_01"], type=pa.string()),
            "data_version": pa.array(["a1b2c3d", "a1b2c3d"], type=pa.string()),
            "x": pa.array([0.0, 1.0], type=pa.float64()),
            "y": pa.array([0.5, 0.6], type=pa.float64()),
        }
    )
    path = tmp_path / "test.parquet"
    writer = open_parquet_writer(str(path), table.schema)
    writer.write_table(table)
    writer.close()

    encodings = _encodings_by_column(path)
    for col in DICTIONARY_COLUMNS:
        assert "RLE_DICTIONARY" in encodings[col], f"{col} should be dictionary-encoded"


def test_byte_stream_split_columns_get_bss_encoding(tmp_path: Path) -> None:
    table = pa.table(
        {
            "x": pa.array([0.0, 1.0, 2.0], type=pa.float64()),
            "y": pa.array([0.5, 0.6, 0.7], type=pa.float64()),
        }
    )
    path = tmp_path / "test.parquet"
    writer = open_parquet_writer(str(path), table.schema)
    writer.write_table(table)
    writer.close()

    encodings = _encodings_by_column(path)
    for col in BYTE_STREAM_SPLIT_COLUMNS:
        assert "BYTE_STREAM_SPLIT" in encodings[col], f"{col} should use BYTE_STREAM_SPLIT"


def test_bss_and_dictionary_columns_are_disjoint() -> None:
    assert not (set(DICTIONARY_COLUMNS) & set(BYTE_STREAM_SPLIT_COLUMNS))


def test_open_parquet_writer_handles_partial_schema(tmp_path: Path) -> None:
    """A schema missing some of DICTIONARY_COLUMNS/BYTE_STREAM_SPLIT_COLUMNS
    (e.g. a test file with only x, y) shouldn't error -- only the columns
    actually present get the policy applied."""
    table = pa.table({"x": pa.array([1.0]), "y": pa.array([2.0])})
    path = tmp_path / "partial.parquet"
    writer = open_parquet_writer(str(path), table.schema)
    writer.write_table(table)
    writer.close()
    assert path.exists()

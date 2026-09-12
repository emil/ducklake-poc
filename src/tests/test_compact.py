from __future__ import annotations

import re
from pathlib import Path

import duckdb

from ducklake_poc.compact import (
    _iter_wave_groups,
    _shot_stage_where_clause,
    stream_compact_to_files,
)

FILENAME_RE = re.compile(r"^(?P<wave>.+)__(?P<version>[^_]+)__[0-9a-f]{8}\.parquet$")


# ---------------------------------------------------------------------------
# _shot_stage_where_clause
# ---------------------------------------------------------------------------


def test_shot_stage_where_clause_whole_shot() -> None:
    """No stage -> compact the whole shot (the "genuine recompact sweep"
    case, e.g. periodic maintenance). This is deliberately the ONLY case
    that touches every stage's data, so it must never be the accidental
    default."""
    where_sql, params = _shot_stage_where_clause(shot=7, stage=None)
    assert where_sql == "shot = ?"
    assert params == [7]


def test_shot_stage_where_clause_scoped_to_one_stage() -> None:
    """This is the actual fix: ingest.py now passes `stage` here so that
    ingesting one diagnostic group's data only ever compacts (and
    deletes/rewrites) that group's own rows -- never another,
    already-compacted stage's rows for the same shot."""
    where_sql, params = _shot_stage_where_clause(shot=7, stage="mirnov")
    assert where_sql == "shot = ? AND stage = ?"
    assert params == [7, "mirnov"]


def _make_table(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """rows: list of (experiment, shot, stage, wave, data_version, x, y)"""
    con.execute(
        """
        CREATE TABLE t (
            experiment VARCHAR, shot INTEGER, stage VARCHAR, wave VARCHAR,
            data_version VARCHAR, x DOUBLE, y DOUBLE
        )
        """
    )
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)", rows)


def _sorted_reader(con: duckdb.DuckDBPyConnection, batch_size: int):
    return con.execute(
        "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"
    ).to_arrow_reader(batch_size)


# ---------------------------------------------------------------------------
# _iter_wave_groups
# ---------------------------------------------------------------------------


def test_iter_wave_groups_splits_on_wave_change() -> None:
    con = duckdb.connect()
    rows = [("e", 1, "s", "w1", "v1", float(i), 0.0) for i in range(5)]
    rows += [("e", 1, "s", "w2", "v1", float(i), 0.0) for i in range(3)]
    _make_table(con, rows)

    groups = list(_iter_wave_groups(_sorted_reader(con, batch_size=100)))
    assert [(g[1], g[2], g[3], g[4].num_rows) for g in groups] == [
        ("s", "w1", "v1", 5),
        ("s", "w2", "v1", 3),
    ]


def test_iter_wave_groups_splits_on_stage_change() -> None:
    con = duckdb.connect()
    rows = [("e", 1, "s1", "w1", "v1", float(i), 0.0) for i in range(4)]
    rows += [("e", 1, "s2", "w1", "v1", float(i), 0.0) for i in range(4)]
    _make_table(con, rows)

    groups = list(_iter_wave_groups(_sorted_reader(con, batch_size=100)))
    assert [g[1] for g in groups] == ["s1", "s2"]


def test_iter_wave_groups_splits_on_data_version_change() -> None:
    con = duckdb.connect()
    rows = [("e", 1, "s", "w1", "v1", float(i), 0.0) for i in range(4)]
    rows += [("e", 1, "s", "w1", "v2", float(i), 0.0) for i in range(4)]
    _make_table(con, rows)

    groups = list(_iter_wave_groups(_sorted_reader(con, batch_size=100)))
    assert [g[3] for g in groups] == ["v1", "v2"]


def test_iter_wave_groups_reassembles_a_wave_split_across_internal_batches() -> None:
    """A single wave's rows can arrive split across several underlying
    Arrow batches (e.g. a big wave read in small chunks) -- the carry-
    forward logic must reassemble them into ONE group, not several."""
    con = duckdb.connect()
    rows = [("e", 1, "s", "w1", "v1", float(i), 0.0) for i in range(250)]
    _make_table(con, rows)

    # force tiny internal batches (much smaller than the wave itself) so
    # the wave necessarily spans multiple underlying Arrow RecordBatches
    groups = list(_iter_wave_groups(_sorted_reader(con, batch_size=17)))
    assert len(groups) == 1
    assert groups[0][4].num_rows == 250


def test_iter_wave_groups_preserves_total_row_count() -> None:
    con = duckdb.connect()
    rows = []
    for stage in ["s1", "s2"]:
        for wave in ["w1", "w2"]:
            for version in ["v1", "v2"]:
                n = 13  # deliberately not a multiple of any batch size below
                rows += [("e", 1, stage, wave, version, float(i), 0.0) for i in range(n)]
    _make_table(con, rows)

    total = sum(g[4].num_rows for g in _iter_wave_groups(_sorted_reader(con, batch_size=10)))
    assert total == len(rows)


# ---------------------------------------------------------------------------
# stream_compact_to_files
# ---------------------------------------------------------------------------


def _build_mixed_dataset(con: duckdb.DuckDBPyConnection) -> int:
    """Small waves in two stages (packable), a wave in two data_versions
    (must never share a row group/file), and one oversized wave (must get
    its own dedicated, pure row groups). Returns total row count."""
    rows = []
    # small, packable waves across two stages
    for stage in ["rogowski", "thomson"]:
        for wave in ["chA", "chB"]:
            rows += [("exp1", 1, stage, wave, "v1", float(i), 0.0) for i in range(50)]
    # same wave, two different data_versions -- must never mix
    for version in ["v1", "v2"]:
        rows += [("exp1", 1, "flux_loops", "loopA", version, float(i), 0.0) for i in range(40)]
    # one oversized wave -- must get dedicated pure row groups
    rows += [("exp1", 1, "mirnov", "probe_01", "v1", float(i), 0.0) for i in range(1000)]
    _make_table(con, rows)
    return len(rows)


def test_stream_compact_no_row_group_or_file_mixes_stage_or_version(tmp_path: Path) -> None:
    """stage is now hive-path-only (see compact.py's PHYSICAL_COLUMNS), so
    "no file spans multiple stages" is checked via the path itself rather
    than parquet_metadata() -- a file living at a single
    experiment=X/shot=Y/stage=Z/ directory structurally can't span
    stages. data_version remains a real physical column, so that's still
    checked via parquet_metadata()."""
    con = duckdb.connect()
    _build_mixed_dataset(con)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=100, target_file_size_bytes=10_000_000
    )
    assert len(paths) > 0

    meta_con = duckdb.connect()
    for p in paths:
        rel = Path(p).relative_to(tmp_path)
        hive_keys = {part.split("=", 1)[0] for part in rel.parts[:-1] if "=" in part}
        assert hive_keys == {"experiment", "shot", "stage"}, f"{p} has a malformed hive path"

        row = meta_con.execute(
            "SELECT count(DISTINCT stats_min_value) FROM parquet_metadata(?) "
            "WHERE path_in_schema='data_version'",
            [p],
        ).fetchone()
        assert row is not None
        assert row[0] == 1, f"{p} spans multiple data_versions"


def test_stream_compact_preserves_total_row_count(tmp_path: Path) -> None:
    con = duckdb.connect()
    total_rows = _build_mixed_dataset(con)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=100, target_file_size_bytes=10_000_000
    )

    meta_con = duckdb.connect()
    written = 0
    for p in paths:
        row = meta_con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()
        assert row is not None
        written += row[0]
    assert written == total_rows


def test_stream_compact_oversized_wave_gets_pure_dedicated_row_groups(tmp_path: Path) -> None:
    con = duckdb.connect()
    _build_mixed_dataset(con)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=100, target_file_size_bytes=10_000_000
    )

    meta_con = duckdb.connect()
    mirnov_files = [p for p in paths if "stage=mirnov" in str(p)]
    assert mirnov_files, "expected at least one file for the oversized mirnov wave"

    for p in mirnov_files:
        rows = meta_con.execute(
            """
            SELECT row_group_id,
                   count(DISTINCT CASE WHEN path_in_schema='wave'
                       THEN stats_min_value END) AS n_waves
            FROM parquet_metadata(?) WHERE path_in_schema='wave'
            GROUP BY row_group_id
            """,
            [p],
        ).fetchall()
        assert all(n_waves == 1 for _rg, n_waves in rows), f"{p} has a mixed row group"

    # 1000 rows / 100 per group = 10 pure row groups total for this wave
    total_mirnov_rgs = 0
    for p in mirnov_files:
        row = meta_con.execute(
            f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{p}')"
        ).fetchone()
        assert row is not None
        total_mirnov_rgs += row[0]
    assert total_mirnov_rgs == 10


def test_stream_compact_filenames_match_convention(tmp_path: Path) -> None:
    con = duckdb.connect()
    _build_mixed_dataset(con)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=100, target_file_size_bytes=10_000_000
    )
    for p in paths:
        assert FILENAME_RE.match(Path(p).name), f"{p} doesn't match the naming convention"


def test_stream_compact_layout_is_hive_partitioned(tmp_path: Path) -> None:
    con = duckdb.connect()
    _build_mixed_dataset(con)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=100, target_file_size_bytes=10_000_000
    )
    for p in paths:
        rel = Path(p).relative_to(tmp_path)
        # exactly experiment=X/shot=Y/stage=Z/filename.parquet -- three
        # hive-partition directory levels, then the file
        assert len(rel.parts) == 4
        assert rel.parts[0] == "experiment=exp1"
        assert rel.parts[1] == "shot=1"
        assert rel.parts[2].startswith("stage=")


def test_stream_compact_oversized_wave_never_splits_across_files_regardless_of_target_size(
    tmp_path: Path,
) -> None:
    """A single wave's dedicated row-group run must land entirely in one
    file even when it grows well past target_file_size_bytes -- this is
    what lets api_server.py safely combine a multi-row-group match into
    one byte-range redirect (see there)."""
    con = duckdb.connect()
    rows = [("exp1", 1, "mirnov", "probe_01", "v1", float(i), 0.0) for i in range(5000)]
    _make_table(con, rows)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    # a tiny target size would force many small files under the OLD
    # policy -- confirm it now stays as exactly one file instead.
    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=200, target_file_size_bytes=2000
    )
    assert len(paths) == 1

    meta_con = duckdb.connect()
    row = meta_con.execute(f"SELECT count(*) FROM read_parquet('{paths[0]}')").fetchone()
    assert row is not None
    assert row[0] == 5000


def test_stream_compact_target_file_size_still_splits_between_separate_waves(
    tmp_path: Path,
) -> None:
    """The size target still applies BETWEEN complete waves -- two
    separate oversized waves in the same partition should still end up
    in separate files once the running total crosses the target, even
    though neither wave is internally split."""
    con = duckdb.connect()
    rows = [("exp1", 1, "mirnov", "probe_01", "v1", float(i), 0.0) for i in range(3000)]
    rows += [("exp1", 1, "mirnov", "probe_02", "v1", float(i), 0.0) for i in range(3000)]
    _make_table(con, rows)
    source_sql = "SELECT * FROM t ORDER BY experiment, shot, stage, wave, data_version, x"

    paths = stream_compact_to_files(
        con, source_sql, tmp_path, rows_per_row_group=200, target_file_size_bytes=2000
    )
    assert len(paths) >= 2

    meta_con = duckdb.connect()
    for p in paths:
        n_waves = meta_con.execute(
            "SELECT count(DISTINCT stats_min_value) FROM parquet_metadata(?) "
            "WHERE path_in_schema='wave'",
            [p],
        ).fetchone()
        assert n_waves is not None
        assert n_waves[0] == 1, f"{p} should contain exactly one wave, not span two"

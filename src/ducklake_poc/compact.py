"""Compact per-wave files into fewer, larger parquet files.

Two strategies are demonstrated side by side:

  builtin   -- DuckLake's own `ducklake_merge_adjacent_files`. Simple,
              transaction-safe, handles time travel/snapshots correctly.
              Row-group and FILE boundaries inside the merged output are
              NOT controlled by us (DuckDB decides them), and we could not
              find documentation guaranteeing it never merges across
              PARTITIONED BY boundaries -- see manifest strategy below and
              the README for why physical layout is handled ourselves
              instead of relying on this for that guarantee.

  manifest  -- A custom compaction: read all rows for a scope (e.g. one
              shot), sorted by (stage, wave, data_version, x), and rewrite
              them with explicit control over both row-group AND file
              boundaries. `stage` and `data_version` are hard boundaries
              at BOTH levels: a row group never mixes them (so pruning
              stats stay meaningful), and neither does a FILE (so a
              physical file's directory -- experiment=X/shot=Y/stage=Z/
              -- and name -- <wave>__<data_version>.parquet -- both
              honestly describe its contents). This is what makes exact
              byte-range addressing per wave possible (see manifest.py).
              Old rows are deleted and the new files are registered inside
              a single DuckLake transaction, so this is still
              transactionally consistent from a reader's POV.

In a real deployment you'd likely run `builtin` broadly (for storage/
catalog health) and `manifest` only for the subset of data where exact
single-wave HTTP addressability actually matters.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import uuid
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import config
from .parquet_encoding import open_parquet_writer

# The physical columns actually written to each parquet file --
# experiment/shot/stage are hive-partition keys, encoded only in the
# directory path (see _open_new_file below), never duplicated as
# physical columns. See synthetic.py's wave_to_sample_table for the full
# rationale (avoiding github.com/duckdb/ducklake/issues/791).
PHYSICAL_COLUMNS = ["wave", "data_version", "x", "y"]

# NOTE ON ROW GROUP SIZING: DuckDB's `COPY ... (ROW_GROUP_SIZE n)` cannot
# produce row groups smaller than its internal vector chunk size (2048
# rows) -- requesting ROW_GROUP_SIZE 1 on more than 2048 rows silently
# floors at 2048 rows/group. To get exact control (e.g. exactly one wave
# per row group) we pull the sorted rows out as an Arrow table and write
# them with pyarrow's ParquetWriter, calling write_table() once per
# desired row-group chunk -- that path has no such floor.


def compact_builtin(con: duckdb.DuckDBPyConnection, target_file_size: str = "128MB") -> None:
    con.execute(
        f"CALL ducklake_set_option('{config.DUCKLAKE_NAME}', 'target_file_size', ?)",
        [target_file_size],
    )
    result = con.execute(
        f"CALL ducklake_merge_adjacent_files('{config.DUCKLAKE_NAME}', '{config.TABLE_NAME}')"
    ).fetchall()
    print(f"builtin merge_adjacent_files: {len(result)} output file(s) produced")


def _iter_wave_groups(reader):
    """Consume a sorted (by experiment, stage, wave, data_version, x)
    Arrow batch stream and re-group it into complete
    (experiment, stage, wave, data_version) chunks, regardless of how the
    underlying batches happened to be cut. A group that spans multiple
    incoming batches is carried forward and only yielded once its
    boundary (the next experiment/stage/wave/data_version starting) is
    found. This is what lets the caller guarantee a row group never mixes
    a *partial* large wave with anything else -- and never mixes two
    different stages or two different data_versions either."""
    carry = None
    for batch in reader:
        table = pa.Table.from_batches([batch])
        if carry is not None:
            table = pa.concat_tables([carry, table])
            carry = None

        experiments = table.column("experiment").to_numpy(zero_copy_only=False)
        stages = table.column("stage").to_numpy(zero_copy_only=False)
        waves = table.column("wave").to_numpy(zero_copy_only=False)
        versions = table.column("data_version").to_numpy(zero_copy_only=False)
        # np.diff doesn't work on string/object arrays; compare consecutive
        # tuples directly instead.
        changed = (
            (experiments[:-1] != experiments[1:])
            | (stages[:-1] != stages[1:])
            | (waves[:-1] != waves[1:])
            | (versions[:-1] != versions[1:])
        )
        boundaries = np.where(changed)[0] + 1
        start = 0
        for b in boundaries:
            yield (
                experiments[start],
                stages[start],
                waves[start],
                versions[start],
                table.slice(start, b - start),
            )
            start = int(b)
        carry = table.slice(start, table.num_rows - start)

    if carry is not None and carry.num_rows > 0:
        yield (
            carry.column("experiment")[0].as_py(),
            carry.column("stage")[0].as_py(),
            carry.column("wave")[0].as_py(),
            carry.column("data_version")[0].as_py(),
            carry,
        )


def stream_compact_to_files(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    lake_root,
    rows_per_row_group: int = 20_000,
    target_file_size_bytes: int = 8 * 1024 * 1024,
) -> list[str]:
    """Stream `source_sql` (must be sorted by (experiment, shot, stage,
    wave, data_version, x)) out to one or more Parquet files with
    controlled row-group AND file boundaries:

      - A (wave, data_version) whose row count fits within
        `rows_per_row_group` may be packed together with adjacent small
        (wave, data_version) groups **from the same stage and the same
        data_version** in the same row group (their x stats won't be
        individually useful for range-pruning, but that's fine -- small
        waves are cheap to fetch whole).
      - A (wave, data_version) larger than `rows_per_row_group` gets its
        own dedicated, contiguous run of row groups, each covering a
        bounded x-range of JUST that wave's data_version.
      - **`stage` and `data_version` are hard partition boundaries at
        the FILE level, not just the row-group level**: a physical file
        never contains rows from two different diagnostic groups or two
        different calibration reruns, even across row groups. This is
        what lets each file live at
        `experiment=X/shot=Y/stage=Z/<wave-or-"multi">__<data_version>__<hash>.parquet`
        and have that path honestly describe its contents, and what makes
        "did compaction move stage X's data somewhere unexpected" a
        question with a structurally guaranteed "no".
      - Physical layout: genuine hive-style directories --
        `lake_root/experiment=<experiment>/shot=<shot>/stage=<stage>/`.
        `experiment`/`shot`/`stage` are NOT stored as physical columns
        inside the files (see PHYSICAL_COLUMNS) -- only `wave`,
        `data_version`, `x`, `y` are. This combination (hive directories,
        partition columns absent from the file's own data) is
        specifically what `ducklake_add_data_files` needs to register
        partitioned tables correctly -- see the README's "Physical
        layout & partitioning" section for why an earlier, flatter
        layout broke that.

    Within a (stage, data_version) partition, target_file_size_bytes
    rolls over to a new file only *between* complete waves -- never in
    the middle of one wave's own dedicated row-group run. That means a
    single wave larger than target_file_size_bytes still lands entirely
    in one file (the target gets exceeded, not honored, in that case),
    which is what guarantees every row group for a given (wave,
    data_version) always lives in exactly one file -- api_server.py
    relies on this to safely combine a multi-row-group match into a
    single byte-range redirect. If you genuinely need to cap individual
    file sizes even for oversized waves, `rows_per_row_group` is the
    knob to lower instead (fewer rows per row group -> smaller total
    bytes before hitting whatever downstream limit matters).

    Since we don't know whether a file will end up covering exactly one
    wave or several packed together until we finish writing it, files are
    written under a temporary name and renamed to their final,
    content-describing name when closed.
    """
    lake_root = Path(lake_root)
    reader = con.execute(source_sql).to_arrow_reader(max(rows_per_row_group, 100_000))

    written_paths: list[str] = []
    writer: pq.ParquetWriter | None = None
    tmp_path: str | None = None
    current_dir: Path | None = None
    current_stage: str | None = None
    current_data_version: str | None = None
    current_waves: set[str] = set()

    def _open_new_file(schema: pa.Schema, out_dir: Path, stage: str, data_version: str) -> None:
        nonlocal writer, tmp_path, current_dir, current_stage, current_data_version, current_waves
        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = str(out_dir / f".tmp_{uuid.uuid4().hex[:12]}.parquet")
        writer = open_parquet_writer(tmp_path, schema)
        current_dir = out_dir
        current_stage = stage
        current_data_version = data_version
        current_waves = set()

    def _finalize_current_file() -> None:
        nonlocal writer, tmp_path
        if writer is None:
            return
        assert tmp_path is not None
        assert current_dir is not None
        writer.close()
        wave_part = next(iter(current_waves)) if len(current_waves) == 1 else "multi"
        final_name = (
            f"{config.safe_filename(wave_part)}__{current_data_version}"
            f"__{uuid.uuid4().hex[:8]}.parquet"
        )
        final_path = str(current_dir / final_name)
        os.replace(tmp_path, final_path)
        written_paths.append(final_path)
        writer = None
        tmp_path = None

    def _write_row_group(
        table_chunk: pa.Table,
        out_dir: Path,
        stage: str,
        data_version: str,
        waves: set[str],
        check_size: bool = True,
    ) -> None:
        nonlocal writer
        # Project down to the physical columns actually stored on disk --
        # experiment/shot/stage live only in the directory path (out_dir),
        # never duplicated inside the file (see PHYSICAL_COLUMNS).
        projected = table_chunk.select(PHYSICAL_COLUMNS)

        partition_changed = writer is not None and (stage, data_version) != (
            current_stage,
            current_data_version,
        )
        size_exceeded = (
            check_size
            and writer is not None
            and not partition_changed
            and tmp_path is not None
            and os.path.getsize(tmp_path) >= target_file_size_bytes
        )
        if writer is None or partition_changed or size_exceeded:
            _finalize_current_file()
            _open_new_file(projected.schema, out_dir, stage, data_version)
        assert writer is not None
        writer.write_table(projected)
        current_waves.update(waves)

    pending_tables: list[pa.Table] = []
    pending_rows = 0
    pending_partition: tuple[Path, str, str] | None = None
    pending_waves: set[str] = set()

    def _flush_pending() -> None:
        nonlocal pending_tables, pending_rows, pending_partition, pending_waves
        if not pending_tables:
            return
        combined = (
            pending_tables[0] if len(pending_tables) == 1 else pa.concat_tables(pending_tables)
        )
        assert pending_partition is not None
        out_dir, stage, data_version = pending_partition
        _write_row_group(combined, out_dir, stage, data_version, pending_waves)
        pending_tables = []
        pending_rows = 0
        pending_partition = None
        pending_waves = set()

    try:
        for experiment, stage, wave, data_version, wave_table in _iter_wave_groups(reader):
            shot = wave_table.column("shot")[0].as_py()
            out_dir = lake_root / f"experiment={experiment}" / f"shot={shot}" / f"stage={stage}"
            partition = (out_dir, stage, data_version)
            n = wave_table.num_rows

            if n > rows_per_row_group:
                # Large wave version: never mix with pending small groups or
                # with itself split unevenly -- flush whatever's pending
                # first, then emit this wave version as its own dedicated
                # run of row groups (still within the same file/partition
                # boundary rules above).
                #
                # Size-based file rollover is only allowed BEFORE the first
                # chunk of this wave (check_size=True there) -- once we've
                # started this wave's dedicated run, we never split it
                # across files purely because it grew past
                # target_file_size_bytes. This means a single oversized
                # wave can produce a file larger than the target, but it
                # guarantees every row group for one (wave, data_version)
                # always lives in exactly one file -- which is what lets
                # api_server.py safely combine a multi-row-group match into
                # a single byte-range redirect (see there).
                _flush_pending()
                for i, start in enumerate(range(0, n, rows_per_row_group)):
                    _write_row_group(
                        wave_table.slice(start, rows_per_row_group),
                        out_dir,
                        stage,
                        data_version,
                        {wave},
                        check_size=(i == 0),
                    )
            else:
                # Never pack across an (experiment/shot dir, stage,
                # data_version) boundary, even if both sides are small --
                # see the hard-boundary note above. Within the same
                # partition, different waves may still be packed together.
                if pending_partition is not None and pending_partition != partition:
                    _flush_pending()
                if pending_rows + n > rows_per_row_group:
                    _flush_pending()
                pending_tables.append(wave_table)
                pending_rows += n
                pending_partition = partition
                pending_waves.add(wave)
        _flush_pending()
    finally:
        _finalize_current_file()

    return written_paths


def _shot_stage_where_clause(shot: int, stage: str | None) -> tuple[str, list[object]]:
    """The WHERE clause (and matching params) scoping compaction to a
    shot, optionally narrowed to one stage. Pulled out as a pure function
    so the scoping logic itself -- the actual fix for "ingesting one
    stage shouldn't re-touch another, already-compacted stage's data" --
    can be unit tested directly, without a live DuckLake attach."""
    if stage is not None:
        return "shot = ? AND stage = ?", [shot, stage]
    return "shot = ?", [shot]


def compact_manifest_friendly(
    con: duckdb.DuckDBPyConnection,
    shot: int,
    rows_per_row_group: int = 20_000,
    target_file_size_bytes: int = 8 * 1024 * 1024,
    stage: str | None = None,
) -> list[str]:
    """Rewrite per-sample rows for one shot (optionally scoped to just one
    stage) into one or more new files, sorted by (experiment, stage, wave,
    data_version, x) so wave-identity, stage/data_version isolation, and
    x-range pruning all work well, then swap them into DuckLake inside a
    single transaction.

    Pass `stage` whenever you're compacting right after ingesting that one
    diagnostic group (see ingest.py) -- without it, a shot-wide compaction
    re-reads and re-writes every OTHER stage's already-compacted data too,
    every single time, even though nothing about it changed. That's not
    just wasted work: it also means compaction doesn't become available
    until every stage for a shot has been ingested, defeating the point of
    ingesting incrementally, stage by stage, as data actually arrives.
    Omit `stage` only for a genuine "recompact this whole shot" sweep
    (e.g. periodic maintenance, or after `--no-compact` ingestion)."""
    where_sql, params = _shot_stage_where_clause(shot, stage)
    scope = f"shot={shot}" + (f" stage={stage}" if stage is not None else "")

    def _attempt() -> list[str]:
        con.execute("BEGIN TRANSACTION")
        paths: list[str] = []
        try:
            count_row = con.execute(
                f"SELECT count(*) FROM {config.TABLE_NAME} WHERE {where_sql}", params
            ).fetchone()
            assert count_row is not None  # SELECT count(*) always returns exactly one row
            n_rows = count_row[0]
            if n_rows == 0:
                con.execute("ROLLBACK")
                print(f"{scope}: no rows to compact")
                return []

            stage_filter = f"AND stage = '{stage}'" if stage is not None else ""
            source_sql = f"""
                SELECT * FROM {config.TABLE_NAME}
                WHERE shot = {shot} {stage_filter}
                ORDER BY experiment, stage, wave, data_version, x
            """
            paths = stream_compact_to_files(
                con,
                source_sql,
                config.LAKE_DATA_DIR,
                rows_per_row_group=rows_per_row_group,
                target_file_size_bytes=target_file_size_bytes,
            )

            con.execute(f"DELETE FROM {config.TABLE_NAME} WHERE {where_sql}", params)
            for path in paths:
                con.execute(
                    f"CALL ducklake_add_data_files("
                    f"'{config.DUCKLAKE_NAME}', '{config.TABLE_NAME}', '{path}')"
                )
            con.execute("COMMIT")
        except Exception:
            # A failed COMMIT (e.g. a DuckLake catalog conflict from a
            # concurrent writer -- see config.retry_on_conflict) already
            # aborts the transaction on its own; issuing ROLLBACK on top
            # of that raises ITS OWN "no transaction is active" error,
            # which would otherwise replace and hide the real one below.
            with contextlib.suppress(duckdb.Error):
                con.execute("ROLLBACK")
            # stream_compact_to_files() above already fully wrote and
            # renamed these to their final names on disk, but the
            # transaction that would have registered them with DuckLake
            # just failed -- they're orphaned. Clean them up now so a
            # retry (which redoes the read+compact+write from scratch,
            # since the underlying rows may have changed) doesn't leak a
            # new set of abandoned files into the lake directory on every
            # conflict.
            for p in paths:
                Path(p).unlink(missing_ok=True)
            raise

        print(f"{scope}: rewrote {n_rows} samples into {len(paths)} file(s)")
        return paths

    return config.retry_on_conflict(_attempt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", choices=["builtin", "manifest"], default="manifest")
    parser.add_argument("--shot", type=int, help="required for --strategy manifest")
    parser.add_argument("--target-file-size", default="128MB", help="for --strategy builtin")
    parser.add_argument(
        "--rows-per-row-group",
        type=int,
        default=20_000,
        help="for --strategy manifest: samples per row group (tune to your sample rate "
        "and expected query window -- see README)",
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="for --strategy manifest: roll to a new file past this size",
    )
    parser.add_argument(
        "--stage",
        default=None,
        help="for --strategy manifest: scope to just one diagnostic group (recommended -- "
        "compacting a whole shot re-touches every other already-compacted stage's data "
        "too, every time). Omit only for a genuine whole-shot recompaction sweep.",
    )
    args = parser.parse_args()

    con = config.get_connection()

    if args.strategy == "builtin":
        compact_builtin(con, target_file_size=args.target_file_size)
    else:
        if args.shot is None:
            parser.error("--shot is required for --strategy manifest")
        compact_manifest_friendly(
            con,
            args.shot,
            args.rows_per_row_group,
            args.target_file_size_bytes,
            stage=args.stage,
        )


if __name__ == "__main__":
    main()

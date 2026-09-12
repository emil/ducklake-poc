"""Ingest synthetic tokamak diagnostic data the way the current production
pipeline does: one parquet file per (wave, data_version), batched
registration into DuckLake via a single ducklake_add_data_files call per
(shot, stage) diagnostic group -- followed immediately by compaction and
a manifest refresh scoped to that SAME (shot, stage), so each stage is
individually ready for fetch_wave.py/api_server.py the moment it's
ingested, rather than waiting for every other stage in the shot too.
This matters because diagnostic groups genuinely arrive independently in
practice (different systems, different processing latencies) -- scoping
compaction to (shot, stage) means ingesting one stage never re-touches
another, already-compacted stage's files. Pass --no-compact to skip this
and inspect the raw, pre-compaction state instead (see
compact.py/manifest.py to run those steps manually later).

Physical layout of raw files: genuine hive-style directories --

    data/raw/experiment=<experiment>/shot=<shot>/stage=<stage>/<wave>__<data_version>.parquet

`experiment`/`shot`/`stage` are deliberately NOT stored as physical
columns inside these files (see synthetic.py's `wave_to_sample_table`) --
they're reconstructed from the directory path, both by DuckLake (which
still declares them in the table schema and has them SET PARTITIONED BY)
and, for local/no-DuckLake testing, by DuckDB's own hive-partitioning
auto-detection on `read_parquet(glob)`. This is what
`ducklake_add_data_files` actually needs to correctly register a
partitioned table without hitting
https://github.com/duckdb/ducklake/issues/791 (partition columns that are
*also* present in the file's own data cause "invalid partition value"
errors) -- see the README's "Physical layout & partitioning" section for
the full story of how this design was arrived at.

Usage:
    python -m ducklake_poc.ingest --shot 1 --stage mirnov
    python -m ducklake_poc.ingest --shot 1                    # every diagnostic group
    python -m ducklake_poc.ingest --shots 3                   # shots 1..3, every group
    python -m ducklake_poc.ingest --shot 1 --no-compact        # raw files only, no compact/manifest
"""

from __future__ import annotations

import argparse
import glob
import time
from pathlib import Path

import duckdb

from . import config
from .compact import compact_manifest_friendly
from .manifest import refresh_manifest_for_file
from .parquet_encoding import open_parquet_writer
from .synthetic import DEFAULT_EXPERIMENT, DIAGNOSTICS, generate_stage, wave_to_sample_table


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
            experiment VARCHAR,
            shot INTEGER,
            stage VARCHAR,
            wave VARCHAR,
            data_version VARCHAR,
            x DOUBLE,
            y DOUBLE
        )
        """
    )
    # Uniqueness/clustering key: (experiment, shot, stage, wave,
    # data_version) -- scientists periodically recalibrate a diagnostic
    # and rerun the pipeline, producing a new, independent data_version of
    # every channel in that stage; old versions are kept, not overwritten,
    # so the same (shot, stage, wave) can have several complete copies.
    # x on the end keeps each (wave, data_version)'s own samples in time
    # order, which is what makes row-group min/max on x tight enough for
    # narrow time-window queries to prune down to a couple of row groups
    # instead of scanning the whole wave.
    con.execute(
        f"ALTER TABLE {config.TABLE_NAME} SET SORTED BY "
        f"(experiment ASC, shot ASC, stage ASC, wave ASC, data_version ASC, x ASC)"
    )
    # Logical (catalog-level) partitioning for pruning benefit -- and,
    # crucially, this now also matches the PHYSICAL layout (see
    # write_stage_files below): experiment/shot/stage are hive-partition
    # keys encoded only in the directory path, never duplicated as
    # physical columns inside the files themselves. That combination is
    # what ducklake_add_data_files actually needs to register partitioned
    # files correctly -- see the module docstring and the README's
    # "Physical layout & partitioning" section for why an earlier version
    # of this (partition columns ALSO present in the file's own data)
    # failed with "invalid partition value for the table configuration".
    con.execute(f"ALTER TABLE {config.TABLE_NAME} SET PARTITIONED BY (experiment, shot, stage)")


def write_stage_files(shot: int, stage: str, experiment: str = DEFAULT_EXPERIMENT) -> list[Path]:
    """Write one small parquet file per (channel, data_version) that `stage`
    produced -- current production shape (one file per wave version), in
    genuine hive-style directories (see module docstring)."""
    out_dir = config.RAW_DATA_DIR / f"experiment={experiment}" / f"shot={shot}" / f"stage={stage}"
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for rec in generate_stage(shot=shot, stage=stage, experiment=experiment):
        table = wave_to_sample_table(rec)
        path = out_dir / f"{config.safe_filename(rec.wave)}__{rec.data_version}.parquet"
        writer = open_parquet_writer(str(path), table.schema)
        writer.write_table(table)
        writer.close()
        paths.append(path)
    return paths


def register_experiment(
    con: duckdb.DuckDBPyConnection, shot: int, stage: str, experiment: str = DEFAULT_EXPERIMENT
) -> int:
    """Single batched ducklake_add_data_files call for an entire diagnostic
    group's channels (all waves for one shot+stage), instead of one call
    per wave. This is the batching already in place -- the remaining
    problem is that each *file* is still tiny, which is why ingest.py now
    runs compaction immediately afterward by default (see main())."""
    glob_pattern = str(
        config.RAW_DATA_DIR
        / f"experiment={experiment}"
        / f"shot={shot}"
        / f"stage={stage}"
        / "*.parquet"
    )
    files = sorted(glob.glob(glob_pattern))
    if not files:
        return 0

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"CALL ducklake_add_data_files('{config.DUCKLAKE_NAME}', '{config.TABLE_NAME}', ?)",
            [files],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(files)


def compact_and_refresh_manifest(
    con: duckdb.DuckDBPyConnection,
    shot: int,
    stage: str,
    rows_per_row_group: int,
    target_file_size_bytes: int,
) -> tuple[int, int]:
    """Compact just-ingested rows for one (shot, stage) -- NOT the whole
    shot -- and refresh wave_manifest for every resulting file (see
    manifest.py), so that stage is immediately servable via
    fetch_wave.py/api_server.py. Returns (files_written,
    manifest_rows_upserted).

    Scoping to `stage` (rather than compacting the whole shot every time)
    matters once ingestion happens incrementally, one diagnostic group at
    a time, the way it actually does here: without it, ingesting a SINGLE
    stage would still re-read and re-write every OTHER already-compacted
    stage's data for that shot, on every call -- wasted work that grows
    with how many times you've incrementally ingested into the same shot,
    and it would mean compaction can't complete until every stage has
    been ingested, defeating the incremental part entirely."""
    paths = compact_manifest_friendly(
        con, shot, rows_per_row_group, target_file_size_bytes, stage=stage
    )
    manifest_rows = 0
    for p in paths:
        manifest_rows += refresh_manifest_for_file(Path(p))
    return len(paths), manifest_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shot", type=int, default=1, help="single shot id to ingest")
    parser.add_argument(
        "--stage",
        choices=sorted(DIAGNOSTICS),
        default=None,
        help="single diagnostic group to ingest (default: all of them)",
    )
    parser.add_argument("--shots", type=int, default=0, help="ingest shots 1..N instead of --shot")
    parser.add_argument("--experiment", default=DEFAULT_EXPERIMENT, help="experimental campaign id")
    parser.add_argument(
        "--no-compact",
        action="store_true",
        help="skip compaction + manifest refresh; leave raw per-wave files as the only output",
    )
    parser.add_argument(
        "--rows-per-row-group",
        type=int,
        default=20_000,
        help="passed through to compaction -- tune to your sample rate and "
        "expected query window (see README)",
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="passed through to compaction -- roll to a new file past this size",
    )
    args = parser.parse_args()

    con = config.get_connection()
    ensure_table(con)

    shots = list(range(1, args.shots + 1)) if args.shots > 0 else [args.shot]
    stages = [args.stage] if args.stage else sorted(DIAGNOSTICS)

    total_files = 0
    total_compacted_files = 0
    total_manifest_rows = 0
    t0 = time.time()
    for shot in shots:
        for stage in stages:
            write_stage_files(shot, stage, args.experiment)
            n = register_experiment(con, shot, stage, args.experiment)
            total_files += n
            print(
                f"  experiment={args.experiment} shot={shot} stage={stage}: "
                f"registered {n} wave files"
            )

            if not args.no_compact:
                n_compacted, n_manifest_rows = compact_and_refresh_manifest(
                    con, shot, stage, args.rows_per_row_group, args.target_file_size_bytes
                )
                total_compacted_files += n_compacted
                total_manifest_rows += n_manifest_rows
                print(
                    f"    shot={shot} stage={stage}: compacted into {n_compacted} file(s), "
                    f"wave_manifest ready ({n_manifest_rows} row-group entries)"
                )

    elapsed = time.time() - t0
    print(
        f"\nIngested {total_files} wave files across {len(shots)} shot(s) x "
        f"{len(stages)} stage(s) in {elapsed:.1f}s"
    )
    if not args.no_compact:
        print(
            f"Compacted into {total_compacted_files} file(s); wave_manifest has "
            f"{total_manifest_rows} new/updated row-group entries -- ready for "
            f"fetch_wave.py / api_server.py"
        )
    else:
        print(
            "--no-compact set: raw files only, run `ducklake-compact` + "
            "`ducklake-manifest` manually"
        )
    row = con.execute(f"SELECT count(*) FROM {config.TABLE_NAME}").fetchone()
    assert row is not None  # SELECT count(*) always returns exactly one row
    print(f"Rows now in {config.TABLE_NAME}: {row[0]}")


if __name__ == "__main__":
    main()

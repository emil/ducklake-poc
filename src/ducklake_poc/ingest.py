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
    python -m ducklake_poc.ingest --shot 1 --enqueue           # write + enqueue for ingest_worker

--enqueue is the queue-driven path (see ingest_queue.py): this process
compacts each (experiment, shot, stage, data_version) LOCALLY, with plain
DuckDB (compact_stage_locally), straight into its final files under
data/lake/, then enqueues them -- one queue row per version. It never
opens a DuckLake connection; the single `ducklake-ingest-worker` only
registers the already-final files and indexes them into wave_manifest.
Since a (stage, data_version) is ingested exactly once, those files are
never rewritten afterwards. That's what lets many producers run at once
without contending on the DuckLake catalog.
"""

from __future__ import annotations

import argparse
import contextlib
import glob
import tempfile
import time
from pathlib import Path

import duckdb
import psycopg2

from . import config
from . import ingest_queue as iq
from .compact import compact_manifest_friendly, stream_compact_to_files
from .manifest import prune_manifest_scope, refresh_manifest_for_file
from .parquet_encoding import open_parquet_writer
from .synthetic import DEFAULT_EXPERIMENT, DIAGNOSTICS, generate_stage, wave_to_sample_table


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create+configure the table on first use only. The `SET SORTED BY`/
    `SET PARTITIONED BY` calls below are each their own schema-altering
    DuckLake transaction -- re-issuing them (even to the same, already-
    current value) is NOT a no-op as far as DuckLake's optimistic
    concurrency is concerned, and reliably conflicts with any other
    transaction concurrently committing to this table (confirmed against
    a real DuckLake/Postgres catalog: two `ducklake-ingest` processes
    started together both fail here almost every time). Since every
    ingest run calls this unconditionally, checking whether the table
    already exists first -- and returning immediately if so -- is what
    keeps steady-state concurrent ingestion from re-triggering this
    contention on every single call, not just avoiding an error via
    retry_on_conflict (still needed for the one real race, at cold start,
    when several first-ever clients all see "doesn't exist yet")."""

    def _create_if_missing() -> None:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [config.TABLE_NAME],
        ).fetchone()
        assert exists is not None  # SELECT count(*) always returns exactly one row
        if exists[0] > 0:
            return

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
        # data_version) -- scientists periodically recalibrate a
        # diagnostic and rerun the pipeline, producing a new, independent
        # data_version of every channel in that stage; old versions are
        # kept, not overwritten, so the same (shot, stage, wave) can have
        # several complete copies. x on the end keeps each (wave,
        # data_version)'s own samples in time order, which is what makes
        # row-group min/max on x tight enough for narrow time-window
        # queries to prune down to a couple of row groups instead of
        # scanning the whole wave.
        con.execute(
            f"ALTER TABLE {config.TABLE_NAME} SET SORTED BY "
            f"(experiment ASC, shot ASC, stage ASC, wave ASC, data_version ASC, x ASC)"
        )
        # Logical (catalog-level) partitioning for pruning benefit -- and,
        # crucially, this now also matches the PHYSICAL layout (see
        # write_stage_files below): experiment/shot/stage are
        # hive-partition keys encoded only in the directory path, never
        # duplicated as physical columns inside the files themselves.
        # That combination is what ducklake_add_data_files actually needs
        # to register partitioned files correctly -- see the module
        # docstring and the README's "Physical layout & partitioning"
        # section for why an earlier version of this (partition columns
        # ALSO present in the file's own data) failed with "invalid
        # partition value for the table configuration".
        con.execute(f"ALTER TABLE {config.TABLE_NAME} SET PARTITIONED BY (experiment, shot, stage)")

    config.retry_on_conflict(_create_if_missing)


def write_stage_files(
    shot: int, stage: str, experiment: str = DEFAULT_EXPERIMENT, root: Path | None = None
) -> list[Path]:
    """Write one small parquet file per (channel, data_version) that `stage`
    produced -- current production shape (one file per wave version), in
    genuine hive-style directories (see module docstring) under `root`
    (default RAW_DATA_DIR)."""
    out_dir = (
        (root or config.RAW_DATA_DIR)
        / f"experiment={experiment}"
        / f"shot={shot}"
        / f"stage={stage}"
    )
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
    register_files(con, files)
    return len(files)


def register_files(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Register `files` (absolute paths) with DuckLake in ONE transaction
    -- all or nothing. Shared by register_experiment above (direct
    ingest) and ingest_worker.py (queue-driven ingest)."""

    def _register() -> None:
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                f"CALL ducklake_add_data_files('{config.DUCKLAKE_NAME}', '{config.TABLE_NAME}', ?)",
                [files],
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
            raise

    config.retry_on_conflict(_register)


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
    been ingested, defeating the incremental part entirely.

    Because this path compacts INSIDE DuckLake, re-ingesting into an
    already-compacted (shot, stage) rewrites its earlier files -- so this
    finally prunes wave_manifest rows in the scope that still point at the
    replaced files (see manifest.prune_manifest_scope). The queue path
    (--enqueue) avoids rewrites altogether by compacting before
    registration."""
    paths = compact_manifest_friendly(
        con, shot, rows_per_row_group, target_file_size_bytes, stage=stage
    )
    manifest_rows = 0
    for p in paths:
        manifest_rows += refresh_manifest_for_file(Path(p))
    # Only after every new file is indexed, so a lookup never finds the
    # scope empty -- at worst it briefly sees old and new copies together.
    prune_manifest_scope([Path(p) for p in paths], shot, stage)
    return len(paths), manifest_rows


def compact_stage_locally(
    shot: int,
    stage: str,
    experiment: str,
    rows_per_row_group: int,
    target_file_size_bytes: int,
) -> dict[str, list[Path]]:
    """Producer-side compaction for --enqueue: generate `stage`'s per-wave
    files into a throwaway staging directory, then compact each
    data_version separately -- plain DuckDB, no DuckLake attach -- into
    its FINAL files under LAKE_DATA_DIR, using the same
    stream_compact_to_files as compact.py (so the same row-group/file
    boundary guarantees hold). Returns {data_version: [final file paths]}.

    One call per data_version, not per stage: the version is the unit that
    gets enqueued and registered atomically, and never rewritten -- a later
    recalibration rerun is a new version with its own files."""
    with tempfile.TemporaryDirectory(prefix="ducklake-stage-") as staging:
        staged = write_stage_files(shot, stage, experiment, root=Path(staging))
        if not staged:
            return {}
        con = duckdb.connect()
        try:
            con.read_parquet([str(p) for p in staged], hive_partitioning=True).create_view("staged")
            versions = [
                r[0]
                for r in con.execute(
                    "SELECT DISTINCT data_version FROM staged ORDER BY 1"
                ).fetchall()
            ]
            return {
                version: [
                    Path(p)
                    for p in stream_compact_to_files(
                        con,
                        """
                        SELECT * FROM staged WHERE data_version = ?
                        ORDER BY experiment, stage, wave, data_version, x
                        """,
                        config.LAKE_DATA_DIR,
                        rows_per_row_group=rows_per_row_group,
                        target_file_size_bytes=target_file_size_bytes,
                        source_params=[version],
                    )
                ]
                for version in versions
            }
        finally:
            con.close()


def enqueue_shots(
    shots: list[int],
    stages: list[str],
    experiment: str,
    rows_per_row_group: int,
    target_file_size_bytes: int,
) -> int:
    """--enqueue mode: for each (shot, stage), compact every data_version
    locally into its final files, then enqueue each version as ONE queue
    row. A version that's already queued/ingested isn't enqueued again --
    the files just written for it are deleted instead, since nothing will
    ever register them. Returns the number of versions newly queued."""
    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        iq.ensure_queue_table(pg_conn)
        total = 0
        for shot in shots:
            for stage in stages:
                by_version = compact_stage_locally(
                    shot, stage, experiment, rows_per_row_group, target_file_size_bytes
                )
                for version, paths in by_version.items():
                    scope = (
                        f"experiment={experiment} shot={shot} stage={stage} data_version={version}"
                    )
                    if iq.enqueue_version(pg_conn, paths, experiment, shot, stage, version):
                        total += 1
                        print(f"  {scope}: enqueued {len(paths)} compacted file(s)")
                    else:
                        for p in paths:
                            p.unlink(missing_ok=True)
                        print(f"  {scope}: already ingested -- discarded this run's files")
        return total
    finally:
        pg_conn.close()


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
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="compact each data_version locally into its final files and enqueue them for "
        "ducklake-ingest-worker, instead of registering/compacting in this process",
    )
    args = parser.parse_args()
    if args.enqueue and args.no_compact:
        parser.error("--enqueue always compacts before enqueueing; drop --no-compact")

    shots = list(range(1, args.shots + 1)) if args.shots > 0 else [args.shot]
    stages = [args.stage] if args.stage else sorted(DIAGNOSTICS)

    if args.enqueue:
        t0 = time.time()
        n = enqueue_shots(
            shots,
            stages,
            args.experiment,
            args.rows_per_row_group,
            args.target_file_size_bytes,
        )
        print(
            f"\nEnqueued {n} data_version(s) across {len(shots)} shot(s) x {len(stages)} "
            f"stage(s) in {time.time() - t0:.1f}s -- run `ducklake-ingest-worker` "
            "to register them"
        )
        return

    con = config.get_connection()
    ensure_table(con)

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

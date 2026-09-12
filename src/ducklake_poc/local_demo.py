"""End-to-end LOCAL demo of the per-sample-row design -- no DuckLake/Postgres
catalog attach required (just plain DuckDB reading local files), so this can
run anywhere to validate the row-group/file-splitting and narrow-window-query
behavior before wiring it through the real DuckLake pipeline.

What it does:
  1. Generates a realistic slice of a shot: several small diagnostic groups
     (rogowski, flux_loops, diamagnetic_loop, thomson) plus mirnov -- whose
     probe_01 channel is forced to 2,000,000 samples across TWO explicit
     data_versions (git-hash-style ids, standing in for a recalibration +
     pipeline rerun). mirnov/probe_01 also stands in for the real physical
     reason Mirnov coils (high-frequency MHD pickup coils) are the
     diagnostic most likely to produce a genuinely huge wave.
  2. Compacts them via the same `stream_compact_to_files` helper compact.py
     uses for the real DuckLake path, sorted by (stage, wave, data_version,
     x), with a deliberately small target file size so the big wave
     visibly splits across multiple files -- with `stage` and
     `data_version` enforced as hard boundaries at the FILE level, not
     just the row-group level, and physical files landing in genuine
     hive-style directories under
     data/lake/local_demo/experiment=<experiment>/shot=<shot>/stage=<stage>/.
  3. Builds the wave_manifest entries for the compacted output (same
     manifest.py used elsewhere).
  4. Runs a narrow x-window fetch against mirnov/probe_01, one specific
     data_version, and reports how many of its row groups were touched vs.
     how many exist in total -- this is the number that should stay small
     and roughly constant even if the wave were 10x bigger.
  5. Confirms isolation: no file mixes two different stages or
     data_versions, and the two mirnov/probe_01 versions never share a
     row group or file.

Usage:
    python -m ducklake_poc.local_demo
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from . import config, manifest
from .compact import stream_compact_to_files
from .parquet_encoding import open_parquet_writer
from .synthetic import (
    DEFAULT_EXPERIMENT,
    DURATION_MS,
    generate_big_wave,
    generate_stage,
    wave_to_sample_table,
)

DEMO_EXPERIMENT = DEFAULT_EXPERIMENT
DEMO_SHOT = 999
SMALL_STAGES = ["rogowski", "flux_loops", "diamagnetic_loop", "thomson"]
BIG_STAGE = "mirnov"
BIG_WAVE_NAME = "mirnov/probe_01"
BIG_WAVE_SAMPLES = 2_000_000
BIG_WAVE_VERSIONS = ["a1b2c3d", "e4f5a6b"]  # explicit recalibration rerun for this channel
ROWS_PER_ROW_GROUP = 20_000
TARGET_FILE_SIZE_BYTES = 8 * 1024 * 1024  # small on purpose, to force a split

RAW_DIR = Path("/tmp/ducklake_local_demo_raw")
# Must live under config.LAKE_DATA_DIR (i.e. inside ./data/lake) so that
# both manifest.file_url_for() and nginx's static serving of ./data work
# unmodified -- this is the same directory the real DuckLake path writes
# compacted output into, just in its own subfolder.
LAKE_DIR = config.LAKE_DATA_DIR / "local_demo"


def _write_wave_file(table, path) -> None:
    writer = open_parquet_writer(str(path), table.schema)
    writer.write_table(table)
    writer.close()


def _raw_path(stage: str, wave: str, data_version: str) -> Path:
    """Genuine hive-style directory, matching what compact.py/ingest.py
    now use for the same reason (see synthetic.py's wave_to_sample_table
    and manifest.py's module docstring): ducklake_add_data_files needs
    partition columns absent from the file's own data, with values
    encoded only in the path."""
    d = RAW_DIR / f"experiment={DEMO_EXPERIMENT}" / f"shot={DEMO_SHOT}" / f"stage={stage}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{wave.replace('/', '_')}__{data_version}.parquet"


def generate_raw_files() -> None:
    if RAW_DIR.exists():
        shutil.rmtree(RAW_DIR)
    RAW_DIR.mkdir(parents=True)

    n_files = 0
    for stage in SMALL_STAGES:
        for rec in generate_stage(shot=DEMO_SHOT, stage=stage, experiment=DEMO_EXPERIMENT):
            _write_wave_file(
                wave_to_sample_table(rec), _raw_path(stage, rec.wave, rec.data_version)
            )
            n_files += 1

    # mirnov: every channel except probe_01 at its normal (small) profile
    # and natural data_version count, probe_01 forced to BIG_WAVE_SAMPLES
    # across two explicit data_versions so the demo is deterministic
    # rather than depending on STAGE_PROFILE's random big-wave draw.
    for rec in generate_stage(shot=DEMO_SHOT, stage=BIG_STAGE, experiment=DEMO_EXPERIMENT):
        if rec.wave == BIG_WAVE_NAME:
            continue
        _write_wave_file(
            wave_to_sample_table(rec), _raw_path(BIG_STAGE, rec.wave, rec.data_version)
        )
        n_files += 1

    for version in BIG_WAVE_VERSIONS:
        big = generate_big_wave(
            DEMO_SHOT,
            BIG_STAGE,
            BIG_WAVE_NAME,
            BIG_WAVE_SAMPLES,
            data_version=version,
            experiment=DEMO_EXPERIMENT,
        )
        _write_wave_file(wave_to_sample_table(big), _raw_path(BIG_STAGE, BIG_WAVE_NAME, version))
        n_files += 1

    print(
        f"generated {n_files} raw wave files across {len(SMALL_STAGES) + 1} diagnostic "
        f"groups (including {BIG_WAVE_NAME} with {BIG_WAVE_SAMPLES:,} samples across "
        f"data_versions {BIG_WAVE_VERSIONS}) in {RAW_DIR}"
    )


def compact_locally() -> list[str]:
    if LAKE_DIR.exists():
        shutil.rmtree(LAKE_DIR)
    LAKE_DIR.mkdir(parents=True)

    con = duckdb.connect()
    # hive_partitioning is auto-detected by DuckDB for key=value directory
    # trees (verified empirically), but it's passed explicitly here so
    # this is a documented decision, not an accident of the default.
    source_sql = f"""
        SELECT * FROM read_parquet('{RAW_DIR}/**/*.parquet', hive_partitioning=true)
        ORDER BY experiment, shot, stage, wave, data_version, x
    """
    paths = stream_compact_to_files(
        con,
        source_sql,
        LAKE_DIR,
        rows_per_row_group=ROWS_PER_ROW_GROUP,
        target_file_size_bytes=TARGET_FILE_SIZE_BYTES,
    )
    total_size = sum(Path(p).stat().st_size for p in paths)
    print(f"compacted into {len(paths)} file(s), {total_size / 1e6:.1f}MB total:")
    for p in paths:
        print(f"    {p}  ({Path(p).stat().st_size / 1e6:.1f}MB)")
    return paths


def build_manifest(paths: list[str]) -> None:
    con = duckdb.connect()
    import psycopg2

    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    with pg_conn.cursor() as cur:
        cur.execute(manifest.CREATE_MANIFEST_SQL)
        cur.execute("DELETE FROM wave_manifest WHERE shot = %s", (DEMO_SHOT,))
    pg_conn.commit()

    total_rows = 0
    for p in paths:
        rows = manifest.build_manifest_rows(con, Path(p))
        manifest.upsert_manifest_rows(pg_conn, rows)
        total_rows += len(rows)
    pg_conn.close()
    print(f"manifest: {total_rows} row-group entries across {len(paths)} file(s)")


def check_no_partition_mixing(paths: list[str]) -> None:
    """Regression check: with multiple diagnostic groups AND multiple
    calibration reruns now compacted together, verify no file ever spans
    two different data_versions (the only one of the three partition
    keys still physically present in the file's own data -- stage and
    experiment are now hive-path-only, so a file spanning two stages is
    structurally impossible: it would require the SAME open ParquetWriter
    to have been created under two different out_dir values, which
    compact.py's own logic never does -- see the file-level partition
    boundary there), and that every file's path is a well-formed
    experiment=/shot=/stage= hive path."""
    con = duckdb.connect()
    for p in paths:
        row = con.execute(
            "SELECT count(DISTINCT stats_min_value) FROM parquet_metadata(?) "
            "WHERE path_in_schema = 'data_version'",
            [p],
        ).fetchone()
        assert row is not None
        if row[0] > 1:
            raise AssertionError(f"{p} spans multiple data_versions")

        rel_parts = Path(p).relative_to(LAKE_DIR).parts
        hive_keys = {part.split("=", 1)[0] for part in rel_parts[:-1] if "=" in part}
        if hive_keys != {"experiment", "shot", "stage"}:
            raise AssertionError(
                f"{p} doesn't have a well-formed experiment=/shot=/stage= path: {rel_parts}"
            )
    print(
        f"partition-isolation check: no file spans multiple data_versions, and every "
        f"file has a well-formed experiment=/shot=/stage= path, across {len(paths)} file(s)"
    )


def query_narrow_window() -> None:
    import psycopg2

    # A literal 1ms window in the middle of the 500ms shot -- now that x is
    # genuine milliseconds (not a 0..1 normalized fraction), this is exactly
    # the "1ms out of a multi-million-sample wave" case from the original
    # design discussion, with no unit workaround needed. Scoped to one
    # specific data_version -- see check below for why that matters.
    x_min, x_max = 250.0, 251.0
    window_ms = x_max - x_min
    target_version = BIG_WAVE_VERSIONS[0]

    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    with conn.cursor() as cur:
        cur.execute(
            """SELECT count(*) FROM wave_manifest
               WHERE experiment=%s AND shot=%s AND stage=%s AND data_version=%s
               AND wave_min<=%s AND wave_max>=%s""",
            (DEMO_EXPERIMENT, DEMO_SHOT, BIG_STAGE, target_version, BIG_WAVE_NAME, BIG_WAVE_NAME),
        )
        total_row_groups = cur.fetchone()[0]

        cur.execute(
            """SELECT count(*), sum(byte_length) FROM wave_manifest
               WHERE experiment=%s AND shot=%s AND stage=%s AND data_version=%s
               AND wave_min<=%s AND wave_max>=%s AND x_max >= %s AND x_min <= %s""",
            (
                DEMO_EXPERIMENT,
                DEMO_SHOT,
                BIG_STAGE,
                target_version,
                BIG_WAVE_NAME,
                BIG_WAVE_NAME,
                x_min,
                x_max,
            ),
        )
        touched_row_groups, touched_bytes = cur.fetchone()

        # Isolation check: the two data_versions of the same wave must
        # never share a row group OR a file -- their (file_path) sets
        # should be completely disjoint (stronger than before: previously
        # only row-group-level disjointness was guaranteed).
        cur.execute(
            """SELECT DISTINCT file_path FROM wave_manifest
               WHERE experiment=%s AND shot=%s AND stage=%s
                 AND wave_min<=%s AND wave_max>=%s AND data_version=%s""",
            (
                DEMO_EXPERIMENT,
                DEMO_SHOT,
                BIG_STAGE,
                BIG_WAVE_NAME,
                BIG_WAVE_NAME,
                BIG_WAVE_VERSIONS[0],
            ),
        )
        v1_files = {r[0] for r in cur.fetchall()}
        cur.execute(
            """SELECT DISTINCT file_path FROM wave_manifest
               WHERE experiment=%s AND shot=%s AND stage=%s
                 AND wave_min<=%s AND wave_max>=%s AND data_version=%s""",
            (
                DEMO_EXPERIMENT,
                DEMO_SHOT,
                BIG_STAGE,
                BIG_WAVE_NAME,
                BIG_WAVE_NAME,
                BIG_WAVE_VERSIONS[1],
            ),
        )
        v2_files = {r[0] for r in cur.fetchall()}
    conn.close()

    overlap = v1_files & v2_files
    if overlap:
        raise AssertionError(
            f"data_version={BIG_WAVE_VERSIONS[0]} and {BIG_WAVE_VERSIONS[1]} "
            f"share file(s): {overlap}"
        )

    print(
        f"\nnarrow-window query: wave={BIG_WAVE_NAME}, data_version={target_version}, "
        f"x in [{x_min:.3f}, {x_max:.3f}] ms"
    )
    print(f"  total row groups for this wave version : {total_row_groups}")
    print(f"  row groups touched by the query         : {touched_row_groups}")
    print(f"  bytes touched                           : {touched_bytes:,}")
    print(
        f"  => reading {100 * touched_row_groups / total_row_groups:.2f}% of the wave's "
        f"row groups to answer a {window_ms:.0f}ms query out of its {DURATION_MS:.0f}ms duration"
    )
    print(
        f"\ndata_version isolation: {BIG_WAVE_VERSIONS[0]} lives in {len(v1_files)} file(s), "
        f"{BIG_WAVE_VERSIONS[1]} lives in {len(v2_files)} file(s), 0 shared -- confirmed disjoint"
    )


def main() -> None:
    generate_raw_files()
    paths = compact_locally()
    check_no_partition_mixing(paths)
    build_manifest(paths)
    query_narrow_window()


if __name__ == "__main__":
    main()

"""End-to-end test of the full local pipeline: generate -> compact ->
manifest -> narrow-window query -> real HTTP fetch. This is the same
sequence local_demo.py runs manually; here it's asserted automatically.
"""

from __future__ import annotations

import shutil

import psycopg2
import pytest

from ducklake_poc import config, fetch_wave, local_demo

pytestmark = pytest.mark.integration


@pytest.fixture
def demo_pipeline(postgres_available: bool, nginx_available: bool, clean_manifest_table):
    if not postgres_available:
        pytest.skip("Postgres not reachable")
    if not nginx_available:
        pytest.skip("nginx not reachable")

    if local_demo.LAKE_DIR.exists():
        shutil.rmtree(local_demo.LAKE_DIR)
    if local_demo.RAW_DIR.exists():
        shutil.rmtree(local_demo.RAW_DIR)

    local_demo.generate_raw_files()
    paths = local_demo.compact_locally()
    yield paths

    shutil.rmtree(local_demo.LAKE_DIR, ignore_errors=True)
    shutil.rmtree(local_demo.RAW_DIR, ignore_errors=True)


def test_no_file_mixes_stage_or_data_version(demo_pipeline) -> None:
    local_demo.check_no_partition_mixing(demo_pipeline)  # raises AssertionError on failure


def test_row_count_preserved_through_compaction(demo_pipeline) -> None:
    import duckdb

    con = duckdb.connect()
    total = 0
    for p in demo_pipeline:
        row = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()
        assert row is not None
        total += row[0]

    # 4 small stages x 2 channels x 3000 samples
    # + rogowski/flux_loops/diamagnetic_loop/thomson-specific channel counts
    # -- rather than hardcode the exact number (which depends on
    # STAGE_PROFILE), cross-check against a fresh raw generation instead.
    raw_row = con.execute(
        f"SELECT count(*) FROM read_parquet('{local_demo.RAW_DIR}/**/*.parquet')"
    ).fetchone()
    assert raw_row is not None
    assert total == raw_row[0]


def test_manifest_round_trip_and_narrow_window_pruning(demo_pipeline) -> None:
    local_demo.build_manifest(demo_pipeline)

    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*) FROM wave_manifest
                   WHERE shot=%s AND stage=%s AND data_version=%s
                   AND wave_min<=%s AND wave_max>=%s""",
                (
                    local_demo.DEMO_SHOT,
                    local_demo.BIG_STAGE,
                    local_demo.BIG_WAVE_VERSIONS[0],
                    local_demo.BIG_WAVE_NAME,
                    local_demo.BIG_WAVE_NAME,
                ),
            )
            total_row_groups = cur.fetchone()[0]

            cur.execute(
                """SELECT count(*) FROM wave_manifest
                   WHERE shot=%s AND stage=%s AND data_version=%s
                   AND wave_min<=%s AND wave_max>=%s
                   AND x_max >= %s AND x_min <= %s""",
                (
                    local_demo.DEMO_SHOT,
                    local_demo.BIG_STAGE,
                    local_demo.BIG_WAVE_VERSIONS[0],
                    local_demo.BIG_WAVE_NAME,
                    local_demo.BIG_WAVE_NAME,
                    250.0,
                    251.0,
                ),
            )
            touched = cur.fetchone()[0]
    finally:
        conn.close()

    # 2,000,000 samples / 20,000 per row group = 100 total row groups
    assert total_row_groups == 100
    # a 1ms window at 4MHz should touch a small, roughly-constant number
    # of row groups, not scale with the wave's total size
    assert 1 <= touched <= 3


def test_data_versions_never_share_a_file(demo_pipeline) -> None:
    local_demo.build_manifest(demo_pipeline)

    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with conn.cursor() as cur:
            files_by_version = {}
            for version in local_demo.BIG_WAVE_VERSIONS:
                cur.execute(
                    """SELECT DISTINCT file_path FROM wave_manifest
                       WHERE shot=%s AND stage=%s AND wave_min<=%s AND wave_max>=%s
                       AND data_version=%s""",
                    (
                        local_demo.DEMO_SHOT,
                        local_demo.BIG_STAGE,
                        local_demo.BIG_WAVE_NAME,
                        local_demo.BIG_WAVE_NAME,
                        version,
                    ),
                )
                files_by_version[version] = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()

    v1_files, v2_files = files_by_version.values()
    assert v1_files, "expected at least one file for the first data_version"
    assert v2_files, "expected at least one file for the second data_version"
    assert v1_files.isdisjoint(v2_files)


def test_fetch_wave_end_to_end_over_http(demo_pipeline) -> None:
    local_demo.build_manifest(demo_pipeline)

    rows = fetch_wave.lookup(
        experiment=local_demo.DEMO_EXPERIMENT,
        shot=local_demo.DEMO_SHOT,
        stage=local_demo.BIG_STAGE,
        wave=local_demo.BIG_WAVE_NAME,
        data_version=local_demo.BIG_WAVE_VERSIONS[0],
        x_min=250.0,
        x_max=251.0,
    )
    assert len(rows) <= 3

    import pyarrow as pa
    import pyarrow.parquet as pq

    from ducklake_poc.range_http_file import RangeHTTPFile

    tables = []
    for row in rows:
        file_url, row_group_id = row[1], row[2]
        f = RangeHTTPFile(file_url)
        pf = pq.ParquetFile(f, pre_buffer=False)
        tables.append(pf.read_row_group(row_group_id))

    combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    x = combined["x"].to_numpy()
    mask = (
        (x >= 250.0)
        & (x <= 251.0)
        & (combined["wave"].to_numpy(zero_copy_only=False) == local_demo.BIG_WAVE_NAME)
    )
    filtered = combined.filter(pa.array(mask))

    # 4MHz over a 1ms window = 4000 samples
    assert filtered.num_rows == 4000

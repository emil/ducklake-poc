"""Build/refresh the `wave_manifest` helper table in Postgres.

This is deliberately NOT part of DuckLake's own catalog tables -- it's our
own small lookup table, living alongside the DuckLake catalog schema in the
same Postgres database (could just as easily be a different database).
Its job is narrow: given (experiment, shot, stage, wave, data_version),
tell a caller exactly which file, which row group, and which byte range
holds that wave's data, so a plain HTTP client (curl, or anything else)
can fetch it directly from nginx without going through DuckDB/DuckLake at
all.

`experiment`/`shot`/`stage` are hive-partition keys encoded only in the
directory path (see synthetic.py's `wave_to_sample_table` and compact.py's
PHYSICAL_COLUMNS for why they're deliberately not duplicated as physical
columns inside the files) -- so they're parsed from the file's path here,
not read via parquet_metadata() like wave/data_version/x are. The footer
is still the ground truth for everything that IS physically in the file;
the path is the ground truth for everything that isn't.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import psycopg2
import psycopg2.extras

from . import config

CREATE_MANIFEST_SQL = """
CREATE TABLE IF NOT EXISTS wave_manifest (
    experiment      TEXT NOT NULL,
    shot            INTEGER NOT NULL,
    stage           TEXT NOT NULL,
    wave_min        TEXT NOT NULL,
    wave_max        TEXT NOT NULL,
    data_version    TEXT NOT NULL,
    x_min           DOUBLE PRECISION NOT NULL,
    x_max           DOUBLE PRECISION NOT NULL,
    file_path       TEXT NOT NULL,  -- relative to LAKE_DATA_DIR (see relative_to_lake_dir)
    file_url        TEXT NOT NULL,
    row_group_id    INTEGER NOT NULL,
    row_group_rows  INTEGER NOT NULL,
    byte_start      BIGINT NOT NULL,
    byte_length     BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (file_path, row_group_id)
);
CREATE INDEX IF NOT EXISTS wave_manifest_lookup
    ON wave_manifest (experiment, shot, stage, data_version, wave_min, wave_max, x_min, x_max);
"""

# One row per row group. Only wave/data_version/x are physically present
# in the file (see PHYSICAL_COLUMNS in compact.py), so only those come
# from parquet_metadata() -- experiment/shot/stage come from the file's
# path instead (see config.parse_hive_partitions). compact.py guarantees
# an entire FILE never spans two different data_versions, so
# data_version is always single-valued here; wave_min/wave_max are equal
# (a single channel) except where several complete small waves from the
# same stage+data_version got packed into one row group, in which case
# they span a (lexicographic) range and the caller filters on overlap
# rather than equality. x_min/x_max are what make narrow time-window
# lookups within a big wave possible.
ROW_GROUP_METADATA_SQL = """
SELECT
    row_group_id,
    any_value(row_group_num_rows) AS row_group_num_rows,
    min(CASE WHEN path_in_schema = 'wave'
        THEN CAST(stats_min_value AS VARCHAR) END) AS wave_min,
    max(CASE WHEN path_in_schema = 'wave'
        THEN CAST(stats_max_value AS VARCHAR) END) AS wave_max,
    min(CASE WHEN path_in_schema = 'data_version'
        THEN CAST(stats_min_value AS VARCHAR) END) AS data_version_min,
    max(CASE WHEN path_in_schema = 'data_version'
        THEN CAST(stats_max_value AS VARCHAR) END) AS data_version_max,
    min(CASE WHEN path_in_schema = 'x'
        THEN CAST(stats_min_value AS DOUBLE) END) AS x_min,
    max(CASE WHEN path_in_schema = 'x'
        THEN CAST(stats_max_value AS DOUBLE) END) AS x_max,
    min(COALESCE(dictionary_page_offset, data_page_offset)) AS byte_start,
    max(row_group_compressed_bytes) AS byte_length
FROM parquet_metadata(?)
GROUP BY row_group_id
ORDER BY row_group_id
"""


def relative_to_lake_dir(path: Path) -> Path:
    """Path relative to LAKE_DATA_DIR -- this, not the absolute path, is
    what gets stored in the manifest and consumed by api_server.py, so
    that a row group looked up by a process on one machine (e.g. the
    dockerized api service) doesn't depend on agreeing with whichever
    machine ran ingestion/compaction about absolute filesystem layout."""
    return path.resolve().relative_to(config.LAKE_DATA_DIR.resolve())


def clean_relative_path(rel_path: Path) -> str:
    """A LAKE_DATA_DIR-relative path with hive `key=value` directory
    segments stripped down to just `value` -- used to build the
    HTTP-facing URLs, which deliberately don't expose the on-disk
    hive-partition directory naming. nginx's rewrite rules (see
    nginx/nginx.conf) translate these clean paths back to the real hive
    paths when actually serving files. Takes an already-relative path
    (see relative_to_lake_dir) rather than resolving against a local
    LAKE_DATA_DIR itself, since the caller (e.g. api_server.py) may be
    running on a different machine/container than whatever produced the
    manifest row."""
    clean_parts = [p.split("=", 1)[1] if "=" in p else p for p in rel_path.parts]
    return "/".join(clean_parts)


def file_url_for(rel_path: Path) -> str:
    return f"{config.NGINX_BASE_URL}/lake/{clean_relative_path(rel_path)}"


def build_manifest_rows(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[tuple]:
    rel_path = relative_to_lake_dir(parquet_path)
    partitions = config.parse_hive_partitions(rel_path)
    missing = {"experiment", "shot", "stage"} - partitions.keys()
    if missing:
        raise ValueError(
            f"{parquet_path} is missing hive-partition directory segment(s) {sorted(missing)} "
            f"-- expected .../experiment=X/shot=Y/stage=Z/<file>.parquet"
        )
    experiment = partitions["experiment"]
    shot = int(partitions["shot"])
    stage = partitions["stage"]

    rows = con.execute(ROW_GROUP_METADATA_SQL, [str(parquet_path)]).fetchall()
    url = file_url_for(rel_path)
    manifest_rows = []
    for (
        row_group_id,
        num_rows,
        wave_min,
        wave_max,
        data_version_min,
        data_version_max,
        x_min,
        x_max,
        byte_start,
        byte_length,
    ) in rows:
        if data_version_min != data_version_max:
            raise ValueError(
                f"row group {row_group_id} in {parquet_path} spans multiple "
                f"data_versions -- manifest assumes compaction stayed within one "
                f"data_version partition"
            )
        manifest_rows.append(
            (
                experiment,
                shot,
                stage,
                wave_min,
                wave_max,
                data_version_min,
                x_min,
                x_max,
                str(rel_path),
                url,
                row_group_id,
                num_rows,
                byte_start,
                byte_length,
            )
        )
    return manifest_rows


def upsert_manifest_rows(pg_conn, rows: list[tuple]) -> None:
    if not rows:
        return
    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO wave_manifest
                (experiment, shot, stage, wave_min, wave_max, data_version, x_min, x_max,
                 file_path, file_url, row_group_id, row_group_rows, byte_start, byte_length)
            VALUES %s
            ON CONFLICT (file_path, row_group_id) DO UPDATE SET
                wave_min = EXCLUDED.wave_min,
                wave_max = EXCLUDED.wave_max,
                data_version = EXCLUDED.data_version,
                x_min = EXCLUDED.x_min,
                x_max = EXCLUDED.x_max,
                byte_start = EXCLUDED.byte_start,
                byte_length = EXCLUDED.byte_length,
                updated_at = now()
            """,
            rows,
        )
    pg_conn.commit()


def index_files(cur, parquet_paths: list[Path]) -> int:
    """Insert wave_manifest rows for newly registered, never-to-be-rewritten
    files (the queue path: ingest_worker.py) using the CALLER's cursor and
    transaction -- no commit here, so the worker can index a version and
    mark it completed atomically. ON CONFLICT DO NOTHING rather than an
    upsert: a file's row groups never change once written, so a row that's
    already there (a retry after a crash) is already correct. Returns how
    many rows were built."""
    con = duckdb.connect()
    try:
        rows = [row for p in parquet_paths for row in build_manifest_rows(con, p)]
    finally:
        con.close()
    if rows:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO wave_manifest
                (experiment, shot, stage, wave_min, wave_max, data_version, x_min, x_max,
                 file_path, file_url, row_group_id, row_group_rows, byte_start, byte_length)
            VALUES %s
            ON CONFLICT (file_path, row_group_id) DO NOTHING
            """,
            rows,
        )
    return len(rows)


def prune_manifest_scope(
    keep_paths: list[Path],
    shot: int,
    stage: str | None = None,
    experiment: str | None = None,
    data_version: str | None = None,
) -> int:
    """Delete wave_manifest rows inside one compaction scope (same filters
    as compact._shot_stage_where_clause) whose file isn't one of
    `keep_paths` -- the files that compaction just wrote. Call only after
    that compaction has committed: every row it removes then points at a
    file DuckLake has already ended, and leaving it would make a lookup
    match both the old and new copies of the same rows (api_server.py
    answers that with 300 Multiple Choices instead of a redirect).

    Only the direct ingest path needs this: it compacts inside DuckLake,
    so re-ingesting into an already-compacted (shot, stage) rewrites its
    files. The queue path never rewrites a file (see index_files). Returns
    how many rows were deleted."""
    clauses = ["shot = %s"]
    params: list[object] = [shot]
    for column, value in (
        ("stage", stage),
        ("experiment", experiment),
        ("data_version", data_version),
    ):
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    params.append([str(relative_to_lake_dir(p)) for p in keep_paths])
    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with pg_conn.cursor() as cur:
            cur.execute(CREATE_MANIFEST_SQL)
            cur.execute(
                f"DELETE FROM wave_manifest WHERE {' AND '.join(clauses)} "
                "AND NOT (file_path = ANY(%s))",
                params,
            )
            n = cur.rowcount
        pg_conn.commit()
        return n
    finally:
        pg_conn.close()


def refresh_manifest_for_file(parquet_path: Path) -> int:
    con = duckdb.connect()
    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with pg_conn.cursor() as cur:
            cur.execute(CREATE_MANIFEST_SQL)
        pg_conn.commit()

        rows = build_manifest_rows(con, parquet_path)
        upsert_manifest_rows(pg_conn, rows)
        return len(rows)
    finally:
        pg_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "parquet_file", help="path to a compacted parquet file to index into the manifest"
    )
    args = parser.parse_args()

    path = Path(args.parquet_file).resolve()
    n = refresh_manifest_for_file(path)
    print(f"wave_manifest: upserted {n} row-group entries for {path}")


if __name__ == "__main__":
    main()

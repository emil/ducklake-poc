"""Reset the DuckLake catalog (Postgres) and local data directories back
to a clean, mutually consistent state.

Why this exists: DuckLake's catalog lives in Postgres -- a separate,
persistent source of truth from the physical parquet files under
data/raw and data/lake. If those directories ever get cleared or
regenerated WITHOUT also resetting the Postgres catalog (e.g. between
iterative test runs, or when starting from a freshly re-extracted
checkout), the catalog keeps references to files that no longer exist on
disk -- and the next query touching that data, even a harmless
`SELECT count(*)`, fails with something like:

    IO Error: Cannot open file ".../mirnov_probe_01__....parquet":
    No such file or directory

This resets BOTH sides together, so they can never drift apart: drops
and recreates the `ducklake_catalog` Postgres database (which also wipes
`wave_manifest`, since it lives in the same database -- see config.py),
and clears data/raw + data/lake on disk. Destructive by design (this is
a dev/POC reset, not a production operation), so it requires --yes.

Usage:
    python -m ducklake_poc.reset --yes
"""

from __future__ import annotations

import argparse
import shutil

import psycopg2

from . import config


def reset_postgres_catalog() -> None:
    """Drop and recreate the DuckLake catalog database. Connects to the
    default `postgres` database to do this, since Postgres won't let a
    session drop the database it's currently connected to. Terminates
    any other backends connected to the target database first -- a
    lingering connection (e.g. a running api_server.py) would otherwise
    make the DROP fail."""
    admin_dsn = {**config.pg_dsn_for_psycopg2(), "dbname": "postgres"}
    conn = psycopg2.connect(**admin_dsn)
    conn.autocommit = True  # CREATE/DROP DATABASE can't run inside a transaction
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (config.PG_DB,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{config.PG_DB}"')
            cur.execute(f'CREATE DATABASE "{config.PG_DB}" OWNER "{config.PG_USER}"')
    finally:
        conn.close()


def clear_data_directories() -> None:
    """Wipe data/raw and data/lake, leaving empty directories behind
    (with a .gitkeep, matching how the repo ships them) rather than
    removing the directories themselves."""
    for d in (config.RAW_DATA_DIR, config.LAKE_DATA_DIR):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
        (d / ".gitkeep").touch()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="required: confirms you want to destroy the DuckLake catalog and all local data",
    )
    args = parser.parse_args()

    if not args.yes:
        print(
            "This will PERMANENTLY delete:\n"
            f"  - the '{config.PG_DB}' Postgres database "
            f"(DuckLake catalog + wave_manifest)\n"
            f"  - everything under {config.RAW_DATA_DIR}\n"
            f"  - everything under {config.LAKE_DATA_DIR}\n"
            "Re-run with --yes to actually do this."
        )
        raise SystemExit(1)

    print(f"Dropping and recreating Postgres database '{config.PG_DB}'...")
    reset_postgres_catalog()
    print(f"Clearing {config.RAW_DATA_DIR} and {config.LAKE_DATA_DIR}...")
    clear_data_directories()
    print("Done -- catalog and local data are back in a clean, consistent state.")


if __name__ == "__main__":
    main()

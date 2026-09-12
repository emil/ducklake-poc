"""Shared configuration for the DuckLake POC scripts.

All paths are relative to the repo root by default so the scripts work the
same whether they're run from a checkout on the host (talking to the
dockerized postgres/nginx) or adapted to run inside a container later.
"""

import os
import re
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]

# Where raw, per-wave parquet files land before being registered with
# DuckLake (mimics the current production ingestion path).
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"

# DuckLake's own managed data path (where DuckLake writes/rewrites files it
# owns, e.g. compaction output). Kept separate from RAW_DATA_DIR so it's
# obvious in the filesystem which files are "ours" (loosely tracked) vs
# "DuckLake's" (lifecycle-managed: compaction/expiry can delete them).
LAKE_DATA_DIR = REPO_ROOT / "data" / "lake"

# nginx serves ./data at this base URL (see docker-compose.yml)
NGINX_HOST_URL = os.environ.get("NGINX_HOST_URL", "http://localhost:8080")
NGINX_BASE_URL = os.environ.get("NGINX_BASE_URL", f"{NGINX_HOST_URL}/data")

# API server (ducklake_poc.api_server) -- see nginx/nginx.conf's
# /range-proxy/ location for the other half of this.
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "5001"))

# Postgres connection used both as the DuckLake catalog backend and to host
# our own "wave_manifest" helper table (see manifest.py).
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "ducklake_catalog")
PG_USER = os.environ.get("PG_USER", "ducklake")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "ducklake")

DUCKLAKE_NAME = "physics_lake"
TABLE_NAME = "waves"


def pg_dsn_for_duckdb() -> str:
    """DSN string as consumed by DuckDB's postgres attach syntax."""
    return f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASSWORD}"


def pg_dsn_for_psycopg2() -> dict:
    return dict(dbname=PG_DB, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)


_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9_.-]")


def safe_filename(wave: str) -> str:
    """wave names like 'thomson_te/A' aren't safe as bare filenames --
    swap anything other than alnum/underscore/dot/hyphen for '_'. Shared
    by ingest.py (raw per-wave files) and compact.py (compacted output
    files)."""
    return _UNSAFE_FILENAME_CHARS.sub("_", wave)


def get_connection() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection attached to the DuckLake catalog. Lives
    here (rather than in ingest.py, where it originated) so both
    ingest.py and compact.py can use it without a circular import --
    ingest.py now calls into compact.py directly to run compaction right
    after ingesting (see ingest.py's module docstring)."""
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("INSTALL postgres")
    con.execute("LOAD ducklake")
    con.execute("LOAD postgres")
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:{pg_dsn_for_duckdb()}' AS {DUCKLAKE_NAME}
        (DATA_PATH '{LAKE_DATA_DIR}/')
        """
    )
    con.execute(f"USE {DUCKLAKE_NAME}")
    return con

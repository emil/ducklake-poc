"""Shared configuration for the DuckLake POC scripts.

All paths are relative to the repo root by default so the scripts work the
same whether they're run from a checkout on the host (talking to the
dockerized postgres/nginx) or adapted to run inside a container later.
"""

import os
import random
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

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


_T = TypeVar("_T")

# Substrings (matched case-insensitively) of DuckLake/Postgres errors that
# are safe to retry rather than treat as a hard failure -- both are a
# direct consequence of running more than one ingest client concurrently
# against the same catalog, not a sign of anything actually wrong. See
# retry_on_conflict's docstring for what produces each one.
_RETRYABLE_ERROR_SNIPPETS = (
    "transaction conflict",
    # Postgres's own uniqueness constraint on its pg_type catalog, hit
    # when two connections race to initialize DuckLake's ducklake_metadata
    # tables in Postgres for the very first time (that bootstrap isn't
    # guarded by DuckLake itself with any locking of its own).
    "duplicate key value violates unique constraint",
)


def _is_retryable_ducklake_error(exc: BaseException) -> bool:
    return isinstance(exc, duckdb.Error) and any(
        snippet in str(exc).lower() for snippet in _RETRYABLE_ERROR_SNIPPETS
    )


def retry_on_conflict(
    fn: Callable[[], _T],
    *,
    max_attempts: int = 8,
    base_delay: float = 0.05,
    max_delay: float = 2.0,
) -> _T:
    """Call `fn()`, retrying with jittered exponential backoff if it
    raises a transient, concurrency-only DuckLake/Postgres error.

    DuckLake's Postgres-backed catalog uses optimistic concurrency: two
    connections that concurrently commit changes to the SAME table --
    even to logically disjoint partitions, e.g. different (shot, stage)
    groups -- can lose a race at commit time ("Transaction conflict -
    attempting to ... but another transaction has ..."). Confirmed
    empirically against a real DuckLake/Postgres catalog: concurrent
    `ducklake_add_data_files` calls on disjoint partitions succeed fine,
    but a DELETE (compaction rewriting a partition) racing against any
    concurrent INSERT into the same table -- even an unrelated
    partition's -- reliably conflicts. The loser must redo its whole
    operation, not just retry the COMMIT, since whatever it read before
    computing its write may now be stale (e.g. compaction's row count and
    row stream need re-reading, not just re-committing). Every write path
    reachable by more than one concurrent ingest client (this function's
    own ATTACH bootstrap, `ensure_table`, `register_experiment`,
    `compact_manifest_friendly`) goes through this -- none of them is
    safe to call concurrently without it.

    `fn` must be safe to call more than once: on a retryable failure it
    is invoked again from scratch, so callers that write files or other
    side effects before the risky commit are responsible for making a
    retry idempotent (see compact_manifest_friendly, which deletes its
    own not-yet-registered output files before retrying)."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except duckdb.Error as e:
            if attempt >= max_attempts or not _is_retryable_ducklake_error(e):
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            time.sleep(delay * (1 + random.random()))


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

    def _attach() -> None:
        con.execute(
            f"""
            ATTACH 'ducklake:postgres:{pg_dsn_for_duckdb()}' AS {DUCKLAKE_NAME}
            (DATA_PATH '{LAKE_DATA_DIR}/')
            """
        )

    # See retry_on_conflict's docstring: the very first ATTACH ever made
    # to a catalog initializes DuckLake's own metadata tables in Postgres,
    # and that initialization has no concurrency guard of its own -- two
    # clients starting up at the same time against a brand new catalog
    # can otherwise crash here before doing anything else.
    retry_on_conflict(_attach)
    con.execute(f"USE {DUCKLAKE_NAME}")
    return con

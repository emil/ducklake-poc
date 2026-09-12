"""Shared fixtures.

Tests are split into two tiers:

  - Pure/unit tests (the majority) exercise logic that doesn't need
    Postgres or nginx at all: synthetic data generation, encoding,
    compaction's row-group/file-boundary logic, and manifest row
    extraction all only need DuckDB + the local filesystem.
  - Integration tests (marked `@pytest.mark.integration`) need a
    reachable Postgres (the wave_manifest table) and/or nginx (real HTTP
    range requests). They're skipped automatically if those aren't
    reachable, rather than failing, so the suite still runs somewhere
    without the docker-compose stack up.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from urllib.parse import urlparse

import psycopg2
import pytest

from ducklake_poc import config


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def postgres_available() -> bool:
    return _port_open(config.PG_HOST, config.PG_PORT)


@pytest.fixture(scope="session")
def nginx_available() -> bool:
    parsed = urlparse(config.NGINX_BASE_URL)
    return _port_open(parsed.hostname or "localhost", parsed.port or 80)


@pytest.fixture
def pg_conn(postgres_available: bool) -> Iterator[psycopg2.extensions.connection]:
    if not postgres_available:
        pytest.skip(f"Postgres not reachable at {config.PG_HOST}:{config.PG_PORT}")
    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def clean_manifest_table(pg_conn) -> Iterator[None]:
    """Ensure wave_manifest exists and is empty for the test shot range
    used across the integration tests, then clean up afterward."""
    from ducklake_poc import manifest

    with pg_conn.cursor() as cur:
        cur.execute(manifest.CREATE_MANIFEST_SQL)
        cur.execute("DELETE FROM wave_manifest WHERE shot >= 90000")
    pg_conn.commit()
    yield
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM wave_manifest WHERE shot >= 90000")
    pg_conn.commit()

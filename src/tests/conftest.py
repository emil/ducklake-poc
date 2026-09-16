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

import duckdb
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


class FakeDuckLakeConnection:
    """Minimal stand-in for a real DuckLake-attached duckdb connection,
    used to unit-test the retry/rollback-masking logic in
    ensure_table/register_experiment/compact_manifest_friendly without a
    live Postgres/DuckLake catalog. It understands just enough SQL shape
    (BEGIN/COMMIT/ROLLBACK, `SELECT count(*) FROM information_schema...`,
    any other `SELECT count(*)`) to drive those functions, and records
    every statement's first line in `.executed` for assertions.

    `commit_effects` is a queue of None (succeed) or Exception (raise)
    consumed one per COMMIT call -- once exhausted, COMMIT always
    succeeds. Mimics a real DuckDB quirk that the code under test relies
    on: a failed COMMIT already aborts the transaction, so a subsequent
    ROLLBACK on the same connection itself raises "no transaction is
    active" rather than silently succeeding.
    """

    def __init__(
        self,
        *,
        commit_effects: list[Exception | None] | None = None,
        count_result: int = 0,
        table_exists: bool = False,
    ) -> None:
        self._commit_effects = list(commit_effects or [])
        self.count_result = count_result
        self.table_exists = table_exists
        self.executed: list[str] = []
        self._in_txn = False
        self._last_result: tuple | None = None

    def execute(self, sql: str, params=None) -> FakeDuckLakeConnection:
        stmt = sql.strip()
        self.executed.append(stmt.splitlines()[0].strip())
        upper = stmt.upper()

        if "INFORMATION_SCHEMA.TABLES" in upper:
            self._last_result = (1 if self.table_exists else 0,)
        elif upper.startswith("SELECT COUNT(*)"):
            self._last_result = (self.count_result,)
        elif upper.startswith("BEGIN"):
            self._in_txn = True
        elif upper.startswith("COMMIT"):
            effect = self._commit_effects.pop(0) if self._commit_effects else None
            self._in_txn = False  # a failed commit aborts the transaction too
            if effect is not None:
                raise effect
        elif upper.startswith("ROLLBACK"):
            if not self._in_txn:
                raise duckdb.TransactionException(
                    "TransactionContext Error: cannot rollback - no transaction is active"
                )
            self._in_txn = False
        return self

    def fetchone(self) -> tuple | None:
        return self._last_result


@pytest.fixture
def fake_ducklake_connection():
    return FakeDuckLakeConnection


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

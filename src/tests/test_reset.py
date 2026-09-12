"""Integration tests for reset.py -- needs a reachable Postgres. Skipped
(not failed) if it isn't, same as the other integration tests."""

from __future__ import annotations

import contextlib

import psycopg2
import pytest

from ducklake_poc import config
from ducklake_poc.reset import clear_data_directories, reset_postgres_catalog


@pytest.fixture
def pg_admin(postgres_available: bool):
    if not postgres_available:
        pytest.skip("Postgres not reachable")
    conn = psycopg2.connect(**{**config.pg_dsn_for_psycopg2(), "dbname": "postgres"})
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.mark.integration
def test_reset_drops_and_recreates_an_empty_functional_database(pg_admin) -> None:
    # put some state into the target database first
    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS reset_test_marker (id int)")
    conn.close()

    reset_postgres_catalog()

    # the marker table must be gone -- a real drop+recreate, not a no-op
    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = 'reset_test_marker'"
            )
            assert cur.fetchone()[0] == 0

            # and the database must be immediately usable again
            cur.execute("CREATE TABLE post_reset_check (id int)")
            cur.execute("INSERT INTO post_reset_check VALUES (1)")
            cur.execute("SELECT * FROM post_reset_check")
            assert cur.fetchall() == [(1,)]
            cur.execute("DROP TABLE post_reset_check")
    finally:
        conn.close()


@pytest.mark.integration
def test_reset_succeeds_with_an_active_lingering_connection(pg_admin) -> None:
    """A running api_server.py (or any other lingering session) must not
    block the reset -- this is exactly the scenario that would otherwise
    make DROP DATABASE fail with "database is being accessed by other
    users"."""
    lingering = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    lingering.autocommit = True
    with lingering.cursor() as cur:
        cur.execute("SELECT 1")  # just to hold the connection open

    reset_postgres_catalog()  # must not raise

    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    conn.close()

    with contextlib.suppress(Exception):
        lingering.close()  # the backend was terminated out from under it -- expected


def test_clear_data_directories_removes_contents_but_keeps_gitkeep(tmp_path, monkeypatch) -> None:
    raw = tmp_path / "raw"
    lake = tmp_path / "lake"
    raw.mkdir()
    lake.mkdir()
    (raw / "stray_experiment=x").mkdir()
    (raw / "stray_experiment=x" / "file.parquet").touch()
    (lake / "another_stray.parquet").touch()

    monkeypatch.setattr(config, "RAW_DATA_DIR", raw)
    monkeypatch.setattr(config, "LAKE_DATA_DIR", lake)

    clear_data_directories()

    assert list(raw.iterdir()) == [raw / ".gitkeep"]
    assert list(lake.iterdir()) == [lake / ".gitkeep"]

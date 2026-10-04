"""Tests for the catalog-level `wave_auc` table function.

Uses a local DuckLake file catalog (no Postgres), but needs the ducklake
extension, so it skips if that can't be installed/loaded."""

from __future__ import annotations

import duckdb
import pytest

from ducklake_poc.ingest import ensure_functions


@pytest.fixture
def lake(tmp_path):
    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
    except duckdb.Error as e:
        pytest.skip(f"ducklake extension unavailable: {e}")
    con.execute(f"ATTACH 'ducklake:{tmp_path}/meta.db' AS lake (DATA_PATH '{tmp_path}/data/')")
    con.execute("USE lake")
    con.execute(
        "CREATE TABLE waves(experiment VARCHAR, shot INTEGER, stage VARCHAR, wave VARCHAR, "
        "data_version VARCHAR, x DOUBLE, y DOUBLE)"
    )
    con.execute(
        """INSERT INTO waves VALUES
        ('e',1,'s','a','v1',0,0),('e',1,'s','a','v1',1,2),('e',1,'s','a','v1',2,2),
        ('e',1,'s','b','v1',0,1),('e',1,'s','b','v1',4,1),
        ('e',1,'s','a','v2',0,100),('e',1,'s','a','v2',1,100)"""
    )
    ensure_functions(con)
    return con


def test_wave_auc_is_trapezoidal_per_wave_and_pins_version(lake) -> None:
    rows = lake.execute("SELECT * FROM wave_auc('e', 1, 's', 'v1') ORDER BY wave").fetchall()
    assert rows == [("a", 3.0), ("b", 4.0)]


def test_wave_auc_x_window(lake) -> None:
    rows = lake.execute("SELECT * FROM wave_auc('e', 1, 's', 'v1', p_x_max := 1)").fetchall()
    assert rows == [("a", 1.0)]


def test_ensure_functions_is_idempotent(lake) -> None:
    ensure_functions(lake)
    assert lake.execute("SELECT count(*) FROM wave_auc('e', 1, 's', 'v1')").fetchone() == (2,)

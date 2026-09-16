"""Tests for ingest.py's concurrency-safety.

Unit tier (no services needed): ensure_table's "only configure the table
once" guard and register_experiment's retry/rollback-masking fix, both
driven against a FakeDuckLakeConnection (see conftest.py) rather than a
live DuckLake catalog.

Integration tier (needs Postgres+nginx up, marked `@pytest.mark.integration`):
runs the real ingest pipeline from multiple threads at once, the way
multiple concurrent ingest clients would, against the actual DuckLake
catalog -- this is what originally surfaced the bugs fixed here (see
CLAUDE.md and the docstrings on config.retry_on_conflict / ensure_table /
register_experiment / compact_manifest_friendly): a bare `ducklake-ingest`
run for two different (shot, stage) pairs at the same time would reliably
crash with a DuckLake "Transaction conflict" the moment either process
called ensure_table or compacted, even though the two pairs share no
data.
"""

from __future__ import annotations

import glob
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import psycopg2
import pytest

from ducklake_poc import config
from ducklake_poc.ingest import (
    compact_and_refresh_manifest,
    ensure_table,
    register_experiment,
    write_stage_files,
)
from ducklake_poc.synthetic import DIAGNOSTICS

# ---------------------------------------------------------------------------
# ensure_table (unit, FakeDuckLakeConnection)
# ---------------------------------------------------------------------------


def test_ensure_table_is_a_noop_when_the_table_already_exists(fake_ducklake_connection) -> None:
    """This is the actual fix: re-issuing SET SORTED BY/SET PARTITIONED BY
    on every single ingest call -- even when nothing changes -- is what
    made ensure_table conflict with any OTHER concurrent writer to this
    table on essentially every concurrent run. Once the table exists,
    ensure_table must do nothing beyond the existence check."""
    con = fake_ducklake_connection(table_exists=True)
    ensure_table(con)
    assert len(con.executed) == 1
    assert "information_schema.tables" in con.executed[0].lower()


def test_ensure_table_creates_and_configures_when_missing(fake_ducklake_connection) -> None:
    con = fake_ducklake_connection(table_exists=False)
    ensure_table(con)
    joined = "\n".join(con.executed)
    assert "CREATE TABLE" in joined
    assert joined.count("ALTER TABLE") == 2
    assert "SET SORTED BY" in joined
    assert "SET PARTITIONED BY" in joined


def test_ensure_table_retries_a_conflicting_alter(fake_ducklake_connection) -> None:
    """Cold-start race: two first-ever clients both see "table doesn't
    exist" and both try to create+configure it; the loser's ALTER
    conflicts and must retry -- on retry it should see the table now
    exists (the winner's commit is visible) and stop immediately, issuing
    no further ALTERs."""
    con = fake_ducklake_connection(
        table_exists=False,
        commit_effects=[
            duckdb.TransactionException(
                'Transaction conflict - attempting to alter table with index "1" - '
                "but another transaction has altered it"
            )
        ],
    )
    # Simulate the winner's commit becoming visible between attempt 1
    # (which conflicts) and attempt 2 (which should see it and bail out
    # via the existence check rather than re-issuing ALTERs).
    real_execute = con.execute

    def execute_then_flip(sql, params=None):
        result = real_execute(sql, params)
        if "COMMIT" in sql.strip().upper():
            con.table_exists = True
        return result

    con.execute = execute_then_flip  # type: ignore[method-assign]

    ensure_table(con)
    joined = "\n".join(con.executed)
    assert joined.count("ALTER TABLE") == 2  # only from the first (failed) attempt


# ---------------------------------------------------------------------------
# register_experiment (unit, FakeDuckLakeConnection)
# ---------------------------------------------------------------------------


def test_register_experiment_retries_transparently_on_conflict(
    monkeypatch, fake_ducklake_connection
) -> None:
    monkeypatch.setattr(glob, "glob", lambda pattern: ["/fake/a.parquet", "/fake/b.parquet"])
    con = fake_ducklake_connection(
        commit_effects=[
            duckdb.TransactionException("Transaction conflict - lost the race"),
        ]
    )
    n = register_experiment(con, shot=1, stage="mirnov")
    assert n == 2
    assert con.executed.count("BEGIN TRANSACTION") == 2


def test_register_experiment_does_not_mask_the_original_error(
    monkeypatch, fake_ducklake_connection
) -> None:
    """Regression test for the rollback-masking bug: a failed COMMIT
    already aborts the transaction (see FakeDuckLakeConnection's
    docstring), so the old `except Exception: con.execute("ROLLBACK");
    raise` pattern replaced the real error with a confusing "cannot
    rollback - no transaction is active" one. The real, non-retryable
    error must be what actually propagates."""
    monkeypatch.setattr(glob, "glob", lambda pattern: ["/fake/a.parquet"])
    boom = duckdb.CatalogException("some genuine, non-retryable bug")
    con = fake_ducklake_connection(commit_effects=[boom])

    with pytest.raises(duckdb.CatalogException, match="genuine, non-retryable bug"):
        register_experiment(con, shot=1, stage="mirnov")


def test_register_experiment_returns_zero_without_touching_the_catalog_when_no_files(
    monkeypatch, fake_ducklake_connection
) -> None:
    monkeypatch.setattr(glob, "glob", lambda pattern: [])
    con = fake_ducklake_connection()
    n = register_experiment(con, shot=1, stage="mirnov")
    assert n == 0
    assert con.executed == []


# ---------------------------------------------------------------------------
# Real concurrency, against a live DuckLake catalog
# ---------------------------------------------------------------------------

CONCURRENCY_EXPERIMENT = "concurrency-test"
# High, dedicated shot range so this never collides with manually-run
# ingests or other tests sharing the same Postgres catalog (same
# convention as conftest.py's clean_manifest_table, which reserves
# shot >= 90000 for test data).
FIRST_SHOT = 91000


@pytest.fixture
def concurrency_ingest_env(postgres_available: bool, tmp_path_factory, monkeypatch):
    """Isolate raw/lake directories per test run under tmp_path, but keep
    using the REAL Postgres catalog (DuckLake's own DATA_PATH is fixed at
    catalog-creation time, so it can't be pointed at a tmp dir without
    resetting the whole catalog) -- data is scoped to CONCURRENCY_EXPERIMENT
    and a dedicated shot range instead, and cleaned up afterward."""
    if not postgres_available:
        pytest.skip(f"Postgres not reachable at {config.PG_HOST}:{config.PG_PORT}")

    con = config.get_connection()
    ensure_table(con)
    con.close()

    yield

    con = config.get_connection()
    con.execute(f"DELETE FROM {config.TABLE_NAME} WHERE experiment = ?", [CONCURRENCY_EXPERIMENT])
    con.close()
    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM wave_manifest WHERE experiment = %s", (CONCURRENCY_EXPERIMENT,)
            )
        pg_conn.commit()
    finally:
        pg_conn.close()
    for base in (config.RAW_DATA_DIR, config.LAKE_DATA_DIR):
        shutil.rmtree(base / f"experiment={CONCURRENCY_EXPERIMENT}", ignore_errors=True)


def _ingest_one(shot: int, stage: str) -> tuple[int, int]:
    """One (shot, stage) group's full pipeline -- write, register, compact,
    manifest -- exactly what ingest.py's main() loop body does, run here
    from its own connection so several of these can be fired off from
    separate threads to simulate separate concurrent ingest clients."""
    con = config.get_connection()
    try:
        write_stage_files(shot, stage, CONCURRENCY_EXPERIMENT)
        register_experiment(con, shot, stage, CONCURRENCY_EXPERIMENT)
        n_files, n_manifest = compact_and_refresh_manifest(
            con, shot, stage, rows_per_row_group=5_000, target_file_size_bytes=1_000_000
        )
        return n_files, n_manifest
    finally:
        con.close()


@pytest.mark.integration
def test_concurrent_ingest_of_different_stages_of_the_same_shot(concurrency_ingest_env) -> None:
    """The exact scenario reported: multiple clients ingesting different
    diagnostic groups of the SAME shot at the same time. Before the fix,
    this reliably raised duckdb.TransactionException from ensure_table
    and/or compact_manifest_friendly within a couple of concurrent runs."""
    shot = FIRST_SHOT
    stages = sorted(DIAGNOSTICS)[:6]

    with ThreadPoolExecutor(max_workers=len(stages)) as pool:
        futures = {pool.submit(_ingest_one, shot, stage): stage for stage in stages}
        errors = {}
        for fut in as_completed(futures):
            stage = futures[fut]
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001 -- collecting failures across threads
                errors[stage] = e

    assert errors == {}, f"concurrent ingest failed for: {errors}"
    _assert_catalog_is_consistent(shot)


@pytest.mark.integration
def test_concurrent_ingest_of_multiple_shots(concurrency_ingest_env) -> None:
    """Multiple clients ingesting entirely different shots at the same
    time -- should never contend with each other in principle, but the
    old ensure_table (re-issuing ALTERs on every call, unconditionally)
    made even this crash under real concurrency."""
    shots = [FIRST_SHOT + 1, FIRST_SHOT + 2, FIRST_SHOT + 3]
    stage = "thomson"

    with ThreadPoolExecutor(max_workers=len(shots)) as pool:
        futures = {pool.submit(_ingest_one, shot, stage): shot for shot in shots}
        errors = {}
        for fut in as_completed(futures):
            shot = futures[fut]
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001
                errors[shot] = e

    assert errors == {}, f"concurrent ingest failed for: {errors}"
    for shot in shots:
        _assert_catalog_is_consistent(shot)


@pytest.mark.integration
def test_concurrent_cold_start_ensure_table_from_multiple_clients(postgres_available: bool) -> None:
    """Simulates several brand-new clients all calling get_connection()
    (and therefore, on a fresh catalog, racing to bootstrap DuckLake's own
    Postgres metadata tables) and ensure_table() at once. Doesn't need
    concurrency_ingest_env since it deliberately doesn't touch any wave
    data -- just exercises the connect+ensure_table path many times
    concurrently against whatever catalog state already exists."""
    if not postgres_available:
        pytest.skip(f"Postgres not reachable at {config.PG_HOST}:{config.PG_PORT}")

    errors: dict[int, Exception] = {}
    lock = threading.Lock()

    def worker(i: int) -> None:
        try:
            con = config.get_connection()
            ensure_table(con)
            con.close()
        except Exception as e:  # noqa: BLE001
            with lock:
                errors[i] = e

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == {}, f"concurrent connect+ensure_table failed for: {errors}"


def _assert_catalog_is_consistent(shot: int) -> None:
    """No duplicate active data-file registrations (the symptom of a
    blind retry re-registering something that actually committed), and no
    orphaned temp/finalized-but-unregistered files left on disk from a
    retried-then-abandoned compaction attempt."""
    con = config.get_connection()
    try:
        dupes = con.execute(
            f"""
            SELECT path, count(*) FROM __ducklake_metadata_{config.DUCKLAKE_NAME}.ducklake_data_file
            WHERE end_snapshot IS NULL AND path LIKE ?
            GROUP BY path HAVING count(*) > 1
            """,
            [f"%experiment={CONCURRENCY_EXPERIMENT}/shot={shot}/%"],
        ).fetchall()
        assert dupes == [], f"duplicate active data-file registrations: {dupes}"
    finally:
        con.close()

    lake_dir = config.LAKE_DATA_DIR / f"experiment={CONCURRENCY_EXPERIMENT}" / f"shot={shot}"
    orphaned_tmp = list(lake_dir.glob("**/.tmp_*"))
    assert orphaned_tmp == [], f"orphaned temp files left behind: {orphaned_tmp}"

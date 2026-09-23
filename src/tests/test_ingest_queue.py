"""Tests for the queue-driven ingest path: producer-side compaction
(ingest.compact_stage_locally / enqueue_shots), ingest_queue.py (the
Postgres queue, one row per data_version) and ingest_worker.py (the single
DuckLake writer draining it).

Unit tier: enqueue-time validation, reading a file's data_version from
its footer, and local compaction.

Integration tier (real Postgres + DuckLake catalog): the once-per-version
constraint, claim semantics, and the worker end to end -- including a
crash between DuckLake's commit and the queue update (must not register
twice), one bad version in a batch (must fail alone, and atomically), and
a recalibration rerun (must leave earlier versions' files and manifest
rows untouched).

The worker claims ANY pending queue row, not just this module's, so the
`queue_env` fixture takes the worker advisory lock itself (skipping if a
real `ducklake-ingest-worker` is running) and skips if the queue holds
someone else's unfinished work.
"""

from __future__ import annotations

import shutil
import threading
from pathlib import Path

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ducklake_poc import config
from ducklake_poc import ingest_queue as iq
from ducklake_poc.ingest import compact_stage_locally, enqueue_shots, ensure_table, register_files
from ducklake_poc.ingest_worker import run_until_idle
from ducklake_poc.manifest import CREATE_MANIFEST_SQL
from ducklake_poc.synthetic import generate_stage

QUEUE_EXPERIMENT = "queue-test"
# Dedicated shot range (conftest.py reserves shot >= 90000 for tests;
# test_ingest.py uses 91000+).
FIRST_SHOT = 93000
STAGE = "thomson"  # few, small waves -- keeps the integration tests fast
COMPACT_OPTS = dict(rows_per_row_group=5_000, target_file_size_bytes=1_000_000)


def _stage_dir(shot: int) -> Path:
    return (
        config.LAKE_DATA_DIR / f"experiment={QUEUE_EXPERIMENT}" / f"shot={shot}" / f"stage={STAGE}"
    )


def _write_file(shot: int, data_version: str, name: str, waves=("a",)) -> Path:
    """A tiny file in the shape compact.py writes (physical columns only)."""
    n = len(waves)
    table = pa.table(
        {
            "wave": list(waves),
            "data_version": [data_version] * n,
            "x": [float(i) for i in range(n)],
            "y": [0.0] * n,
        }
    )
    _stage_dir(shot).mkdir(parents=True, exist_ok=True)
    path = _stage_dir(shot) / name
    pq.write_table(table, path)
    return path


def _compact(shot: int) -> dict[str, list[Path]]:
    return compact_stage_locally(shot, STAGE, QUEUE_EXPERIMENT, **COMPACT_OPTS)


def _enqueue_all(pg, shot: int, by_version: dict[str, list[Path]]) -> None:
    for version, paths in by_version.items():
        assert iq.enqueue_version(pg, paths, QUEUE_EXPERIMENT, shot, STAGE, version)


def _generated_rows(shot: int) -> int:
    return sum(len(rec.x) for rec in generate_stage(shot, STAGE, QUEUE_EXPERIMENT))


@pytest.fixture
def lake_in_tmp(tmp_path, monkeypatch) -> Path:
    monkeypatch.setattr(config, "LAKE_DATA_DIR", tmp_path / "lake")
    return tmp_path / "lake"


# ---------------------------------------------------------------------------
# read_data_version / validate_version_files / compact_stage_locally (unit)
# ---------------------------------------------------------------------------


def test_read_data_version_rejects_a_file_spanning_two_versions(lake_in_tmp) -> None:
    p = _write_file(1, "v1", "mixed.parquet")
    pq.write_table(pa.table({"wave": ["a", "a"], "data_version": ["v1", "v2"], "x": [0.0, 1.0]}), p)
    with pytest.raises(ValueError, match="exactly one data_version"):
        iq.read_data_version(p)


def test_read_data_version_rejects_a_non_parquet_file(tmp_path) -> None:
    p = tmp_path / "bogus.parquet"
    p.write_bytes(b"definitely not parquet")
    with pytest.raises(ValueError):  # pyarrow.ArrowInvalid is a ValueError
        iq.read_data_version(p)


def test_validate_version_files_accepts_its_own_files(lake_in_tmp) -> None:
    p = _write_file(1, "v1", "f.parquet")
    assert iq.validate_version_files([p], QUEUE_EXPERIMENT, 1, STAGE, "v1") == [str(p.resolve())]


def test_validate_version_files_rejects_a_different_version(lake_in_tmp) -> None:
    p = _write_file(1, "v1", "f.parquet")
    with pytest.raises(ValueError, match="holds data_version 'v1', not 'v2'"):
        iq.validate_version_files([p], QUEUE_EXPERIMENT, 1, STAGE, "v2")


def test_validate_version_files_rejects_a_different_group(lake_in_tmp) -> None:
    """DuckLake takes partition values from the path -- it must agree with
    what the row claims."""
    p = _write_file(2, "v1", "f.parquet")
    with pytest.raises(ValueError, match="hive partitions"):
        iq.validate_version_files([p], QUEUE_EXPERIMENT, 1, STAGE, "v1")


def test_validate_version_files_rejects_files_outside_the_lake(lake_in_tmp, tmp_path) -> None:
    """wave_manifest paths are relative to LAKE_DATA_DIR and nginx serves
    it -- a file anywhere else could be registered but never fetched."""
    elsewhere = tmp_path / "elsewhere.parquet"
    shutil.copy(_write_file(1, "v1", "f.parquet"), elsewhere)
    with pytest.raises(ValueError, match="not under"):
        iq.validate_version_files([elsewhere], QUEUE_EXPERIMENT, 1, STAGE, "v1")


def test_validate_version_files_rejects_an_empty_version() -> None:
    with pytest.raises(ValueError, match="at least one file"):
        iq.validate_version_files([], QUEUE_EXPERIMENT, 1, STAGE, "v1")


def test_compact_stage_locally_writes_final_single_version_files(lake_in_tmp) -> None:
    by_version = _compact(1)

    assert by_version
    all_paths = [p for paths in by_version.values() for p in paths]
    assert all(p.is_relative_to(_stage_dir(1)) for p in all_paths)
    for version, paths in by_version.items():
        assert all(iq.read_data_version(p) == version for p in paths)
    row = duckdb.sql(
        "SELECT count(*) FROM read_parquet(?)", params=[[str(p) for p in all_paths]]
    ).fetchone()
    assert row == (_generated_rows(1),)
    # the staging directory is gone; nothing but final files was left behind
    assert sorted(_stage_dir(1).iterdir()) == sorted(all_paths)


# ---------------------------------------------------------------------------
# integration fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def queue_env(postgres_available: bool):
    if not postgres_available:
        pytest.skip(f"Postgres not reachable at {config.PG_HOST}:{config.PG_PORT}")

    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    iq.ensure_queue_table(pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_MANIFEST_SQL)
    pg_conn.commit()
    if not iq.try_acquire_worker_lock(pg_conn):
        pg_conn.close()
        pytest.skip("a ducklake-ingest-worker is running against this database")
    others = _scalar(
        pg_conn,
        f"SELECT count(*) FROM {iq.QUEUE_TABLE} "
        "WHERE status IN ('pending', 'processing') AND experiment <> %s",
        (QUEUE_EXPERIMENT,),
    )
    if others:
        pg_conn.close()
        pytest.skip(f"{iq.QUEUE_TABLE} holds unfinished work from other experiments")
    _cleanup(pg_conn)

    con = config.get_connection()
    ensure_table(con)
    try:
        yield con, pg_conn
    finally:
        # Always release the connections (and with pg_conn, the worker
        # lock) even if cleanup fails -- otherwise every later test in
        # the session skips as "a worker is running".
        try:
            _cleanup(pg_conn, con)
        finally:
            con.close()
            pg_conn.close()


def _cleanup(pg_conn, con=None) -> None:
    if con is not None:
        con.execute(f"DELETE FROM {config.TABLE_NAME} WHERE experiment = ?", [QUEUE_EXPERIMENT])
        # For files this tiny DuckLake records the DELETE as an *inlined*
        # delete and keeps the data file active -- removing it from disk
        # below would then break every later scan ("Cannot open file").
        # Rewriting retires fully deleted files (it only touches files
        # with deletions, and doesn't change what the table contains).
        con.execute(
            f"CALL ducklake_rewrite_data_files('{config.DUCKLAKE_NAME}', '{config.TABLE_NAME}')"
        )
    with pg_conn.cursor() as cur:
        cur.execute(f"DELETE FROM {iq.QUEUE_TABLE} WHERE experiment = %s", (QUEUE_EXPERIMENT,))
        cur.execute("DELETE FROM wave_manifest WHERE experiment = %s", (QUEUE_EXPERIMENT,))
    pg_conn.commit()
    for base in (config.RAW_DATA_DIR, config.LAKE_DATA_DIR):
        shutil.rmtree(base / f"experiment={QUEUE_EXPERIMENT}", ignore_errors=True)


def _scalar(pg_conn, sql: str, params: tuple = ()):
    with pg_conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    pg_conn.commit()
    assert row is not None
    return row[0]


def _statuses(pg_conn, shot: int) -> dict[str, str]:
    """{data_version: status} for one test shot."""
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT data_version, status FROM {iq.QUEUE_TABLE} "
            "WHERE experiment = %s AND shot = %s",
            (QUEUE_EXPERIMENT, shot),
        )
        rows = cur.fetchall()
    pg_conn.commit()
    return dict(rows)


def _waves_rows(con, shot: int) -> int:
    row = con.execute(
        f"SELECT count(*) FROM {config.TABLE_NAME} WHERE experiment = ? AND shot = ?",
        [QUEUE_EXPERIMENT, shot],
    ).fetchone()
    assert row is not None
    return row[0]


def _manifest(pg_conn, shot: int) -> set[tuple]:
    """(data_version, file_path, row_group_id, byte_start, is_active_file)
    for every wave_manifest row of one test shot."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.data_version, m.file_path, m.row_group_id, m.byte_start,
                   EXISTS (SELECT 1 FROM ducklake_data_file f
                           WHERE f.end_snapshot IS NULL AND f.path LIKE '%%/' || m.file_path)
            FROM wave_manifest m WHERE m.experiment = %s AND m.shot = %s
            """,
            (QUEUE_EXPERIMENT, shot),
        )
        rows = set(cur.fetchall())
    pg_conn.commit()
    return rows


def _duplicate_active_paths(pg_conn, shot: int) -> list:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT path, count(*) FROM ducklake_data_file "
            "WHERE end_snapshot IS NULL AND path LIKE %s GROUP BY path HAVING count(*) > 1",
            (f"%experiment={QUEUE_EXPERIMENT}/shot={shot}/%",),
        )
        rows = cur.fetchall()
    pg_conn.commit()
    return rows


# ---------------------------------------------------------------------------
# queue semantics (integration, Postgres only)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_a_version_is_enqueued_only_once(queue_env) -> None:
    _, pg = queue_env
    shot = FIRST_SHOT
    first = _write_file(shot, "v1", "first.parquet")
    second = _write_file(shot, "v1", "second.parquet")

    assert iq.enqueue_version(pg, [first], QUEUE_EXPERIMENT, shot, STAGE, "v1") is True
    assert iq.enqueue_version(pg, [second], QUEUE_EXPERIMENT, shot, STAGE, "v1") is False
    iq.claim_batch(pg, 10)  # in progress
    assert iq.enqueue_version(pg, [second], QUEUE_EXPERIMENT, shot, STAGE, "v1") is False

    paths = _scalar(
        pg, f"SELECT array_agg(file_paths) FROM {iq.QUEUE_TABLE} WHERE shot = %s", (shot,)
    )
    assert paths == [[str(first)]]


@pytest.mark.integration
def test_a_failed_version_is_revived_with_the_new_files(queue_env) -> None:
    _, pg = queue_env
    shot = FIRST_SHOT + 1
    iq.enqueue_version(
        pg, [_write_file(shot, "v1", "broken.parquet")], QUEUE_EXPERIMENT, shot, STAGE, "v1"
    )
    (item,) = iq.claim_batch(pg, 10)
    iq.release_with_error(pg, [item.id], "boom", max_attempts=1)
    assert _statuses(pg, shot) == {"v1": "failed"}

    fixed = _write_file(shot, "v1", "fixed.parquet")
    assert iq.enqueue_version(pg, [fixed], QUEUE_EXPERIMENT, shot, STAGE, "v1") is True
    (revived,) = iq.claim_batch(pg, 10)
    assert revived.id == item.id and revived.file_paths == [str(fixed)]


@pytest.mark.integration
def test_concurrent_claims_never_hand_out_the_same_row_twice(queue_env) -> None:
    _, pg = queue_env
    shot = FIRST_SHOT + 2
    for i in range(100):
        version = f"v{i:03d}"
        path = _write_file(shot, version, f"{version}.parquet")
        iq.enqueue_version(pg, [path], QUEUE_EXPERIMENT, shot, STAGE, version)

    claimed: list[int] = []
    lock = threading.Lock()
    errors: list[Exception] = []

    def claimer() -> None:
        conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
        try:
            while batch := iq.claim_batch(conn, 3):
                with lock:
                    claimed.extend(it.id for it in batch)
        except Exception as e:  # noqa: BLE001 -- collecting failures across threads
            errors.append(e)
        finally:
            conn.close()

    threads = [threading.Thread(target=claimer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(claimed) == len(set(claimed)) == 100


@pytest.mark.integration
def test_worker_lock_is_exclusive(queue_env) -> None:
    other = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        assert iq.try_acquire_worker_lock(other) is False
    finally:
        other.close()


# ---------------------------------------------------------------------------
# producer + ingest_worker end to end (integration, real DuckLake catalog)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_worker_registers_and_indexes_each_version(queue_env) -> None:
    con, pg = queue_env
    shot = FIRST_SHOT + 10
    by_version = _compact(shot)
    _enqueue_all(pg, shot, by_version)
    all_paths = [str(p) for paths in by_version.values() for p in paths]

    stats = run_until_idle(con, pg)

    assert stats.registered_versions == len(by_version)
    assert stats.failed == 0
    assert set(_statuses(pg, shot).values()) == {"completed"}
    assert _waves_rows(con, shot) == _generated_rows(shot)
    assert iq.active_registered_paths(pg, all_paths) == set(all_paths)
    manifest = _manifest(pg, shot)
    assert len(manifest) == stats.manifest_rows
    assert all(active for *_, active in manifest)
    assert {dv for dv, *_ in manifest} == set(by_version)


@pytest.mark.integration
def test_producer_discards_its_files_for_an_already_ingested_version(queue_env) -> None:
    con, pg = queue_env
    shot = FIRST_SHOT + 11
    assert enqueue_shots([shot], [STAGE], QUEUE_EXPERIMENT, **COMPACT_OPTS) > 0
    run_until_idle(con, pg)
    files_before = sorted(_stage_dir(shot).iterdir())

    assert enqueue_shots([shot], [STAGE], QUEUE_EXPERIMENT, **COMPACT_OPTS) == 0

    assert sorted(_stage_dir(shot).iterdir()) == files_before  # no orphans left behind


@pytest.mark.integration
def test_rerun_never_touches_earlier_versions(queue_env) -> None:
    con, pg = queue_env
    shot = FIRST_SHOT + 12
    _enqueue_all(pg, shot, _compact(shot))
    run_until_idle(con, pg)
    before = _manifest(pg, shot)

    rerun = _write_file(shot, "rerun01", "rerun01.parquet", waves=("thomson_te/A",))
    iq.enqueue_version(pg, [rerun], QUEUE_EXPERIMENT, shot, STAGE, "rerun01")
    assert run_until_idle(con, pg).registered_versions == 1

    after = _manifest(pg, shot)
    assert {r for r in after if r[0] != "rerun01"} == before
    assert all(active for *_, active in after)


@pytest.mark.integration
def test_crash_after_ducklake_commit_does_not_register_twice(queue_env) -> None:
    """The window the design can't close transactionally: DuckLake
    committed, the worker died before indexing + marking completed."""
    con, pg = queue_env
    shot = FIRST_SHOT + 13
    by_version = _compact(shot)
    _enqueue_all(pg, shot, by_version)

    claimed = iq.claim_batch(pg, 100)
    register_files(con, [p for it in claimed for p in it.file_paths])
    # ...worker dies here. Its replacement, on acquiring the lock:
    assert iq.recover_orphaned(pg) == len(by_version)

    stats = run_until_idle(con, pg)

    assert stats.already_registered == len(by_version)
    assert stats.registered_versions == 0
    assert _waves_rows(con, shot) == _generated_rows(shot)
    assert _duplicate_active_paths(pg, shot) == []
    assert set(_statuses(pg, shot).values()) == {"completed"}
    assert _manifest(pg, shot)  # indexed on recovery


@pytest.mark.integration
def test_a_bad_version_fails_alone_and_atomically(queue_env) -> None:
    """A version whose file vanished after enqueueing fails the batch's
    single DuckLake call; the retry must register the good version, and
    must not leave the bad one half-registered."""
    con, pg = queue_env
    shot = FIRST_SHOT + 14
    good = _write_file(shot, "good", "good.parquet")
    bad_kept = _write_file(shot, "bad", "bad_kept.parquet")
    bad_gone = _write_file(shot, "bad", "bad_gone.parquet")
    iq.enqueue_version(pg, [good], QUEUE_EXPERIMENT, shot, STAGE, "good")
    iq.enqueue_version(pg, [bad_kept, bad_gone], QUEUE_EXPERIMENT, shot, STAGE, "bad")
    bad_gone.unlink()

    stats = run_until_idle(con, pg, max_attempts=1)

    assert stats.registered_versions == 1
    assert stats.failed == 1
    assert _statuses(pg, shot) == {"good": "completed", "bad": "failed"}
    assert iq.active_registered_paths(pg, [str(bad_kept)]) == set()
    assert {dv for dv, *_ in _manifest(pg, shot)} == {"good"}
    last_error = _scalar(
        pg,
        f"SELECT last_error FROM {iq.QUEUE_TABLE} WHERE shot = %s AND data_version = 'bad'",
        (shot,),
    )
    assert last_error

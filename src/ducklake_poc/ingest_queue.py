"""Postgres-backed work queue decoupling "a producer wrote a data_version's
final files" from "those files are registered in DuckLake and indexed in
wave_manifest".

The unit of work is one (experiment, shot, stage, data_version) -- one
queue row per version, carrying the paths of its already-compacted
files. Producers (`ducklake-ingest --enqueue`) compact each version
locally, straight into its final files under LAKE_DATA_DIR, before
enqueueing it (see ingest.compact_stage_locally), so nothing downstream
ever rewrites them. A single worker (ingest_worker.py, guarded by a
Postgres advisory lock) registers each version's files in one DuckLake
transaction, then indexes them into wave_manifest and marks the version
completed in one Postgres transaction.

`UNIQUE (experiment, shot, stage, data_version)` is the whole
"ingested only once" invariant: a version's waves are immutable, so a
second enqueue of the same version -- a producer retry, a rerun of the
same calibration -- is a no-op, never a second row.

Why the worker can't wrap claim + ducklake_add_data_files + complete in
one transaction: ducklake_add_data_files runs in DuckDB, whose DuckLake
commit goes through its OWN Postgres session, so it can never be atomic
with the psycopg2 session holding the queue row. The crash window between
"DuckLake committed" and "queue row completed" is closed instead by
`active_registered_paths()`: since a version's files are registered in
one DuckLake transaction, they're either all active or none are, and the
worker checks before registering.

Depends only on config.py, same as the rest of the package's shared base.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from . import config

QUEUE_TABLE = "ingest_queue"

# Session-level advisory lock key held by the one active ingest worker
# for its whole lifetime (see ingest_worker.py). Arbitrary constant, just
# needs to not collide with anything else in this database.
WORKER_LOCK_KEY = 0x6475636B  # "duck"
# Transaction-level advisory lock serializing CREATE TABLE IF NOT EXISTS
# below -- concurrent first-time DDL otherwise races on Postgres's own
# pg_type uniqueness constraint (the same failure mode config.py's
# retry_on_conflict documents for DuckLake's own bootstrap).
_DDL_LOCK_KEY = 0x6475636C

CREATE_QUEUE_SQL = f"""
CREATE TABLE IF NOT EXISTS {QUEUE_TABLE} (
    id           BIGSERIAL PRIMARY KEY,
    experiment   TEXT NOT NULL,
    shot         INTEGER NOT NULL,
    stage        TEXT NOT NULL,
    data_version TEXT NOT NULL,
    file_paths   TEXT[] NOT NULL CHECK (cardinality(file_paths) > 0),  -- absolute
    status       VARCHAR(20) NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    UNIQUE (experiment, shot, stage, data_version)  -- a version is ingested once
);
-- claim_batch's scan
CREATE INDEX IF NOT EXISTS ingest_queue_pending ON {QUEUE_TABLE} (id)
    WHERE status = 'pending';
"""


@dataclass(frozen=True)
class QueueItem:
    id: int
    experiment: str
    shot: int
    stage: str
    data_version: str
    file_paths: list[str]
    attempts: int


def ensure_queue_table(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (_DDL_LOCK_KEY,))
        cur.execute(CREATE_QUEUE_SQL)
    pg_conn.commit()


def read_data_version(path: Path) -> str:
    """The single data_version a compacted file holds, from its Parquet
    footer's column statistics alone -- no data pages read. compact.py
    never lets a file span two versions; this is the enqueue-time check
    that the file really is the version it's being enqueued as."""
    md = pq.read_metadata(path)
    names = [md.schema.column(i).name for i in range(md.num_columns)]
    if "data_version" not in names:
        raise ValueError(f"{path} has no 'data_version' column")
    idx = names.index("data_version")
    seen: set[str] = set()
    for rg in range(md.num_row_groups):
        stats = md.row_group(rg).column(idx).statistics
        if stats is None or not stats.has_min_max:
            raise ValueError(f"{path}: no min/max statistics for 'data_version'")
        seen.update((stats.min, stats.max))
    if len(seen) != 1:
        raise ValueError(f"{path}: expected exactly one data_version, found {sorted(seen)}")
    return seen.pop()


def validate_version_files(
    paths: Sequence[Path], experiment: str, shot: int, stage: str, data_version: str
) -> list[str]:
    """Resolve `paths` to absolute strings, checking that each one is
    really part of (experiment, shot, stage, data_version):
      - it lives under LAKE_DATA_DIR -- wave_manifest stores paths
        relative to it and nginx serves it (see manifest.relative_to_lake_dir);
      - its hive path segments match experiment/shot/stage (DuckLake takes
        the partition values from the path);
      - its footer holds exactly `data_version`."""
    if not paths:
        raise ValueError("a data_version must have at least one file")
    expected = {"experiment": experiment, "shot": str(shot), "stage": stage}
    lake_dir = config.LAKE_DATA_DIR.resolve()
    resolved = []
    for p in paths:
        abs_path = p.resolve()
        if not abs_path.is_relative_to(lake_dir):
            raise ValueError(f"{abs_path} is not under {lake_dir}")
        parsed = config.parse_hive_partitions(abs_path)
        actual = {k: parsed.get(k) for k in expected}
        if actual != expected:
            raise ValueError(
                f"{abs_path} has hive partitions {actual}, expected {expected} "
                "(experiment/shot/stage must match the directory path)"
            )
        found = read_data_version(abs_path)
        if found != data_version:
            raise ValueError(f"{abs_path} holds data_version {found!r}, not {data_version!r}")
        resolved.append(str(abs_path))
    return resolved


def enqueue_version(
    pg_conn,
    paths: Sequence[Path],
    experiment: str,
    shot: int,
    stage: str,
    data_version: str,
) -> bool:
    """Enqueue one data_version's final files as a single row. Returns True
    if it was queued (or a `failed` attempt revived with these files),
    False if the version is already queued, in progress, or ingested --
    in which case the caller's files are redundant and nothing will ever
    register them."""
    resolved = validate_version_files(paths, experiment, shot, stage, data_version)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {QUEUE_TABLE} (experiment, shot, stage, data_version, file_paths)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (experiment, shot, stage, data_version) DO UPDATE SET
                    file_paths = EXCLUDED.file_paths,
                    status = 'pending', attempts = 0, last_error = NULL,
                    claimed_at = NULL, processed_at = NULL
                WHERE {QUEUE_TABLE}.status = 'failed'
                RETURNING id
                """,
                (experiment, shot, stage, data_version, resolved),
            )
            queued = cur.fetchone() is not None
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    return queued


def claim_batch(pg_conn, batch_size: int) -> list[QueueItem]:
    """Atomically move up to `batch_size` pending versions to `processing`
    and return them. SKIP LOCKED isn't needed for correctness under the
    single-worker advisory lock, but keeps this safe (no double-claims)
    if that ever changes. Commits immediately -- no queue lock is held
    across the DuckLake call (see module docstring)."""
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {QUEUE_TABLE}
            SET status = 'processing', claimed_at = now(), attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM {QUEUE_TABLE}
                WHERE status = 'pending'
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, experiment, shot, stage, data_version, file_paths, attempts
            """,
            (batch_size,),
        )
        items = sorted((QueueItem(*row) for row in cur.fetchall()), key=lambda it: it.id)
    pg_conn.commit()
    return items


def recover_orphaned(pg_conn) -> int:
    """Return every `processing` row to `pending`. Only safe to call while
    holding WORKER_LOCK_KEY: the lock guarantees no other live worker, so
    anything still `processing` belongs to one that died mid-batch.
    Re-processing those is safe because the worker re-checks
    active_registered_paths() on every claim."""
    with pg_conn.cursor() as cur:
        cur.execute(
            f"UPDATE {QUEUE_TABLE} SET status = 'pending', claimed_at = NULL "
            "WHERE status = 'processing'"
        )
        n = cur.rowcount
    pg_conn.commit()
    return n


def mark_completed(cur, ids: Sequence[int]) -> None:
    """Uses the CALLER's cursor and transaction (no commit), so the worker
    can mark a version completed atomically with indexing its files into
    wave_manifest."""
    if ids:
        cur.execute(
            f"UPDATE {QUEUE_TABLE} SET status = 'completed', processed_at = now(), "
            "last_error = NULL WHERE id = ANY(%s)",
            (list(ids),),
        )


def release_with_error(pg_conn, ids: Sequence[int], error: str, max_attempts: int) -> None:
    """Record `error` and hand the rows back: to `pending` for another try,
    or to `failed` once they've used up `max_attempts` claims."""
    if not ids:
        return
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {QUEUE_TABLE}
            SET status = CASE WHEN attempts >= %s THEN 'failed' ELSE 'pending' END,
                last_error = %s, claimed_at = NULL
            WHERE id = ANY(%s)
            """,
            (max_attempts, error, list(ids)),
        )
    pg_conn.commit()


def active_registered_paths(pg_conn, paths: Sequence[str]) -> set[str]:
    """Which of `paths` DuckLake currently has as ACTIVE data files, read
    straight from its catalog tables in this same Postgres database.
    ducklake_add_data_files keeps the absolute path it's given
    (`path_is_relative = false`) even for files inside its own DATA_PATH --
    confirmed against a real catalog."""
    if not paths:
        return set()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ducklake_data_file') IS NOT NULL")
        row = cur.fetchone()
        if row is None or not row[0]:
            pg_conn.commit()
            return set()
        cur.execute(
            "SELECT path FROM ducklake_data_file "
            "WHERE end_snapshot IS NULL AND NOT path_is_relative AND path = ANY(%s)",
            (list(paths),),
        )
        found = {r[0] for r in cur.fetchall()}
    pg_conn.commit()
    return found


def try_acquire_worker_lock(pg_conn) -> bool:
    """Session-level: held until `pg_conn` closes (or the backend dies),
    across any number of commits. Postgres releases it automatically if
    the worker process crashes, which is what lets a standby take over."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (WORKER_LOCK_KEY,))
        row = cur.fetchone()
    pg_conn.commit()
    return bool(row and row[0])

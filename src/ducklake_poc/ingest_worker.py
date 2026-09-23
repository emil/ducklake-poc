"""The single DuckLake writer draining ingest_queue (see ingest_queue.py):
registers each queued data_version's already-compacted files, then
indexes them into wave_manifest.

Per version, two transactions and nothing else:
  1. DuckLake: ducklake_add_data_files for all of the version's files --
     so a version becomes queryable all at once, never half-registered.
  2. Postgres: insert the files' wave_manifest rows AND mark the queue
     row completed -- so the HTTP fetch path never serves a version SQL
     can't see yet, and a completed version always has its manifest.
No compaction and no manifest refresh: producers compact each version
before enqueueing it, and a version is ingested exactly once, so its
files -- and therefore their row-group byte offsets -- never change.

Exactly one worker runs at a time, enforced by a Postgres session-level
advisory lock -- extra instances wait as hot standbys and take over
automatically when the active one's connection goes away.

Crash safety: the lock means any row still `processing` at startup
belongs to a dead worker, so it's returned to `pending` immediately; and
every claimed version is first checked against DuckLake's own catalog
(`active_registered_paths`) -- one whose registration committed right
before a crash skips straight to step 2 rather than being registered
twice.

Usage:
    python -m ducklake_poc.ingest_worker               # run forever
    python -m ducklake_poc.ingest_worker --once        # drain until idle, then exit
"""

from __future__ import annotations

import argparse
import io
import signal
import sys
import time
from dataclasses import dataclass, fields
from pathlib import Path

import duckdb
import psycopg2

from . import config
from . import ingest_queue as iq
from .ingest import ensure_table, register_files
from .manifest import CREATE_MANIFEST_SQL, index_files


@dataclass
class PassStats:
    registered_versions: int = 0
    already_registered: int = 0
    failed: int = 0
    manifest_rows: int = 0

    def did_work(self) -> bool:
        return any(getattr(self, f.name) for f in fields(self))

    def add(self, other: PassStats) -> None:
        for f in fields(self):
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))


def _error_text(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"


def _all_paths(items: list[iq.QueueItem]) -> list[str]:
    return [p for it in items for p in it.file_paths]


def _partition_by_registration(
    pg_conn, items: list[iq.QueueItem]
) -> tuple[list[iq.QueueItem], list[iq.QueueItem], list[iq.QueueItem]]:
    """(fully registered, not registered at all, partially registered).
    A version's files are always registered in one DuckLake transaction,
    so "partially" means something outside this worker touched them."""
    active = iq.active_registered_paths(pg_conn, _all_paths(items))
    done, todo, partial = [], [], []
    for it in items:
        hits = sum(p in active for p in it.file_paths)
        (done if hits == len(it.file_paths) else todo if hits == 0 else partial).append(it)
    return done, todo, partial


def _index_and_complete(pg_conn, items: list[iq.QueueItem], stats: PassStats) -> None:
    """Step 2: manifest rows + completion, in one Postgres transaction."""
    if not items:
        return
    try:
        with pg_conn.cursor() as cur:
            n = index_files(cur, [Path(p) for p in _all_paths(items)])
            iq.mark_completed(cur, [it.id for it in items])
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    stats.manifest_rows += n


def _fail(pg_conn, items: list[iq.QueueItem], error: str, max_attempts: int, stats: PassStats):
    iq.release_with_error(pg_conn, [it.id for it in items], error, max_attempts)
    stats.failed += len(items)
    for it in items:
        print(f"  {_scope(it)}: {error}")


def _scope(it: iq.QueueItem) -> str:
    return (
        f"experiment={it.experiment} shot={it.shot} stage={it.stage} data_version={it.data_version}"
    )


def _register_individually(
    con: duckdb.DuckDBPyConnection,
    pg_conn,
    items: list[iq.QueueItem],
    max_attempts: int,
    stats: PassStats,
) -> None:
    """Fallback once a whole batch has failed: one bad version (a missing
    or unreadable file) fails the entire ducklake_add_data_files call, so
    retry version by version to confine the failure to the one at fault.
    Every failure is re-checked against the catalog first, since an error
    surfacing from COMMIT doesn't prove nothing was committed."""
    for it in items:
        try:
            register_files(con, it.file_paths)
        except duckdb.Error as e:
            if iq.active_registered_paths(pg_conn, it.file_paths):
                _index_and_complete(pg_conn, [it], stats)
                stats.registered_versions += 1
            else:
                _fail(pg_conn, [it], _error_text(e), max_attempts, stats)
            continue
        _index_and_complete(pg_conn, [it], stats)
        stats.registered_versions += 1


def run_once(
    con: duckdb.DuckDBPyConnection,
    pg_conn,
    *,
    batch_size: int = 20,
    max_attempts: int = 3,
) -> PassStats:
    """Claim up to `batch_size` versions and take each one to completed (or
    back to pending/failed). All the batch's still-unregistered files go
    to DuckLake in one call; if that fails, version by version."""
    stats = PassStats()
    items = iq.claim_batch(pg_conn, batch_size)
    if not items:
        return stats

    done, todo, partial = _partition_by_registration(pg_conn, items)
    _fail(pg_conn, partial, "only some of this version's files are registered", 0, stats)
    _index_and_complete(pg_conn, done, stats)
    stats.already_registered += len(done)

    # Claimed more than max_attempts times without ever being released
    # with an error: only workers that died holding them can do that, so
    # something about these versions is crashing the worker. Fail them
    # instead of crash-looping forever.
    exhausted = [it for it in todo if it.attempts > max_attempts]
    _fail(pg_conn, exhausted, f"exceeded {max_attempts} attempts", max_attempts, stats)
    todo = [it for it in todo if it.attempts <= max_attempts]
    if not todo:
        return stats

    try:
        register_files(con, _all_paths(todo))
    except duckdb.Error as e:
        print(f"  batch of {len(todo)} version(s) failed ({_error_text(e)}), retrying one by one")
        committed, todo, partial = _partition_by_registration(pg_conn, todo)
        _fail(pg_conn, partial, "only some of this version's files are registered", 0, stats)
        _index_and_complete(pg_conn, committed, stats)
        stats.registered_versions += len(committed)
        _register_individually(con, pg_conn, todo, max_attempts, stats)
        return stats

    _index_and_complete(pg_conn, todo, stats)
    stats.registered_versions += len(todo)
    for it in todo:
        print(f"  {_scope(it)}: registered {len(it.file_paths)} file(s)")
    return stats


def run_until_idle(con: duckdb.DuckDBPyConnection, pg_conn, **kwargs) -> PassStats:
    """Call run_once until an iteration finds nothing to do, returning the
    summed stats. Used by --once (cron-style runs) and the tests."""
    total = PassStats()
    while True:
        s = run_once(con, pg_conn, **kwargs)
        if not s.did_work():
            return total
        total.add(s)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="drain until idle, then exit")
    parser.add_argument(
        "--batch-size", type=int, default=20, help="data_versions per DuckLake registration"
    )
    parser.add_argument(
        "--poll-interval", type=float, default=2.0, help="seconds to sleep when idle"
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="claims per queue row before it's marked failed",
    )
    args = parser.parse_args()
    # Long-running daemon: flush each log line as it happens even when
    # stdout is a file/pipe (e.g. `docker logs`), and treat SIGTERM (what
    # `docker stop` sends) like Ctrl-C so the finally block below runs.
    # Stopping mid-batch is safe either way -- see recover_orphaned.
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    opts = dict(batch_size=args.batch_size, max_attempts=args.max_attempts)

    # This connection holds the worker lock for the process's lifetime --
    # if it drops, the next queue call on it raises and the worker exits,
    # rather than carrying on unlocked alongside a standby that took over.
    pg_conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    iq.ensure_queue_table(pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_MANIFEST_SQL)
    pg_conn.commit()
    announced_standby = False
    while not iq.try_acquire_worker_lock(pg_conn):
        if args.once:
            raise SystemExit("another ingest worker is active; exiting (--once)")
        if not announced_standby:
            print("another ingest worker is active; waiting as standby...")
            announced_standby = True
        time.sleep(args.poll_interval)

    recovered = iq.recover_orphaned(pg_conn)
    if recovered:
        print(f"returned {recovered} orphaned 'processing' row(s) to 'pending'")

    con = config.get_connection()
    ensure_table(con)
    print("ingest worker active")

    try:
        if args.once:
            s = run_until_idle(con, pg_conn, **opts)
            print(
                f"idle: registered {s.registered_versions} version(s) "
                f"({s.already_registered} already registered), {s.failed} failed, "
                f"{s.manifest_rows} wave_manifest row(s) added"
            )
            return
        while True:
            if not run_once(con, pg_conn, **opts).did_work():
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        print("stopping")
    finally:
        con.close()
        pg_conn.close()


if __name__ == "__main__":
    main()

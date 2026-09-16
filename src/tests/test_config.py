"""Unit tests for config.retry_on_conflict -- the shared retry/backoff
helper that makes ensure_table/register_experiment/compact_manifest_friendly
(and the initial catalog ATTACH) safe to call from multiple concurrent
ingest clients. See those functions' docstrings, and CLAUDE.md, for why
this exists: DuckLake's Postgres-backed catalog uses optimistic
concurrency, so concurrent commits to the same table -- even to disjoint
partitions -- routinely lose a race and must retry the whole operation,
not just the commit.
"""

from __future__ import annotations

import duckdb
import pytest

from ducklake_poc import config


def test_retry_on_conflict_returns_result_without_retrying_on_success() -> None:
    calls = []

    def fn():
        calls.append(1)
        return "ok"

    result = config.retry_on_conflict(fn, base_delay=0.001, max_delay=0.001)
    assert result == "ok"
    assert len(calls) == 1


def test_retry_on_conflict_retries_a_transaction_conflict_and_succeeds() -> None:
    calls = []

    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise duckdb.TransactionException(
                'Transaction conflict - attempting to delete from table with index "1" '
                "- but another transaction has inserted into it"
            )
        return "ok"

    result = config.retry_on_conflict(fn, max_attempts=5, base_delay=0.001, max_delay=0.001)
    assert result == "ok"
    assert len(calls) == 3


def test_retry_on_conflict_retries_the_catalog_bootstrap_race() -> None:
    """The one-time race at cold start: concurrent first-ever ATTACHes
    both trying to create DuckLake's own metadata tables in Postgres --
    surfaced as a plain duckdb.Error (not a TransactionException), with a
    Postgres uniqueness-violation message rather than DuckLake's own
    "Transaction conflict" wording."""
    calls = []

    def fn():
        calls.append(1)
        if len(calls) < 2:
            raise duckdb.Error(
                "Failed to initialize DuckLake: ... ERROR:  duplicate key value violates "
                'unique constraint "pg_type_typname_nsp_index"'
            )
        return "ok"

    result = config.retry_on_conflict(fn, max_attempts=5, base_delay=0.001, max_delay=0.001)
    assert result == "ok"
    assert len(calls) == 2


def test_retry_on_conflict_does_not_retry_a_non_retryable_error() -> None:
    calls = []

    def fn():
        calls.append(1)
        raise duckdb.CatalogException("Table with name bogus does not exist!")

    with pytest.raises(duckdb.CatalogException, match="does not exist"):
        config.retry_on_conflict(fn, max_attempts=5, base_delay=0.001, max_delay=0.001)
    assert len(calls) == 1


def test_retry_on_conflict_gives_up_after_max_attempts() -> None:
    calls = []

    def fn():
        calls.append(1)
        raise duckdb.TransactionException("Transaction conflict - always loses")

    with pytest.raises(duckdb.TransactionException, match="Transaction conflict"):
        config.retry_on_conflict(fn, max_attempts=4, base_delay=0.001, max_delay=0.001)
    assert len(calls) == 4


def test_retry_on_conflict_does_not_retry_non_duckdb_errors() -> None:
    """A bug in `fn` itself (e.g. a KeyError) must propagate immediately,
    not get treated as a retryable catalog conflict."""
    calls = []

    def fn():
        calls.append(1)
        raise ValueError("not a duckdb error at all")

    with pytest.raises(ValueError, match="not a duckdb error"):
        config.retry_on_conflict(fn, max_attempts=5, base_delay=0.001, max_delay=0.001)
    assert len(calls) == 1

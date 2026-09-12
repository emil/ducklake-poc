from __future__ import annotations

import uuid
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ducklake_poc import config
from ducklake_poc.range_http_file import RangeHTTPFile

pytestmark = pytest.mark.integration


@pytest.fixture
def served_parquet_file(nginx_available: bool) -> Iterator[tuple[str, pa.Table]]:
    if not nginx_available:
        pytest.skip("nginx not reachable")

    table = pa.table(
        {
            "wave": pa.array(list(range(50)), type=pa.int64()),
            "x": pa.array([float(i) for i in range(50)]),
        }
    )
    out_dir = config.LAKE_DATA_DIR / "_pytest_range_http"
    out_dir.mkdir(parents=True, exist_ok=True)
    # A unique filename per test invocation, rather than a fixed name
    # reused (delete + recreate) across tests -- reusing the same path
    # intermittently served a stale 404 from nginx's bind-mounted view of
    # the filesystem (Docker Desktop's macOS file-sharing layer doesn't
    # always invalidate a path's metadata cache fast enough for an
    # immediate unlink+recreate at that same path to be visible on the
    # next request; a brand new path has no stale cache entry to race
    # against). Reproduced directly: ~50% failure rate reusing one path
    # vs. 0/8 failures with a unique path per write.
    path = out_dir / f"range_test_{uuid.uuid4().hex}.parquet"

    writer = pq.ParquetWriter(str(path), table.schema)
    for i in range(table.num_rows):
        writer.write_table(table.slice(i, 1))  # one row group per row
    writer.close()

    url = f"{config.NGINX_BASE_URL}/lake/_pytest_range_http/{path.name}"
    yield url, table

    path.unlink(missing_ok=True)


def test_range_http_file_size_matches_actual_file(served_parquet_file) -> None:
    url, _table = served_parquet_file
    f = RangeHTTPFile(url)
    assert f.size > 0


def test_range_http_file_read_matches_direct_bytes(served_parquet_file) -> None:
    import requests

    url, _table = served_parquet_file
    expected = requests.get(url).content

    f = RangeHTTPFile(url)
    f.seek(0)
    got = f.read(len(expected))
    assert got == expected


def test_pyarrow_reads_correct_row_group_via_range_requests(served_parquet_file) -> None:
    url, table = served_parquet_file
    f = RangeHTTPFile(url)
    pf = pq.ParquetFile(f, pre_buffer=False)
    assert pf.num_row_groups == table.num_rows  # one row group per row

    target_row_group = 37
    result = pf.read_row_group(target_row_group)
    assert result.column("wave")[0].as_py() == target_row_group


def test_pyarrow_partial_read_issues_few_small_requests(served_parquet_file) -> None:
    """The whole point of RangeHTTPFile: reading one row group out of a
    file with pre_buffer=False should issue a small, constant number of
    HTTP requests (footer + target row group), not fetch the whole file."""
    url, table = served_parquet_file
    f = RangeHTTPFile(url)
    pf = pq.ParquetFile(f, pre_buffer=False)
    f.request_log.clear()
    pf.read_row_group(10)

    assert len(f.request_log) <= 3
    total_bytes_requested = sum(end - start for start, end in f.request_log)
    assert total_bytes_requested < f.size / 2  # much less than the whole file

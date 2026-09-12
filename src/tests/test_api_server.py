"""Integration tests for api_server.py: does the full redirect chain
(Flask -> 302 -> nginx range-proxy -> 206) actually serve the correct
bytes? Requires Postgres, nginx (with the /range-proxy/ location), and
the Flask app itself running on config.API_HOST:API_PORT.

These are skipped (not failed) if the API server isn't reachable, same
as the other integration tests -- there's no in-process fixture that
starts Flask here, since the app needs to run behind the real nginx
config for the redirect chain to mean anything.
"""

from __future__ import annotations

import socket

import pyarrow.parquet as pq
import pytest
import requests

from ducklake_poc import config, local_demo
from ducklake_poc.range_http_file import RangeHTTPFile

pytestmark = pytest.mark.integration


def _api_available() -> bool:
    try:
        with socket.create_connection(
            (config.API_HOST.replace("0.0.0.0", "127.0.0.1"), config.API_PORT), timeout=0.5
        ):
            return True
    except OSError:
        return False


@pytest.fixture
def demo_data(postgres_available: bool, nginx_available: bool, clean_manifest_table):
    if not postgres_available or not nginx_available:
        pytest.skip("Postgres or nginx not reachable")
    if not _api_available():
        pytest.skip(
            f"API server not reachable on {config.API_PORT} -- "
            f"start it with `uv run ducklake-api` before running this test"
        )

    import shutil

    if local_demo.LAKE_DIR.exists():
        shutil.rmtree(local_demo.LAKE_DIR)
    if local_demo.RAW_DIR.exists():
        shutil.rmtree(local_demo.RAW_DIR)

    local_demo.generate_raw_files()
    paths = local_demo.compact_locally()
    local_demo.build_manifest(paths)
    yield

    shutil.rmtree(local_demo.LAKE_DIR, ignore_errors=True)
    shutil.rmtree(local_demo.RAW_DIR, ignore_errors=True)


def _api_url(**params) -> str:
    base = f"http://127.0.0.1:{config.API_PORT}/wave"
    return base + "?" + "&".join(f"{k}={v}" for k, v in params.items())


def test_narrow_window_redirects_to_single_range(demo_data) -> None:
    url = _api_url(
        experiment=local_demo.DEMO_EXPERIMENT,
        shot=local_demo.DEMO_SHOT,
        stage=local_demo.BIG_STAGE,
        wave=local_demo.BIG_WAVE_NAME.replace("/", "%2F"),
        data_version=local_demo.BIG_WAVE_VERSIONS[0],
        x_min=250,
        x_max=251,
    )
    resp = requests.get(url, allow_redirects=False)
    assert resp.status_code == 302
    assert "range-proxy" in resp.headers["Location"]
    assert "r=bytes%3D" in resp.headers["Location"] or "r=bytes=" in resp.headers["Location"]


def test_full_redirect_chain_serves_correct_bytes(demo_data) -> None:
    url = _api_url(
        experiment=local_demo.DEMO_EXPERIMENT,
        shot=local_demo.DEMO_SHOT,
        stage=local_demo.BIG_STAGE,
        wave=local_demo.BIG_WAVE_NAME.replace("/", "%2F"),
        data_version=local_demo.BIG_WAVE_VERSIONS[0],
        x_min=250,
        x_max=251,
    )
    resp = requests.get(url, allow_redirects=True)
    assert resp.status_code == 206
    assert resp.headers["Content-Type"] == "application/vnd.apache.parquet"

    # cross-check against a direct RangeHTTPFile + pyarrow read of the same
    # row group, per the redirect's own X-Row-Group-Ids header
    redirect_resp = requests.get(url, allow_redirects=False)
    row_group_id = int(redirect_resp.headers["X-Row-Group-Ids"].split(",")[0])
    file_url = (
        redirect_resp.headers["Location"]
        .split("?")[0]
        .replace("http://localhost:8080/range-proxy/", f"{config.NGINX_BASE_URL}/")
    )

    f = RangeHTTPFile(file_url)
    pf = pq.ParquetFile(f, pre_buffer=False)
    expected_table = pf.read_row_group(row_group_id)

    # the API-served raw bytes should have exactly the length nginx reported
    assert len(resp.content) == int(resp.headers["Content-Length"])
    assert expected_table.num_rows == 20_000
    assert set(expected_table["wave"].to_pylist()) == {local_demo.BIG_WAVE_NAME}
    assert set(expected_table["data_version"].to_pylist()) == {local_demo.BIG_WAVE_VERSIONS[0]}


def test_whole_wave_query_now_collapses_to_a_single_redirect(demo_data) -> None:
    """With compact.py guaranteeing one (wave, data_version) never spans
    multiple files, even a whole-wave query (previously the 300-triggering
    case) now collapses into a single redirect covering all 100 row
    groups at once."""
    url = _api_url(
        experiment=local_demo.DEMO_EXPERIMENT,
        shot=local_demo.DEMO_SHOT,
        stage=local_demo.BIG_STAGE,
        wave=local_demo.BIG_WAVE_NAME.replace("/", "%2F"),
        data_version=local_demo.BIG_WAVE_VERSIONS[0],
    )
    resp = requests.get(url, allow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["X-Row-Group-Rows"] == "2000000"
    assert len(resp.headers["X-Row-Group-Ids"].split(",")) == 100


def test_missing_params_returns_400(demo_data) -> None:
    resp = requests.get(f"http://127.0.0.1:{config.API_PORT}/wave?shot=999", allow_redirects=False)
    assert resp.status_code == 400


def test_no_match_returns_404(demo_data) -> None:
    url = _api_url(
        experiment=local_demo.DEMO_EXPERIMENT,
        shot=local_demo.DEMO_SHOT,
        stage=local_demo.BIG_STAGE,
        wave="nonexistent",
        data_version=local_demo.BIG_WAVE_VERSIONS[0],
    )
    resp = requests.get(url, allow_redirects=False)
    assert resp.status_code == 404

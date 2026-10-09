"""Tests for lake_client.py.

The HTTP tests run against a small fake lake server in a thread. It
imitates the four nginx and API locations that the client uses:
`/lake-index/`, `/ingest-queue/`, `/api/wave` and `/data/lake/`. It needs
no Postgres and no nginx. The SQL tests use a local DuckLake catalog
file and skip if DuckDB cannot load the ducklake extension.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, unquote, urlsplit

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ducklake_poc import api_server
from ducklake_poc.lake_client import LakeClient, LakeClientError, NotFoundError

EXPERIMENT = "exp-a"
SHOT = 7
STAGE = "mirnov"
DV = "a1b2c3d"
OTHER_DV = "ffff000"
QUEUED_DV = "9999999"


def _write(path: Path, waves: list[tuple[str, int]], dv: str) -> None:
    """Write one file. Each (wave, n_rows) is one row group, x = 0..n-1."""
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("wave", pa.string()),
            ("data_version", pa.string()),
            ("x", pa.float64()),
            ("y", pa.float64()),
        ]
    )
    with pq.ParquetWriter(path, schema) as writer:
        for wave, n in waves:
            writer.write_table(
                pa.table(
                    {
                        "wave": [wave] * n,
                        "data_version": [dv] * n,
                        "x": [float(i) for i in reversed(range(n))],  # unsorted on purpose
                        "y": [float(i) * 2 for i in reversed(range(n))],
                    },
                    schema=schema,
                ),
            )


class FakeLake(BaseHTTPRequestHandler):
    root: Path
    queue: dict[str, dict]
    hits: list[str]

    def log_message(self, format: str, *args) -> None:  # silence
        pass

    def _json(self, code: int, body, headers: dict | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _stage_dir(self, experiment: str, shot: str, stage: str) -> Path:
        return self.root / f"experiment={experiment}" / f"shot={shot}" / f"stage={stage}"

    def do_HEAD(self) -> None:
        self._serve_file(head=True)

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        type(self).hits.append(unquote(url.path))
        path = unquote(url.path)
        if m := re.fullmatch(r"/lake-index/([^/]+)/([^/]+)/([^/]+)/", path):
            d = self._stage_dir(*m.groups())
            if not d.is_dir():
                return self._json(404, {})
            return self._json(
                200,
                [{"name": f.name, "type": "file", "size": f.stat().st_size} for f in d.iterdir()],
            )
        if path == "/ingest-queue/":
            q = parse_qs(url.query)
            key = f"{q['experiment'][0]}/{q['shot'][0]}/{q['stage'][0]}/{q['data_version'][0]}"
            item = type(self).queue.get(key)
            return self._json(200, item) if item else self._json(404, {"error": "not queued"})
        if path == "/api/wave":
            return self._wave(parse_qs(url.query))
        self._serve_file(head=False)

    def _wave(self, q: dict[str, list[str]]) -> None:
        """Find the row groups of the wave, like the manifest lookup does."""
        d = self._stage_dir(q["experiment"][0], q["shot"][0], q["stage"][0])
        matches = []
        for f in sorted(d.glob(f"*__{q['data_version'][0]}__*.parquet")):
            md = pq.ParquetFile(f)
            for rg in range(md.num_row_groups):
                if q["wave"][0] in md.read_row_group(rg, columns=["wave"])["wave"].to_pylist():
                    matches.append((f, rg))
        if not matches:
            return self._json(404, {"error": "no manifest entries"})
        host = f"http://127.0.0.1:{cast(ThreadingHTTPServer, self.server).server_port}"

        def clean(f: Path) -> str:
            rel = f.relative_to(self.root)
            return "/".join(p.split("=", 1)[-1] for p in rel.parts)

        if len({f for f, _ in matches}) == 1:
            f = matches[0][0]
            ids = ",".join(str(rg) for _, rg in matches)
            location = f"{host}/range-proxy/lake/{clean(f)}?r=bytes%3D0-1"
            return self._redirect(location, ids)
        body = {
            "matches": [
                {"file_url": f"http://other-host/data/lake/{clean(f)}", "row_group_id": rg}
                for f, rg in matches
            ]
        }
        self._json(300, body)

    def _redirect(self, location: str, ids: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("X-Row-Group-Ids", ids)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _serve_file(self, head: bool) -> None:
        path = unquote(urlsplit(self.path).path)
        m = re.fullmatch(r"/data/lake/([^/]+)/([^/]+)/([^/]+)/([^/]+)", path)
        f = self._stage_dir(*m.groups()[:3]) / m.group(4) if m else None
        if f is None or not f.is_file():
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = f.read_bytes()
        rng = self.headers.get("Range")
        if rng:
            start, end = (int(v) for v in rng.removeprefix("bytes=").split("-"))
            part = data[start : end + 1]
            self.send_response(206)
        else:
            part = data
            self.send_response(200)
        self.send_header("Content-Length", str(len(data) if head else len(part)))
        self.end_headers()
        if not head:
            self.wfile.write(part)


@pytest.fixture
def lake(tmp_path: Path) -> Iterator[tuple[str, type[FakeLake]]]:
    root = tmp_path / "lake"
    d = root / f"experiment={EXPERIMENT}" / f"shot={SHOT}" / f"stage={STAGE}"
    # version DV: one dedicated 2-row-group wave, plus one packed file
    _write(
        d / f"mirnov_probe_01__{DV}__aaaa1111.parquet",
        [("mirnov/probe_01", 50), ("mirnov/probe_01", 50)],
        DV,
    )
    _write(d / f"multi__{DV}__bbbb2222.parquet", [("mirnov/probe_02", 5)], DV)
    # another version, a temporary file, and a file of the queued version
    _write(d / f"mirnov_probe_01__{OTHER_DV}__cccc3333.parquet", [("mirnov/probe_01", 3)], OTHER_DV)
    (d / ".tmp_deadbeef.parquet").write_bytes(b"partial")
    _write(
        d / f"mirnov_probe_09__{QUEUED_DV}__dddd4444.parquet", [("mirnov/probe_09", 4)], QUEUED_DV
    )

    handler = type("Handler", (FakeLake,), {"root": root, "queue": {}, "hits": []})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", handler
    finally:
        server.shutdown()
        server.server_close()


def _fetch(client: LakeClient, tmp_path: Path, **kw):
    return client.fetch(
        experiment=EXPERIMENT, shot=SHOT, stage=STAGE, dest_dir=tmp_path / "out", **kw
    )


# --- stage download ----------------------------------------------------


def test_fetch_stage_downloads_only_the_requested_version(lake, tmp_path: Path) -> None:
    base, handler = lake
    paths = _fetch(LakeClient(base), tmp_path, data_version=DV)

    assert sorted(p.name for p in paths) == [
        f"mirnov_probe_01__{DV}__aaaa1111.parquet",
        f"multi__{DV}__bbbb2222.parquet",
    ]
    assert sum(pq.read_metadata(p).num_rows for p in paths) == 105


def test_fetch_stage_uses_http_listing_only(lake, tmp_path: Path) -> None:
    """No call to /api/ (the manifest path) and none to the queue."""
    base, handler = lake
    _fetch(LakeClient(base), tmp_path, data_version=DV)

    assert not any(h.startswith(("/api/", "/ingest-queue/")) for h in handler.hits)
    assert handler.hits[0] == f"/lake-index/{EXPERIMENT}/{SHOT}/{STAGE}/"


def test_fetch_stage_skips_temporary_files(lake, tmp_path: Path) -> None:
    base, _ = lake
    paths = _fetch(LakeClient(base), tmp_path, data_version=DV)
    assert not any(p.name.startswith(".") for p in paths)
    assert not list((tmp_path / "out").glob(".*"))  # no leftover .part files


def test_fetch_stage_falls_back_to_the_ingest_queue(lake, tmp_path: Path) -> None:
    base, handler = lake
    version = "1234567"  # not in the directory listing
    d = handler.root / f"experiment={EXPERIMENT}" / f"shot={SHOT}" / f"stage={STAGE}"
    # The file name has no version marker, so the listing finds no file
    # for this version. Only the queue knows the file.
    hidden = d / "hidden_name.parquet"
    _write(hidden, [("mirnov/probe_05", 4)], version)
    handler.queue[f"{EXPERIMENT}/{SHOT}/{STAGE}/{version}"] = {
        "status": "pending",
        "files": [f"{EXPERIMENT}/{SHOT}/{STAGE}/hidden_name.parquet"],
    }

    paths = _fetch(LakeClient(base), tmp_path, data_version=version)

    assert [p.name for p in paths] == ["hidden_name.parquet"]
    assert any(h.startswith("/ingest-queue/") for h in handler.hits)
    assert not any(h.startswith("/api/") for h in handler.hits)


def test_fetch_stage_unknown_version_raises_not_found(lake, tmp_path: Path) -> None:
    base, _ = lake
    with pytest.raises(NotFoundError):
        _fetch(LakeClient(base), tmp_path, data_version="0000000")


def test_fetch_stage_failed_queue_entry_raises(lake, tmp_path: Path) -> None:
    base, handler = lake
    handler.queue[f"{EXPERIMENT}/{SHOT}/{STAGE}/bad0000"] = {"status": "failed", "files": ["x"]}
    with pytest.raises(LakeClientError, match="failed"):
        _fetch(LakeClient(base), tmp_path, data_version="bad0000")


def test_fetch_stage_missing_directory_raises_not_found(lake, tmp_path: Path) -> None:
    base, _ = lake
    with pytest.raises(NotFoundError):
        LakeClient(base).fetch(
            experiment="nope", shot=1, stage="none", data_version=DV, dest_dir=tmp_path
        )


# --- wave download -----------------------------------------------------


def test_fetch_wave_returns_a_valid_sorted_parquet(lake, tmp_path: Path) -> None:
    base, handler = lake
    [path] = _fetch(LakeClient(base), tmp_path, data_version=DV, wave="mirnov/probe_01")

    table = pq.read_table(path)  # a full file with a footer
    assert table.num_rows == 100
    assert set(table["wave"].to_pylist()) == {"mirnov/probe_01"}
    x = table["x"].to_pylist()
    assert x == sorted(x)
    assert any(h == "/api/wave" for h in handler.hits)


def test_fetch_wave_filters_other_waves_out_of_a_packed_file(lake, tmp_path: Path) -> None:
    base, handler = lake
    d = handler.root / f"experiment={EXPERIMENT}" / f"shot={SHOT}" / f"stage={STAGE}"
    _write(d / f"multi__{DV}__ffff6666.parquet", [("mirnov/probe_03", 3)], DV)
    # Build one row group that holds two waves.
    schema = pq.read_schema(d / f"multi__{DV}__bbbb2222.parquet")
    packed = d / f"multi__{DV}__bbbb2222.parquet"
    packed.unlink()
    pq.write_table(
        pa.table(
            {
                "wave": ["mirnov/probe_02"] * 3 + ["mirnov/probe_04"] * 2,
                "data_version": [DV] * 5,
                "x": [2.0, 0.0, 1.0, 1.0, 0.0],
                "y": [0.0] * 5,
            },
            schema=schema,
        ),
        packed,
    )

    [path] = _fetch(LakeClient(base), tmp_path, data_version=DV, wave="mirnov/probe_02")

    table = pq.read_table(path)
    assert table["wave"].to_pylist() == ["mirnov/probe_02"] * 3
    assert table["x"].to_pylist() == [0.0, 1.0, 2.0]


def test_fetch_wave_time_slice_is_half_open(lake, tmp_path: Path) -> None:
    base, _ = lake
    [path] = _fetch(
        LakeClient(base), tmp_path, data_version=DV, wave="mirnov/probe_01", x_min=10, x_max=12
    )
    assert pq.read_table(path)["x"].to_pylist() == [10.0, 10.0, 11.0, 11.0]


def test_fetch_wave_combines_matches_from_several_files(lake, tmp_path: Path) -> None:
    """A 300 answer from the API lists matches in several files."""
    base, handler = lake
    d = handler.root / f"experiment={EXPERIMENT}" / f"shot={SHOT}" / f"stage={STAGE}"
    _write(d / f"multi__{DV}__aaaa0000.parquet", [("mirnov/probe_01", 2)], DV)

    [path] = _fetch(LakeClient(base), tmp_path, data_version=DV, wave="mirnov/probe_01")

    assert pq.read_metadata(path).num_rows == 102


def test_fetch_wave_unknown_wave_raises_not_found(lake, tmp_path: Path) -> None:
    base, _ = lake
    with pytest.raises(NotFoundError):
        _fetch(LakeClient(base), tmp_path, data_version=DV, wave="mirnov/missing")


def test_fetch_rejects_a_half_given_window(tmp_path: Path) -> None:
    client = LakeClient("http://unused")
    with pytest.raises(ValueError, match="together"):
        _fetch(client, tmp_path, data_version=DV, wave="w", x_min=1.0)
    with pytest.raises(ValueError, match="need a wave"):
        _fetch(client, tmp_path, data_version=DV, x_min=1.0, x_max=2.0)


def test_base_url_trailing_slash_is_ignored(lake, tmp_path: Path) -> None:
    base, _ = lake
    paths = _fetch(LakeClient(base + "/"), tmp_path, data_version=DV)
    assert len(paths) == 2


# --- SQL access --------------------------------------------------------


def test_query_without_catalog_raises() -> None:
    with pytest.raises(LakeClientError, match="catalog"):
        LakeClient("http://unused").query("SELECT 1")


@pytest.fixture
def local_catalog(tmp_path: Path) -> str:
    """A DuckLake catalog in a local file, with one table `waves`."""
    try:
        con = duckdb.connect()
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
    except duckdb.Error as e:
        pytest.skip(f"ducklake extension not available: {e}")
    meta = tmp_path / "meta.ducklake"
    con.execute(f"ATTACH 'ducklake:{meta}' AS physics_lake (DATA_PATH '{tmp_path / 'data'}/')")
    con.execute("CREATE TABLE physics_lake.waves (wave VARCHAR, x DOUBLE, y DOUBLE)")
    con.execute("INSERT INTO physics_lake.waves VALUES ('w', 1.0, 10.0), ('w', 2.0, 20.0)")
    con.close()
    return str(meta)


def test_query_runs_sql_on_the_catalog(local_catalog: str) -> None:
    with LakeClient("http://unused", catalog=local_catalog) as client:
        table = client.query("SELECT x, y FROM waves WHERE wave = ? ORDER BY x", ["w"])
    assert table.to_pydict() == {"x": [1.0, 2.0], "y": [10.0, 20.0]}


def test_query_accepts_the_ducklake_prefix(local_catalog: str) -> None:
    with LakeClient("http://unused", catalog=f"ducklake:{local_catalog}") as client:
        assert client.query("SELECT count(*) AS n FROM waves")["n"].to_pylist() == [2]


def test_query_reuses_one_connection(local_catalog: str) -> None:
    client = LakeClient("http://unused", catalog=local_catalog)
    client.query("SELECT 1")
    first = client._con
    client.query("SELECT 2")
    assert client._con is first
    client.close()
    assert client._con is None


# --- API endpoint for the queue ------------------------------------------


def test_ingest_queue_endpoint_returns_clean_relative_paths(monkeypatch, tmp_path: Path) -> None:
    from ducklake_poc import config

    monkeypatch.setattr(config, "LAKE_DATA_DIR", tmp_path)
    absolute = tmp_path / "experiment=e" / "shot=3" / "stage=s" / "f__v__1.parquet"
    absolute.parent.mkdir(parents=True)
    absolute.touch()
    monkeypatch.setattr(api_server, "_queued_version", lambda *a: ("pending", [str(absolute)]))

    resp = api_server.app.test_client().get(
        "/ingest_queue?experiment=e&shot=3&stage=s&data_version=v"
    )

    assert resp.status_code == 200
    assert resp.get_json() == {"status": "pending", "files": ["e/3/s/f__v__1.parquet"]}


def test_ingest_queue_endpoint_unknown_version_is_404(monkeypatch) -> None:
    monkeypatch.setattr(api_server, "_queued_version", lambda *a: None)
    resp = api_server.app.test_client().get(
        "/ingest_queue?experiment=e&shot=3&stage=s&data_version=v"
    )
    assert resp.status_code == 404


def test_ingest_queue_endpoint_missing_params_is_400() -> None:
    assert api_server.app.test_client().get("/ingest_queue?shot=3").status_code == 400

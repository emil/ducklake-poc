"""Client for the wave lake. It reads the lake over HTTP, and it can also
run SQL through DuckDB and DuckLake.

Create the client with the base URL of the lake. The base URL is the
nginx server (for example `http://localhost:8080`). Give the DuckLake
catalog string as a second argument only if you need SQL.

    client = LakeClient("http://localhost:8080")
    client = LakeClient("http://localhost:8080", catalog="postgres:dbname=ducklake_catalog")

Two access paths:

1. `fetch()` downloads Parquet files over HTTP.
   - With `wave`, it downloads one wave (optionally a time slice).
     The API service looks up the row groups in wave_manifest. The
     client reads only those row groups and writes a valid Parquet file.
   - Without `wave`, it downloads the complete stage. This path uses
     nginx only. It does not use wave_manifest or any Parquet footer.
     A processing pipeline can use it for a stage that is not yet
     ingested into DuckLake.

2. `query()` runs SQL on the DuckLake table `waves`. It needs the
   `catalog` argument. The rules in QUERYING.md apply to every query.

Examples:

    # Download a complete stage. The files go to ./out.
    client = LakeClient("http://localhost:8080")
    paths = client.fetch(
        experiment="campaign-2026a", shot=1, stage="mirnov", data_version="a1b2c3d",
        dest_dir="out",
    )

    # Download one wave.
    paths = client.fetch(
        experiment="campaign-2026a", shot=1, stage="mirnov", data_version="a1b2c3d",
        wave="mirnov/probe_03", dest_dir="out",
    )

    # Download a time slice of one wave: x in [250, 251).
    paths = client.fetch(
        experiment="campaign-2026a", shot=1, stage="mirnov", data_version="a1b2c3d",
        wave="mirnov/probe_03", x_min=250, x_max=251, dest_dir="out",
    )

    # Run SQL. Always pin experiment, shot, stage and data_version.
    catalog = "postgres:dbname=ducklake_catalog host=localhost user=ducklake"
    with LakeClient("http://localhost:8080", catalog=catalog) as client:
        table = client.query(
            "SELECT x, y FROM waves WHERE experiment = ? AND shot = ? AND stage = ? "
            "AND data_version = ? AND wave = ? AND x >= ? AND x < ? ORDER BY x",
            ["campaign-2026a", 1, "mirnov", "a1b2c3d", "mirnov/probe_03", 250, 251],
        )

Stage download, step by step:

    1. GET /lake-index/<experiment>/<shot>/<stage>/   (nginx JSON listing)
       Keep the files of the requested data_version.
    2. If there is no such file, GET /ingest-queue/?experiment=..&shot=..
       &stage=..&data_version=..   (nginx proxies this to the API, which
       reads the ingest_queue table).
    3. GET /data/lake/<experiment>/<shot>/<stage>/<file> for each file.

The Parquet files keep their names. Each name has the form
`<wave-or-multi>__<data_version>__<hash>.parquet`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from . import config
from .range_http_file import RangeHTTPFile

DEFAULT_TIMEOUT_SECONDS = 60.0
_CHUNK_BYTES = 1 << 20


class LakeClientError(Exception):
    """The lake could not give the requested data."""


class NotFoundError(LakeClientError):
    """The requested stage, wave or data_version does not exist."""


class LakeClient:
    """Read the wave lake over HTTP, and over SQL when a catalog is given.

    `base_url` is the nginx URL of the lake, for example
    `http://localhost:8080`.

    `catalog` is optional. It is the DuckLake catalog string for the
    DuckDB ATTACH statement. The prefix `ducklake:` is optional. Example:
    `postgres:dbname=ducklake_catalog host=localhost user=ducklake`.
    Without it, `query()` raises an error.
    """

    def __init__(
        self,
        base_url: str,
        catalog: str | None = None,
        *,
        session: requests.Session | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.catalog = catalog
        self.timeout = timeout
        self._session = session or requests.Session()
        self._con: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # HTTP access
    # ------------------------------------------------------------------

    def fetch(
        self,
        *,
        experiment: str,
        shot: int,
        stage: str,
        data_version: str,
        wave: str | None = None,
        x_min: float | None = None,
        x_max: float | None = None,
        dest_dir: str | os.PathLike[str] = "lake_downloads",
    ) -> list[Path]:
        """Download Parquet files and return their local paths.

        Without `wave`: download every file of the stage for the given
        `data_version`. This uses nginx only (see the module docstring).
        The files can hold several waves.

        With `wave`: download that wave as one Parquet file in `dest_dir`.
        The file has the columns `wave`, `data_version`, `x` and `y`,
        sorted by `x`. `x_min` and `x_max` (given together) keep only the
        samples with `x_min <= x < x_max`.

        `data_version` is required. A data_version has no order, so the
        client never selects "the latest" for you.

        Raises NotFoundError if the lake has no such data.
        """
        if (x_min is None) != (x_max is None):
            raise ValueError("x_min and x_max must be given together")
        if wave is None and x_min is not None:
            raise ValueError("x_min and x_max need a wave")
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        if wave is None:
            return self._fetch_stage(experiment, shot, stage, data_version, out_dir)
        return [
            self._fetch_wave(experiment, shot, stage, data_version, wave, x_min, x_max, out_dir)
        ]

    def list_stage_files(
        self, *, experiment: str, shot: int, stage: str, data_version: str
    ) -> list[str]:
        """Return the URLs of the files of one stage version. No file is
        downloaded. The lookup uses nginx only. If the stage directory
        has no file of this version, it uses the ingest queue."""
        names = self._list_stage_directory(experiment, shot, stage, data_version)
        if names:
            prefix = f"{self._lake_url(experiment, str(shot), stage)}/"
            return [prefix + quote(n) for n in names]

        clean_paths = self._list_queued_files(experiment, shot, stage, data_version)
        return [self._lake_url(*p.split("/")) for p in clean_paths]

    def _fetch_stage(
        self, experiment: str, shot: int, stage: str, data_version: str, out_dir: Path
    ) -> list[Path]:
        urls = self.list_stage_files(
            experiment=experiment, shot=shot, stage=stage, data_version=data_version
        )
        return [self._download(url, out_dir) for url in urls]

    def _list_stage_directory(
        self, experiment: str, shot: int, stage: str, data_version: str
    ) -> list[str]:
        """File names in the stage directory that belong to `data_version`.
        An empty list means "none found" (also for a missing directory)."""
        url = (
            f"{self.base_url}/lake-index/"
            f"{quote(experiment, safe='')}/{shot}/{quote(stage, safe='')}/"
        )
        resp = self._session.get(url, timeout=self.timeout)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        marker = f"__{data_version}__"
        return sorted(
            entry["name"]
            for entry in resp.json()
            if entry.get("type") == "file"
            and entry["name"].endswith(".parquet")
            # A name that starts with "." is a temporary file of a
            # compaction that is not complete.
            and not entry["name"].startswith(".")
            and marker in entry["name"]
        )

    def _list_queued_files(
        self, experiment: str, shot: int, stage: str, data_version: str
    ) -> list[str]:
        """Clean relative paths of the files that the ingest queue holds
        for this version."""
        resp = self._session.get(
            f"{self.base_url}/ingest-queue/",
            params={
                "experiment": experiment,
                "shot": shot,
                "stage": stage,
                "data_version": data_version,
            },
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            raise NotFoundError(
                f"no files for experiment={experiment} shot={shot} stage={stage} "
                f"data_version={data_version}: not in the stage directory, not in the ingest queue"
            )
        resp.raise_for_status()
        body = resp.json()
        if body["status"] == "failed":
            raise LakeClientError(
                f"the ingest of experiment={experiment} shot={shot} stage={stage} "
                f"data_version={data_version} failed"
            )
        return list(body["files"])

    def _fetch_wave(
        self,
        experiment: str,
        shot: int,
        stage: str,
        data_version: str,
        wave: str,
        x_min: float | None,
        x_max: float | None,
        out_dir: Path,
    ) -> Path:
        params: dict[str, Any] = {
            "experiment": experiment,
            "shot": shot,
            "stage": stage,
            "wave": wave,
            "data_version": data_version,
        }
        if x_min is not None:
            params.update(x_min=x_min, x_max=x_max)

        # The API answers with a redirect to a raw row-group byte range.
        # That range is not a valid Parquet file (it has no footer), so the
        # client does not follow it. It reads the file name and the
        # row-group numbers from the answer, and then reads those row
        # groups through the Parquet footer.
        resp = self._session.get(
            f"{self.base_url}/api/wave",
            params=params,
            allow_redirects=False,
            timeout=self.timeout,
        )
        targets: dict[str, list[int]] = {}
        if resp.status_code == 302:
            location = resp.headers["Location"]
            file_url = self._data_url(
                unquote(urlsplit(location).path).removeprefix("/range-proxy/")
            )
            ids = [int(i) for i in resp.headers["X-Row-Group-Ids"].split(",")]
            targets[file_url] = ids
        elif resp.status_code == 300:
            # The matches span several files, or have a gap.
            for match in resp.json()["matches"]:
                file_url = self._data_url(
                    unquote(urlsplit(match["file_url"]).path).split("/data/", 1)[1]
                )
                targets.setdefault(file_url, []).append(match["row_group_id"])
        elif resp.status_code == 404:
            raise NotFoundError(resp.json().get("error", "wave not found"))
        else:
            resp.raise_for_status()
            raise LakeClientError(f"unexpected status {resp.status_code} from /api/wave")

        tables = [
            pq.ParquetFile(RangeHTTPFile(url, self._session), pre_buffer=False).read_row_groups(
                sorted(set(ids))
            )
            for url, ids in targets.items()
        ]
        table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        table = _select_wave(table, wave, x_min, x_max)

        dest = out_dir / f"{config.safe_filename(wave)}__{data_version}.parquet"
        _write_atomically(table, dest)
        return dest

    def _data_url(self, clean_path: str) -> str:
        """URL of a file below /data/ from a clean path such as
        `lake/<experiment>/<shot>/<stage>/<file>`."""
        return f"{self.base_url}/data/{quote(clean_path)}"

    def _lake_url(self, *segments: str) -> str:
        return f"{self.base_url}/data/lake/" + "/".join(quote(s, safe="") for s in segments)

    def _download(self, url: str, out_dir: Path) -> Path:
        dest = out_dir / unquote(url.rsplit("/", 1)[1])
        tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
        try:
            with self._session.get(url, stream=True, timeout=self.timeout) as resp:
                if resp.status_code == 404:
                    raise NotFoundError(f"file not found: {url}")
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(_CHUNK_BYTES):
                        f.write(chunk)
            os.replace(tmp, dest)
        finally:
            tmp.unlink(missing_ok=True)
        return dest

    # ------------------------------------------------------------------
    # SQL access
    # ------------------------------------------------------------------

    def query(self, sql: str, params: Sequence[Any] | None = None) -> pa.Table:
        """Run `sql` on the DuckLake catalog and return an Arrow table.
        Use `?` placeholders and give the values in `params`.

        The first call opens the DuckDB connection and attaches the
        catalog. The attached catalog is the default, so write `waves`,
        not `physics_lake.waves`.

        Follow QUERYING.md. Pin experiment, shot, stage and one
        data_version. The result is fully in memory, so reduce it in SQL
        first.

        Raises LakeClientError if the client has no catalog.
        """
        con = self._connection()
        return con.execute(sql, list(params) if params is not None else None).to_arrow_table()

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self.catalog is None:
            raise LakeClientError("query() needs a catalog: LakeClient(base_url, catalog=...)")
        if self._con is None:
            con = duckdb.connect()
            con.execute("INSTALL ducklake")
            con.execute("LOAD ducklake")
            if "postgres:" in self.catalog:
                con.execute("INSTALL postgres")
                con.execute("LOAD postgres")
            attach = (
                self.catalog if self.catalog.startswith("ducklake:") else f"ducklake:{self.catalog}"
            )
            # ATTACH does not accept bound parameters. Double each single
            # quote so that the catalog string stays one SQL literal.
            literal = attach.replace("'", "''")
            con.execute(f"ATTACH '{literal}' AS {config.DUCKLAKE_NAME}")
            con.execute(f"USE {config.DUCKLAKE_NAME}")
            self._con = con
        return self._con

    # ------------------------------------------------------------------
    # Life cycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the DuckDB connection (if open) and the HTTP session."""
        if self._con is not None:
            self._con.close()
            self._con = None
        self._session.close()

    def __enter__(self) -> LakeClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _select_wave(table: pa.Table, wave: str, x_min: float | None, x_max: float | None) -> pa.Table:
    """Keep the rows of `wave` (and of the x window, if given), sorted by
    x. The row groups that the manifest finds can hold other waves, because
    small waves share a packed row group."""
    keep = pc.field("wave") == wave
    if x_min is not None:
        keep = keep & (pc.field("x") >= x_min) & (pc.field("x") < x_max)
    return table.filter(keep).sort_by("x")


def _write_atomically(table: pa.Table, dest: Path) -> None:
    tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
    try:
        pq.write_table(table, tmp)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)

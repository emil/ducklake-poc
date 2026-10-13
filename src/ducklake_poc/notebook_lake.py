"""Shared lake setup for the marimo notebooks.

A notebook calls `open_lake()` and gets one `NotebookLake` object. The object
hides the difference between the two data sources:

- **Real catalog.** Give `catalog` (a DuckLake attach string). Queries run
  through `LakeClient`. The pinned versions come from the `ingest_queue`
  table.
- **Demo lake.** Give no `catalog`. The function writes synthetic shots with
  the production generator and compacts them with the production compactor.
  Then it exposes them through a DuckDB view with the same columns as the
  DuckLake table. Each query runs on the view without a change.

The demo lake uses the same directory and the same READY signature as
`notebooks/querying_the_lake.py`. If both notebooks use the same shots and
stages, they share one demo lake.

The view in demo mode is a glob over the demo directory. It is acceptable
for a few shots only. Do not use a glob view at production scale
(QUERYING.md rule 6).
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Callable, Iterable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from . import config, synthetic
from .compact import stream_compact_to_files
from .parquet_encoding import open_parquet_writer
from .synthetic import wave_to_sample_table

DEFAULT_DEMO_SHOTS = tuple(range(1, 13))
DEFAULT_DEMO_ROOT = Path(tempfile.gettempdir()) / "ducklake_marimo_demo"


@dataclass
class NotebookLake:
    """A lake that a notebook can query. Create it with `open_lake()`."""

    mode: str
    experiment: str
    shots: list[int]
    stages: list[str]
    pinned: dict[tuple[int, str], str]
    _query: Callable[[str, Sequence[object] | None], pd.DataFrame]
    # (shot, stage) pairs that have several versions, and the versions of each. The pin of
    # such a pair is the first version by name. A data_version has no order.
    ambiguous: dict[tuple[int, str], list[str]] = field(default_factory=dict)
    # All known versions of each (shot, stage).
    versions: dict[tuple[int, str], list[str]] = field(default_factory=dict)
    # Demo lake only: the DuckDB connection and the directory of the compacted files.
    # They let a notebook read Parquet footers. The real catalog has `wave_manifest`.
    connection: duckdb.DuckDBPyConnection | None = None
    lake_dir: Path | None = None
    _waves: dict[tuple[int, str, str], list[str]] = field(default_factory=dict)

    def q(self, sql: str, params: Sequence[object] | None = None) -> pd.DataFrame:
        """Run SQL on the lake. Use `?` to bind values."""
        return self._query(sql, params)

    def pin(self, shot: int, stage: str) -> str:
        """Return the data_version of one (shot, stage). Raise KeyError if it is missing."""
        return self.pinned[(int(shot), stage)]

    def stages_of(self, shot: int) -> list[str]:
        """Return the stages that have data for one shot."""
        return sorted(st for s, st in self.pinned if s == int(shot))

    def versions_of(self, shot: int, stage: str) -> list[str]:
        """Return all data_versions of one (shot, stage). The first is the pinned one."""
        found = self.versions.get((int(shot), stage), [])
        first = self.pinned.get((int(shot), stage))
        return ([first] if first in found else []) + [v for v in found if v != first]

    def list_waves(self, shot: int, stage: str, version: str | None = None) -> list[str]:
        """Return the waves of one (shot, stage) at one version (default: the pinned one).

        All partition filters are pinned, so the query reads one partition.
        """
        version = version or self.pin(shot, stage)
        key = (int(shot), stage, version)
        if key not in self._waves:
            df = self.q(
                f"SELECT DISTINCT wave FROM {config.TABLE_NAME} "
                "WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? "
                "ORDER BY wave",
                [self.experiment, int(shot), stage, version],
            )
            self._waves[key] = df["wave"].tolist()
        return self._waves[key]


def pins_cte(shots: Iterable[int], stage: str, pin: Callable[[int, str], str]) -> tuple[str, list]:
    """Build the `pins` CTE for a cross-shot query (QUERYING.md, "Cross-shot work").

    Return the SQL fragment and its parameters. The CTE holds one
    (shot, data_version) pair for each shot, so each shot uses exactly one version.
    """
    pairs = [(int(s), pin(int(s), stage)) for s in shots]
    values = ", ".join("(?, ?)" for _ in pairs)
    params: list = [value for pair in pairs for value in pair]
    return f"pins(shot, data_version) AS (VALUES {values})", params


def _build_demo_lake(
    root: Path, experiment: str, shots: Sequence[int], stages: Sequence[str]
) -> None:
    """Write the demo shots, then compact them with the production code."""
    raw = root / "raw"
    lake = root / "lake"
    for directory in (raw, lake):
        if directory.exists():
            shutil.rmtree(directory)
    for shot in shots:
        for stage in stages:
            out = raw / f"experiment={experiment}" / f"shot={shot}" / f"stage={stage}"
            out.mkdir(parents=True, exist_ok=True)
            for rec in synthetic.generate_stage(shot=shot, stage=stage, experiment=experiment):
                table = wave_to_sample_table(rec)
                name = f"{config.safe_filename(rec.wave)}__{rec.data_version}.parquet"
                writer = open_parquet_writer(str(out / name), table.schema)
                writer.write_table(table)
                writer.close()
    source_sql = (
        f"SELECT * FROM read_parquet('{raw}/**/*.parquet', hive_partitioning=true) "
        "ORDER BY experiment, shot, stage, wave, data_version, x"
    )
    stream_compact_to_files(duckdb.connect(), source_sql, lake)


def _open_demo(
    root: Path,
    experiment: str,
    shots: Sequence[int],
    stages: Sequence[str],
    build_context: Callable[[], AbstractContextManager[object]] | None,
    setup_sql: Sequence[str] = (),
) -> NotebookLake:
    ready = root / "READY"
    signature = f"{list(stages)}|{list(shots)}"
    if not ready.exists() or ready.read_text() != signature:
        with build_context() if build_context is not None else nullcontext():
            _build_demo_lake(root, experiment, shots, stages)
        ready.write_text(signature)

    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW {config.TABLE_NAME} AS "
        f"SELECT * FROM read_parquet('{root / 'lake'}/**/*.parquet', hive_partitioning=true)"
    )
    for statement in setup_sql:
        con.execute(statement)
    # The demo has no timestamps. A version has no order, so pin the first by name.
    # This is the same shortcut as `on_ambiguous="first"`.
    found = con.execute(
        f"SELECT DISTINCT shot, stage, data_version FROM {config.TABLE_NAME}"
    ).fetchall()
    versions = {key: sorted(v) for key, v in group_versions(found).items()}
    pinned = {key: v[0] for key, v in versions.items()}

    def query(sql: str, params: Sequence[object] | None) -> pd.DataFrame:
        return con.execute(sql, list(params or [])).df()

    lake = _finish(
        "local demo lake (synthetic shots, no services needed)", experiment, pinned, query
    )
    lake.versions = versions
    lake.connection = con
    lake.lake_dir = root / "lake"
    return lake


def group_versions(rows: Iterable[tuple[int, str, str]]) -> dict[tuple[int, str], list[str]]:
    """Group the data_versions of each (shot, stage). Keep the order of the rows."""
    found: dict[tuple[int, str], list[str]] = {}
    for shot, stage, data_version in rows:
        versions = found.setdefault((int(shot), str(stage)), [])
        if str(data_version) not in versions:
            versions.append(str(data_version))
    return found


def pin_newest(rows: Iterable[tuple[int, str, str]]) -> dict[tuple[int, str], str]:
    """Pin the first data_version of each (shot, stage).

    The rows come newest first (`ingest_queue.processed_at DESC`). A data_version has
    no order, so only the sort of the rows can tell which one is the newest.
    """
    pinned: dict[tuple[int, str], str] = {}
    for shot, stage, data_version in rows:
        pinned.setdefault((int(shot), str(stage)), str(data_version))
    return pinned


AmbiguousPolicy = Literal["error", "first"]


def pin_unique(
    rows: Iterable[tuple[int, str, str]], on_ambiguous: AmbiguousPolicy = "error"
) -> tuple[dict[tuple[int, str], str], dict[tuple[int, str], list[str]]]:
    """Pin the data_version of each (shot, stage). Do not guess without a rule.

    Without timestamps, nothing tells which of several versions is the right one
    (QUERYING.md, rule 2). `on_ambiguous` decides what to do for a pair with more
    than one version:

    - `"error"`: raise a ValueError that lists the first pairs.
    - `"first"`: pin the first version by name. This is a demo shortcut. Return the
      ambiguous pairs, so the caller can show them.

    Return the pins and the ambiguous pairs.
    """
    found: dict[tuple[int, str], list[str]] = {}
    for shot, stage, data_version in rows:
        found.setdefault((int(shot), str(stage)), []).append(str(data_version))
    found = {key: sorted(set(v)) for key, v in found.items()}
    ambiguous = {key: v for key, v in found.items() if len(v) > 1}
    if ambiguous and on_ambiguous == "error":
        shown = "; ".join(
            f"shot {s} stage {st}: {', '.join(v)}" for (s, st), v in sorted(ambiguous.items())[:5]
        )
        raise ValueError(
            f"{len(ambiguous)} (shot, stage) pairs have more than one data_version, and the "
            "catalog has no timestamp to choose the newest. Ask which version to use, "
            "ingest with --enqueue (it records processed_at), or open the lake with "
            f'on_ambiguous="first". First pairs: {shown}'
        )
    return {key: v[0] for key, v in found.items()}, ambiguous


def _pin_versions(
    cur, experiment: str, on_ambiguous: AmbiguousPolicy
) -> tuple[
    dict[tuple[int, str], str],
    dict[tuple[int, str], list[str]],
    dict[tuple[int, str], list[str]],
]:
    """Find the data_version of each (shot, stage) from Postgres. Do not scan `waves`.

    The queue path records `ingest_queue.processed_at`, so the newest version is known.
    The direct path does not create `ingest_queue`. Then `wave_manifest` lists the
    versions. A (shot, stage) with several versions follows `on_ambiguous`.

    Return the pins, the ambiguous pairs, and all versions of each pair.
    """
    cur.execute("SELECT to_regclass('ingest_queue') IS NOT NULL")
    has_queue = bool(cur.fetchone()[0])
    if has_queue:
        cur.execute(
            "SELECT shot, stage, data_version FROM ingest_queue "
            "WHERE experiment = %s AND status = 'completed' "
            "ORDER BY processed_at DESC NULLS LAST",
            (experiment,),
        )
        rows = cur.fetchall()
        if rows:
            return pin_newest(rows), {}, group_versions(rows)
    cur.execute("SELECT to_regclass('wave_manifest') IS NOT NULL")
    if not cur.fetchone()[0]:
        raise RuntimeError(
            "The catalog has no data: neither `ingest_queue` nor `wave_manifest` exists. "
            "Ingest first, for example `uv run ducklake-ingest --shots 3`."
        )
    cur.execute(
        "SELECT DISTINCT shot, stage, data_version FROM wave_manifest WHERE experiment = %s",
        (experiment,),
    )
    rows = cur.fetchall()
    pinned, ambiguous = pin_unique(rows, on_ambiguous)
    versions = {key: sorted(v) for key, v in group_versions(rows).items()}
    return pinned, ambiguous, versions


def _open_catalog(
    catalog: str, lake_url: str, experiment: str, on_ambiguous: AmbiguousPolicy
) -> NotebookLake:
    import psycopg2

    from .lake_client import LakeClient

    client = LakeClient(lake_url, catalog=catalog)
    with psycopg2.connect(**config.pg_dsn_for_psycopg2()) as pg, pg.cursor() as cur:
        pinned, ambiguous, versions = _pin_versions(cur, experiment, on_ambiguous)

    def query(sql: str, params: Sequence[object] | None) -> pd.DataFrame:
        return client.query(sql, list(params) if params else None).to_pandas()

    lake = _finish(f"DuckLake catalog `{catalog}`", experiment, pinned, query)
    lake.ambiguous = ambiguous
    lake.versions = versions
    return lake


def _finish(
    mode: str,
    experiment: str,
    pinned: dict[tuple[int, str], str],
    query: Callable[[str, Sequence[object] | None], pd.DataFrame],
) -> NotebookLake:
    return NotebookLake(
        mode=mode,
        experiment=experiment,
        shots=sorted({s for s, _ in pinned}),
        stages=sorted({st for _, st in pinned}),
        pinned=pinned,
        _query=query,
    )


def open_lake(
    catalog: str | None = None,
    *,
    lake_url: str | None = None,
    experiment: str | None = None,
    demo_shots: Sequence[int] = DEFAULT_DEMO_SHOTS,
    demo_stages: Sequence[str] | None = None,
    demo_root: Path = DEFAULT_DEMO_ROOT,
    on_ambiguous: AmbiguousPolicy = "error",
    demo_setup_sql: Sequence[str] = (),
    build_context: Callable[[], AbstractContextManager[object]] | None = None,
) -> NotebookLake:
    """Open the real catalog if `catalog` is set. Otherwise open the demo lake.

    `on_ambiguous` is for the real catalog only. It decides what to do when a (shot, stage)
    has several versions and no timestamp: raise an error, or pin the first by name.

    `demo_setup_sql` runs on the connection of the demo lake. A notebook can use it to
    install helper functions that the real catalog already has.

    `build_context` is a factory for a context manager. It wraps the build of
    the demo lake, and only when the build is necessary. A notebook can pass
    `lambda: mo.status.spinner(title="...")`.
    """
    experiment = experiment or synthetic.DEFAULT_EXPERIMENT
    if catalog:
        return _open_catalog(catalog, lake_url or config.NGINX_HOST_URL, experiment, on_ambiguous)
    stages = list(demo_stages) if demo_stages is not None else sorted(synthetic.DIAGNOSTICS)
    return _open_demo(demo_root, experiment, demo_shots, stages, build_context, demo_setup_sql)

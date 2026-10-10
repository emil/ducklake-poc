# CLAUDE.md

This file gives guidance to Claude Code (claude.ai/code) for work on code in this repository.

## What this is

This is a proof-of-concept for a physics wave-data lake. It uses DuckLake (catalog on Postgres)
with **one row for each sample** (not for each wave), so that Parquet row groups can be pruned by
time. Compaction gives large waves their own dedicated row groups. A `wave_manifest` Postgres
table lets a plain HTTP byte-range fetch read a narrow time slice of a wave without DuckDB.

`README.md` has the design rationale: schema, partitioning, compaction algorithm, encoding
choices, and the queue-driven ingest. Read it before you make a non-trivial change to
`compact.py`, `manifest.py`, or the hive-partitioning layout. It explains *why* each part is
as it is, not only what it does.

## Commands

```bash
docker compose up -d              # postgres:5432 + nginx:8080 (+ api, not tested in dev sandboxes)
cd src
uv sync                           # creates .venv, installs dependencies from uv.lock
uv sync --dev                     # + pytest, ruff, basedpyright, pyinstrument
uv sync --extra notebooks         # + jupyter, jupysql, marimo (notebooks/)

uv run marimo run ../notebooks/querying_the_lake.py    # slide deck: querying and plotting (--edit to edit)
LAKE_CATALOG="postgres:dbname=ducklake_catalog host=localhost user=ducklake" \
    uv run marimo run ../notebooks/querying_the_lake.py  # same deck on the real catalog (default: local demo lake)
uv run ruff check .               # lint
uv run ruff format .              # format
uv run basedpyright .             # type check
uv run pytest                     # all tests (unit + integration; integration skips if services are down)
uv run pytest tests/test_compact.py::test_iter_wave_groups_reassembles_a_wave_split_across_internal_batches  # one test
uv run pytest -m "not integration"  # unit tests only, no Postgres or nginx needed
uv run pyinstrument -m ducklake_poc.local_demo   # profile a run

uv run ducklake-reset --yes       # drop and recreate the Postgres catalog AND clear data/raw + data/lake
uv run python -m ducklake_poc.local_demo   # standalone demo, no DuckLake or catalog attach needed
uv run ducklake-ingest --shot 1 [--stage mirnov]   # write raw files + compact + refresh manifest
uv run ducklake-ingest --shot 1 --enqueue          # compact each data_version locally + enqueue (no DuckLake access)
uv run ducklake-ingest-worker [--once]            # single writer: register queued versions + index wave_manifest
uv run ducklake-compact --strategy manifest --shot 1 --stage mirnov   # compact again, standalone
uv run ducklake-manifest data/lake/experiment=.../shot=.../stage=.../<file>.parquet  # index one file again
uv run ducklake-fetch-wave --experiment campaign-2026a --shot 1 --stage mirnov \
    --wave mirnov/probe_03 --data-version a1b2c3d --x-min 250 --x-max 251
uv run ducklake-api               # Flask API on :5001, redirects into the nginx range-proxy
```

To run one pytest test, use `uv run pytest tests/test_foo.py::test_name`, or filter with `-k`.

**Always reset the catalog when you clear or regenerate the local `data/` files alone** (for
example between test runs). The Postgres catalog and the files on disk are two separate sources
of truth. If they differ, each query that uses a stale reference fails with
`IO Error: Cannot open file ... No such file or directory`. `ducklake-reset --yes` resets both
together. It also ends open connections (for example a running `ducklake-api`) before it drops
the database.

## Editor type-checking setup

There are **two** basedpyright configs. Keep them the same:
- `[tool.basedpyright]` in `src/pyproject.toml` (used by `uv run basedpyright`).
- `pyrightconfig.json` in the repo root (used by editors whose workspace root is the repo root).
  A root `pyrightconfig.json` completely overrides each `pyproject.toml` config in the tree.

The root config points `venvPath` and `venv` to `src/.venv`. It excludes `notebooks/`. Without
this, native `.ipynb` support reports the JupySQL `%%sql` magic and `get_ipython()` as errors.

## Architecture

### Module dependency shape

`config.py` is the shared base (paths, DB connection, `safe_filename`). All other modules
depend on it. It does not depend on any other module of the package. This lets `ingest.py` call
`compact.py` directly (to compact immediately after ingest) without a circular import.
`get_connection()` and `safe_filename()` were in `ingest.py`. They moved to `config.py` for this
reason.

```
config.py  <-- synthetic.py, parquet_encoding.py, manifest.py, compact.py, ingest.py,
               fetch_wave.py, api_server.py, range_http_file.py, reset.py,
               ingest_queue.py
ingest.py  --> compact.py --> manifest.py   (ingest calls compact calls manifest, for each (shot, stage))
ingest.py  --> ingest_queue.py              (--enqueue: compact locally, then enqueue each version)
ingest_worker.py --> ingest_queue.py + ingest.py (register_files) + manifest.py (index_files)
fetch_wave.py, api_server.py --> manifest.py (read wave_manifest) + range_http_file.py (HTTP range reads)
```

### The pipeline: ingest -> compact -> manifest -> fetch

1. **`synthetic.py`** generates realistic per-sample rows for named tokamak diagnostics
   (`DIAGNOSTICS` and `STAGE_PROFILE` dicts). The sample-rate profiles are different for each
   diagnostic *stage* (for example Mirnov at 4 MHz and Thomson at about 1 kHz pulsed). They are
   not an arbitrary "rare big wave" setting. `data_version` (git-hash-style) models
   recalibration reruns. The generator draws it once for each `(shot, stage)` and applies it to
   the whole diagnostic group.
2. **`ingest.py`** writes one raw Parquet file for each `(wave, data_version)` to
   `data/raw/experiment=.../shot=.../stage=.../`. It registers the files with one batched
   `ducklake_add_data_files` call for each `(shot, stage)`. By default, it then calls
   `compact.py` and `manifest.py` immediately after that group registers. Each diagnostic group
   is queryable when its own ingestion ends. It does not wait for other stages. `--no-compact`
   skips this step. `--stage` limits one call to one diagnostic group. Without `--stage`, the
   call compacts the whole shot again. Use that only for periodic maintenance, not for normal
   ingest.
3. **`compact.py`** is the core part that enforces the invariants. It groups the sorted row
   stream again into complete `(stage, wave, data_version)` chunks (`_iter_wave_groups`). This
   does not depend on batch boundaries. Then it does these steps:
   - It gives an oversized wave its own dedicated, contiguous row groups. It never mixes them
     with the rows of another wave, not even in part.
   - It packs small channel-versions together only in the same `(stage, data_version)`.
   - It treats `stage` and `data_version` as hard partition boundaries at **both** the row-group
     level and the **file** level. A file rollover happens between complete waves, never in the
     middle of a wave. One oversized wave always lands in exactly one file, also when it is
     larger than `--target-file-size-bytes` ("one wave, one file"). This lets the HTTP API
     combine row groups into one redirect safely.

   The compactor writes a file under a temporary name. When it closes the file, it knows if the
   file has one wave or packed waves. Then it renames the file to
   `<wave-or-"multi">__<data_version>__<hash>.parquet`.
4. **`manifest.py`** reads the Parquet footer directly (`parquet_metadata()`, not through
   DuckLake). It takes the `x_min` and `x_max` of each row group and the byte offsets. It parses
   `experiment`, `shot` and `stage` from the hive-style **path** (not from columns, see below).
   Then it writes the rows into the `wave_manifest` Postgres table.
5. **`fetch_wave.py`** and **`api_server.py`** find the matching row groups in `wave_manifest`
   and fetch only those bytes. The function `plan_response()` in `api_server.py` is a pure
   function (tested in `test_api_server_logic.py`). It combines contiguous row groups into one
   redirect when this is safe. It returns `300 Multiple Choices` when the matches are in more
   than one file, or when `row_group_id` has a gap. A gap means that the bytes between the row
   groups belong to a row group of a *different* wave. This is a correctness guard, not only an
   optimization.

### Queue-driven ingest (`ingest_queue.py`, `ingest_worker.py`)

This path is the alternative to the in-process register and compact of step 2. It depends on one
invariant: the pipeline ingests a `(stage, data_version)` exactly once, and it never changes.

- **Producers** (`ducklake-ingest --enqueue`) compact each version locally
  (`ingest.compact_stage_locally`: plain DuckDB and `stream_compact_to_files`). They write
  straight to final files in `data/lake/`. Then they enqueue **one row for each version**
  (`file_paths TEXT[]`, `UNIQUE (experiment, shot, stage, data_version)`).
- **The worker** is one `ducklake-ingest-worker` (Postgres advisory lock; extra instances are
  hot standbys). It registers the files of a version in one DuckLake transaction. Then it
  inserts their `wave_manifest` rows and marks the version completed in one Postgres
  transaction.
- This path does not compact again and does not refresh the manifest, because it never rewrites
  files.

The README section "Queue-driven ingest" explains the key invariants in full:
- The DuckLake commit and the queue update cannot be one transaction (DuckLake commits through
  its own Postgres session). Before the worker registers files, it checks which files of the
  versions are already *active* in `ducklake_data_file`. This check prevents a double
  registration after a crash. Do not remove it.
- The worker writes the manifest rows only after the DuckLake registration, together with the
  completion. Because of this, the HTTP path never serves a version that SQL cannot see.
- Files stay unregistered in the DuckLake `DATA_PATH` until the worker registers them. For this
  reason, `ducklake_delete_orphaned_files` needs an `older_than` value above the queue lag.

### Physical layout: why `experiment`, `shot` and `stage` are hive path segments, not columns

`experiment`, `shot` and `stage` exist only in the directory path
(`experiment=X/shot=Y/stage=Z/<file>.parquet`). They are never physical columns in the Parquet
files. `wave`, `data_version`, `x` and `y` are the only physical columns (`PHYSICAL_COLUMNS` in
`compact.py`). This is a requirement, not a style choice. `ducklake_add_data_files` fails with
"invalid partition value for the table configuration" if a `PARTITIONED BY` column is *also* in
the data of the file. The README section "Schema" gives the upstream DuckLake issue. In queries,
these three columns behave like ordinary columns. This is true through the DuckLake table and
through `read_parquet(..., hive_partitioning=true)`. nginx has a rewrite rule
(`nginx/nginx.conf`). It translates clean URLs (`.../lake/<experiment>/<shot>/<stage>/<file>`)
to the real `key=value` path before it serves the file. This includes the `/range-proxy/`
loopback.

The boundary logic of `compact.py` guarantees that a file never mixes stages or data_versions.
The `PARTITIONED BY` setting of DuckLake and `ducklake_merge_adjacent_files` do not guarantee
this. The design does not use the partitioning machinery of DuckLake for this guarantee on
purpose. No documentation confirms that `merge_adjacent_files` respects partition boundaries.

### HTTP fetch path

```
client -> GET /wave?...            (Flask ducklake-api, :5001)
       <- 302 Location: nginx/range-proxy/...?r=bytes=A-B
client -> GET that Location        (nginx, :8080)
       <- 206 Partial Content
```

The nginx `/range-proxy/` location proxies to the nginx `/data/` location of the same server. It
adds a `Range` header that it builds from the `?r=` query argument. It does not use the `Range`
header that the real client sent. With this method, the server tells nginx which bytes to serve
on the *first* request of the client. At that time, the client cannot know the byte offsets. The
returned bytes are a raw row-group fragment. They are not a standalone valid `.parquet` file (no
footer). If a consumer needs a read that follows the specification, see the docstring of
`fetch_wave.py` for the validated-read alternative.

### Column encoding policy (`parquet_encoding.py`)

Raw ingestion and compaction share this policy. It does not use the Parquet defaults.
- Dictionary encoding for `wave` and `data_version` (few distinct values, repeated often).
- `BYTE_STREAM_SPLIT` for the float64 columns `x` and `y`. The separation of byte planes
  compresses smooth signal data much better than interleaved doubles.
- ZSTD level 2. This level is selected for low CPU cost on the write side during continuous
  ingestion. It is not selected for the highest ratio.

In Parquet, dictionary encoding and `BYTE_STREAM_SPLIT` exclude each other for one column. The
module asserts that the two lists never overlap.

## Querying the lake

Each query on `waves`, `wave_manifest`, or the fetch path must follow the guidelines below. This
applies to SQL files, notebooks, Python and ad hoc queries. The guidelines are for the
production scale target (about 10^12 rows, about 3,000 waves for each shot in about 40 stages).
At this scale, queries without filters, and queries that mix versions, are wrong or do not
finish.

@QUERYING.md

## Test tiers

- **Unit** (`test_synthetic.py`, `test_parquet_encoding.py`, `test_compact.py`,
  `test_manifest.py`, `test_fetch_wave.py`): DuckDB and the local filesystem only. No services
  are necessary.
- **Integration** (`test_range_http_file.py`, `test_local_demo_integration.py`,
  `test_api_server.py`, `test_reset.py`, most of `test_ingest_queue.py`; marked
  `@pytest.mark.integration`): these tests need a reachable Postgres or nginx, or both.
  `conftest.py` probes for both. It calls `pytest.skip` if they are not reachable. Because of
  this, `uv run pytest` passes on a machine without `docker compose up`.

## Known gaps (see the README section "What is not verified")

- The real `ducklake_add_data_files` call on an attached DuckLake catalog is not confirmed again
  after the hive-partitioning fix. The environment of the build had no access to
  `extensions.duckdb.org`.
- `manifest.py` stores `file_path` as an **absolute path from the machine that ran the
  compaction**. The dockerized `api` service can fail to resolve it if its filesystem layout is
  different. The manifest does not yet store paths relative to `LAKE_DATA_DIR`.
- The `/range-proxy/` nginx location is not `internal`, on purpose. Because of this, anyone can
  guess the byte ranges. This is acceptable for the non-sensitive data of this POC. It is not
  acceptable for data with access control, unless you add a control such as a signed query
  string.

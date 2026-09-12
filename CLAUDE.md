# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A proof-of-concept for a physics wave-data lake: DuckLake (catalog on Postgres) with
**per-sample-row** ingestion (one row per sample, not per wave, so Parquet row groups can be
pruned by time), compaction that gives large waves their own dedicated row groups, and a
`wave_manifest` Postgres table that lets a plain HTTP byte-range fetch pull a narrow time-slice
of a wave without going through DuckDB at all.

The full design rationale (schema decisions, partitioning, compaction algorithm, encoding
choices, and a "lessons learned" section documenting real bugs hit while building this) lives in
`README.md` — read it before making non-trivial changes to `compact.py`, `manifest.py`, or the
hive-partitioning layout; it explains *why* things are the way they are, not just what they do.

## Commands

```bash
docker compose up -d              # postgres:5432 + nginx:8080 (+ api, untested in dev sandboxes)
cd src
uv sync                           # creates .venv, installs deps from uv.lock
uv sync --dev                     # + pytest, ruff, basedpyright, pyinstrument
uv sync --extra notebooks         # + jupyter/jupysql for notebooks/wave_exploration.ipynb

uv run ruff check .               # lint
uv run ruff format .              # format
uv run basedpyright .             # type check
uv run pytest                     # full test suite (unit + integration, integration auto-skips if services aren't up)
uv run pytest tests/test_compact.py::test_iter_wave_groups_reassembles_a_wave_split_across_internal_batches  # single test
uv run pytest -m "not integration"  # unit tests only, no Postgres/nginx needed
uv run pyinstrument -m ducklake_poc.local_demo   # profile a run

uv run ducklake-reset --yes       # drop+recreate Postgres catalog AND clear data/raw + data/lake together
uv run python -m ducklake_poc.local_demo   # self-contained demo, no DuckLake/catalog attach needed
uv run ducklake-ingest --shot 1 [--stage mirnov]   # write raw files + compact + refresh manifest
uv run ducklake-compact --strategy manifest --shot 1 --stage mirnov   # re-compact standalone
uv run ducklake-manifest data/lake/experiment=.../shot=.../stage=.../<file>.parquet  # re-index one file
uv run ducklake-fetch-wave --experiment campaign-2026a --shot 1 --stage mirnov \
    --wave mirnov/probe_03 --data-version a1b2c3d --x-min 250 --x-max 251
uv run ducklake-api               # Flask API on :5001, redirects into nginx's range-proxy
```

Run a single pytest test with `uv run pytest tests/test_foo.py::test_name` or filter with `-k`.

**Always reset the catalog when local `data/` files were cleared or regenerated independently**
(e.g. between iterative test runs) — the Postgres catalog is a separate source of truth from the
files on disk, and letting them drift produces `IO Error: Cannot open file ... No such file or
directory` on any query touching the stale reference. `ducklake-reset --yes` resets both sides
together atomically (and terminates lingering connections, e.g. a running `ducklake-api`, before
dropping the database).

## Editor type-checking setup

There are **two** basedpyright configs and they must stay in sync: `src/pyproject.toml`'s
`[tool.basedpyright]` (used by `uv run basedpyright`) and the root-level `pyrightconfig.json`
(used by editors whose workspace root is the repo root, since a root `pyrightconfig.json`
completely overrides any `pyproject.toml` config found elsewhere in the tree). The root config
points `venvPath`/`venv` at `src/.venv` and excludes `notebooks/` (native `.ipynb` support
otherwise flags JupySQL's `%%sql` magic and `get_ipython()` as errors).

## Architecture

### Module dependency shape

`config.py` is the shared base (paths, DB connection, `safe_filename`) — everything else depends
on it, and it depends on nothing else in this package. This exists specifically so `ingest.py`
can call directly into `compact.py` (to compact immediately after ingesting) without a circular
import: `get_connection()` and `safe_filename()` used to live in `ingest.py` and were moved to
`config.py` for exactly this reason.

```
config.py  <-- synthetic.py, parquet_encoding.py, manifest.py, compact.py, ingest.py,
               fetch_wave.py, api_server.py, range_http_file.py, reset.py
ingest.py  --> compact.py --> manifest.py   (ingest calls compact calls manifest, per (shot, stage))
fetch_wave.py, api_server.py --> manifest.py (read wave_manifest) + range_http_file.py (HTTP range reads)
```

### The pipeline: ingest -> compact -> manifest -> fetch

1. **`synthetic.py`** generates realistic per-sample rows for named tokamak diagnostics
   (`DIAGNOSTICS`/`STAGE_PROFILE` dicts) — sample-rate profiles vary genuinely by diagnostic
   *stage* (e.g. Mirnov at 4MHz vs. Thomson at ~1kHz pulsed), not an arbitrary "rare big wave"
   knob. `data_version` (git-hash-style) models recalibration reruns, drawn once per `(shot,
   stage)` and applied across the whole diagnostic group.
2. **`ingest.py`** writes one raw Parquet file per `(wave, data_version)` to
   `data/raw/experiment=.../shot=.../stage=.../`, registers them via a single batched
   `ducklake_add_data_files` call per `(shot, stage)`, then — by default, per `(shot, stage)`
   immediately after that group registers — calls into `compact.py` and `manifest.py` so each
   diagnostic group is queryable the moment its own ingestion finishes, without waiting on other
   stages. `--no-compact` skips this; `--stage` scopes a single call to one diagnostic group
   (omitting it recompacts the whole shot, which is for periodic maintenance, not normal ingest).
3. **`compact.py`** is the core invariant-enforcing piece. It re-groups the sorted row stream
   into complete `(stage, wave, data_version)` chunks (`_iter_wave_groups`) regardless of
   underlying batch boundaries, then:
   - gives an oversized wave its own dedicated, contiguous row groups (never mixed with another
     wave's rows, even partially),
   - packs small channel-versions together only within the same `(stage, data_version)`,
   - treats `stage` and `data_version` as hard partition boundaries at **both** the row-group and
     the **file** level — a file's rollover happens between complete waves, never mid-wave, so a
     single oversized wave always lands in exactly one file even if that exceeds
     `--target-file-size-bytes` ("one wave, one file" — this is what lets the HTTP API safely
     combine multiple row groups into one redirect).
   Files are written under a temp name and renamed on close (`<wave-or-"multi">__<data_version>__<hash>.parquet`)
   once it's known whether the file ended up single-wave or packed.
4. **`manifest.py`** reads the Parquet footer directly (`parquet_metadata()`, bypassing DuckLake)
   for row-group-level `x_min`/`x_max` and byte offsets, and parses `experiment`/`shot`/`stage`
   from the hive-style **path** (not columns — see below) into the `wave_manifest` Postgres table.
5. **`fetch_wave.py`** / **`api_server.py`** look up matching row groups in `wave_manifest` and
   fetch only those bytes. `api_server.py`'s `plan_response()` (pure function, tested in
   `test_api_server_logic.py`) combines contiguous row groups into a single redirect when safe,
   and falls back to `300 Multiple Choices` when matches span multiple files or have a
   `row_group_id` gap — a gap means the bytes in between belong to a *different* wave's row
   group, so it's a correctness guard, not just an optimization.

### Physical layout: why `experiment`/`shot`/`stage` are hive path segments, not columns

`experiment`/`shot`/`stage` exist only in the directory path
(`experiment=X/shot=Y/stage=Z/<file>.parquet`), never as physical columns inside the Parquet
files — `wave`/`data_version`/`x`/`y` are the only physical columns (`compact.py`'s
`PHYSICAL_COLUMNS`). This is required, not a style choice: `ducklake_add_data_files` fails with
"invalid partition value for the table configuration" if a `PARTITIONED BY` column is *also*
present in the file's own data (see `README.md`'s "Physical layout & partitioning" section for
the upstream DuckLake issue this matches). Through the DuckLake table or
`read_parquet(..., hive_partitioning=true)`, these three still behave like ordinary columns when
queried. nginx has a rewrite rule (`nginx/nginx.conf`) that translates clean URLs
(`.../lake/<experiment>/<shot>/<stage>/<file>`) to the real `key=value` path before serving,
including through the `/range-proxy/` loopback.

`compact.py`'s own boundary logic — not DuckLake's `PARTITIONED BY` or
`ducklake_merge_adjacent_files` — is what actually guarantees a file never mixes stages or
data_versions; that guarantee was deliberately not delegated to DuckLake's built-in partitioning
machinery, since no documentation confirms `merge_adjacent_files` respects partition boundaries.

### HTTP fetch path

```
client -> GET /wave?...            (Flask ducklake-api, :5001)
       <- 302 Location: nginx/range-proxy/...?r=bytes=A-B
client -> GET that Location        (nginx, :8080)
       <- 206 Partial Content
```

nginx's `/range-proxy/` location proxies back to its own `/data/` location, injecting a `Range`
header built from the `?r=` query arg (not whatever `Range` header the real client sent) — this
is how the server tells nginx which bytes to serve on the client's *first* request, before the
client could possibly know byte offsets itself. The bytes returned are a raw row-group fragment,
not a standalone valid `.parquet` file (no footer); see `fetch_wave.py`'s docstring for the
validated-read alternative when a consumer needs a spec-compliant read.

### Column encoding policy (`parquet_encoding.py`)

Shared by both raw ingestion and compaction, not left to Parquet defaults: dictionary encoding
for `wave`/`data_version` (low cardinality, repeats a lot), `BYTE_STREAM_SPLIT` for the `x`/`y`
float64 columns (byte-plane separation compresses smoothly-varying signal data much better than
interleaved doubles), ZSTD level 2 (chosen for write-side CPU cost during continuous ingestion,
not maximum ratio). Dictionary encoding and `BYTE_STREAM_SPLIT` are mutually exclusive per Parquet
column, so the module asserts the two lists never overlap.

## Test tiers

- **Unit** (`test_synthetic.py`, `test_parquet_encoding.py`, `test_compact.py`, `test_manifest.py`,
  `test_fetch_wave.py`): DuckDB + local filesystem only, no services required.
- **Integration** (`test_range_http_file.py`, `test_local_demo_integration.py`,
  `test_api_server.py`, `test_reset.py`; marked `@pytest.mark.integration`): need a reachable
  Postgres and/or nginx. `conftest.py` probes for both and `pytest.skip`s cleanly if unreachable,
  so `uv run pytest` passes on a machine without `docker compose up`.

## Known gaps (see README's "What's not verified" section)

- The actual `ducklake_add_data_files` call against a real attached DuckLake catalog has not
  been re-confirmed after the hive-partitioning fix (no `extensions.duckdb.org` access in the
  environment this was built in).
- `manifest.py` stores `file_path` as an **absolute path from whichever machine ran compaction**;
  the dockerized `api` service could fail resolving it if its filesystem layout differs from
  wherever compaction ran. Not yet switched to storing paths relative to `LAKE_DATA_DIR`.
- The `/range-proxy/` nginx location is deliberately not `internal`, so byte ranges are
  effectively guessable/public — acceptable for this POC's non-sensitive data, not for anything
  access-controlled without adding e.g. a signed query string.

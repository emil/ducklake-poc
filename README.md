# DuckLake wave-data POC

This is a proof-of-concept for a physics wave-data lake. It uses DuckLake (catalog on Postgres)
with **one row for each sample**, compaction that respects wave boundaries, and a
`wave_manifest` Postgres table. The manifest lets a plain HTTP byte-range fetch read a narrow
time slice of a wave without DuckDB.

## Schema

```
experiment    VARCHAR   -- campaign, for example "campaign-2026a"
shot          INTEGER   -- shot or discharge id
stage         VARCHAR   -- diagnostic group, for example "mirnov", "thomson", "rogowski"
wave          VARCHAR   -- channel, for example "mirnov/probe_03"
data_version  VARCHAR   -- git-hash-style id of the calibration or processing run
                           that made this copy of the wave
x             DOUBLE    -- elapsed milliseconds since the start of the shot
y             DOUBLE    -- amplitude sample
```

The table has one row for each **sample**, not for each wave. The uniqueness and clustering key
is `(experiment, shot, stage, wave, data_version)`. The table is `SORTED BY (..., x)`. Because
of this, the `x` min and max statistics of each row group are tight. A narrow time window then
reads a few row groups, not the whole wave.

`data_version` models recalibration. The pipeline processes a diagnostic group again from time
to time. Each rerun makes a new, independent copy of every channel in the group. The lake keeps
old versions and does not overwrite them. `data_version` is a **hard partition boundary** with
`stage`, at the row-group level and at the file level. A query always needs exactly one
version. The name of a file must fully describe its contents.

Only `wave`, `data_version`, `x` and `y` are physical Parquet columns (`PHYSICAL_COLUMNS` in
`compact.py`). `experiment`, `shot` and `stage` are only in the hive-style directory path
(`experiment=X/shot=Y/stage=Z/...`). They are not columns in the file. This is a requirement:
`ducklake_add_data_files` rejects a file whose data has a value for a `PARTITIONED BY` column
(see [ducklake#791](https://github.com/duckdb/ducklake/issues/791)). For queries, all seven
columns behave as normal columns. This is true through the DuckLake table, and also through
`read_parquet(..., hive_partitioning=true)` on local files. nginx rewrites a clean URL
(`.../lake/<experiment>/<shot>/<stage>/<file>`) to the real `key=value` path before it serves
the file (`nginx/nginx.conf`).

## Layout

```
docker-compose.yml     postgres + nginx + api (Flask)
nginx/nginx.conf       static file server with range support + /range-proxy/ loopback
data/raw/              per-wave Parquet files (before compaction)
data/lake/             files owned by DuckLake + compacted output
notebooks/wave_exploration.ipynb      JupySQL: plots, trapezoidal AUC, pruning statistics
notebooks/querying_the_lake.py        marimo slide deck: query rules, plots, multi-shot work
src/ducklake_poc/
  config.py            shared paths and connection settings, safe_filename
  synthetic.py         generator of per-sample rows for realistic diagnostics
  ingest.py            raw files -> batched add_data_files -> compact -> manifest
                       (with --enqueue: compact locally -> ingest_queue)
  ingest_queue.py      Postgres work queue: one row for each (experiment, shot, stage, data_version)
  ingest_worker.py     the single DuckLake writer: registers and indexes each queued version
  compact.py           compaction that respects wave boundaries (see Pipeline, below)
  manifest.py          Parquet footer + hive path -> wave_manifest
  parquet_encoding.py  shared column-encoding policy
  range_http_file.py   seekable file-like object over HTTP Range
  fetch_wave.py        manifest lookup -> curl or pyarrow, whole wave or x range
  api_server.py        HTTP API: lookup -> redirect into the nginx range-proxy
  lake_client.py       client: HTTP fetch and SQL
  local_demo.py        standalone demo, no DuckLake or docker needed
  reset.py             drops and recreates the catalog, clears data/
```

## Setup

```bash
docker compose up -d
cd src && uv sync
uv run ducklake-reset --yes    # always run this after you clear or regenerate data/ alone
```

`CLAUDE.md` has the full command reference (ingest, compact, manifest, fetch, api, test tiers,
and editor type-checking setup). To run the marimo slide deck, run
`uv sync --extra notebooks`. Then run `uv run marimo run ../notebooks/querying_the_lake.py`.

## Pipeline: ingest -> compact -> manifest -> fetch

1. **`ingest.py`** writes one raw Parquet file for each `(wave, data_version)`. It registers
   each `(shot, stage)` group with one batched `ducklake_add_data_files` call. Then it
   compacts the group and refreshes the manifest immediately. Each diagnostic group is
   queryable when its own ingestion ends. It does not wait for other stages.
2. **`compact.py`** groups the sorted row stream again into complete
   `(stage, wave, data_version)` chunks. This does not depend on batch boundaries
   (`_iter_wave_groups`). Then it does these steps:
   - It gives a very large wave its own dedicated, contiguous row groups. It never mixes them
     with the rows of another wave.
   - It packs small channel-versions together only in the same `(stage, data_version)`.
   - It treats `stage` and `data_version` as hard boundaries at the row-group level and at the
     **file** level. A file rollover for size happens only between complete waves, never in the
     middle of a wave. One `(wave, data_version)` always lands in exactly one file ("one wave,
     one file"). This lets the HTTP API combine row groups into one redirect safely.

   The compactor writes a file under a temporary name. When it closes the file, it knows if the
   file has one wave or packed waves. Then it renames the file to
   `<wave-or-"multi">__<data_version>__<hash>.parquet`.
3. **`manifest.py`** reads the Parquet footer directly (`parquet_metadata()`). It takes the
   `x_min` and `x_max` of each row group and the byte offsets. It parses `experiment`, `shot`
   and `stage` from the hive path. Then it upserts the rows into `wave_manifest`.
4. **`fetch_wave.py`** and **`api_server.py`** find the matching row groups and fetch only those
   bytes. The function `plan_response()` in `api_server.py` combines contiguous row groups into
   one redirect when this is safe. It returns `300 Multiple Choices` when the matches are in
   more than one file, or when `row_group_id` has a gap. A gap means that the bytes between the
   row groups belong to a row group of a *different* wave. This is a correctness guard, not only
   an optimization.

## Queue-driven ingest (`--enqueue` + `ducklake-ingest-worker`)

The pipeline ingests a `(stage, data_version)` exactly once, and it never changes. The queue
path uses this fact. A producer compacts each version *before* it adds the version to the
queue. The producer writes straight to the final files. Nothing downstream rewrites a file. The
pipeline indexes each file into `wave_manifest` exactly once and never refreshes it. The one
DuckLake writer registers only finished files. Producers do not compete for the catalog. These
conflicts are the ones that `config.retry_on_conflict` hides on the direct path.

```
producer (ducklake-ingest --enqueue, many at the same time)
  generate the per-wave files of the stage into a temporary staging directory
  for each data_version: stream_compact_to_files (plain DuckDB, no DuckLake)
                    -> final files in data/lake/experiment=/shot=/stage=/
  INSERT one queue row for each version (file_paths[])
    UNIQUE (experiment, shot, stage, data_version): row exists -> no-op,
    and the producer deletes the files that it just wrote; 'failed' -> revived

ducklake-ingest-worker (exactly one active: pg_try_advisory_lock; the others wait as standby)
  when it gets the lock: each 'processing' row belongs to a dead worker -> back to 'pending'
  loop:
    claim <= batch_size versions (UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING)
    versions whose files are already active in DuckLake -> go directly to indexing
    ducklake_add_data_files(all their files)  -- one DuckLake transaction
      on failure: retry each version alone, so a bad version fails alone
    for each batch, one Postgres transaction: INSERT wave_manifest rows + mark completed
```

**One row for each version.** `UNIQUE (experiment, shot, stage, data_version)` states "ingested
once" directly. A producer retry, or a rerun of the same calibration, cannot queue a version
twice. Before the producer inserts a row, it checks each file:
- The file is under `LAKE_DATA_DIR` (the manifest paths are relative to it, and nginx serves
  it).
- The file is in the correct hive directory.
- The file holds exactly that `data_version`, according to its Parquet footer.

**Atomic versions.** The worker registers the files of a version in one DuckLake transaction.
A version becomes queryable all at once. The worker inserts the manifest rows in the same
Postgres transaction that marks the version completed. Because of this, the HTTP fetch path
never serves a version that SQL cannot see. A completed version always has its manifest. A rerun
is a new version with new files. The files and manifest rows of earlier versions are never
changed.

**Why not one transaction for everything?** `ducklake_add_data_files` runs in DuckDB. DuckLake
commits through its *own* Postgres session. Because of this, the commit cannot be atomic with
the psycopg2 session that holds the queue row. The worker closes the crash window between the
two commits. Before it registers files, it checks the DuckLake catalog (`ducklake_data_file`, in
the same database). The files of a version are all active (skip to indexing) or none are
active. `ducklake_add_data_files` keeps the absolute path that it receives
(`path_is_relative = false`), also for files inside its own `DATA_PATH`. This was confirmed
against a real catalog.

Limits:
- **Orphan cleanup:** Files stay unregistered in the DuckLake `DATA_PATH` from the time that a
  producer writes them until the worker registers them. `ducklake_delete_orphaned_files` can
  delete them. If you run it, set `older_than` to a value much longer than the worst-case lag of
  the queue. A producer can crash after it writes files and before it enqueues them. Such files
  are real orphans. The cleanup is for those files.
- **Keep the paths apart:** The direct path (`ducklake-ingest` without `--enqueue`, and
  `ducklake-compact`) compacts inside DuckLake and rewrites files. Do not use it on versions
  that the queue ingested.
- **Deletes of tiny files:** For very small files, DuckLake records a `DELETE` as an *inlined*
  delete and keeps the data file active. Run `ducklake_rewrite_data_files` before you remove
  such files from disk (the test cleanup does this).

## HTTP API

```
client -> GET /wave?...            (Flask ducklake-api, :5001)
       <- 302 Location: nginx/range-proxy/...?r=bytes=A-B
client -> GET that Location        (nginx, :8080)
       <- 206 Partial Content
```

The nginx `/range-proxy/` location proxies to the nginx `/data/` location of the same server. It
adds a `Range` header that it builds from the `?r=` query argument. It does not use the `Range`
header of the client. With this method, the server tells nginx which bytes to serve before the
client can know the byte offsets. The result is byte-for-byte identical to a direct
`curl -r <start>-<end>` on the same file (verified). The returned bytes are a raw row-group
fragment. They are not a standalone valid `.parquet` file (no footer). For the validated-read
alternative, see the docstring of `fetch_wave.py`.

`/range-proxy/` is not `internal` in nginx, on purpose. The API sends a real redirect, not
`X-Accel-Redirect`. Because of this, anyone can guess the byte ranges. This is acceptable for
the non-sensitive data of this POC. It is not acceptable for data with access control, unless
you add a control such as a signed query string.

### curl examples

All examples assume `docker compose up -d` (nginx on `:8080`, `ducklake-api` on `:5001`). They
also assume a wave that is already ingested with `experiment=campaign-2026a`, `shot=1`,
`stage=mirnov`, `wave=mirnov/probe_03` and `data_version=a1b2c3d`.

**1. End to end through the API.** `-L` follows the 302 into nginx and prints the raw row-group
bytes. Write them to a file. Do not write them to a terminal:

```bash
curl -L "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=250&x_max=251" \
  -o chunk.bin
```

**2. Only the redirect.** Do not follow it. This shows the computed byte range and the
`X-Row-Group-*` headers before you fetch data:

```bash
curl -i "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=250&x_max=251"
# HTTP/1.1 302 FOUND
# Location: http://localhost:8080/range-proxy/lake/campaign-2026a/1/mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet?r=bytes=4096-8191
# X-Row-Group-Ids: 3
# X-Row-Group-Rows: 65536
# X-Row-Group-X-Range: 250.0,251.4
```

**3. Skip the API. Use `/range-proxy/` directly** with a known byte range and the clean path
(no `key=value`). The `Location` header of the API points to this URL. The rewrite rule in
`nginx.conf` translates it to the real hive-partitioned path on the server:

```bash
curl -i "http://localhost:8080/range-proxy/lake/campaign-2026a/1/mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet?r=bytes=4096-8191" \
  -o chunk.bin
```

**4. Skip the API and the range-proxy.** Send a plain `Range` request to `/data/`. Use the
*real* on-disk hive-partition path (`experiment=.../shot=.../stage=...`). The rewrite rule
expands the clean path above and this path to the same bytes. Both are verified byte-for-byte
identical:

```bash
curl -r 4096-8191 \
  "http://localhost:8080/data/lake/experiment=campaign-2026a/shot=1/stage=mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet" \
  -o chunk.bin
```

In all four cases, the response body is a raw row-group fragment. It is not a standalone valid
`.parquet` file (no footer). A Parquet reader that expects a full file cannot parse it. For the
validated-read alternative, see the docstring of `fetch_wave.py`.

The `x_min` and `x_max` of a query can match row groups in more than one file, or row groups
that are not contiguous in one file. Then the API cannot give the result as one byte range. It
returns `300 Multiple Choices` with a JSON body that lists each match:

```bash
curl -s "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=0&x_max=10000" | python3 -m json.tool
```

## Lake client (`lake_client.py`)

`LakeClient(base_url, catalog=None)` reads the lake over HTTP. With a `catalog`, it also runs
SQL.

- `fetch(experiment=, shot=, stage=, data_version=, dest_dir=)` downloads the complete stage.
  This path uses only nginx. It lists `/lake-index/<experiment>/<shot>/<stage>/` and keeps the
  files of the `data_version`. Then it downloads them from `/data/lake/`. It reads no
  `wave_manifest` row and no Parquet footer. If the directory has no file of that version, the
  client asks `/ingest-queue/`. nginx proxies that location to the API view `/ingest_queue`.
  This view reads the `ingest_queue` table.
- `fetch(..., wave="mirnov/probe_03", x_min=, x_max=)` downloads one wave, or a time slice of
  it, as one valid Parquet file. It uses `/api/wave`. nginx proxies this location to the API.
- `query(sql, params)` runs SQL on the `waves` table. It needs the `catalog` argument. Follow
  `QUERYING.md`.

The module docstring has examples. The nginx locations `/lake-index/`, `/ingest-queue/` and
`/api/` are not tested against a real nginx yet. The unit tests use a fake server.

## Column encoding (`parquet_encoding.py`)

- Dictionary encoding for `wave` and `data_version`. These columns have few distinct values and
  repeat often.
- `BYTE_STREAM_SPLIT` for `x` and `y` (float64 samples). The separation of byte planes
  compresses smooth signal data much better than interleaved doubles.
- ZSTD level 2. This level is selected for low CPU cost on the write side during continuous
  ingestion. It is not selected for the highest ratio.
- In Parquet, dictionary encoding and `BYTE_STREAM_SPLIT` exclude each other for one column. The
  module asserts that the two lists never overlap.

Measured on the `local_demo.py` dataset: 61.2 MB with the plain ZSTD defaults, and 29.7 MB with
this policy. Most of the gain comes from `BYTE_STREAM_SPLIT` on `x` and `y`.

## What is *not* verified in this environment

- The environment had no access to `extensions.duckdb.org`. Because of this, `INSTALL ducklake`
  and the real `ducklake_add_data_files` call on an attached catalog are not confirmed again
  after the hive-partitioning fix.
- `manifest.py` stores `file_path` as an absolute path from the machine that ran the
  compaction. The dockerized `api` service can fail to resolve it if its filesystem layout is
  different. The manifest does not yet store paths relative to `LAKE_DATA_DIR`.
- This sandbox has no Docker daemon. Postgres and nginx were tested with a local Postgres 16 and
  a local nginx, not with the `docker-compose.yml` stack. `docker-compose.yml` mounts the
  Postgres 18 volume at `/var/lib/postgresql` (not `.../data`, which is correct for 16). This
  follows the layout change of the 18.x image.

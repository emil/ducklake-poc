# DuckLake wave-data POC

Proof-of-concept for a physics wave-data lake: DuckLake (catalog on Postgres) with
**per-sample-row** ingestion, wave-boundary-aware compaction, and a `wave_manifest`
Postgres table that lets a plain HTTP byte-range fetch pull a narrow time-slice of a
wave without going through DuckDB.

## Schema

```
experiment    VARCHAR   -- campaign, e.g. "campaign-2026a"
shot          INTEGER   -- shot/discharge id
stage         VARCHAR   -- diagnostic group, e.g. "mirnov", "thomson", "rogowski"
wave          VARCHAR   -- channel, e.g. "mirnov/probe_03"
data_version  VARCHAR   -- git-hash-style id of the calibration/processing run
                           that produced this copy of the wave
x             DOUBLE    -- elapsed milliseconds since shot start
y             DOUBLE    -- amplitude sample
```

One row per **sample**, not per wave. Uniqueness/clustering key is `(experiment,
shot, stage, wave, data_version)`; the table is `SORTED BY (..., x)` so row-group
`x` min/max stats are tight enough to prune a narrow time window to a couple of row
groups instead of scanning the whole wave.

`data_version` models recalibration: a diagnostic group is periodically
reprocessed, producing a new independent copy of every channel in that group.
Old versions are kept, not overwritten, and it's a **hard partition boundary**
(row-group *and* file level) alongside `stage` — a query always wants exactly one
version, and a file's contents should be fully described by its name.

Only `wave`/`data_version`/`x`/`y` are physical Parquet columns
(`compact.py`'s `PHYSICAL_COLUMNS`); `experiment`/`shot`/`stage` live only in the
hive-style directory path (`experiment=X/shot=Y/stage=Z/...`), never duplicated as
columns — required because `ducklake_add_data_files` rejects a file whose data
contains a value for a `PARTITIONED BY` column (see [ducklake#791](https://github.com/duckdb/ducklake/issues/791)).
Through the DuckLake table, or `read_parquet(..., hive_partitioning=true)`
locally, all seven columns still behave normally for querying. nginx rewrites
clean URLs (`.../lake/<experiment>/<shot>/<stage>/<file>`) into the real
`key=value` path before serving (`nginx/nginx.conf`).

## Layout

```
docker-compose.yml     postgres + nginx + api (Flask)
nginx/nginx.conf       range-enabled static file server + /range-proxy/ loopback
data/raw/              per-wave parquet files (pre-compaction)
data/lake/             DuckLake-owned + compacted output
notebooks/wave_exploration.ipynb   JupySQL: plots, trapezoidal AUC, pruning stats
src/ducklake_poc/
  config.py            shared paths/connection settings, safe_filename
  synthetic.py         per-sample-row generator for realistic diagnostics
  ingest.py            raw files -> batched add_data_files -> compact -> manifest
                       (or, with --enqueue, compact locally -> ingest_queue)
  ingest_queue.py      Postgres work queue: one row per (experiment, shot, stage, data_version)
  ingest_worker.py     single DuckLake writer: register + index each queued version
  compact.py           wave-boundary-aware compaction (see Pipeline, below)
  manifest.py          parquet footer + hive-path -> wave_manifest
  parquet_encoding.py  shared column-encoding policy
  range_http_file.py   seekable file-like object over HTTP Range
  fetch_wave.py         manifest lookup -> curl/pyarrow, whole-wave or x-range
  api_server.py         HTTP API: lookup -> redirect into nginx's range-proxy
  local_demo.py         standalone demo, no DuckLake/docker required
  reset.py              drops+recreates catalog, clears data/
```

## Setup

```bash
docker compose up -d
cd src && uv sync
uv run ducklake-reset --yes    # always after clearing/regenerating data/ independently
```

See `CLAUDE.md` for the full command reference (ingest/compact/manifest/fetch/api,
test tiers, editor type-checking setup).

## Pipeline: ingest -> compact -> manifest -> fetch

1. **`ingest.py`** writes one raw Parquet file per `(wave, data_version)`,
   registers each `(shot, stage)` group with a single batched
   `ducklake_add_data_files` call, then immediately compacts and refreshes the
   manifest for that group — so each diagnostic group is queryable as soon as its
   own ingestion finishes, without waiting on other stages.
2. **`compact.py`** re-groups the sorted row stream into complete
   `(stage, wave, data_version)` chunks regardless of batch boundaries
   (`_iter_wave_groups`), then:
   - gives an oversized wave its own dedicated, contiguous row groups, never
     mixed with another wave's rows;
   - packs small channel-versions together only within the same
     `(stage, data_version)`;
   - treats `stage`/`data_version` as hard boundaries at both the row-group and
     **file** level — a size-triggered file rollover only happens between
     complete waves, never mid-wave, so one `(wave, data_version)` always lands
     in exactly one file ("one wave, one file" — this is what lets the HTTP API
     safely combine row groups into a single redirect).
   Files are written under a temp name and renamed on close to
   `<wave-or-"multi">__<data_version>__<hash>.parquet` once it's known whether the
   file ended up single-wave or packed.
3. **`manifest.py`** reads the Parquet footer directly (`parquet_metadata()`) for
   row-group `x_min`/`x_max` and byte offsets, parses `experiment`/`shot`/`stage`
   from the hive path, and upserts into `wave_manifest`.
4. **`fetch_wave.py`** / **`api_server.py`** look up matching row groups and fetch
   only those bytes. `api_server.py`'s `plan_response()` combines contiguous row
   groups into one redirect when safe, and falls back to `300 Multiple Choices`
   when matches span multiple files or have a `row_group_id` gap (a gap means the
   bytes in between belong to a *different* wave's row group — a correctness
   guard, not just an optimization).

## Queue-driven ingest (`--enqueue` + `ducklake-ingest-worker`)

A `(stage, data_version)` is ingested exactly once and never changes. The
queue path relies on that: producers compact each version *before* enqueueing
it, straight into its final files, so nothing downstream ever rewrites a file,
and every file is indexed into `wave_manifest` exactly once, never refreshed.
The one DuckLake writer only registers finished files, so producers never
contend on the catalog (the conflicts `config.retry_on_conflict` papers over on
the direct path).

```
producer (ducklake-ingest --enqueue, many at once)
  generate the stage's per-wave files into a temp staging dir
  per data_version: stream_compact_to_files (plain DuckDB, no DuckLake)
                    -> final files under data/lake/experiment=/shot=/stage=/
  INSERT one queue row per version (file_paths[])
    UNIQUE (experiment, shot, stage, data_version): already there -> no-op,
    and the producer deletes the files it just wrote; 'failed' -> revived

ducklake-ingest-worker (exactly one active: pg_try_advisory_lock; others wait as standby)
  on lock: every 'processing' row belongs to a dead worker -> back to 'pending'
  loop:
    claim <= batch_size versions (UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING)
    versions whose files DuckLake already has active -> skip straight to indexing
    ducklake_add_data_files(all their files)  -- one DuckLake transaction
      on failure: retry version by version, so a bad version fails alone
    per batch, one Postgres transaction: INSERT wave_manifest rows + mark completed
```

**One row per version.** `UNIQUE (experiment, shot, stage, data_version)`
states "ingested once" directly: a producer retry, or rerunning the same
calibration, can never queue a version twice. Before a row is inserted, each
file is checked to be under `LAKE_DATA_DIR` (manifest paths are relative to
it, and nginx serves it), in the right hive directory, and to hold exactly that
`data_version` according to its Parquet footer.

**Atomic versions.** A version's files are registered in one DuckLake
transaction, so a version becomes queryable all at once. Its manifest rows are
inserted in the same Postgres transaction that marks it completed, so the HTTP
fetch path never serves a version SQL can't see, and a completed version always
has its manifest. A rerun is a new version with new files, so earlier versions'
files and manifest rows are never touched.

**Why not one transaction for everything?** `ducklake_add_data_files` runs in
DuckDB, and DuckLake commits through its *own* Postgres session, so it can
never be atomic with the psycopg2 session holding the queue row. The crash
window between the two is closed by checking DuckLake's catalog
(`ducklake_data_file`, same database) before registering: a version's files
are either all active (skip to indexing) or none are. `ducklake_add_data_files`
keeps the absolute path it's given (`path_is_relative = false`), even for files
inside its own `DATA_PATH`. That was confirmed against a real catalog.

Limits:
- **Orphan cleanup:** files sit in DuckLake's `DATA_PATH` unregistered from the
  moment a producer writes them until the worker registers them.
  `ducklake_delete_orphaned_files` would delete them, so if it runs, give it an
  `older_than` well above the queue's worst-case lag. A producer that crashes
  after writing but before enqueueing leaves genuinely orphaned files, which
  that cleanup is for.
- **Keep the paths apart:** the direct path (`ducklake-ingest` without
  `--enqueue`, `ducklake-compact`) compacts inside DuckLake and rewrites files.
  Don't point it at versions the queue ingested.
- **Tiny-file deletes:** for very small files, DuckLake records a `DELETE` as an
  *inlined* delete and keeps the data file active. Run
  `ducklake_rewrite_data_files` before removing such files from disk (the test
  cleanup does).

## HTTP API

```
client -> GET /wave?...            (Flask ducklake-api, :5001)
       <- 302 Location: nginx/range-proxy/...?r=bytes=A-B
client -> GET that Location        (nginx, :8080)
       <- 206 Partial Content
```

nginx's `/range-proxy/` location proxies to its own `/data/` location, injecting a
`Range` header built from the `?r=` query arg rather than the client's own `Range`
header — this is how the server tells nginx which bytes to serve before the client
could know byte offsets itself. Verified byte-for-byte identical to a direct
`curl -r <start>-<end>` against the same file. The returned bytes are a raw
row-group fragment, not a standalone valid `.parquet` file (no footer) — see
`fetch_wave.py`'s docstring for the validated-read alternative.

`/range-proxy/` is deliberately not `internal` in nginx (the API issues a real
redirect, not `X-Accel-Redirect`), so byte ranges are effectively guessable/public
— acceptable for this POC's non-sensitive data, not for anything access-controlled
without adding e.g. a signed query string.

### curl examples

All examples assume `docker compose up -d` (nginx on `:8080`, `ducklake-api` on
`:5001`) and a wave already ingested at `experiment=campaign-2026a`, `shot=1`,
`stage=mirnov`, `wave=mirnov/probe_03`, `data_version=a1b2c3d`.

**1. End to end through the API** (`-L` follows the 302 into nginx and prints the
raw row-group bytes — redirect it to a file rather than a terminal):

```bash
curl -L "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=250&x_max=251" \
  -o chunk.bin
```

**2. Just the redirect**, without following it — useful for seeing the computed
byte range and the `X-Row-Group-*` headers before fetching anything:

```bash
curl -i "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=250&x_max=251"
# HTTP/1.1 302 FOUND
# Location: http://localhost:8080/range-proxy/lake/campaign-2026a/1/mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet?r=bytes=4096-8191
# X-Row-Group-Ids: 3
# X-Row-Group-Rows: 65536
# X-Row-Group-X-Range: 250.0,251.4
```

**3. Skip the API, hit `/range-proxy/` directly** with an already-known byte
range and the clean (no `key=value`) path — this is what the API's `Location`
header points at, and what `nginx.conf`'s rewrite rule translates to the real
hive-partitioned path server-side:

```bash
curl -i "http://localhost:8080/range-proxy/lake/campaign-2026a/1/mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet?r=bytes=4096-8191" \
  -o chunk.bin
```

**4. Skip the API and the range-proxy too** — a plain `Range` request straight
against `/data/`, using the *real* on-disk hive-partition path
(`experiment=.../shot=.../stage=...`). This is the form the manifest's rewrite
rule expands both the clean path above and this one into; both reach the same
bytes, and both are verified byte-for-byte identical:

```bash
curl -r 4096-8191 \
  "http://localhost:8080/data/lake/experiment=campaign-2026a/shot=1/stage=mirnov/mirnov_probe_03__a1b2c3d__1a2b3c4d.parquet" \
  -o chunk.bin
```

In all four cases the response body is a raw row-group fragment, not a
standalone valid `.parquet` file (no footer) — pipe it into a parquet reader
expecting a full file and it will fail to parse; see `fetch_wave.py`'s
docstring for the validated-read alternative.

If the query's `x_min`/`x_max` matches row groups that span multiple files, or
that aren't contiguous within one file, the API can't express the result as a
single byte range and instead returns `300 Multiple Choices` with a JSON body
listing every match:

```bash
curl -s "http://localhost:5001/wave?experiment=campaign-2026a&shot=1&stage=mirnov&wave=mirnov/probe_03&data_version=a1b2c3d&x_min=0&x_max=10000" | python3 -m json.tool
```

## Lake client (`lake_client.py`)

`LakeClient(base_url, catalog=None)` reads the lake over HTTP. With a `catalog`, it also runs SQL.

- `fetch(experiment=, shot=, stage=, data_version=, dest_dir=)` downloads the complete stage.
  This path uses nginx only: it lists `/lake-index/<experiment>/<shot>/<stage>/`, keeps the files of
  the `data_version`, and downloads them from `/data/lake/`. It reads no `wave_manifest` row and no
  Parquet footer. If the directory has no file of that version, the client asks `/ingest-queue/`.
  nginx proxies that location to the API view `/ingest_queue`, which reads the `ingest_queue` table.
- `fetch(..., wave="mirnov/probe_03", x_min=, x_max=)` downloads one wave (or a time slice) as one
  valid Parquet file. It uses `/api/wave`, which nginx proxies to the API.
- `query(sql, params)` runs SQL on the `waves` table. It needs the `catalog` argument.
  Follow `QUERYING.md`.

The module docstring has examples. The nginx locations `/lake-index/`, `/ingest-queue/` and
`/api/` are not tested against a real nginx yet. The unit tests use a fake server.

## Column encoding (`parquet_encoding.py`)

- Dictionary encoding for `wave`/`data_version` (low cardinality, repeats a lot).
- `BYTE_STREAM_SPLIT` for `x`/`y` (float64 samples) — byte-plane separation
  compresses smoothly-varying signal data much better than interleaved doubles.
- ZSTD level 2 — chosen for write-side CPU cost during continuous ingestion, not
  maximum ratio.
- Dictionary and `BYTE_STREAM_SPLIT` are mutually exclusive per column in
  Parquet, so the module asserts the two lists never overlap.

Measured on the `local_demo.py` dataset: 61.2MB with plain ZSTD defaults vs.
29.7MB with this policy — mostly from `BYTE_STREAM_SPLIT` on `x`/`y`.

## What's *not* verified in this environment

- Built without `extensions.duckdb.org` access, so `INSTALL ducklake` and the
  actual `ducklake_add_data_files` call against a real attached catalog have not
  been re-confirmed after the hive-partitioning fix.
- `manifest.py` stores `file_path` as an absolute path from whichever machine ran
  compaction; the dockerized `api` service could fail resolving it if its
  filesystem layout differs. Not yet switched to storing paths relative to
  `LAKE_DATA_DIR`.
- No Docker daemon in this sandbox: Postgres/nginx were tested via a locally
  installed Postgres 16 + nginx, not the `docker-compose.yml` stack itself.
  `docker-compose.yml` mounts the Postgres 18 volume at `/var/lib/postgresql`
  (not `.../data`, correct for 16) per the image's 18.x layout change.

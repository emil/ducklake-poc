# DuckLake wave-data POC

Small proof-of-concept for the physics wave-data lake design discussed:
DuckLake catalog on Postgres, **per-sample-row** ingestion (so Parquet row
groups can be pruned by time, not just by wave identity), compaction that
gives large waves their own dedicated row groups, and a lightweight
manifest table that lets a plain `curl` fetch a narrow time-slice of a
wave over HTTP without going through DuckDB at all.

## Schema

```
experiment    VARCHAR   -- experimental campaign, e.g. "campaign-2026a"
shot          INTEGER   -- shot/discharge id
stage         VARCHAR   -- diagnostic group, e.g. "mirnov", "thomson", "rogowski"
wave          VARCHAR   -- channel that diagnostic produced, e.g. "mirnov/probe_03",
                           "thomson_te/A"
data_version  VARCHAR   -- git-commit-hash-style id of the calibration constants
                           + processing code that produced this copy of the wave
                           (see below)
x             DOUBLE    -- elapsed time in MILLISECONDS since the shot's start.
                           The absolute start time itself (`started_at`) lives in
                           a separate per-shot metadata record, which isn't
                           modeled here since it's not relevant to the storage/
                           query design this POC is about.
y             DOUBLE    -- one amplitude sample
```

`stage` and `wave` are both realistic, named tokamak diagnostics (see
`synthetic.py`'s `DIAGNOSTICS` dict) rather than arbitrary integers --
magnetic sensors (rogowski, mirnov, flux loops, diamagnetic loops),
optical/laser diagnostics (Thomson scattering, interferometry, ECE), and
particle/radiation sensors (bolometers, CXRS), each with the channel
names that diagnostic actually produces. Sample-count profiles differ by
*stage* for a real physical reason, not an arbitrary "rare big wave"
knob: each stage has a sample rate in kHz (== samples/ms) applied over a
shared 500ms shot duration, and Mirnov's big case (4MHz) is a genuinely
plausible high-frequency MHD digitization rate, giving exactly
2,000,000 samples over the 500ms shot; Thomson scattering, by contrast,
is a pulsed spatial-profile measurement at ~1kHz repetition, giving only
a few hundred points per channel. See `STAGE_PROFILE` in `synthetic.py`
for the full set.

`data_version` models recalibration: scientists periodically revise a
diagnostic's calibration and rerun the processing pipeline against the
same raw capture, producing a new, independent full copy of every channel
in that diagnostic group for that shot. **Old versions are kept, not
overwritten** -- so the same `(shot, stage, wave)` can legitimately have
several complete, independent sets of samples, one per `data_version`. It's
modeled as a **short git-style hash** (e.g. `a1b2c3d`), not a sequential
integer, since in a real system it would identify the exact processing
code + calibration constants used -- there's no inherent chronological
ordering in the hash value itself. A system wanting "give me the latest
version" would need a timestamp, likely from the same unmodeled per-shot
metadata record that holds `started_at`; this POC doesn't need that,
since every example below either knows the specific version it wants or
looks one up explicitly rather than assuming order. A calibration change
applies to a whole diagnostic group at once (you don't recalibrate a
single Mirnov probe in isolation), so the synthetic generator draws the
set of version hashes once per `(shot, stage)` and applies it to every
channel in that stage (see `_generate_version_ids` in `synthetic.py`).
**`data_version` is a hard partition boundary**, same as `stage` --
enforced at *both* the row-group level and the file level (see
Compaction, below): a query almost always wants exactly one version
(never a mix), and a physical file's name should honestly describe a
single version's contents.

`experiment` is the campaign/facility-run a shot belongs to -- a shot
number is assumed unique on its own, so `experiment` isn't part of the
wave uniqueness key, but it *is* the top level of the physical directory
layout and the `PARTITIONED BY` clause (see Compaction, below).

One row per **sample**, not per wave. The uniqueness/clustering key is
`(experiment, shot, stage, wave, data_version)`, and the table is
`SORTED BY (experiment, shot, stage, wave, data_version, x)` --
clustering by `(stage, wave, data_version)` keeps one wave-version's rows
together, `x` on the end keeps them in time order, which is what makes
row-group `x` min/max statistics tight enough to prune a narrow
time-window query down to a couple of row groups instead of scanning the
whole wave.

**Note**: this is the *logical* schema -- all seven columns behave like
ordinary columns when queried, whether through the DuckLake table or via
`read_parquet(..., hive_partitioning=true)` locally. Only four of them
(`wave`, `data_version`, `x`, `y`) are actually stored inside each
Parquet file, though; `experiment`/`shot`/`stage` live only in the
directory path. See "Physical layout & partitioning" below for why that
split exists and what it took to get there.

## Stack

- **Postgres** (docker) -- DuckLake's catalog backend, *and* home to our
  own `wave_manifest` helper table.
- **nginx** (docker) -- serves `./data` as static files with HTTP Range
  support.
- **Python** (run on the host, via `uv`) -- ingestion, compaction, and the
  manifest/fetch demo scripts.

```
docker-compose.yml        postgres + nginx + api (Flask)
pyrightconfig.json         editor-facing type-check config (see Development, below)
nginx/nginx.conf          range-enabled static file server + /range-proxy/ loopback
data/raw/                 per-wave parquet files (current ingestion shape)
data/lake/                DuckLake-owned files + compacted output
notebooks/
  wave_exploration.ipynb    JupySQL + Python: plots, trapezoidal AUC, pruning stats
src/
  pyproject.toml / uv.lock
  Dockerfile                 builds the api service (untested here -- see below)
  ducklake_poc/
    config.py              shared paths/connection settings
    synthetic.py            per-sample-row (shot, stage, wave, x, y) generator, realistic diagnostics
    ingest.py                writes per-wave files + batched add_data_files, then compacts + refreshes the manifest
    compact.py               wave-boundary-aware compaction (see below)
    manifest.py               builds wave_manifest from parquet footer + hive-path metadata
    parquet_encoding.py       shared column-encoding policy (dictionary/BYTE_STREAM_SPLIT/zstd)
    range_http_file.py        minimal seekable file-like object over HTTP Range
    fetch_wave.py              manifest lookup -> curl / pyarrow, whole-wave or x-range
    api_server.py               HTTP API: lookup -> redirect into nginx's range-proxy
    local_demo.py              standalone demo: no DuckLake/docker required
    reset.py                    drops+recreates the Postgres catalog, clears data/ (see below)
  tests/
    conftest.py                service-availability fixtures (skips integration tests cleanly)
    test_synthetic.py          determinism, sample-rate math, schema shape
    test_parquet_encoding.py   actual on-disk encodings, verified via parquet_metadata()
    test_compact.py            row-group/file boundary logic, incl. cross-batch reassembly
    test_manifest.py           footer stat extraction, mixed-partition rejection
    test_fetch_wave.py         overlap-consistency check (pure logic, no DB)
    test_range_http_file.py    [integration] real HTTP range requests against nginx
    test_api_server_logic.py    plan_response: multi-row-group combining, gap-safety
    test_api_server.py         [integration] full redirect chain, byte-verified
    test_local_demo_integration.py  [integration] full pipeline, real Postgres + nginx
    test_reset.py                [integration] catalog+data reset, incl. active-connection case
```

## Setup

```bash
docker compose up -d          # postgres on 5432, nginx on 8080
cd src
uv sync                       # creates .venv and installs deps from uv.lock
```

## Resetting to a clean state

```bash
uv run ducklake-reset --yes
```

**DuckLake's catalog lives in Postgres -- a separate, persistent source
of truth from the physical parquet files under `data/raw` and
`data/lake`.** If those directories ever get cleared or regenerated
*without* also resetting the Postgres catalog (e.g. between iterative
test runs, or when starting from a freshly re-extracted checkout), the
catalog keeps references to files that no longer exist on disk -- and
the next query touching that data, even a harmless `SELECT count(*)`,
fails with something like:

```
IO Error: Cannot open file ".../mirnov_probe_01__....parquet": No such file or directory
```

`ducklake-reset` fixes this by resetting **both sides together, so they
can never drift apart**: it drops and recreates the `ducklake_catalog`
Postgres database (which also wipes `wave_manifest`, since it lives in
the same database), and clears `data/raw` + `data/lake` on disk.
Destructive by design, hence `--yes`. Verified to work even with an
active lingering connection to the database (e.g. a running
`ducklake-api`) -- it terminates other backends before dropping, since
Postgres otherwise refuses to drop a database that's in use. Reach for
this any time you're not sure the catalog and the local files agree,
rather than manually deleting one side and not the other.

## Development

```bash
cd src
uv sync --dev                 # adds pytest, ruff, basedpyright, pyinstrument

uv run ruff check .           # lint
uv run ruff format .          # format
uv run basedpyright .         # type check (typeCheckingMode = "standard")
uv run pytest                 # run the test suite
uv run pyinstrument -m ducklake_poc.local_demo   # profile a run
```

### Editor / standalone basedpyright setup

There's also a `pyrightconfig.json` at the **repo root** (not inside
`src/`), for editors that run `basedpyright`/`pyright` directly rather
than through `uv run`. This matters because **a root-level
`pyrightconfig.json` completely overrides any `[tool.basedpyright]`
section in `pyproject.toml`, anywhere in the tree** -- if your editor's
workspace root is the outer `ducklake-poc/` folder, it won't see
`src/pyproject.toml`'s config at all. Two things a minimal config (just
`exclude` + `typeCheckingMode`) misses, both fixed here:

- **No `venvPath`/`venv`** -- without pointing at `src/.venv` (where
  `uv sync` actually installs everything), every third-party import
  (`duckdb`, `pyarrow`, `pytest`, ...) shows as unresolved across every
  file. Verified by reproducing this exact failure mode (37 errors, all
  `reportMissingImports`/`reportMissingModuleSource`) with a bare
  `{"exclude": [...], "typeCheckingMode": "standard"}` config and no
  `venvPath`, then confirming it drops to 0 once `venvPath`/`venv` are
  set correctly.
- **Notebooks getting linted as plain Python** -- pyright has native
  `.ipynb` support, and without excluding `notebooks/`, it flags
  `get_ipython()`, magic-command-injected variables, and `%%sql` syntax
  as undefined/invalid -- all inherent to how JupySQL notebooks work,
  not real issues. `notebooks` is in `exclude` for exactly this reason.

`uv run basedpyright` (from `src/`) and the root `pyrightconfig.json`
(for editors) are configured to agree: same `typeCheckingMode`, same
`pythonVersion` (3.12, matching `.python-version`).

All three -- ruff, basedpyright, and the full pytest suite -- currently
pass clean. The test suite is split into two tiers:

- **Unit tests** (`test_synthetic.py`, `test_parquet_encoding.py`,
  `test_compact.py`, `test_manifest.py`, `test_fetch_wave.py`) need only
  DuckDB and the local filesystem -- no Postgres or nginx required.
  `test_compact.py` in particular exercises the exact edge case that
  caused real bugs earlier in this project: a wave whose rows are split
  across multiple underlying Arrow batches must still be reassembled into
  one group before row-group boundaries are decided
  (`test_iter_wave_groups_reassembles_a_wave_split_across_internal_batches`).
- **Integration tests** (`test_range_http_file.py`,
  `test_local_demo_integration.py`, marked `@pytest.mark.integration`)
  need a reachable Postgres and/or nginx. `conftest.py` checks for both
  and calls `pytest.skip(...)` rather than failing if they're not
  running, so the suite still passes on a machine without the
  docker-compose stack up -- it just skips what it can't exercise.

`pyinstrument` surfaced a genuine finding while writing this section: for
the full `local_demo.py` run, **`_iter_wave_groups` accounts for ~85% of
total compaction time** (11.6s of 13.0s), almost certainly from the
per-batch `to_numpy(zero_copy_only=False)` conversions on four string
columns (`experiment`, `stage`, `wave`, `data_version`) needed to detect
group boundaries. Not fixed here since it wasn't the point of this POC,
but worth profiling before scaling this compaction approach to real data
volumes -- a cheaper boundary-detection strategy (e.g. hashing the tuple
once per batch instead of four separate full-column numpy conversions)
would be the first thing to try.

## Notebook: SQL + Python exploration (JupySQL)

```bash
cd src
uv sync --extra notebooks     # adds jupyter, jupysql, duckdb-engine, pandas, matplotlib
uv run jupyter lab ../notebooks/wave_exploration.ipynb
```

The notebook is self-contained -- it regenerates the same multi-diagnostic,
multi-data_version dataset as `local_demo.py` and reads the compacted Parquet files directly
via DuckDB, so it doesn't need Postgres, nginx, or a DuckLake catalog
attach to run. It uses [JupySQL](https://jupysql.ploomber.io/) to mix
`%%sql` cells (aggregation, a trapezoidal-integration window function,
row-group pruning stats via `parquet_metadata()`) with Python/matplotlib
for plotting and a `numpy.trapezoid` cross-check of the SQL-computed
integration. Ships with outputs already populated so it's viewable
without running it first.

## Try it without Docker first: `local_demo.py`

Before touching DuckLake at all, there's a fully self-contained demo that
only needs Postgres and nginx reachable (no DuckLake extension, no
catalog attach) -- useful for validating the row-group/manifest mechanics
on their own:

```bash
uv run python -m ducklake_poc.local_demo
```

This generates a realistic slice of a shot -- several small diagnostic
groups (rogowski, flux_loops, diamagnetic_loop, thomson) plus mirnov,
whose `probe_01` channel is forced to **2,000,000 samples across two
data_versions** (`a1b2c3d` and `e4f5a6b`, standing in for a recalibration
+ pipeline rerun) -- compacted with a deliberately small 8MB target file
size, builds the manifest, and runs a narrow time-window query against
one specific version. It also runs regression checks confirming no
*file* (not just row group) ever spans two different diagnostic groups
or two different data_versions, that filenames honestly describe their
contents, and that the two data_versions never share a row group or
file. Sample output:

```
generated 18 raw wave files across 5 diagnostic groups (including mirnov/probe_01 with 2,000,000 samples across data_versions ['a1b2c3d', 'e4f5a6b']) in /tmp/ducklake_local_demo_raw
compacted into 7 file(s), 29.8MB total:
    .../local_demo/experiment=campaign-2026a/shot=999/stage=diamagnetic_loop/diamagnetic_loop_main__02e1926__a7b67fe6.parquet  (0.0MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=flux_loops/multi__65b5046__8ce85d3f.parquet  (0.1MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=mirnov/mirnov_probe_01__a1b2c3d__de2e9643.parquet  (14.6MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=mirnov/mirnov_probe_01__e4f5a6b__b108167e.parquet  (14.6MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=mirnov/multi__a658d00__2824641f.parquet  (0.5MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=rogowski/multi__b44edf3__cea8462a.parquet  (0.1MB)
    .../local_demo/experiment=campaign-2026a/shot=999/stage=thomson/multi__54c5297__d3aab893.parquet  (0.0MB)
partition-isolation check: no file spans multiple data_versions, and every file has a well-formed experiment=/shot=/stage= path, across 7 file(s)
manifest: 208 row-group entries across 7 file(s)

narrow-window query: wave=mirnov/probe_01, data_version=a1b2c3d, x in [250.000, 251.000] ms
  total row groups for this wave version : 100
  row groups touched by the query         : 1
  bytes touched                           : 146,028
  => reading 1.00% of the wave's row groups to answer a 1ms query out of its 500ms duration

data_version isolation: a1b2c3d lives in 1 file(s), e4f5a6b lives in 1 file(s), 0 shared -- confirmed disjoint
```

Note each `mirnov/probe_01` data_version is now a single 14.6MB file
despite the 8MB target -- see "One wave, one file" in the HTTP API
section below for why the target gets exceeded rather than splitting
the wave. Note also the genuine hive directories
(`experiment=campaign-2026a/shot=999/stage=mirnov/...`) -- see "Physical
layout & partitioning" for why `experiment`/`shot`/`stage` moved from
filename prefixes to real directory levels.

Then fetch that same slice for real, over HTTP -- URLs stay clean (no
`key=` prefixes) despite the on-disk hive layout, via an nginx rewrite
rule (see "Physical layout & partitioning"):

```bash
uv run ducklake-fetch-wave --experiment campaign-2026a --shot 999 --stage mirnov \
    --wave mirnov/probe_01 --data-version a1b2c3d --x-min 250 --x-max 251
```

## 1. Ingest -- writes raw files, then compacts and refreshes the manifest automatically

```bash
uv run ducklake-ingest --shot 1 --stage mirnov     # one diagnostic group
uv run ducklake-ingest --shot 1                    # every diagnostic group for shot 1
uv run ducklake-ingest --shots 5                   # shots 1..5, every diagnostic group
```

`ducklake-ingest` used to only do the first half of this: write raw
per-wave files and register them with `ducklake_add_data_files`, leaving
compaction and the manifest refresh as separate manual steps
(`ducklake-compact` + `ducklake-manifest`). It now runs both for you by
default -- immediately after **each individual (shot, stage)** diagnostic
group is registered, `ingest.py` calls straight into `compact.py`'s
manifest-friendly compaction (scoped to that same shot+stage) and
`manifest.py`'s footer-based extraction for every file it produces, so
**each diagnostic group is ready for `fetch_wave.py`/`api_server.py` the
moment its own ingestion finishes** -- no separate step required for the
common case, and no need to wait for every other stage in the shot too.

That scoping matters, not just as an optimization: diagnostic groups
genuinely arrive independently in practice (different acquisition
systems, different processing latencies), so `ducklake-ingest --shot 1
--stage mirnov` run on its own must only touch mirnov's data. Compacting
the *whole shot* on every call -- which is what an earlier version of
this did -- would re-read and re-write every other, already-compacted
stage's files too, every single time, purely because they happened to
share a shot number. `compact_manifest_friendly()` still accepts an
un-scoped, whole-shot mode (pass `stage=None`, or omit `--stage` on
`ducklake-compact` directly) for a genuine periodic "recompact
everything" maintenance sweep -- that's just no longer what
`ducklake-ingest` does by default.

```bash
# skip the automatic compact+manifest step, e.g. to inspect the raw,
# pre-compaction state (many small per-wave files) directly
uv run ducklake-ingest --shot 1 --no-compact

# tune compaction the same way `ducklake-compact` itself accepts
uv run ducklake-ingest --shot 1 --rows-per-row-group 20000 --target-file-size-bytes 8388608
```

Each (channel, data_version) still lands in its own raw parquet file at
ingestion (now with one row per sample instead of one row total),
registered via a single batched `ducklake_add_data_files` call per
(shot, stage) diagnostic group. Raw files land in genuine hive-style
directories --
`data/raw/experiment=<experiment>/shot=<shot>/stage=<stage>/<wave>__<data_version>.parquet`
-- with `experiment`/`shot`/`stage` encoded only in the path, never
duplicated as physical columns inside the files (see "Physical layout &
partitioning" below for why that specific combination matters).

`compact.py` and `manifest.py` no longer import from `ingest.py` --
`get_connection()` and `safe_filename()` moved to `config.py` so
`ingest.py` could import `compact.py` (to call compaction directly)
without a circular import.

## 2. Compact -- still available standalone, for re-compacting later or the built-in strategy

```bash
# DuckLake's own compaction -- row-group AND file boundaries are not something we control.
uv run ducklake-compact --strategy builtin --target-file-size 128MB

# Our own compaction -- wave-boundary-aware row-group AND file sizing (see below).
# ducklake-ingest already runs this automatically per (shot, stage) (see
# above) -- this is for re-compacting later (e.g. after more
# data_versions were added for one stage), or for tuning parameters
# without re-ingesting.
uv run ducklake-compact --strategy manifest --shot 1 --stage mirnov \
    --rows-per-row-group 20000 --target-file-size-bytes 8388608

# omit --stage only for a genuine whole-shot recompaction sweep (e.g.
# periodic maintenance) -- this touches every stage's data, every time,
# so it's not what ducklake-ingest uses by default (see above for why).
uv run ducklake-compact --strategy manifest --shot 1
```

**How the manifest-friendly compaction actually groups rows** (this is
the important part, and it took real bugs to get right -- see "lessons
learned" below):

- Rows are read in `(experiment, shot, stage, wave, data_version, x)`
  order and re-grouped into complete per-`(stage, wave, data_version)`
  chunks (`_iter_wave_groups` in `compact.py`), regardless of how the
  underlying batches happen to be cut.
- A channel-version with **fewer** rows than `--rows-per-row-group` may
  be packed together with adjacent small channel-versions **from the
  same diagnostic group and the same data_version** into one row group
  -- packing never crosses a stage or data_version boundary, even if
  both sides are small. Packed groups' `x` stats won't be individually
  useful for pruning, but that's fine -- nobody range-queries a 100KB
  channel.
- A channel-version with **more** rows than `--rows-per-row-group` gets
  its own dedicated, contiguous run of row groups -- **never** mixed
  with another wave's rows, even partially.
- **`stage` and `data_version` are hard partition boundaries at *both*
  the row-group level and the FILE level.** Not just "a row group stays
  single-valued" -- a whole physical *file* never contains rows from two
  different diagnostic groups or two different calibration reruns, even
  across multiple row groups. Within a `(stage, data_version)` partition,
  `--target-file-size-bytes` rolls files over *between* complete waves,
  but never in the middle of one wave's own dedicated row-group run --
  so a single oversized wave always lands in exactly one file, even if
  that means exceeding the target size (see "One wave, one file" below).
  A partition change still *always* starts a new file regardless of
  size. This is what makes the filename convention below actually
  trustworthy, and structurally guarantees compaction never "moves a
  stage's data into another stage's file".
- `--rows-per-row-group` should be tuned to your sample rate and the
  narrowest time window you expect to query: too large and a narrow query
  over-reads; too small and you multiply row-group/metadata count for no
  benefit. If "1ms" is your worst case, size row groups to cover roughly
  a few ms to a few tens of ms each.

Since a file isn't known to be single-wave or multi-wave (packed) until
it's finished being written, files are written under a temporary name and
renamed on close to:

```
<lake_root>/experiment=<experiment>/shot=<shot>/stage=<stage>/<wave-or-"multi">__<data_version>__<hash>.parquet
```

-- e.g. `experiment=campaign-2026a/shot=999/stage=mirnov/mirnov_probe_01__a1b2c3d__fb3f6126.parquet`
for a file dedicated to one (large) wave version, or
`experiment=campaign-2026a/shot=999/stage=thomson/multi__54c5297__b71dde84.parquet`
for a file holding several packed small channels from the same
stage+version. The trailing hash keeps names unique across compaction
reruns. Note `stage` is now a real directory level, not a filename
prefix -- see "Physical layout & partitioning" below for why.

## 3. Build the manifest -- also automatic now, standalone for manual re-indexing

`ducklake-ingest` already calls this for every file its compaction step
produces. Run it directly if you need to re-index a file by hand (e.g.
after `ducklake-compact --strategy builtin`, which doesn't refresh
`wave_manifest` on its own, or to rebuild an entry that's out of sync):

```bash
uv run ducklake-manifest data/lake/experiment=<experiment>/shot=<shot>/stage=<stage>/<filename>.parquet
```

```sql
CREATE TABLE wave_manifest (
    experiment TEXT, shot INTEGER, stage TEXT, wave_min TEXT, wave_max TEXT,
    data_version TEXT, x_min, x_max,
    file_path, file_url,
    row_group_id, row_group_rows,
    byte_start, byte_length,
    updated_at,
    PRIMARY KEY (file_path, row_group_id)
);
```

`x_min`/`x_max` per row group (pulled from the Parquet footer's own
column statistics) are what make narrow time-window lookups possible.
`wave_min`/`wave_max` differ (rather than being equal) only for mixed
row groups holding several packed small channels **from the same
diagnostic group and the same data_version** -- `experiment`, `stage`,
and `data_version` are always single-valued per row group (and, more
strongly, per whole file -- see Compaction, above), since packing never
crosses any of those boundaries.

## 4. Fetch a wave -- whole, or a narrow time slice

```bash
# whole wave
uv run ducklake-fetch-wave --experiment campaign-2026a --shot 1 --stage mirnov \
    --wave mirnov/probe_03 --data-version a1b2c3d

# just a time window (the "1ms out of 2M samples" case)
uv run ducklake-fetch-wave --experiment campaign-2026a --shot 1 --stage mirnov \
    --wave mirnov/probe_03 --data-version a1b2c3d --x-min 250 --x-max 251
```

`--data-version` is required and matched exactly (not ranged) -- since
it's a hard partition boundary (at both the row-group and file level),
every row group has exactly one, known data_version. Looks up matching
row groups in the manifest, fetches only those via `RangeHTTPFile` +
pyarrow (`pre_buffer=False`, so only the footer + the matched row groups
are actually transferred over HTTP), and locally filters to the exact
wave/x-window requested (a matched row group can still contain a few
unrelated rows if it was a mixed/packed group).

## 5. HTTP API: server-computed byte-range redirects

`fetch_wave.py` is a CLI that does the manifest lookup and the HTTP fetch
itself. `api_server.py` exposes the same lookup as a small Flask HTTP API
that instead **redirects the client into nginx**, so nginx -- not this
Flask process -- streams the actual bytes:

```bash
uv run ducklake-api   # starts on :5001 by default

curl -Li "http://localhost:5001/wave?experiment=campaign-2026a&shot=999&stage=mirnov&wave=mirnov/probe_01&data_version=a1b2c3d&x_min=250&x_max=251"
```

```
client -> GET /wave?...                              (Flask, :5001)
       <- 302 Location: nginx/range-proxy/...?r=bytes=A-B
client -> GET that Location                           (nginx, :8080)
       <- 206 Partial Content, Content-Type: application/vnd.apache.parquet
```

**How nginx is told which bytes to serve, since the client doesn't send a
`Range` header on the first request** (they don't know byte offsets --
that's the whole point of the lookup): `nginx/nginx.conf` has a
`/range-proxy/` location that proxies back to nginx's own `/data/`
location (a "loopback"), injecting a `Range` header built from a query
arg rather than reading whatever `Range` header the actual client sent:

```nginx
location /range-proxy/ {
    proxy_pass http://127.0.0.1/data/;
    proxy_set_header Range $arg_r;
    proxy_hide_header Content-Type;
    add_header Content-Type application/vnd.apache.parquet always;
}
```

Flask computes `bytes=<byte_start>-<byte_end>` from the manifest lookup
and bakes it into the redirect's query string
(`?r=bytes=7407837-7554163`); nginx's static file serving then does its
normal Range-aware `sendfile` handling, oblivious to whether the `Range`
came from a real client or this injected one. **Verified empirically,
not just configured**: a byte-for-byte `cmp` between the API-driven
response and a direct `curl -r <start>-<end>` against the same file
came back identical, and re-decoding the served bytes with
`RangeHTTPFile` + pyarrow confirmed they're exactly the expected row
group (correct row count, correct `wave`/`data_version`, correct `x`
range).

Two things worth being explicit about:

- **A redirect can only point at one URL** -- but that's less limiting
  than it sounds, because compaction guarantees a single `(wave,
  data_version)` never splits across files regardless of
  `--target-file-size-bytes` (see "One wave, one file" below). So even a
  query matching several row groups collapses into one combined redirect
  as long as they're all in the same file and their `row_group_id`s form
  one unbroken run -- `min(byte_start)` to `max(byte_end)` across the
  matches is then a single valid, contiguous range (row groups are
  written back-to-back with no gaps: verified, `byte_end ==
  next.byte_start` exactly). `/wave` only falls back to **300 Multiple
  Choices** with a JSON body listing every match when matches genuinely
  span multiple files, or -- checked defensively, even though it
  shouldn't be reachable given the guarantee above -- when the matched
  `row_group_id`s have a gap. A gap matters specifically because the
  bytes "in between" two non-adjacent row groups aren't padding -- they're
  a complete row group belonging to some *other* wave or packed group.
  Silently including them would mean serving someone else's actual
  sample data to a caller who didn't ask for it, not a merely-inefficient
  overfetch, which is why `plan_response()` refuses to guess rather than
  assuming contiguity. This logic is pulled into a pure function
  specifically so both cases (multi-file, and the gap case) can be
  exercised directly with synthetic rows in `test_api_server_logic.py`,
  since real data can't easily produce a genuine gap.
- **The `/range-proxy/` location is deliberately not marked `internal`**
  in nginx, since the API server issues a real client-facing redirect
  (not `X-Accel-Redirect`), so nginx must accept the follow-up request
  from outside. This means a client could hand-construct arbitrary
  `?r=bytes=...` values directly, bypassing the manifest lookup
  entirely -- harmless for this POC (the byte ranges are already
  effectively public via the manifest, and the data isn't sensitive),
  but worth locking down (e.g. a signed query string, or moving to
  `X-Accel-Redirect` with an `internal` location if the client and
  Flask can be trusted to share the same origin) before using this
  pattern for anything access-controlled.

### One wave, one file

`compact.py`'s file-rollover logic changed alongside this: previously,
`--target-file-size-bytes` could split a single oversized wave across
multiple files mid-way through writing it (this is exactly what produced
the two ~8.5MB/~6.2MB `mirnov/probe_01` files shown in earlier examples
in this README). That's now disallowed -- a size-triggered file roll is
only permitted *before* the first row group of a new wave, never in the
middle of one wave's own dedicated run. Concretely: the wave's file can
end up **larger** than `--target-file-size-bytes` (the target is
exceeded, not enforced, in that case), but every row group for one
`(wave, data_version)` is now guaranteed to live in exactly one file.

This is what makes the `/wave` endpoint's combining logic safe to rely
on: a real query for one wave, however wide, will never hit the
"spans multiple files" fallback, only the packed/mixed-wave case might
(and even that stays single-file in practice, since packed groups are
small by construction). Verified end to end: the same `mirnov/probe_01`
data that previously produced 2 files now produces exactly 1 (~14.6MB),
and a whole-wave query against it now returns a single 302 covering all
100 row groups at once, rather than the 300 Multiple Choices it used to
return.

If you need a hard cap on individual file size even for oversized waves,
`--rows-per-row-group` is the knob to lower instead -- fewer rows per row
group means smaller total bytes before whichever downstream limit
actually matters is reached.

The returned bytes are a *row group's* raw bytes, not a standalone valid
`.parquet` file (no footer) -- same caveat as everywhere else in this
repo that does raw byte-range fetches; see `fetch_wave.py`'s docstring
for the "validated read" alternative if a consumer needs a spec-compliant
read instead of a raw fragment.

**Dockerized deployment caveat**: `docker-compose.yml` includes an `api`
service (`src/Dockerfile`), but this could not be tested end-to-end in
this sandbox (no Docker daemon here) -- only the local `uv run
ducklake-api` path (against the sandbox's locally-installed Postgres and
nginx) was actually exercised. One specific risk worth checking before
relying on the dockerized path: `manifest.py` stores each row group's
`file_path` as an **absolute path from whichever machine ran
compaction**. If the `api` container's filesystem layout differs from
wherever `compact.py` actually ran (likely, since compaction runs in
`ingest`/`compact` contexts that may not share the container's `/app`
layout), `Path(file_path).relative_to(config.LAKE_DATA_DIR)` in
`api_server.py` would raise instead of resolving -- worth switching
`manifest.py` to store paths relative to `LAKE_DATA_DIR` instead of
absolute ones if the API server needs to run somewhere with a different
filesystem view than compaction does.

## Physical layout & partitioning

This came up directly while designing the schema: does it make sense to
`ALTER TABLE ... SET PARTITIONED BY (experiment, shot, stage)`, and can
this actually work with `ducklake_add_data_files` (the ingestion pattern
this project uses throughout -- write external files, then register
them, rather than `INSERT`-ing rows directly)?

**Short answer: yes, but only once `experiment`/`shot`/`stage` are
removed as physical columns from the files themselves.** This took a
real production error to fully pin down. The first version of this
design used `PARTITIONED BY` with a flat, non-hive file layout and kept
`experiment`/`shot`/`stage` as ordinary columns in every file (useful for
dictionary encoding and for querying independent of file layout). Running
`uv run ducklake-ingest` against it failed with:

```
_duckdb.InvalidInputException: Invalid Input Error: File "..." contains
an invalid partition value for the table configuration.
```

That error matches [DuckLake issue #791](https://github.com/duckdb/ducklake/issues/791)
exactly: `ducklake_add_data_files` fails whenever a `PARTITIONED BY`
column is *also* present in the file's own data, regardless of whether
the file's path is hive-style or flat. [PR #252](https://github.com/duckdb/ducklake/pull/252)
(the feature that added hive-partition support to `add_data_files` in
the first place) is explicit about this precondition -- it's built for
"columns that are not present in the files themselves but only in the
file path". A column present in *both* places is a different, narrower
case that's still an open bug as of writing.

So the fix wasn't "use real hive folders instead of flat ones" on its
own -- it's **removing `experiment`/`shot`/`stage` as physical columns
entirely**, keeping them *only* in the directory path, which is exactly
the case #252 was built for:

- **Physical layout is now genuine hive-style**:
  `experiment=<experiment>/shot=<shot>/stage=<stage>/<wave-or-"multi">__<data_version>__<hash>.parquet`.
  `wave`/`data_version`/`x`/`y` remain real physical columns (see
  `compact.py`'s `PHYSICAL_COLUMNS`); `experiment`/`shot`/`stage` do not.
- When queried through the DuckLake table (which still declares all
  seven columns and has `SET PARTITIONED BY (experiment, shot, stage)`),
  or read locally via `read_parquet(glob, hive_partitioning=true)`
  (verified: DuckDB auto-detects `key=value` directories and reconstructs
  them as ordinary columns even without the flag), `experiment`/`shot`/
  `stage` still behave like normal columns for querying -- they're just
  not stored redundantly inside every file.
- `manifest.py` -- which reads the raw Parquet footer directly via
  `parquet_metadata()`, bypassing DuckLake entirely -- now parses
  `experiment`/`shot`/`stage` from the file's **path** instead
  (`_parse_hive_partitions`), since they're no longer in the footer's
  column statistics. `wave`/`data_version`/`x` extraction is unchanged.
- HTTP-facing URLs stay clean (no `key=` prefixes) via
  `manifest.clean_relative_path()` -- nginx has a rewrite rule
  (`nginx/nginx.conf`) that translates `.../lake/<experiment>/<shot>/<stage>/<file>`
  back to the real `.../lake/experiment=X/shot=Y/stage=Z/<file>` path
  before serving. This applies transparently to the `/range-proxy/`
  loopback's internally-generated sub-requests too, since a `proxy_pass`
  request gets matched against nginx's location blocks fresh, just like
  any other incoming request -- verified empirically, along with two
  real bugs the implementation surfaced: an infinite rewrite loop (the
  regex also matched its own rewritten output, since `experiment=X` still
  satisfies a naive `[^/]+` capture -- fixed by excluding `=` from the
  three partition segments specifically), and a mismatch with an extra
  wrapper directory (`local_demo.py` nests its output one level deeper
  than the real pipeline does, to avoid colliding with real data -- fixed
  by making the rewrite tolerant of an arbitrary prefix before the actual
  hive segments, and preserving that prefix through the rewrite).

We did also check whether `ducklake_merge_adjacent_files` could be
trusted to respect `PARTITIONED BY` (never merging a stage's files into
another stage's) -- we could not find documentation guaranteeing that,
only that it respects schema version and the table's current sort order.
Given that gap, physical file placement and the "never mix stage/
data_version" guarantee are still handled entirely by our own
`compact.py`, not by DuckLake's built-in compaction -- `PARTITIONED BY`
is relied on now specifically for `ducklake_add_data_files` registration
and for direct-SQL-query pruning, not for compaction's own guarantees.

**What this cost**: `experiment` and `stage` dropped out of
`parquet_encoding.py`'s dictionary-encoding list (nothing left to encode
-- they're not physical columns anymore). `manifest.py`'s per-row-group
consistency check for those two also became structurally unnecessary
rather than merely verified: a file living at one specific
`experiment=X/shot=Y/stage=Z/` path can't physically disagree with
itself the way a redundant column could.

## Column encoding

Compaction (and raw ingestion) write files using a shared encoding policy
(`parquet_encoding.py`), rather than plain defaults:

- **Dictionary encoding for `wave` and `data_version`**: `wave` is the
  specific channel produced (at most a few dozen distinct channels
  across a whole shot), `data_version` is constant within any given
  compacted file (compact.py makes it a hard partition boundary at the
  FILE level) -- both good dictionary candidates, repeated values
  collapse to small integer codes into a shared dictionary page.
  `experiment`/`stage` aren't in this list: they're hive-partition keys
  encoded only in the directory path (see "Physical layout &
  partitioning"), not physical columns, so there's nothing to
  dictionary-encode for them here.
- **`BYTE_STREAM_SPLIT` for `x`/`y`** (the float64 sample columns):
  ordinary Parquet layout interleaves each double's 8 bytes; BYTE_STREAM_SPLIT
  groups all of byte-0 together, all of byte-1 together, etc. across the
  column instead, which compresses far better for smoothly-varying
  physical signals since the sign/exponent bytes (which repeat a lot)
  aren't interleaved with high-entropy mantissa bytes anymore.
- **ZSTD level 2**: close to ZSTD's default speed, chosen because
  ingestion happens continuously during the busiest 15-minute experiment
  windows -- a higher level buys a few more percent of size reduction at
  meaningfully more CPU time per write. Worth revisiting based on measured
  ingestion latency vs. storage cost in production.

Dictionary and BYTE_STREAM_SPLIT are mutually exclusive per column in
Parquet, so `parquet_encoding.py` asserts the two column lists never
overlap.

Measured effect on the `local_demo.py` dataset (18 wave-version files
across 5 diagnostic groups, including two 2,000,000-sample
`mirnov/probe_01` data_versions): **61.2MB with plain ZSTD** (defaults,
no dictionary/BYTE_STREAM_SPLIT) vs. **29.7MB** with this policy applied
-- roughly half the size, mostly from BYTE_STREAM_SPLIT on `x`/`y` given
how much of the data is float64 samples.

## Lessons learned building this (worth carrying back to production)

- **A row group must never contain a *partial* slice of a large wave
  mixed with anything else.** Our first version of the compaction logic
  just batched the globally sorted stream into fixed-size chunks by row
  count, with no awareness of wave boundaries. The result: one row group
  ended up containing the tail of several small waves *plus* the first
  2,000 rows of the 2-million-row wave. Since every wave's `x`
  independently resets to its own `[0, duration]` range, that row group's
  `x` statistics came out as the full `[0, 1]` -- technically correct,
  completely useless for pruning, and it broke assumptions in
  `fetch_wave.py`'s consistency check downstream. Fixed by re-grouping the
  sorted stream into complete per-(stage, wave) chunks before deciding
  row-group boundaries (`_iter_wave_groups`), and only ever letting
  oversized channels get their own dedicated row groups.
- **Once `stage` became a real diagnostic-group column instead of a fixed
  test constant, packing had to respect stage boundaries too.** Moving
  from an integer `stage` (one value per demo run) to real named
  diagnostic groups meant a single shot's compacted output could span
  several stages at once -- e.g. the tail of `diamagnetic_loop`'s one
  channel packed into the same row group as the start of `ece`'s first
  channel. That would have made `manifest.py`'s "a row group belongs to
  exactly one `(shot, stage)`" assumption false in practice, not just in
  theory. Fixed by flushing the pending small-channel pack on any stage
  change, even under the row-group size threshold -- `local_demo.py` now
  runs an explicit regression check for this (`check_no_partition_mixing`)
  rather than just assuming the fix holds.
- **`np.diff()` doesn't work on string columns.** The original boundary
  detection in `_iter_wave_groups` used `np.diff(waves) != 0`, which is
  fine for the original integer `wave` column but silently wrong (or
  outright erroring) once `wave` became a string. Fixed with a direct
  elementwise inequality comparison instead (`waves[:-1] != waves[1:]`),
  which works for both numeric and string/object numpy arrays.
- **Python's built-in `hash()` on strings is randomized per process**
  (`PYTHONHASHSEED`), so seeding the synthetic data's RNG from
  `hash((shot, stage))` made the "is this channel randomly oversized"
  draw in `generate_stage` non-reproducible across runs -- the same
  demo could produce 2 compacted files in one run and 4 in the next,
  purely from an unrelated Mirnov channel randomly also becoming huge.
  Fixed by seeding from a stable hash (`zlib.crc32`) instead.
- **Adding `data_version` meant extending "never mix" to a second
  dimension, not just adding a column.** Once recalibration reruns
  became part of the schema, the same `(stage, wave)` legitimately
  produces multiple independent copies of a channel over time. Since a
  query almost always wants exactly one version (never a mix of
  calibration runs), `data_version` had to become a hard partition
  boundary exactly like `stage` -- packing small channels together now
  flushes on *either* a stage change *or* a data_version change, and the
  atomic per-wave grouping unit (`_iter_wave_groups`) had to grow from
  `(stage, wave)` to `(stage, wave, data_version)`. `local_demo.py` now
  explicitly forces one channel to have two data_versions and asserts
  their row-group sets are completely disjoint, rather than trusting the
  boundary logic by inspection.
- **`ROW_GROUP_SIZE` in DuckDB's `COPY`/`INSERT` path floors at the
  internal vector chunk size (2048 rows)** and can't be used to get
  fine-grained control at scale -- see the git history for the empirical
  test. `pyarrow.parquet.ParquetWriter.write_table()` called once per
  desired chunk has no such floor, which is what both compaction
  strategies rely on.
- **`SORTED BY` must include `x`, not just `(shot, stage, wave)`.**
  Without it, row-group stats on `x` span each wave's entire duration,
  and time-window pruning doesn't work at all.
- **The manifest's overlap-consistency check only applies to "pure"
  single-wave row groups** (`wave_min == wave_max == wave`). Mixed/packed
  row groups are expected to have broad, overlapping `x` ranges across
  different waves -- that's a normal trade-off for small waves, not a
  sign of corruption, so they're excluded from the check.
- **Making `x` genuine milliseconds instead of a normalized `[0, 1]`
  fraction removed an awkward unit workaround, not just a cosmetic
  change.** Earlier, "1ms" queries were approximated as a fraction of
  the wave's total normalized duration (`2000 / n_samples`), which was
  documented as a stand-in rather than a literal unit. Once `x` was
  redefined as real elapsed milliseconds (via a per-stage sample rate in
  kHz applied over a shared shot duration -- see `STAGE_PROFILE`), a
  "1ms window" became a literal `x BETWEEN t AND t+1` with no conversion
  factor, and it surfaced that `local_demo.py`'s query still used the old
  fractional-window math -- caught by re-running the demo after the
  schema change rather than assuming the numbers still made sense.
- **Making `data_version` a string (not a sequential integer) meant the
  "gain/offset by version number" calibration-perturbation logic had to
  be rethought**, since git-style hashes have no inherent order. Fixed
  by deriving the perturbation from a hash of the `data_version` string
  itself rather than an ordinal index, which also removed a parameter
  (`version_index`) that would otherwise have had to be threaded through
  the generator alongside the hash for no real reason.
- **Elevating `stage`/`data_version` from a row-group boundary to a
  *file-level* boundary was necessary once physical filenames needed to
  describe file contents.** The requested naming convention
  (`<stage>__<wave>__<data_version>.parquet`) only makes sense if a
  single file's `(stage, data_version)` is genuinely constant --
  otherwise the name would lie about the file's contents whenever a
  small-wave-packing row group happened to fall next to a different
  stage/version's row group in the same file. Fixed by tracking the
  currently-open file's partition and forcing a new file (not just a
  new row group) on any stage or data_version change, in addition to
  the existing size-based rollover.
- **A file's final name isn't knowable until it's fully written** (is it
  one dedicated wave, or several packed small ones?), so files are now
  written under a temporary name and renamed on close once the set of
  waves actually written to it is known -- a `"multi"` marker is used in
  place of a wave name for packed files. `local_demo.py`'s
  `check_no_partition_mixing` now also asserts the filename's stage
  prefix matches the file's actual content, not just that content
  doesn't mix.
- **DuckLake's `PARTITIONED BY` and `ducklake_merge_adjacent_files` were
  deliberately *not* relied on for the physical-layout guarantees this
  design needs**, after research turned up real gaps: `ducklake_add_data_files`
  (used here, since files are written externally and registered rather
  than `INSERT`-ed) has documented bugs picking up partition values from
  hive-style paths, and no documentation could be found guaranteeing
  `merge_adjacent_files` never merges across partition boundaries. Given
  we'd already built hard stage/data_version boundaries into our own
  compaction for the row-group-level pruning story, extending that same
  logic to the file level was more reliable than trusting DuckLake's
  partitioning machinery for a guarantee it doesn't clearly document.
  `PARTITIONED BY` is still applied at the catalog level for whatever
  pruning benefit DuckLake gets from it independently.
- **Mismatched triple-quote delimiters in the notebook build script**
  (opening a Python string with `'''` and closing it with `"""`) silently
  swallowed everything up to the next real `'''`, corrupting several
  notebook cells at once with no clear error at the point of the mistake
  -- caught by actually executing the generated notebook rather than
  trusting that the build script "looked right". A related fix: JupySQL's
  `:named_parameter` binding doesn't work reliably for DuckDB in this
  setup (needs `SqlMagic.named_parameters = "enabled"` and still fails on
  some query shapes), so dynamic values (like Thomson's randomly-drawn
  `data_version`) are spliced via an f-string and `run_line_magic`/
  `run_cell_magic` called programmatically instead -- verified in
  isolation before trusting it in the notebook.
- **"A redirect can only point at one URL" was initially handled too
  bluntly** -- any match with more than one row group fell back to 300,
  even when combining them into a single range would have been perfectly
  safe. The user asked directly whether row groups for the same wave
  are contiguous and whether min/max byte offsets could just be
  combined -- verified empirically first (consecutive row groups' byte
  ranges touch exactly, `byte_end == next.byte_start`) before trusting
  it, then implemented the collapse in `plan_response()`.
- **A follow-up question caught a real overclaim**: describing a
  hypothetical row-group gap as producing only "harmless extra bytes"
  was wrong -- bytes between two non-adjacent matched row groups in the
  same file are a *complete row group belonging to some other wave*, not
  padding. Silently including them would mean serving someone else's
  sample data to a caller who didn't request it. Fixed two ways: (1) an
  explicit contiguity check in `plan_response()` that refuses to
  collapse (falls back to 300) if the matched `row_group_id`s have a
  gap, rather than assuming contiguity from "same file" alone, and (2)
  changing `compact.py`'s file-rollover so a single `(wave,
  data_version)` never splits across files regardless of
  `--target-file-size-bytes` (only rolling files *between* complete
  waves) -- which also makes the gap case essentially unreachable
  through the normal pipeline, though the check stays in place as a
  defensive guard rather than relying on that being true forever. Since
  real data can no longer easily produce a genuine gap, the check itself
  is verified with hand-crafted synthetic rows in
  `test_api_server_logic.py` instead.
- **Folding compaction into ingestion required breaking a circular
  import, not just calling a function.** `compact.py` originally
  imported `get_connection()`/`_safe_filename()` from `ingest.py`; once
  `ingest.py` needed to call *into* `compact.py` (to compact right after
  ingesting), that became a cycle. Fixed by moving both utilities into
  `config.py` (which both modules already depended on anyway), rather
  than reaching for a deferred/local import to paper over it -- the
  module boundary was wrong, not just the import order.
- **The first version of "compact after ingest" compacted the wrong
  scope.** It ran once per *shot*, after every stage had been ingested --
  which both defeated the point of ingesting incrementally (compaction
  couldn't start until the slowest diagnostic group finished) and, worse,
  meant `ducklake-ingest --shot 1 --stage mirnov` run on its own would
  still re-read and re-write every *other*, already-compacted stage's
  files for that shot too, since `compact_manifest_friendly()`'s query
  was scoped to `WHERE shot = ?` with no stage filter at all. Caught by a
  user question, not by testing -- there was no test exercising two
  separate incremental ingests into the same shot. Fixed by adding a
  `stage` parameter (`_shot_stage_where_clause()`, unit tested directly
  since the full DuckLake-attached path still isn't testable here) and
  moving the compact+manifest call inside the per-stage loop in
  `ingest.py`, so each diagnostic group only ever touches its own data.
- **A shipped bug fix that only fixed the sandbox.** The nginx clean-URL
  rewrite was debugged and corrected against the sandbox's *live*
  `/etc/nginx/sites-enabled/default`, but the fix was never copied back
  into the actual `nginx/nginx.conf` file in the repo -- so the archive
  shipped the original, broken regex (no `local_demo/`-prefix tolerance,
  and vulnerable to the infinite-rewrite-loop bug) even after it had
  supposedly been fixed and verified. Caught only when a real user hit
  the exact 404 the bug predicts. The fix this time was re-verified by
  extracting the actual shipped zip, deploying the actual repository
  file (not a hand-patched copy) against a reconstruction of the
  reported failure, and confirming the built zip contains the fix before
  calling it done -- "I tested it" should mean testing the artifact
  that ships, not a config that happens to live in the same session.
- **The catalog and the physical files are two separate sources of truth
  that can silently drift apart, and nothing in this POC enforced
  keeping them together until now.** A real user hit `IO Error: Cannot
  open file ... No such file or directory` on a plain `SELECT count(*)`
  -- the DuckLake catalog (in Postgres) still referenced a compacted
  file that no longer existed under `data/lake`, almost certainly from
  clearing/regenerating local data between test iterations without also
  resetting the catalog. This isn't a bug in the ingest/compact logic
  itself; it's an inherent operational hazard of any external-catalog +
  physical-storage architecture (the same class of problem Iceberg/Delta
  deployments have to manage too). Rather than just documenting "remember
  to reset both", added `ducklake-reset` to make the two sides
  impossible to reset independently by accident -- verified it actually
  drops and recreates a functional, empty database (not a no-op), and
  specifically verified it succeeds even with an active lingering
  connection (e.g. a running `ducklake-api`), which would otherwise make
  `DROP DATABASE` fail outright.

## What's *not* verified in this environment

This was built and tested in a sandboxed environment without access to
`extensions.duckdb.org`, so `INSTALL ducklake` inside DuckDB (needed by
`ingest.py` and `compact.py` to actually attach to the DuckLake catalog)
could not be exercised here -- this includes the `ALTER TABLE ... SET
PARTITIONED BY` call in `ingest.py` and, most importantly, the
`ducklake_add_data_files` calls themselves. The hive-partitioning fix
described in "Physical layout & partitioning" was diagnosed from a real
error report (`uv run ducklake-ingest` against an actual DuckLake attach
failing with "invalid partition value for the table configuration"),
cross-referenced against the matching upstream GitHub issue, and
implemented and verified as thoroughly as this sandbox allows -- but the
actual `ducklake_add_data_files` call succeeding against a real DuckLake
catalog with the new layout has **not** been directly re-confirmed here.
If you hit this again after the fix, that's the thing to check first.
Everything else -- Postgres, nginx with real Range-request behavior
(including the clean-URL rewrite and its interaction with the
`/range-proxy/` loopback), the wave/stage/data_version-boundary-aware
compaction logic, the `parquet_metadata()` + hive-path manifest
extraction, the `wave_manifest` round trip, and the `RangeHTTPFile` +
pyarrow narrow-window fetch -- was tested end-to-end against real,
locally-running Postgres and nginx instances via `local_demo.py`. Run
`docker compose up` on a machine with normal internet access and the
DuckLake extension install should just work as documented.

This sandbox also has no Docker daemon and no access to the Docker Hub
registry, so `docker-compose.yml`'s `postgres:18-alpine` image itself
could not be pulled or run here -- all the Postgres testing above used a
locally apt-installed PostgreSQL 16, not a running 18 container. Before
relying on the 18 bump, worth confirming `docker compose up` actually
starts cleanly on a machine with registry access. One thing already
accounted for from PostgreSQL's own release notes: the official image
made `PGDATA` version-specific starting in 18 (`/var/lib/postgresql/18/docker`)
and moved the declared `VOLUME` up to `/var/lib/postgresql` -- the old
`pgdata:/var/lib/postgresql/data` mount (correct for 16) would have
silently mounted the wrong directory and lost all data on container
restart, so the compose file now mounts at `/var/lib/postgresql` instead.


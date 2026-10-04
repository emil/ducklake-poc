# Querying the lake

Guidelines for writing queries against the wave lake (DuckLake `physics_lake.waves`, the
`wave_manifest` Postgres table, or the HTTP fetch path) at production scale. The demo dataset is
small enough that almost any query works; these rules are about the target scale, where most
naive queries don't finish.

## Scale envelope (design target)

| Quantity | Value | Derived from |
|---|---|---|
| Total rows (samples) | ~10^12 | target |
| Waves per shot | ~3,000 | target |
| Stages per shot | ~40 (≈75 waves/stage on average, highly skewed) | target |
| Hive partitions per shot | 40 (`experiment=/shot=/stage=`) | × number of shots × data_versions kept |
| Row groups total | ~5×10^7 | 10^12 / 20,000 (`rows_per_row_group` default) |
| `wave_manifest` rows | ~5×10^7 per retained data_version set | one row per row group |
| Uncompressed `x`+`y` | ~16 TB | 16 B/row; on disk roughly half (README measured ~2× from the encoding policy) |

Wave sizes are skewed. A 4 MHz Mirnov probe is ~2M samples over 500 ms, and a pulsed
Thomson channel can be a few hundred. "One wave" can mean 1 row group or 100+, so plan around
the stage you're querying.

## Hard rules

1. **Never scan `waves` without partition filters.** Every query against the DuckLake table
   must pin `experiment` and `stage` with equality, and `shot` with equality, `IN (...)`, or a
   bounded `BETWEEN`. An unfiltered `count(*)`, `count(DISTINCT ...)`, or `SELECT DISTINCT
   wave` touches all ~10^12 rows. Answer metadata questions from the manifest or catalog
   instead (see "Choosing an access path").

2. **Pin exactly one `data_version` per `(shot, stage)`.** Recalibration reruns keep every old
   version, so leaving `data_version` unconstrained returns every copy of every sample: counts
   multiply, aggregates are wrong, and window functions pair up samples from unrelated
   versions. "Mostly one version" is not good enough, so state the version explicitly.
   - `data_version` is a git-hash-style string and **has no ordering**. Never use
     `max(data_version)` / `min(data_version)` / `ORDER BY data_version` to mean "latest". The
     notebook's `min(data_version)` only picks *a* version for the demo.
   - To get "latest" for a queue-ingested version, use
     `SELECT data_version FROM ingest_queue WHERE experiment=%s AND shot=%s AND stage=%s AND
     status='completed' ORDER BY processed_at DESC LIMIT 1`. The direct ingest path records no
     timestamp, so this is a known gap. Ask the user which version they want, don't guess.

3. **Keep predicates prunable.** Filters must compare the bare column to a literal or parameter:
   `shot = 1`, `stage = 'mirnov'`, `x >= 250 AND x < 251`. Wrapping a partition or sort column in
   a function or cast (`CAST(shot AS VARCHAR) LIKE '1%'`, `shot % 10 = 0`, `round(x) = 250`,
   `lower(stage) = ...`) prevents DuckLake from pruning files via catalog stats and DuckDB from
   pruning row groups via footer stats. Every file then gets opened.

4. **Always filter `wave` together with `stage`.** Wave names are only unique within a stage.
   The same channel name can exist in two stages.

5. **Project only the columns you need.** It's usually `x, y`, sometimes `wave`. Never use
   `SELECT *` in anything that returns rows. `experiment`/`shot`/`stage` are free (they come
   from the path), but every physical column you name gets read and decoded.

6. **Query through the DuckLake table, not a raw `read_parquet` glob.**
   `read_parquet('data/lake/**/*.parquet')` at this scale lists millions of files before
   reading any of them. It is also *wrong*, not just slow. `data/lake/` also holds files the
   catalog doesn't consider live: files waiting in `ingest_queue` (not yet registered), and
   files a direct-path recompaction has replaced (ended, but not yet cleaned up). Both produce
   duplicate samples. A glob is acceptable only for ad-hoc inspection of a single, specific
   partition directory, e.g. `data/lake/experiment=X/shot=1/stage=mirnov/*.parquet`, and only
   if you accept those caveats. The `sql/wave_size_buckets_*.sql` scripts use the full-lake glob
   and are demo-scale only.

7. **Don't pull raw samples into Python or a plot.** One Mirnov wave is ~2M rows. A stage is
   ~75 of those. Reduce in SQL first (see "Downsampling for plots"), or `COPY ... TO` Parquet
   for bulk exports. Never `fetchall()` / `.df()` an unbounded result.

## Choosing an access path

| Question | Use | Why |
|---|---|---|
| Which shots / stages / data_versions exist? | `wave_manifest` or `ingest_queue` (Postgres) | `SELECT DISTINCT experiment, shot, stage, data_version FROM wave_manifest WHERE ...` uses the lookup index. No Parquet is read. |
| Which waves exist in a `(shot, stage, version)`? Their x extent? | `wave_manifest` | Pure row groups (`wave_min = wave_max`) name the wave directly. Packed row groups give a lexicographic range, so for those, read the (small) data. |
| Sample count of one big wave | `wave_manifest`: `sum(row_group_rows)` over pure row groups for that wave | Exact for waves that got dedicated row groups. Packed (small) waves need a data read, which is cheap because they're small by definition. |
| One wave, or a time slice of one, for an app or client | HTTP fetch path (`ducklake-api` / `ducklake-fetch-wave`) | Manifest lookup + byte range: no DuckDB, and the cost is roughly constant regardless of wave size. |
| Analysis within one `(shot, stage, version)` | DuckDB over `physics_lake.waves` with all partition filters | Catalog prunes to that partition's files. Footer stats prune row groups on `wave`/`x`. |
| Analysis across many shots | Batched job: loop over shots, reduce per wave, `COPY` results to Parquet | See "Cross-shot work". |
| Which files/snapshots back a table? | `ducklake_list_files`, `ducklake_snapshots('physics_lake')` | Catalog metadata, no data read. |

### `wave_manifest` query rules

- Lead with equality on `experiment, shot, stage, data_version`. That's the prefix of the
  `wave_manifest_lookup` index. A manifest query without all four is a scan of ~5×10^7 rows.
- Match a wave by **overlap**, not equality: `wave_min <= :wave AND wave_max >= :wave`. Packed
  row groups hold several small waves in a range.
- Match a time window by overlap: `x_max >= :x_min AND x_min <= :x_max`.
- Packed row groups have broad, overlapping `x` stats (each packed wave contributes its own full
  time range), so an `x` filter barely prunes them. That's expected and cheap. Don't "fix" it.
- Two *pure* row groups for the same wave+version in different files with overlapping `x` means
  a duplicate or partial compaction (`fetch_wave.check_no_overlapping_pure_row_groups`). Stop
  and report it. Don't deduplicate silently.

## Query patterns

Examples use DuckDB SQL against an attached catalog (`config.get_connection()`). `?` placeholders
are bound parameters. Prefer binding over string-splicing values into SQL.

### Time slice of one wave

```sql
SELECT x, y
FROM waves
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
  AND wave = ?
  AND x >= ? AND x < ?          -- half-open, so adjacent slices don't double-count
ORDER BY x;
```

`ORDER BY x` is cheap here because the slice is small. Don't rely on file order without it,
since DuckDB doesn't guarantee output order.

### Downsampling for plots (min/max decimation)

Plot at most a few thousand points per wave. Use per-bucket min/max, not `avg` alone, so spikes
and edges (disruptions, ELMs) survive:

```sql
SELECT
    floor(x / ?) * ? AS x_bucket,      -- bucket width in ms, e.g. (x_max - x_min) / 2000
    min(y) AS y_min,
    max(y) AS y_max,
    avg(y) AS y_mean
FROM waves
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
GROUP BY x_bucket
ORDER BY x_bucket;
```

### Per-wave reductions (AUC, RMS, peak)

Window functions must partition by wave, and data_version must be pinned in `WHERE` (rule 2).
Otherwise `lead()` pairs samples from different waves or versions:

```sql
WITH ordered AS (
    SELECT wave, x, y,
           lead(x) OVER w AS x_next,
           lead(y) OVER w AS y_next
    FROM waves
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
    WINDOW w AS (PARTITION BY wave ORDER BY x)
)
SELECT wave, sum(0.5 * (y + y_next) * (x_next - x)) AS auc
FROM ordered
WHERE x_next IS NOT NULL
GROUP BY wave;
```

A window over a whole stage sorts every sample in it. For high-rate stages (Mirnov), add an `x`
window or restrict to the waves you need before reaching for windows.

### Comparing waves with different sample rates

Stages sample at very different rates (Mirnov ~4 MHz, Thomson ~1 kHz pulsed). Their `x` values
almost never match exactly, so **never equi-join on `x`**. Either bucket both sides to a common
grid first, or use an `ASOF JOIN` from the sparse side to the dense side:

```sql
SELECT t.x, t.y AS te, m.y AS b_dot
FROM (SELECT x, y FROM waves WHERE experiment = ? AND shot = ? AND stage = 'thomson'
        AND data_version = ? AND wave = ?) t
ASOF JOIN
     (SELECT x, y FROM waves WHERE experiment = ? AND shot = ? AND stage = 'mirnov'
        AND data_version = ? AND wave = ? AND x >= ? AND x < ?) m
  ON t.x >= m.x;
```

Each side has its own `data_version`, because versions are per stage.

### Cross-shot work

- Treat every multi-shot query as a batch job, not an interactive query. Loop over shots in
  Python (or a bounded `shot IN (...)` chunk), reduce each wave to a few numbers in SQL, and
  write results with `COPY (...) TO 'out/shot=<n>.parquet' (FORMAT parquet)`. Re-query the
  small result set.
- Resolve the data_version per `(shot, stage)` up front, using `ingest_queue` or the user's
  choice. Different shots will have different versions.
- Set resource limits on the connection for anything long-running: `SET memory_limit = '...'`,
  `SET threads = ...`, `SET temp_directory = '...'`. For large exports where order doesn't
  matter, also set `SET preserve_insertion_order = false`.

## Verifying a query before running it at scale

- Run `EXPLAIN ANALYZE` on a single-shot version first and check how many files and row groups
  were actually scanned. A single-wave time slice should touch about 1–2 row groups. If the scan
  count grows with the size of the stage or shot, a predicate isn't pruning (rule 3).
- Estimate the row count from `wave_manifest` (`sum(row_group_rows)` over the matching
  partitions) before running a query that returns rows.
- For reproducible analyses, pin a catalog snapshot (`SELECT ... FROM waves AT (VERSION => n)`,
  with `n` taken from `ducklake_snapshots('physics_lake')`), so a version registered mid-analysis
  doesn't change the results.

## Things that look reasonable but aren't

- `SELECT count(DISTINCT wave) FROM waves` is a full scan. Use the manifest.
- Picking the latest data_version with `ORDER BY data_version DESC` is wrong, because hashes are
  unordered.
- `WHERE wave LIKE 'mirnov/%'` without `stage = 'mirnov'` scans every stage's files. The `wave`
  prefix is not a partition key.
- `LIMIT 100` on a query with `ORDER BY` or `GROUP BY` doesn't bound the scan. Only partition
  and `x` filters do.
- Downsampling with `USING SAMPLE` drops peaks and is non-deterministic. Use min/max
  decimation for anything a physicist will look at.
- Concatenating HTTP-fetched row-group bytes and opening them as a `.parquet` file fails,
  because they're fragments without a footer. Use `fetch_wave.py`'s validated-read path.

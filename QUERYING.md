# Querying the lake

This document gives rules for queries on the wave lake at production scale. The wave lake has
three access points: the DuckLake table `physics_lake.waves`, the `wave_manifest` Postgres
table, and the HTTP fetch path. The demo dataset is small, so almost any query works on it. The
rules are for the target scale. At that scale, most simple queries do not finish.

## Scale envelope (design target)

| Quantity | Value | Derived from |
|---|---|---|
| Total rows (samples) | ~10^12 | target |
| Waves per shot | ~3,000 | target |
| Stages per shot | ~40 (about 75 waves for each stage on average, very uneven) | target |
| Hive partitions per shot | 40 (`experiment=/shot=/stage=`) | multiply by the number of shots and the kept data_versions |
| Row groups total | ~5×10^7 | 10^12 / 20,000 (`rows_per_row_group` default) |
| `wave_manifest` rows | ~5×10^7 for each retained set of data_versions | one row for each row group |
| Uncompressed `x`+`y` | ~16 TB | 16 B for each row. On disk, about half (the README measured about 2× from the encoding policy) |

Wave sizes are not equal. A 4 MHz Mirnov probe has about 2M samples in 500 ms. A pulsed Thomson
channel can have a few hundred samples. "One wave" can be 1 row group or more than 100 row
groups. Plan your query for the stage that you query.

## Hard rules

1. **Do not scan `waves` without partition filters.** Each query on the DuckLake table must pin
   `experiment` and `stage` with equality. It must pin `shot` with equality, `IN (...)`, or a
   bounded `BETWEEN`. A query without filters reads all ~10^12 rows. This applies to
   `count(*)`, `count(DISTINCT ...)` and `SELECT DISTINCT wave`. Answer metadata questions from
   the manifest or the catalog (see "Choosing an access path").

2. **Pin exactly one `data_version` for each `(shot, stage)`.** Each recalibration rerun keeps
   all old versions. If you do not set `data_version`, the query returns every copy of every
   sample. The counts are too high, the aggregates are wrong, and window functions pair samples
   from different versions. "Mostly one version" is not enough. State the version.
   - `data_version` is a git-hash-style string. It **has no order**. Do not use
     `max(data_version)`, `min(data_version)` or `ORDER BY data_version` to find the latest
     version. The notebook `min(data_version)` selects only *a* version for the demo.
   - To find the latest queue-ingested version, use
     `SELECT data_version FROM ingest_queue WHERE experiment=%s AND shot=%s AND stage=%s AND
     status='completed' ORDER BY processed_at DESC LIMIT 1`. The direct ingest path does not
     record a timestamp. This is a known gap. Ask the user which version they want. Do not
     guess.

3. **Keep predicates prunable.** Compare the bare column to a literal or a parameter. Examples:
   `shot = 1`, `stage = 'mirnov'`, `x >= 250 AND x < 251`. Do not put a partition column or a
   sort column in a function or a cast. Examples of bad predicates:
   `CAST(shot AS VARCHAR) LIKE '1%'`, `shot % 10 = 0`, `round(x) = 250`, `lower(stage) = ...`.
   Such a predicate stops DuckLake from pruning files with catalog statistics. It also stops
   DuckDB from pruning row groups with footer statistics. Then the query opens every file.

4. **Always filter `wave` together with `stage`.** A wave name is unique only in its stage. Two
   stages can have the same channel name.

5. **Select only the columns that you need.** Usually this is `x, y`, and sometimes `wave`. Do
   not use `SELECT *` in a query that returns rows. `experiment`, `shot` and `stage` are free,
   because they come from the path. The query reads and decodes each physical column that you
   name.

6. **Query the DuckLake table. Do not query a raw `read_parquet` glob.**
   At this scale, `read_parquet('data/lake/**/*.parquet')` lists millions of files before it
   reads one file. The result is also *wrong*, not only slow. `data/lake/` holds files that the
   catalog does not use: files that wait in `ingest_queue` (not registered yet), and files that
   a direct-path recompaction replaced (ended, but not deleted yet). Both give duplicate
   samples. You can use a glob to look at one specific partition directory, for example
   `data/lake/experiment=X/shot=1/stage=mirnov/*.parquet`. Accept the limits above when you do.
   The `sql/wave_size_buckets_*.sql` scripts use the full-lake glob. They are for demo scale
   only.

7. **Do not pull raw samples into Python or into a plot.** One Mirnov wave has about 2M rows.
   A stage has about 75 such waves. Reduce the data in SQL first (see "Downsampling for plots").
   For bulk exports, use `COPY ... TO` Parquet. Do not call `fetchall()` or `.df()` on an
   unbounded result.

## Choosing an access path

| Question | Use | Why |
|---|---|---|
| Which shots, stages and data_versions exist? | `wave_manifest` or `ingest_queue` (Postgres) | `SELECT DISTINCT experiment, shot, stage, data_version FROM wave_manifest WHERE ...` uses the lookup index. The query reads no Parquet. |
| Which waves exist in a `(shot, stage, version)`? What is their x extent? | `wave_manifest` | Pure row groups (`wave_min = wave_max`) give the wave name directly. Packed row groups give a lexicographic range. For packed row groups, read the (small) data. |
| Sample count of one big wave | `wave_manifest`: `sum(row_group_rows)` for the pure row groups of that wave | The count is exact for waves with dedicated row groups. Packed (small) waves need a data read. This is cheap, because the waves are small. |
| One wave, or a time slice of one wave, for an application or a client | HTTP fetch path (`ducklake-api` or `ducklake-fetch-wave`) | The path uses a manifest lookup and a byte range. It does not use DuckDB. The cost is almost constant for each wave size. |
| Analysis in one `(shot, stage, version)` | DuckDB on `physics_lake.waves` with all partition filters | The catalog prunes to the files of that partition. Footer statistics prune row groups on `wave` and `x`. |
| Analysis across many shots | One range query (`shot BETWEEN` or `IN`) with one pinned version for each shot. Use a batch for each shot only if the range query is too slow. | See "Cross-shot work". |
| Which files and snapshots back a table? | `ducklake_list_files`, `ducklake_snapshots('physics_lake')` | These are catalog metadata. The query reads no data. |

### `wave_manifest` query rules

- Start with equality on `experiment, shot, stage, data_version`. These columns are the prefix
  of the `wave_manifest_lookup` index. A manifest query without all four columns scans ~5×10^7
  rows.
- Match a wave by **overlap**, not by equality: `wave_min <= :wave AND wave_max >= :wave`. A
  packed row group holds several small waves in one range.
- Match a time window by overlap: `x_max >= :x_min AND x_min <= :x_max`.
- Packed row groups have wide `x` statistics that overlap. Each packed wave adds its own full
  time range. An `x` filter does not prune them well. This is expected and cheap. Do not "fix"
  it.
- Two *pure* row groups of the same wave and version in different files can have overlapping
  `x`. This shows a duplicate or a partial compaction
  (`fetch_wave.check_no_overlapping_pure_row_groups`). Stop and report it. Do not remove the
  duplicate silently.

## Query patterns

The examples use DuckDB SQL on an attached catalog (`config.get_connection()`). A `?` is a bound
parameter. Bind values. Do not insert values into the SQL string.

### Time slice of one wave

```sql
SELECT x, y
FROM waves
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
  AND wave = ?
  AND x >= ? AND x < ?          -- half-open: adjacent slices do not count a sample twice
ORDER BY x;
```

`ORDER BY x` is cheap here, because the slice is small. Always use it. DuckDB does not
guarantee the order of the output.

### Downsampling for plots (min/max decimation)

Plot a few thousand points at most for each wave. Use the min and max of each bucket. Do not
use `avg` alone. Then spikes and edges (disruptions, ELMs) stay in the plot:

```sql
SELECT
    floor(x / ?) * ? AS x_bucket,      -- bucket width in ms, for example (x_max - x_min) / 2000
    min(y) AS y_min,
    max(y) AS y_max,
    avg(y) AS y_mean
FROM waves
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
GROUP BY x_bucket
ORDER BY x_bucket;
```

### Reductions for each wave (AUC, RMS, peak)

A window function must partition by wave. Pin data_version in `WHERE` (rule 2). If you do
not, `lead()` pairs samples from different waves or versions:

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

A window over a whole stage sorts every sample of the stage. For high-rate stages (Mirnov), add
an `x` window, or select only the waves that you need, before you use a window.

### Comparing waves with different sample rates

Stages have very different sample rates (Mirnov about 4 MHz, Thomson about 1 kHz pulsed). Their
`x` values almost never match exactly. **Do not join on `x` with equality.** Use one of these
methods:
- Bucket both sides to a common grid first.
- Use an `ASOF JOIN` from the sparse side to the dense side.

```sql
SELECT t.x, t.y AS te, m.y AS b_dot
FROM (SELECT x, y FROM waves WHERE experiment = ? AND shot = ? AND stage = 'thomson'
        AND data_version = ? AND wave = ?) t
ASOF JOIN
     (SELECT x, y FROM waves WHERE experiment = ? AND shot = ? AND stage = 'mirnov'
        AND data_version = ? AND wave = ? AND x >= ? AND x < ?) m
  ON t.x >= m.x;
```

Each side has its own `data_version`, because versions are set for each stage.

### Cross-shot work

Start with **one range query**. A range of shots is one bounded query. Use `shot BETWEEN ? AND ?`
or `shot IN (...)`. The catalog prunes to the shots that you name.

Pin the version of each shot with a small `VALUES` table. Then each shot uses exactly one
`data_version` (rule 2):

```sql
WITH pins(shot, data_version) AS (VALUES (?, ?), (?, ?))      -- one pair for each shot
SELECT w.shot, w.wave, sqrt(avg(w.y * w.y)) AS rms, max(abs(w.y)) AS peak
FROM waves w
JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ?
GROUP BY w.shot, w.wave;
```

- Find the data_version of each `(shot, stage)` before you run the query. Use `ingest_queue`, or
  ask the user. Different shots have different versions.
- Reduce in SQL. Return a few numbers for each wave. Do not return raw samples (rule 7).

**Use a per-shot batch only if the range query is too slow or does not fit in memory.** First,
run the range query on 2-3 shots. Multiply the time by the number of shots. Estimate the number
of rows from `wave_manifest`. Use a batch if one of these conditions is true:

| Symptom of the range query | Why a batch helps |
|---|---|
| The query takes longer than your time limit (minutes, not seconds). | Each shot is a short, separate query. |
| The query spills to disk or reaches `memory_limit`. | One shot limits the memory use. |
| One failure loses all the work. | You can rerun a failed shot alone. The finished shots stay on disk. |
| You use the result many times. | Run `COPY` once. Then query the small Parquet files. |
| A window function sorts every sample of a high-rate stage. | The sort is for one shot, not for all shots. |

| Example | Choice |
|---|---|
| RMS of 3 Thomson channels (about 500 samples each) over 200 shots | Range query (about 10^5 rows). |
| Peak of 1 Mirnov channel over 20 shots | Range query. The `wave` and `stage` filters prune well. |
| Peak of every Mirnov channel over 500 shots at full rate | Batch (10^10 to 10^11 rows). |
| `wave_auc` over a whole Mirnov stage for 100 shots | Batch. The window function sorts every sample. |
| Same reduction, run again when a new shot arrives | Batch. Do it for the new shot only. |

A batch is a loop over shots in Python. Each loop runs one bounded query. Each query writes a
few rows:

```python
for shot in shots:
    version = versions[(shot, stage)]
    con.execute(
        f"COPY ({REDUCE_SQL}) TO 'out/shot={shot}.parquet' (FORMAT parquet)",
        [experiment, shot, stage, version],
    )
small = con.execute("SELECT * FROM read_parquet('out/shot=*.parquet')").df()
```

- Do not add `LIMIT` to make a slow range query faster. `LIMIT` does not bound the scan.
- Set resource limits on the connection for long-running work: `SET memory_limit = '...'`,
  `SET threads = ...`, `SET temp_directory = '...'`. For large exports where row order is not
  important, also set `SET preserve_insertion_order = false`.

## Verifying a query before running it at scale

- Run `EXPLAIN ANALYZE` on a single shot first. Check how many files and row groups the query
  scans. A time slice of one wave must read about 1 or 2 row groups. If the scan count
  increases with the size of the stage or shot, a predicate does not prune (rule 3).
- Estimate the number of rows from `wave_manifest` (`sum(row_group_rows)` for the matching
  partitions) before you run a query that returns rows.
- For a repeatable analysis, pin a catalog snapshot
  (`SELECT ... FROM waves AT (VERSION => n)`). Take `n` from
  `ducklake_snapshots('physics_lake')`. Then a version that registers during the analysis does
  not change the results.

## Things that look correct but are not

- `SELECT count(DISTINCT wave) FROM waves` is a full scan. Use the manifest.
- `ORDER BY data_version DESC` does not give the latest data_version, because hashes have no
  order.
- `WHERE wave LIKE 'mirnov/%'` without `stage = 'mirnov'` scans the files of every stage. The
  `wave` prefix is not a partition key.
- `LIMIT 100` on a query with `ORDER BY` or `GROUP BY` does not bound the scan. Only partition
  filters and `x` filters bound it.
- `USING SAMPLE` for downsampling removes peaks and gives different results each time. Use
  min/max decimation for all data that a physicist looks at.
- Row-group bytes from the HTTP fetch path are fragments without a footer. If you join the bytes
  and open them as a `.parquet` file, the read fails. Use the validated-read path of
  `fetch_wave.py`.

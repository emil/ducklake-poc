-- Buckets each wave (one row per (shot, stage, wave, data_version),
-- matching the atomic unit compact.py's _iter_wave_groups operates on)
-- by its sample count, scoped per shot, to eyeball how row-group sizes
-- should vary shot-to-shot -- e.g. a shot dominated by "over 1M" waves
-- wants a much larger --rows-per-row-group than one that's all "up to
-- 10K" (see README / compact.py's docstring for the sizing tradeoff
-- this feeds into).
--
-- Run against the compacted lake with the DuckDB CLI from the repo root:
--
--   duckdb :memory: < sql/wave_size_buckets.sql
--
-- Point GLOB_PATTERN at data/raw/**/*.parquet instead to size waves
-- *before* compaction has ever run.

CREATE OR REPLACE VIEW waves AS
    SELECT * FROM read_parquet('data/lake/**/*.parquet', hive_partitioning = true);

WITH wave_sizes AS (
    -- stage is included here (though not in the final grouping) because
    -- it's still part of what identifies one wave instance -- two
    -- different stages can otherwise share a wave name.
    SELECT
        shot,
        stage,
        wave,
        data_version,
        count(*) AS n_samples
    FROM waves
    GROUP BY shot, stage, wave, data_version
),
bucketed AS (
    SELECT
        shot,
        n_samples,
        CASE
            WHEN n_samples <= 10000   THEN 1
            WHEN n_samples <= 100000  THEN 2
            WHEN n_samples <= 1000000 THEN 3
            ELSE 4
        END AS bucket_rank,
        CASE
            WHEN n_samples <= 10000   THEN 'up to 10K'
            WHEN n_samples <= 100000  THEN 'up to 100K'
            WHEN n_samples <= 1000000 THEN 'up to 1M'
            ELSE 'over 1M'
        END AS sample_bucket
    FROM wave_sizes
)
SELECT
    shot,
    sample_bucket,
    count(*)       AS n_waves,
    min(n_samples) AS min_samples,
    max(n_samples) AS max_samples,
    sum(n_samples) AS total_samples
FROM bucketed
GROUP BY shot, sample_bucket, bucket_rank
ORDER BY shot, bucket_rank;

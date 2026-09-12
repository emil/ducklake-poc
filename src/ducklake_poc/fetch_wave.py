"""Look up a wave version (optionally restricted to a narrow x/time range)
in the wave_manifest table and fetch just the overlapping row groups.

`data_version` is a git-commit-hash-style string identifying the
calibration + processing code that produced this copy of the wave (not a
sequential integer -- there's no inherent ordering in the hash value
itself). It's required and matched exactly (not ranged) -- it's a hard
partition boundary in compact.py (at both the row-group AND file level),
so every row group has a single, known data_version, and a query almost
always wants exactly one version (typically "the latest", which in a real
system would need to be looked up via a timestamp elsewhere, not derived
from the hash), never a mix.

Two lookup modes:

  --wave only            -> every row group belonging to that wave version
                             (could be many, for a big wave -- expected).
  --wave --x-min --x-max -> only the row groups whose x range overlaps the
                             requested window. This is the "1ms out of a
                             2M-sample wave" case: with the table sorted
                             by (experiment, shot, stage, wave,
                             data_version, x) and compaction giving large
                             waves their own dedicated row groups (see
                             compact.py), this should return a small,
                             roughly constant number of row groups
                             regardless of how big the wave is.

Usage:
    python -m ducklake_poc.fetch_wave --experiment campaign-2026a --shot 1 \
        --stage mirnov --wave mirnov/probe_03 --data-version a1b2c3d
    python -m ducklake_poc.fetch_wave --experiment campaign-2026a --shot 1 \
        --stage mirnov --wave mirnov/probe_03 --data-version a1b2c3d \
        --x-min 250.0 --x-max 251.0
"""

from __future__ import annotations

import argparse

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from . import config
from .range_http_file import RangeHTTPFile

COLUMNS = (
    "file_path, file_url, row_group_id, row_group_rows, wave_min, wave_max, "
    "data_version, x_min, x_max, byte_start, byte_length"
)

WHOLE_WAVE_SQL = f"""
SELECT {COLUMNS}
FROM wave_manifest
WHERE experiment = %s AND shot = %s AND stage = %s AND data_version = %s
  AND wave_min <= %s AND wave_max >= %s
ORDER BY row_group_id
"""

X_RANGE_SQL = f"""
SELECT {COLUMNS}
FROM wave_manifest
WHERE experiment = %s AND shot = %s AND stage = %s AND data_version = %s
  AND wave_min <= %s AND wave_max >= %s
  AND x_max >= %s AND x_min <= %s
ORDER BY row_group_id
"""


def check_no_overlapping_pure_row_groups(rows: list[tuple], wave: str, data_version: str) -> None:
    """Raise SystemExit if two "pure" (single-wave) row groups for the
    same wave+data_version claim overlapping x ranges in different files.

    Pulled out of lookup() so it can be unit tested directly against
    synthetic manifest rows, without a database.

    Consistency check: under the compaction logic in compact.py, a wave
    large enough to need multiple row groups is NEVER mixed with another
    wave's rows, and stage/data_version are hard partition boundaries at
    both the row-group AND file level -- so a "pure" row group
    (wave_min == wave_max == that wave) always has a single, known
    data_version already, and two pure row groups for the SAME
    wave+data_version with overlapping x ranges is never valid (samples
    are sorted and partitioned, not duplicated). That indicates a
    duplicate/partial compaction, so fail loudly rather than silently
    returning duplicated or inconsistent data.

    Mixed row groups (wave_min != wave_max -- several small waves from
    the same stage+data_version packed together) are deliberately
    excluded from this check: their x stats are expected to be
    broad/overlapping since each packed wave's x independently resets,
    which is a normal precision trade-off for small waves, not corruption.
    """
    pure = [r for r in rows if r[4] == r[5] == wave]
    intervals = sorted(((r[7], r[8], r) for r in pure), key=lambda t: t[0])
    for (a_min, a_max, a_row), (b_min, b_max, b_row) in zip(intervals, intervals[1:], strict=False):
        if a_max > b_min and a_row[0] != b_row[0]:
            raise SystemExit(
                f"overlapping x ranges for wave={wave} data_version={data_version} "
                f"across different files -- "
                f"{a_row[0]} rg{a_row[2]} [{a_min},{a_max}] vs "
                f"{b_row[0]} rg{b_row[2]} [{b_min},{b_max}]. This should be "
                f"impossible and indicates a duplicate or partial compaction; "
                f"investigate before trusting a fetch for this wave."
            )


def lookup(
    experiment: str,
    shot: int,
    stage: str,
    wave: str,
    data_version: str,
    x_min: float | None,
    x_max: float | None,
):
    conn = psycopg2.connect(**config.pg_dsn_for_psycopg2())
    try:
        with conn.cursor() as cur:
            if x_min is None:
                cur.execute(WHOLE_WAVE_SQL, (experiment, shot, stage, data_version, wave, wave))
            else:
                cur.execute(
                    X_RANGE_SQL,
                    (experiment, shot, stage, data_version, wave, wave, x_min, x_max),
                )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise SystemExit(
            f"no manifest entries for experiment={experiment} shot={shot} stage={stage} "
            f"wave={wave} data_version={data_version}"
        )

    check_no_overlapping_pure_row_groups(rows, wave, data_version)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", type=str, required=True)
    parser.add_argument("--shot", type=int, required=True)
    parser.add_argument("--stage", type=str, required=True)
    parser.add_argument("--wave", type=str, required=True)
    parser.add_argument("--data-version", type=str, required=True)
    parser.add_argument("--x-min", type=float, default=None)
    parser.add_argument("--x-max", type=float, default=None)
    args = parser.parse_args()

    if (args.x_min is None) != (args.x_max is None):
        parser.error("--x-min and --x-max must be given together")

    rows = lookup(
        args.experiment,
        args.shot,
        args.stage,
        args.wave,
        args.data_version,
        args.x_min,
        args.x_max,
    )

    mode = "x-range" if args.x_min is not None else "whole-wave"
    print(
        f"manifest hit ({mode}): experiment={args.experiment} shot={args.shot} "
        f"stage={args.stage} wave={args.wave} data_version={args.data_version}"
    )
    print(f"  matched row groups: {len(rows)}")

    total_bytes = 0
    tables = []
    for (
        _file_path,
        file_url,
        row_group_id,
        row_group_rows,
        wave_min,
        wave_max,
        _data_version,
        x_min,
        x_max,
        _byte_start,
        byte_length,
    ) in rows:
        mixed = " (mixed waves)" if wave_min != wave_max else ""
        print(
            f"    rg={row_group_id:>4}  x=[{x_min:.6f}, {x_max:.6f}]  "
            f"rows={row_group_rows:>7}  bytes={byte_length:>10}{mixed}  file={file_url}"
        )
        total_bytes += byte_length

        f = RangeHTTPFile(file_url)
        pf = pq.ParquetFile(f, pre_buffer=False)
        tables.append(pf.read_row_group(row_group_id))

    combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    # Always filter locally to the exact wave (and x window, if given) --
    # matched row groups may include mixed ones that contain other waves too.
    # (experiment/stage/data_version don't need a local filter: they're
    # exact matches in SQL already, since every row group has exactly one.)
    wave_mask = combined["wave"].to_numpy(zero_copy_only=False) == args.wave
    if args.x_min is not None:
        x = combined["x"].to_numpy()
        wave_mask &= (x >= args.x_min) & (x <= args.x_max)
    combined = combined.filter(pa.array(wave_mask))

    print(f"\n  fetched {len(tables)} row group(s), {total_bytes} bytes total")
    print(f"  rows after exact filter: {combined.num_rows}")


if __name__ == "__main__":
    main()

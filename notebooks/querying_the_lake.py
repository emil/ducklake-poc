import marimo

__generated_with = "0.25.1"
app = marimo.App(
    width="full",
    app_title="Querying the wave lake",
    layout_file="layouts/querying_the_lake.slides.json",
)


# ---------------------------------------------------------------------------
# Slide 1: title
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Querying the wave lake

        **Fast queries and plots on 10¹² samples. Do not scan them all.**

        - DuckLake holds one row for each sample. It partitions by `experiment`, `shot` and `stage`.
        - This deck shows single-shot queries, downsampling and multi-shot reductions.
        - It shows the rules from `QUERYING.md` with live queries.

        *Use the arrow keys to move between slides.*
        """
    )
    return


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
@app.cell
def _():
    import os
    import sys
    import tempfile
    import time
    from pathlib import Path

    import duckdb
    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    return Path, duckdb, mo, np, os, pd, plt, sys, tempfile, time


# ---------------------------------------------------------------------------
# Data source. Local demo lake by default. A real DuckLake catalog when the
# LAKE_CATALOG environment variable is set.
# ---------------------------------------------------------------------------
@app.cell
def _(Path, duckdb, os, pd, tempfile):
    from ducklake_poc import config, ingest, synthetic
    from ducklake_poc.compact import stream_compact_to_files
    from ducklake_poc.lake_client import LakeClient
    from ducklake_poc.parquet_encoding import open_parquet_writer
    from ducklake_poc.synthetic import wave_to_sample_table

    CATALOG = os.environ.get("LAKE_CATALOG")
    LAKE_URL = os.environ.get("LAKE_URL", config.NGINX_HOST_URL)
    EXPERIMENT = os.environ.get("LAKE_EXPERIMENT", synthetic.DEFAULT_EXPERIMENT)

    DEMO_STAGES = ["rogowski", "mirnov", "thomson"]
    DEMO_SHOTS = list(range(1, 13))
    DEMO_ROOT = Path(tempfile.gettempdir()) / "ducklake_marimo_demo"
    DEMO_LAKE = DEMO_ROOT / "lake"
    REDUCTIONS_DIR = DEMO_ROOT / "reductions"
    REDUCTIONS_DIR.mkdir(parents=True, exist_ok=True)

    def _build_demo_lake() -> None:
        """Write the demo shots. Compact them with the production code."""
        import shutil

        raw = DEMO_ROOT / "raw"
        for d in (raw, DEMO_LAKE):
            if d.exists():
                shutil.rmtree(d)
        for shot in DEMO_SHOTS:
            for stage in DEMO_STAGES:
                out = raw / f"experiment={EXPERIMENT}" / f"shot={shot}" / f"stage={stage}"
                out.mkdir(parents=True, exist_ok=True)
                for rec in synthetic.generate_stage(shot=shot, stage=stage, experiment=EXPERIMENT):
                    table = wave_to_sample_table(rec)
                    name = f"{config.safe_filename(rec.wave)}__{rec.data_version}.parquet"
                    writer = open_parquet_writer(str(out / name), table.schema)
                    writer.write_table(table)
                    writer.close()
        source_sql = (
            f"SELECT * FROM read_parquet('{raw}/**/*.parquet', hive_partitioning=true) "
            "ORDER BY experiment, shot, stage, wave, data_version, x"
        )
        stream_compact_to_files(duckdb.connect(), source_sql, DEMO_LAKE)
        (DEMO_ROOT / "READY").touch()

    if CATALOG:
        client = LakeClient(LAKE_URL, catalog=CATALOG)
        _con = None
        MODE = f"DuckLake catalog `{CATALOG}`"
    else:
        client = None
        if not (DEMO_ROOT / "READY").exists():
            _build_demo_lake()
        _con = duckdb.connect()
        # This view replaces the DuckLake table in the demo. It has the same
        # columns, so each query runs without change on the real table.
        # Do not use a glob view at production scale (QUERYING.md rule 6).
        _con.execute(
            f"CREATE VIEW {config.TABLE_NAME} AS "
            f"SELECT * FROM read_parquet('{DEMO_LAKE}/**/*.parquet', hive_partitioning=true)"
        )
        # The real catalog stores `wave_auc`. Here we install the same definition.
        _con.execute(ingest._WAVE_AUC_SQL)
        MODE = "local demo lake (synthetic shots, no services needed)"

    def q(sql: str, params: list | None = None) -> pd.DataFrame:
        """Run SQL on the lake. Return a DataFrame. Use `?` to bind values."""
        if client is not None:
            return client.query(sql, params).to_pandas()
        return _con.execute(sql, params or []).df()

    def _load_versions() -> pd.DataFrame:
        """Return all known (shot, stage, data_version) rows.

        Production: read the `ingest_queue` table (Postgres). Do not scan `waves`.
        Demo: the lake is small, so one DISTINCT query is acceptable.
        """
        if client is not None:
            import psycopg2

            with psycopg2.connect(**config.pg_dsn_for_psycopg2()) as pg, pg.cursor() as cur:
                cur.execute(
                    "SELECT shot, stage, data_version, processed_at FROM ingest_queue "
                    "WHERE experiment = %s AND status = 'completed'",
                    (EXPERIMENT,),
                )
                rows = cur.fetchall()
            return pd.DataFrame(rows, columns=["shot", "stage", "data_version", "processed_at"])
        df = q(f"SELECT DISTINCT shot, stage, data_version FROM {config.TABLE_NAME}")
        df["processed_at"] = pd.NaT
        return df

    all_versions = _load_versions().sort_values(["shot", "stage", "data_version"])

    # Select one version for each (shot, stage). "Latest" is the newest
    # `processed_at`. A data_version has no order. The demo has no
    # timestamps, so it takes the first version by name. This is a demo
    # shortcut. In real work, ask the user.
    _by_time = all_versions.sort_values("processed_at", ascending=False, na_position="last")
    pinned = {
        (int(r.shot), r.stage): r.data_version
        for r in (_by_time if _by_time["processed_at"].notna().any() else all_versions)
        .drop_duplicates(["shot", "stage"])
        .itertuples()
    }
    SHOTS = sorted({s for s, _ in pinned})
    STAGES = sorted({st for _, st in pinned})

    def pin(shot: int, stage: str) -> str:
        return pinned[(int(shot), stage)]

    return (
        CATALOG,
        DEMO_LAKE,
        EXPERIMENT,
        MODE,
        REDUCTIONS_DIR,
        SHOTS,
        STAGES,
        all_versions,
        config,
        pin,
        pinned,
        q,
    )


# ---------------------------------------------------------------------------
# Slide 2: the scale problem
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(MODE, SHOTS, STAGES, mo):
    mo.md(
        f"""
        ## Why queries need rules

        | Quantity | Design target |
        |---|---|
        | Rows (one per sample) | ~10¹² |
        | Waves per shot | ~3,000 across ~40 stages |
        | Row groups | ~5 × 10⁷ |
        | `x` + `y`, uncompressed | ~16 TB |

        A simple `SELECT count(*) FROM waves` reads **all** of it.
        The demo data is small, so every query works here.
        The rules are for the target scale.

        > **Data source:** {MODE}
        > **Shots:** {SHOTS[0]}–{SHOTS[-1]} ({len(SHOTS)})  ·  **Stages:** {", ".join(STAGES)}
        """
    )
    return


# ---------------------------------------------------------------------------
# Slide 3: the access paths
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Select the access path first

        | Question | Use |
        |---|---|
        | Which shots, stages and versions exist? | `ingest_queue` or `wave_manifest` (Postgres) |
        | One wave, or a time slice, for an application | HTTP fetch path (`LakeClient.fetch`) |
        | Analysis in one `(shot, stage, version)` | SQL on `waves` with all partition filters |
        | Analysis across shots | One range query. Use a batch only if it is too slow. |

        ```python
        client = LakeClient("http://localhost:8080", catalog=CATALOG)
        client.fetch(experiment=..., shot=1, stage="mirnov", data_version=v,
                     wave="mirnov/probe_03", x_min=250, x_max=251)   # HTTP, no DuckDB
        client.query("SELECT ... FROM waves WHERE ...", params)       # SQL, Arrow table
        ```
        """
    )
    return


# ---------------------------------------------------------------------------
# Slide 4: rules 1-2 -- partition filters and data_version
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Four filters for each query

        1. `experiment = ?` and `stage = ?`: use equality.
        2. `shot = ?`, or `IN (...)`, or a bounded `BETWEEN`.
        3. `data_version = ?`: use **exactly one** for each `(shot, stage)`.
        4. `wave = ?`: always use it together with `stage`.

        Keep each predicate bare. Write `x >= ? AND x < ?`. Do not write `round(x) = ?`.
        Select only the columns that you need.

        **Next slide:** what happens when you do not pin the version.
        """
    )
    return


@app.cell(hide_code=True)
def _(EXPERIMENT, all_versions, config, mo, pin, plt, q):
    # Find a (shot, stage) that has more than one recalibration rerun.
    _counts = all_versions.groupby(["shot", "stage"]).size()
    _multi = _counts[_counts > 1]
    if _multi.empty:
        mo.output.replace(mo.md("No `(shot, stage)` in this lake has more than one version."))
        mo.stop(True)
    _shot, _stage = (int(_multi.index[0][0]), _multi.index[0][1])
    _version = pin(_shot, _stage)

    _base = (
        f"SELECT count(*) AS n FROM {config.TABLE_NAME} "
        "WHERE experiment = ? AND shot = ? AND stage = ?"
    )
    _unpinned = int(q(_base, [EXPERIMENT, _shot, _stage])["n"][0])
    _pinned = int(q(_base + " AND data_version = ?", [EXPERIMENT, _shot, _stage, _version])["n"][0])

    _fig, _ax = plt.subplots(figsize=(6, 3))
    _ax.bar(
        ["data_version unpinned", f"pinned ({_version})"],
        [_unpinned, _pinned],
        color=["#c0392b", "#2c7fb8"],
    )
    _ax.set_ylabel("samples counted")
    _ax.set_title(f"shot {_shot}, stage {_stage}: {len(all_versions[(all_versions.shot == _shot) & (all_versions.stage == _stage)])} versions")
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(
                f"## Rule 2: pin one `data_version`\n\n"
                f"Each recalibration rerun keeps **all** old versions. "
                f"Without the filter, the count is **{_unpinned / _pinned:.1f}×** too high. "
                f"A window function also pairs samples from different versions.\n\n"
                f"`data_version` is a hash. It has **no order**. "
                f"Do not use `max(data_version)` to find the latest version."
            ),
            _fig,
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 5: time slice of one wave
# ---------------------------------------------------------------------------
@app.cell
def _(SHOTS, mo):
    shot_ui = mo.ui.dropdown(
        options={str(s): s for s in SHOTS}, value=str(SHOTS[0]), label="shot"
    )
    window_ui = mo.ui.range_slider(
        start=0, stop=500, step=1, value=[100, 140], label="x window (ms)", full_width=True
    )
    return shot_ui, window_ui


@app.cell(hide_code=True)
def _(EXPERIMENT, config, mo, pin, plt, q, shot_ui, window_ui):
    _wave, _stage = "mirnov/probe_03", "mirnov"
    _x0, _x1 = window_ui.value
    mo.stop(_x1 <= _x0, mo.md("Choose a window with `x_min < x_max`."))
    _version = pin(shot_ui.value, _stage)

    TIME_SLICE_SQL = f"""SELECT x, y
FROM {config.TABLE_NAME}
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
  AND wave = ?
  AND x >= ? AND x < ?      -- half-open: adjacent slices do not double-count
ORDER BY x"""
    _df = q(TIME_SLICE_SQL, [EXPERIMENT, shot_ui.value, _stage, _version, _wave, _x0, _x1])

    _fig, _ax = plt.subplots(figsize=(9, 3.2))
    _ax.plot(_df["x"], _df["y"], linewidth=0.8)
    _ax.set_xlabel("x (ms)")
    _ax.set_ylabel("y")
    _ax.set_title(f"{_wave}  ·  shot {shot_ui.value}  ·  v={_version}  ·  {len(_df):,} samples")
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md("## Time slice of one wave\n\nThe query reads 1 or 2 row groups. The wave size does not change this."),
            mo.hstack([shot_ui, window_ui], justify="start"),
            _fig,
            mo.md(f"```sql\n{TIME_SLICE_SQL}\n```"),
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 6: min/max decimation
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    buckets_ui = mo.ui.slider(
        start=20, stop=2000, step=20, value=200, label="buckets", full_width=True
    )
    return (buckets_ui,)


@app.cell(hide_code=True)
def _(EXPERIMENT, buckets_ui, config, mo, pin, plt, q, shot_ui):
    _wave, _stage = "mirnov/probe_03", "mirnov"
    _version = pin(shot_ui.value, _stage)
    _width = 500.0 / buckets_ui.value  # shot duration is 500 ms

    DECIMATE_SQL = f"""SELECT
    floor(x / ?) * ? AS x_bucket,
    min(y) AS y_min,
    max(y) AS y_max,
    avg(y) AS y_mean
FROM {config.TABLE_NAME}
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
GROUP BY x_bucket
ORDER BY x_bucket"""
    _df = q(DECIMATE_SQL, [_width, _width, EXPERIMENT, shot_ui.value, _stage, _version, _wave])

    _fig, _ax = plt.subplots(figsize=(9, 3.2))
    _ax.fill_between(_df["x_bucket"], _df["y_min"], _df["y_max"], alpha=0.35, label="min–max")
    _ax.plot(_df["x_bucket"], _df["y_mean"], linewidth=1.0, label="mean")
    _ax.set_xlabel("x (ms)")
    _ax.set_ylabel("y")
    _ax.legend(loc="upper right")
    _ax.set_title(f"{_wave} · shot {shot_ui.value} · {len(_df)} points plotted")
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(
                "## Plot a whole wave: min/max decimation\n\n"
                "Plot a few thousand points at most. The **min and max** of each bucket keep "
                "spikes and edges. `avg` alone and `USING SAMPLE` remove them."
            ),
            buckets_ui,
            _fig,
            mo.md(f"```sql\n{DECIMATE_SQL}\n```"),
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 7: per-wave reductions with window functions
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(EXPERIMENT, config, mo, pin, plt, q, shot_ui):
    _stage = "rogowski"
    _version = pin(shot_ui.value, _stage)
    AUC_CALL = "SELECT wave, auc FROM wave_auc(?, ?, ?, ?)  -- experiment, shot, stage, data_version"
    _df = q(AUC_CALL + " ORDER BY wave", [EXPERIMENT, shot_ui.value, _stage, _version])

    _fig, _ax = plt.subplots(figsize=(6, 3))
    _ax.bar(_df["wave"], _df["auc"], color="#2c7fb8")
    _ax.axhline(0, color="black", linewidth=0.6)
    _ax.set_ylabel("area under curve")
    _ax.set_title(f"trapezoidal AUC · shot {shot_ui.value} · {_stage} · v={_version}")
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(
                "## Reduction for each wave: `wave_auc`\n\n"
                "This is a catalog table function. It uses a window function "
                "`PARTITION BY wave ORDER BY x`. The partition columns and `data_version` are "
                "**required** arguments. You cannot mix versions by mistake.\n\n"
                f"```sql\n{AUC_CALL}\n```"
            ),
            _fig,
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 8: multi-shot batch (the core slide)
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Across shots: start with ONE range query

        A range of shots is **one bounded query**. The filter `shot BETWEEN ? AND ?` prunes it.
        Pin the version of each shot with a small `VALUES` table.

        ```sql
        WITH pins(shot, data_version) AS (VALUES (?, ?), (?, ?), ...)   -- one per shot
        SELECT w.shot, w.wave, sqrt(avg(w.y * w.y)) AS rms, max(abs(w.y)) AS peak
        FROM waves w
        JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
        WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ?
        GROUP BY w.shot, w.wave
        ```

        - There is one round trip and one plan. DuckDB uses all cores for all shots.
        - **Use this method first.** Use a batch only if the query is too slow (next slides).
        """
    )
    return


@app.cell(hide_code=True)
def _(EXPERIMENT, SHOTS, config, mo, pin, q, time):
    RANGE_STAGE = "mirnov"
    _pins = [(s, pin(s, RANGE_STAGE)) for s in SHOTS]
    _values = ", ".join(["(?, ?)"] * len(_pins))
    RANGE_SQL = f"""WITH pins(shot, data_version) AS (VALUES {_values})
SELECT w.shot, w.wave,
       sqrt(avg(w.y * w.y)) AS rms,
       max(abs(w.y))        AS peak
FROM {config.TABLE_NAME} w
JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ?
GROUP BY w.shot, w.wave"""
    _params = [v for pair in _pins for v in pair] + [EXPERIMENT, RANGE_STAGE, min(SHOTS), max(SHOTS)]

    _t0 = time.perf_counter()
    _df = q(RANGE_SQL, _params)
    RANGE_SECONDS = time.perf_counter() - _t0

    mo.md(
        f"### Live: one range query for {len(SHOTS)} shots\n\n"
        f"Stage `{RANGE_STAGE}`, shots {min(SHOTS)} to {max(SHOTS)}: **{len(_df)} rows** "
        f"in **{RANGE_SECONDS:.2f} s**.\n\n"
        "At demo scale the query is fast. **Do not use a batch here.**"
    )
    return RANGE_SECONDS, RANGE_STAGE, RANGE_SQL


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## When to use a per-shot batch

        Use a batch **only if the range query is too slow or does not fit in memory**.
        First, run the range query on 2 or 3 shots. Multiply the time by the number of shots.
        Estimate the rows with `sum(row_group_rows)` from `wave_manifest`.

        | Symptom of the range query | Why a batch helps |
        |---|---|
        | The query takes longer than your time limit (minutes, not seconds). | Each shot is a short, separate query. |
        | The query spills to disk or reaches `memory_limit`. | One shot limits the memory use. |
        | One failure loses all the work. | You can rerun a failed shot alone. The finished shots stay on disk. |
        | You use the result many times. | Run `COPY` once. Then query the small Parquet files. |
        | A window function sorts every sample of a high-rate stage. | The sort is for one shot, not for all shots. |

        | Example | Choice |
        |---|---|
        | RMS of 3 Thomson channels (about 500 samples each) over 200 shots | **Range query.** About 300,000 rows. |
        | Peak of 1 Mirnov channel over 20 shots | **Range query.** The `wave` and `stage` filters prune well. |
        | Peak of **every** Mirnov channel over 500 shots (full rate, about 2M rows for each wave) | **Batch.** 10¹⁰ to 10¹¹ rows. |
        | `wave_auc` over a whole Mirnov stage for 100 shots | **Batch.** The window function sorts every sample. |
        | Same reduction, run again when a new shot arrives | **Batch.** Do it for the new shot only. |

        *Do not add `LIMIT` to a slow range query. `LIMIT` does not bound the scan.*
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## The batch method

        ```python
        for shot in shots:                       # 1. loop in Python
            version = pin(shot, stage)           # 2. find one version for each (shot, stage)
            q(f"COPY ({REDUCE_SQL}) TO 'out/shot={shot}.parquet' (FORMAT parquet)",
              [experiment, shot, stage, version])   # 3. reduce in SQL, write a few rows
        small = read_parquet('out/*.parquet')    # 4. query the small result again
        ```

        - Each shot is one **bounded** query. It prunes to one partition.
        - Different shots have different versions. Find each version before the loop.
        - Do not use `fetchall()` on raw samples. A Mirnov wave has about 2M rows at full rate.
        - For long runs, set `SET memory_limit`, `SET threads` and `SET temp_directory`.
        """
    )
    return


@app.cell
def _(mo):
    run_ui = mo.ui.run_button(label="Run the batch")
    return (run_ui,)


@app.cell(hide_code=True)
def _(EXPERIMENT, RANGE_SECONDS, REDUCTIONS_DIR, SHOTS, config, mo, pin, q, run_ui, time):
    REDUCE_SQL = f"""SELECT wave,
       sqrt(avg(y * y)) AS rms,
       max(abs(y))      AS peak,
       count(*)         AS n_samples
FROM {config.TABLE_NAME}
WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
GROUP BY wave"""

    BATCH_STAGE = "mirnov"
    mo.stop(
        not run_ui.value,
        mo.vstack(
            [
                mo.md(f"```sql\n{REDUCE_SQL}\n```"),
                mo.md(f"Press the button to reduce **{len(SHOTS)} shots** of `{BATCH_STAGE}`."),
                run_ui,
            ]
        ),
    )

    _t0 = time.perf_counter()
    for _f in REDUCTIONS_DIR.glob("*.parquet"):
        _f.unlink()
    for _shot in SHOTS:
        _out = REDUCTIONS_DIR / f"shot={_shot}.parquet"
        q(
            f"COPY (SELECT {_shot} AS shot, * FROM ({REDUCE_SQL})) TO '{_out}' (FORMAT parquet)",
            [EXPERIMENT, _shot, BATCH_STAGE, pin(_shot, BATCH_STAGE)],
        )
    BATCH_SECONDS = time.perf_counter() - _t0

    mo.vstack(
        [
            mo.md(f"```sql\n{REDUCE_SQL}\n```"),
            run_ui,
            mo.md(
                f"The batch reduced **{len(SHOTS)} shots** in **{BATCH_SECONDS:.1f} s**. "
                f"The single range query took **{RANGE_SECONDS:.2f} s**. At demo scale, "
                "each query in the loop adds overhead, so the loop is not faster. "
                "A batch is useful when the range query is slow or does not fit in memory.\n\n"
                f"Output: `{REDUCTIONS_DIR}/shot=<n>.parquet`."
            ),
        ]
    )
    return BATCH_SECONDS, BATCH_STAGE, REDUCE_SQL


@app.cell(hide_code=True)
def _(BATCH_STAGE, REDUCTIONS_DIR, duckdb, mo, np, plt):
    # Query the small result set again. It has a few rows for each shot.
    _files = sorted(REDUCTIONS_DIR.glob("shot=*.parquet"))
    mo.stop(not _files, mo.md("Run the batch on the previous slide first."))
    _res = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{REDUCTIONS_DIR}/shot=*.parquet') ORDER BY shot, wave"
    ).df()
    _rms = _res.pivot(index="wave", columns="shot", values="rms")
    _peak = _res.pivot(index="wave", columns="shot", values="peak")

    _fig, (_a1, _a2) = plt.subplots(1, 2, figsize=(11, 3.6))
    for _wave, _row in _rms.iterrows():
        _a1.plot(_rms.columns, _row.to_numpy(), marker="o", markersize=3, label=_wave.split("/")[-1])
    _a1.set_xlabel("shot")
    _a1.set_ylabel("RMS")
    _a1.set_title(f"RMS per channel across shots · {BATCH_STAGE}")
    _a1.legend(ncol=2, fontsize=7)

    _im = _a2.imshow(_peak.to_numpy(), aspect="auto", cmap="viridis")
    _a2.set_xticks(np.arange(len(_peak.columns)), [str(c) for c in _peak.columns])
    _a2.set_yticks(np.arange(len(_peak.index)), [w.split("/")[-1] for w in _peak.index])
    _a2.set_xlabel("shot")
    _a2.set_title("peak |y| (shot × channel)")
    _fig.colorbar(_im, ax=_a2)
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(f"### Result: {len(_res)} rows from a lake of millions of samples"),
            _fig,
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 9: comparing stages with different sample rates
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(EXPERIMENT, config, mo, pin, plt, q, shot_ui, window_ui):
    _x0, _x1 = window_ui.value
    mo.stop(_x1 <= _x0, mo.md("Choose a window with `x_min < x_max`."))

    ASOF_SQL = f"""SELECT t.x, t.y AS te, m.y AS b_dot
FROM (SELECT x, y FROM {config.TABLE_NAME}
       WHERE experiment = ? AND shot = ? AND stage = 'thomson'
         AND data_version = ? AND wave = 'thomson_te/A'
         AND x >= ? AND x < ?) t
ASOF JOIN
     (SELECT x, y FROM {config.TABLE_NAME}
       WHERE experiment = ? AND shot = ? AND stage = 'mirnov'
         AND data_version = ? AND wave = 'mirnov/probe_03'
         AND x >= ? AND x < ?) m
  ON t.x >= m.x"""
    _df = q(
        ASOF_SQL,
        [
            EXPERIMENT, shot_ui.value, pin(shot_ui.value, "thomson"), _x0, _x1,
            EXPERIMENT, shot_ui.value, pin(shot_ui.value, "mirnov"), _x0, _x1,
        ],
    )  # fmt: skip

    _fig, _ax = plt.subplots(figsize=(9, 3.2))
    _ax.plot(_df["x"], _df["b_dot"], linewidth=0.8, label="mirnov/probe_03 (dense side)")
    _ax2 = _ax.twinx()
    _ax2.plot(_df["x"], _df["te"], "o-", color="#c0392b", markersize=3, label="thomson_te/A")
    _ax.set_xlabel("x (ms)")
    _ax.set_ylabel("mirnov y")
    _ax2.set_ylabel("thomson y")
    _ax.set_title(f"shot {shot_ui.value} · {len(_df)} matched points")
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(
                "## Stages with different sample rates\n\n"
                "Mirnov runs at kHz to MHz. Thomson is pulsed and slow. Their `x` values "
                "almost never match, so **do not join on `x` with equality**. Use `ASOF JOIN` "
                "from the sparse side to the dense side. Each side pins its **own** "
                "`data_version`.\n\n"
                "*This slide uses the shot and window controls from the earlier slides.*"
            ),
            _fig,
            mo.md(f"```sql\n{ASOF_SQL}\n```"),
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 10: how much did the query touch?
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(CATALOG, DEMO_LAKE, EXPERIMENT, duckdb, mo, pin, plt, shot_ui, window_ui):
    mo.stop(
        CATALOG is not None,
        mo.md(
            "## What did the query read?\n\nThis slide reads Parquet footers of local files. "
            "On a real catalog, use `EXPLAIN ANALYZE` and `ducklake_list_files`."
        ),
    )
    _wave, _stage = "mirnov/probe_03", "mirnov"
    _x0, _x1 = window_ui.value
    _version = pin(shot_ui.value, _stage)
    _meta = duckdb.connect()

    def _glob(path: str) -> str:
        return f"{DEMO_LAKE}/{path}/*.parquet"

    def _count_files(pattern: str) -> int:
        return len(_meta.execute("SELECT * FROM glob(?)", [pattern]).fetchall())

    def _row_groups(files_sql: str, where: str = "") -> int:
        # There is one row for each column chunk. Use the `x` column to count each row group once.
        return _meta.execute(
            f"""
            WITH rg AS (
                SELECT file_name, row_group_id,
                       max(CASE WHEN path_in_schema = 'wave' THEN stats_min_value END) AS wmin,
                       max(CASE WHEN path_in_schema = 'wave' THEN stats_max_value END) AS wmax,
                       max(CASE WHEN path_in_schema = 'x' THEN stats_min_value END)::DOUBLE AS xmin,
                       max(CASE WHEN path_in_schema = 'x' THEN stats_max_value END)::DOUBLE AS xmax
                FROM parquet_metadata({files_sql})
                GROUP BY file_name, row_group_id
            )
            SELECT count(*) FROM rg {where}
            """
        ).fetchone()[0]

    _lake_all = f"'{DEMO_LAKE}/**/*.parquet'"
    _shot_all = f"'{_glob(f'experiment={EXPERIMENT}/shot={shot_ui.value}/stage=*')}'"
    _part_files = f"'{_glob(f'experiment={EXPERIMENT}/shot={shot_ui.value}/stage={_stage}')}'"
    _ver_files = f"'{DEMO_LAKE}/experiment={EXPERIMENT}/shot={shot_ui.value}/stage={_stage}/*__{_version}__*.parquet'"

    _steps = [
        ("whole lake", _row_groups(_lake_all)),
        ("+ shot", _row_groups(_shot_all)),
        ("+ stage", _row_groups(_part_files)),
        ("+ data_version", _row_groups(_ver_files)),
        (
            "+ wave & x window",
            _row_groups(
                _ver_files,
                f"WHERE wmin <= '{_wave}' AND wmax >= '{_wave}' "
                f"AND xmax >= {_x0} AND xmin < {_x1}",
            ),
        ),
    ]
    _fig, _ax = plt.subplots(figsize=(8, 3.2))
    _ax.barh([s for s, _ in _steps][::-1], [n for _, n in _steps][::-1], color="#2c7fb8")
    _ax.set_xscale("log")
    _ax.set_xlabel("row groups that could be read (log)")
    _fig.tight_layout()
    _ = _count_files  # kept for ad-hoc file counts in the notebook editor

    mo.vstack(
        [
            mo.md(
                "## What did the query read?\n\n"
                "Each filter prunes row groups. The `x` and `wave` filters work **only** if the "
                "column is bare in the predicate (rule 3). A cast or a function makes the last "
                "bar as long as the first bar.\n\n"
                "At production scale, the first bar is about 5 × 10⁷."
            ),
            _fig,
        ]
    )
    return


# ---------------------------------------------------------------------------
# Slide 11: traps
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Things that look correct but are not

        | Looks correct | Why it fails |
        |---|---|
        | `SELECT count(DISTINCT wave) FROM waves` | It is a full scan. Use the manifest. |
        | `ORDER BY data_version DESC LIMIT 1` | Hashes have no order. Use `ingest_queue.processed_at`. |
        | `WHERE wave LIKE 'mirnov/%'` | `wave` is not a partition key. Pin `stage`. |
        | `LIMIT 100` after `ORDER BY` or `GROUP BY` | It does not bound the scan. Only partition filters and `x` filters bound it. |
        | `USING SAMPLE` to downsample | It removes peaks. Use min/max decimation. |
        | `read_parquet('data/lake/**/*.parquet')` | It is slow **and wrong**. It counts queued files and replaced files. |
        | `JOIN ... ON a.x = b.x` | The sample rates are different. Use `ASOF JOIN`. |
        | `.df()` on raw samples | One Mirnov wave has about 2M rows. Reduce in SQL. |
        """
    )
    return


# ---------------------------------------------------------------------------
# Slide 12: summary
# ---------------------------------------------------------------------------
@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Summary

        1. **Select the access path.** Metadata: Postgres. One wave: HTTP. Analysis: SQL.
        2. **Pin the partition and the version.** Use `experiment`, `shot`, `stage` and one `data_version`.
        3. **Reduce in SQL.** Decimate for plots. Aggregate for statistics.
        4. **Use one range query for many shots.** Use a per-shot batch only if the query is too slow.
        5. **Check what the query read.** Use `EXPLAIN ANALYZE` and the row-group counts from the manifest.

        Full rules: `QUERYING.md`. Design background: `README.md`.

        *To run this notebook on a real catalog:*
        `LAKE_CATALOG="postgres:dbname=ducklake_catalog host=localhost user=ducklake" marimo run notebooks/querying_the_lake.py`
        """
    )
    return


if __name__ == "__main__":
    app.run()

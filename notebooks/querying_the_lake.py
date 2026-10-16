import marimo

__generated_with = "0.25.1"
app = marimo.App(
    width="full",
    app_title="Querying the wave lake",
    layout_file="layouts/querying_the_lake.slides.json",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Querying the wave lake

    **Fast, interactive queries on 10¹² samples. Do not scan them all.**

    - DuckLake holds one row for each sample. It partitions by `experiment`, `shot` and `stage`.
    - Each chart in this notebook is a live query. Change a control and the query runs again.
    - Each query follows the rules of `QUERYING.md`. The SQL is under each chart.

    *Select a stage, a shot and a wave on the explorer slide. Brush the overview to zoom.*
    """)
    return


@app.cell
def _():
    import json
    import os
    import re
    import subprocess
    import tempfile
    import time
    from pathlib import Path

    import altair as alt
    import duckdb
    import marimo as mo
    import pandas as pd

    # Altair stops at 5,000 rows by default. The assignment also keeps the return
    # value of this call out of the cell output (it showed as a slide).
    _ = alt.data_transformers.disable_max_rows()
    return Path, alt, duckdb, json, mo, os, pd, re, subprocess, tempfile, time


@app.cell
def _(mo, os, pd, time):
    from ducklake_poc import config, ingest, synthetic
    from ducklake_poc.notebook_lake import DEFAULT_DEMO_ROOT, open_lake

    # The shared setup is in `ducklake_poc.notebook_lake`. Without LAKE_CATALOG it builds a
    # synthetic demo lake with the production generator and compactor.
    CATALOG = os.environ.get("LAKE_CATALOG")
    EXPERIMENT = os.environ.get("LAKE_EXPERIMENT", synthetic.DEFAULT_EXPERIMENT)
    DURATION = synthetic.DURATION_MS
    REDUCTIONS_DIR = DEFAULT_DEMO_ROOT / "reductions"
    REDUCTIONS_DIR.mkdir(parents=True, exist_ok=True)

    lake = open_lake(
        CATALOG,
        lake_url=os.environ.get("LAKE_URL"),
        experiment=EXPERIMENT,
        # A catalog without timestamps cannot tell which version is the newest. The notebook
        # pins the first version by name. This is a demo shortcut. Set
        # LAKE_VERSION_POLICY=error to refuse. In real work, ask the user.
        on_ambiguous=os.environ.get("LAKE_VERSION_POLICY", "first"),  # pyright: ignore[reportArgumentType]
        # The real catalog stores `wave_auc`. In the demo we install the same definition.
        demo_setup_sql=[ingest._WAVE_AUC_SQL],
        build_context=lambda: mo.status.spinner(title="Building the demo lake (first run only)"),
    )
    MODE = lake.mode
    if lake.ambiguous:
        MODE += (
            f" - {len(lake.ambiguous)} of {len(lake.pinned)} (shot, stage) pairs have several "
            "versions. The first by name is pinned (demo shortcut)"
        )
    SHOTS = lake.shots
    STAGES = lake.stages
    pinned = lake.pinned
    pin = lake.pin
    versions_of = lake.versions_of
    list_waves = lake.list_waves
    q = lake.q

    def timed_q(sql: str, params: list | None = None) -> tuple[pd.DataFrame, float]:
        """Run SQL on the lake. Return the DataFrame and the seconds used."""
        t0 = time.perf_counter()
        df = q(sql, params)
        return df, time.perf_counter() - t0

    def sql_block(sql: str):
        """Show a SQL statement in a collapsed section."""
        return mo.accordion({"SQL": mo.md(f"```sql\n{sql}\n```")})

    # One row for each known version of each (shot, stage).
    all_versions = pd.DataFrame(
        [(s, st, v) for (s, st), vs in sorted(lake.versions.items()) for v in vs],
        columns=["shot", "stage", "data_version"],
    )

    def _demo_row_groups(files: str, where: str = "", params: list | None = None) -> int:
        # There is one row for each column chunk. Group them to count each row group once.
        assert lake.connection is not None
        return lake.connection.execute(
            f"""
            WITH rg AS (
                SELECT file_name, row_group_id,
                       max(CASE WHEN path_in_schema = 'wave' THEN stats_min_value END) AS wmin,
                       max(CASE WHEN path_in_schema = 'wave' THEN stats_max_value END) AS wmax,
                       max(CASE WHEN path_in_schema = 'x' THEN stats_min_value END)::DOUBLE AS xmin,
                       max(CASE WHEN path_in_schema = 'x' THEN stats_max_value END)::DOUBLE AS xmax
                FROM parquet_metadata('{files}')
                GROUP BY file_name, row_group_id
            )
            SELECT count(*) FROM rg {where}
            """,
            params or [],
        ).fetchone()[0]

    def row_group_funnel(
        shot: int,
        stage: str,
        version: str,
        wave: str,
        x0: float,
        x1: float,
        whole_lake: bool = True,
    ) -> list[tuple[str, int]]:
        """Count the row groups that each filter keeps. The count reads
        Parquet footers (demo) or `wave_manifest` (catalog). It reads no samples."""
        shot = int(shot)
        window = "WHERE wmin <= ? AND wmax >= ? AND xmax >= ? AND xmin < ?"
        window_params = [wave, wave, x0, x1]
        if lake.connection is not None:
            demo_lake = lake.lake_dir
            lake_dir = f"{demo_lake}/experiment={EXPERIMENT}"
            steps = []
            if whole_lake:
                steps.append(("whole lake", _demo_row_groups(f"{demo_lake}/**/*.parquet")))
            steps += [
                ("+ shot", _demo_row_groups(f"{lake_dir}/shot={shot}/stage=*/*.parquet")),
                ("+ stage", _demo_row_groups(f"{lake_dir}/shot={shot}/stage={stage}/*.parquet")),
            ]
            by_version = f"{lake_dir}/shot={shot}/stage={stage}/*__{version}__*.parquet"
            steps.append(("+ data_version", _demo_row_groups(by_version)))
            steps.append(("+ wave & x window", _demo_row_groups(by_version, window, window_params)))
            return steps

        import psycopg2

        base = "SELECT count(*) FROM wave_manifest WHERE experiment = %s"
        levels = [
            ("+ experiment", "", []),
            ("+ shot", " AND shot = %s", [shot]),
            ("+ stage", " AND shot = %s AND stage = %s", [shot, stage]),
            (
                "+ data_version",
                " AND shot = %s AND stage = %s AND data_version = %s",
                [shot, stage, version],
            ),
            (
                "+ wave & x window",
                " AND shot = %s AND stage = %s AND data_version = %s"
                " AND wave_min <= %s AND wave_max >= %s AND x_max >= %s AND x_min < %s",
                [shot, stage, version, wave, wave, x0, x1],
            ),
        ]
        steps = []
        with psycopg2.connect(**config.pg_dsn_for_psycopg2()) as pg, pg.cursor() as cur:
            for label, where, params in levels:
                cur.execute(base + where, [EXPERIMENT, *params])
                steps.append((label, int(cur.fetchone()[0])))
        return steps

    return (
        DURATION,
        EXPERIMENT,
        MODE,
        REDUCTIONS_DIR,
        SHOTS,
        STAGES,
        all_versions,
        config,
        lake,
        list_waves,
        pin,
        pinned,
        q,
        row_group_funnel,
        sql_block,
        timed_q,
        versions_of,
    )


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
    > **Shots:** {SHOTS[0]}–{SHOTS[-1]} ({len(SHOTS)})  ·  **Stages:** {len(STAGES)} ({", ".join(STAGES)})
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Four filters for each query

    1. `experiment = ?` and `stage = ?`: use equality.
    2. `shot = ?`, or `IN (...)`, or a bounded `BETWEEN`.
    3. `data_version = ?`: use **exactly one** for each `(shot, stage)`.
    4. `wave = ?`: always use it together with `stage`.

    Keep each predicate bare. Write `x >= ? AND x < ?`. Do not write `round(x) = ?`.
    Select only the columns that you need.

    **Next slide:** what happens when you do not pin the version.
    """)
    return


@app.cell
def _(all_versions, mo):
    _counts = all_versions.groupby(["shot", "stage"]).size()
    _multi = _counts[_counts > 1]
    _options = {
        f"shot {int(s)} · {st} ({n} versions)": (int(s), st) for (s, st), n in _multi.items()
    }
    multi_ui = mo.ui.dropdown(
        options=_options or {"no (shot, stage) has more than one version": None},
        value=next(iter(_options), "no (shot, stage) has more than one version"),
        label="(shot, stage) with reruns",
        full_width=True,
    )
    return (multi_ui,)


@app.cell(hide_code=True)
def _(EXPERIMENT, alt, config, mo, multi_ui, pd, pin, sql_block, timed_q):
    mo.stop(multi_ui.value is None, mo.vstack([multi_ui, mo.md("No stage has a rerun.")]))
    _shot, _stage = multi_ui.value
    PIN_SQL = f"""SELECT data_version, count(*) AS n
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ?
    GROUP BY data_version"""
    _df, _s = timed_q(PIN_SQL, [EXPERIMENT, _shot, _stage])
    _pinned = pin(_shot, _stage)
    _total = int(_df["n"].sum())
    _one = int(_df.loc[_df["data_version"] == _pinned, "n"].iloc[0])
    _bars = pd.concat(
        [
            _df.assign(kind="one version"),
            pd.DataFrame({"data_version": ["all (unpinned)"], "n": [_total], "kind": ["unpinned"]}),
        ]
    )
    _chart = (
        alt.Chart(_bars)
        .mark_bar()
        .encode(
            x=alt.X("data_version:N", sort=None, title=None),
            y=alt.Y("n:Q", title="samples counted"),
            color=alt.Color(
                "kind:N",
                scale=alt.Scale(domain=["one version", "unpinned"], range=["#2c7fb8", "#c0392b"]),
                legend=None,
            ),
            tooltip=["data_version", "n"],
        )
        .properties(width=420, height=240)
    )
    mo.vstack(
        [
            mo.md(
                "## Rule 2: pin one `data_version`\n\n"
                "Each recalibration rerun keeps **all** old versions. "
                f"Without the filter, the count is **{_total / _one:.1f}×** too high. "
                "A window function also pairs samples from different versions.\n\n"
                "`data_version` is a hash. It has **no order**. "
                "Do not use `max(data_version)` to find the latest version."
            ),
            multi_ui,
            _chart,
            sql_block(PIN_SQL),
        ]
    )
    return


@app.cell
def _(DURATION, SHOTS, STAGES, mo):
    stage_ui = mo.ui.dropdown(
        options=STAGES, value="mirnov" if "mirnov" in STAGES else STAGES[0], label="stage"
    )
    shot_ui = mo.ui.dropdown(
        options={str(s): s for s in SHOTS}, value=str(SHOTS[0]), label="shot"
    )
    window_ui = mo.ui.range_slider(
        start=0, stop=DURATION, step=1, value=[100, 140], label="window (ms)", full_width=True
    )
    buckets_ui = mo.ui.slider(
        start=20, stop=2000, step=20, value=400, label="buckets", full_width=True
    )
    norm_ui = mo.ui.checkbox(label="normalize (z-score)", value=True)
    shots_ui = mo.ui.multiselect(
        options={str(s): s for s in SHOTS},
        value=[str(s) for s in SHOTS[:3]],
        label="shots",
    )
    metric_ui = mo.ui.dropdown(
        options={
            "RMS": "sqrt(avg(w.y * w.y))",
            "peak |y|": "max(abs(w.y))",
            "mean": "avg(w.y)",
            "peak to peak": "max(w.y) - min(w.y)",
        },
        value="RMS",
        label="reduction",
    )
    shot_range_ui = mo.ui.range_slider(
        start=min(SHOTS),
        stop=max(SHOTS),
        step=1,
        value=[min(SHOTS), max(SHOTS)],
        label="shots",
        full_width=True,
    )
    return (
        buckets_ui,
        metric_ui,
        norm_ui,
        shot_range_ui,
        shot_ui,
        shots_ui,
        stage_ui,
        window_ui,
    )


@app.cell
def _(mo, pin, shot_ui, stage_ui, versions_of):
    _versions = versions_of(shot_ui.value, stage_ui.value)
    mo.stop(not _versions, mo.md(f"No data for shot {shot_ui.value}, stage `{stage_ui.value}`."))
    version_ui = mo.ui.dropdown(
        options=_versions, value=pin(shot_ui.value, stage_ui.value), label="data_version"
    )
    return (version_ui,)


@app.cell
def _(list_waves, mo, shot_ui, stage_ui, version_ui):
    _waves = list_waves(shot_ui.value, stage_ui.value, version_ui.value)
    mo.stop(not _waves, mo.md("This version has no waves."))
    wave_ui = mo.ui.dropdown(
        options=_waves,
        value=next((w for w in _waves if w.endswith("probe_03")), _waves[0]),
        label="wave",
    )
    channels_ui = mo.ui.multiselect(options=_waves, value=_waves[:3], label="channels")
    return channels_ui, wave_ui


@app.cell(hide_code=True)
def _(
    DURATION,
    EXPERIMENT,
    alt,
    buckets_ui,
    config,
    mo,
    shot_ui,
    sql_block,
    stage_ui,
    timed_q,
    version_ui,
    wave_ui,
    window_ui,
):
    OVERVIEW_SQL = f"""SELECT
        floor(x / ?) * ? AS x_bucket,
        min(y) AS y_min,
        max(y) AS y_max,
        avg(y) AS y_mean
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
    GROUP BY x_bucket
    ORDER BY x_bucket"""
    _width = DURATION / buckets_ui.value
    overview_df, _seconds = timed_q(
        OVERVIEW_SQL,
        [_width, _width, EXPERIMENT, shot_ui.value, stage_ui.value, version_ui.value, wave_ui.value],
    )

    _brush = alt.selection_interval(encodings=["x"], name="brush")
    _base = alt.Chart(overview_df)
    _band = (
        _base.mark_area(opacity=0.35)
        .encode(
            x=alt.X("x_bucket:Q", title="x (ms)", scale=alt.Scale(domain=[0, DURATION])),
            y=alt.Y("y_min:Q", title="y"),
            y2="y_max:Q",
        )
        .add_params(_brush)
    )
    _line = _base.mark_line().encode(
        x="x_bucket:Q",
        y="y_mean:Q",
        tooltip=["x_bucket", "y_min", "y_max", "y_mean"],
    )
    overview_chart = mo.ui.altair_chart((_band + _line).properties(width="container", height=260))

    mo.vstack(
        [
            mo.md(
                "## Explore one wave\n\n"
                "Select a stage, a shot, a version and a wave. The chart shows the **whole wave** "
                f"as {buckets_ui.value} min/max buckets. **Drag on the chart** to select a window. "
                "The next slide queries only that window."
            ),
            mo.hstack([stage_ui, shot_ui, version_ui, wave_ui], justify="start"),
            mo.hstack([buckets_ui, window_ui], widths=[1, 2]),
            overview_chart,
            mo.md(
                f"*{len(overview_df)} buckets in {_seconds * 1000:.0f} ms. "
                "Without a brush, the slider sets the window.*"
            ),
            sql_block(OVERVIEW_SQL),
        ]
    )
    return overview_chart, overview_df


@app.cell(hide_code=True)
def _(
    DURATION,
    EXPERIMENT,
    alt,
    buckets_ui,
    config,
    mo,
    overview_chart,
    overview_df,
    row_group_funnel,
    shot_ui,
    sql_block,
    stage_ui,
    timed_q,
    version_ui,
    wave_ui,
    window_ui,
):
    _selected = overview_chart.value
    _brushed = 0 < len(_selected) < len(overview_df)
    if _brushed:
        _bucket = DURATION / buckets_ui.value
        ZOOM_X0 = float(_selected["x_bucket"].min())
        ZOOM_X1 = float(_selected["x_bucket"].max()) + _bucket
    else:
        ZOOM_X0, ZOOM_X1 = (float(v) for v in window_ui.value)
    mo.stop(ZOOM_X1 <= ZOOM_X0, mo.md("Select a window with `x_min < x_max`."))

    _raw_limit = 20_000
    _where = (
        f"FROM {config.TABLE_NAME}\n"
        "WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?\n"
        "  AND x >= ? AND x < ?"
    )
    _pins = [EXPERIMENT, shot_ui.value, stage_ui.value, version_ui.value, wave_ui.value]
    _n = int(
        timed_q(f"SELECT count(*) AS n\n{_where}", [*_pins, ZOOM_X0, ZOOM_X1])[0]["n"].iloc[0]
    )

    if _n <= _raw_limit:
        ZOOM_SQL = f"SELECT x, y\n{_where}\nORDER BY x"
        _df, _seconds = timed_q(ZOOM_SQL, [*_pins, ZOOM_X0, ZOOM_X1])
        _chart = (
            alt.Chart(_df)
            .mark_line()
            .encode(x=alt.X("x:Q", title="x (ms)"), y=alt.Y("y:Q"), tooltip=["x", "y"])
        )
        _mode = f"raw samples ({_n:,} is at most {_raw_limit:,})"
    else:
        ZOOM_SQL = (
            "SELECT floor(x / ?) * ? AS x_bucket,\n"
            "       min(y) AS y_min, max(y) AS y_max, avg(y) AS y_mean\n"
            f"{_where}\nGROUP BY x_bucket\nORDER BY x_bucket"
        )
        _width = (ZOOM_X1 - ZOOM_X0) / buckets_ui.value
        _df, _seconds = timed_q(ZOOM_SQL, [_width, _width, *_pins, ZOOM_X0, ZOOM_X1])
        _base = alt.Chart(_df)
        _chart = _base.mark_area(opacity=0.35).encode(
            x=alt.X("x_bucket:Q", title="x (ms)"), y=alt.Y("y_min:Q", title="y"), y2="y_max:Q"
        ) + _base.mark_line().encode(x="x_bucket:Q", y="y_mean:Q")
        _mode = f"min/max decimation ({_n:,} samples is more than {_raw_limit:,})"

    _funnel = row_group_funnel(
        shot_ui.value,
        stage_ui.value,
        version_ui.value,
        wave_ui.value,
        ZOOM_X0,
        ZOOM_X1,
        whole_lake=False,
    )
    _total_rg, _read_rg = _funnel[-2][1], _funnel[-1][1]

    mo.vstack(
        [
            mo.md(
                f"## Zoom: {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms "
                f"({'brush' if _brushed else 'slider'})\n\n"
                f"The query returns **{_mode}**."
            ),
            mo.hstack(
                [
                    mo.stat(f"{_n:,}", label="samples in window", bordered=True),
                    mo.stat(f"{len(_df):,}", label="points returned", bordered=True),
                    mo.stat(f"{_seconds * 1000:.0f} ms", label="query time", bordered=True),
                    mo.stat(
                        f"{_read_rg} of {_total_rg}",
                        label="row groups read (this version)",
                        bordered=True,
                    ),
                ],
                justify="start",
            ),
            (_chart.properties(width="container", height=300).interactive()),
            sql_block(ZOOM_SQL),
        ]
    )
    return ZOOM_X0, ZOOM_X1


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    ZOOM_X0,
    ZOOM_X1,
    alt,
    buckets_ui,
    channels_ui,
    config,
    mo,
    norm_ui,
    shot_ui,
    sql_block,
    stage_ui,
    timed_q,
    version_ui,
):
    _header = mo.md(
        "## Compare channels of one stage\n\n"
        "One query reads many waves. `wave IN (...)` is safe, because `stage` is pinned."
    )
    mo.stop(not channels_ui.value, mo.vstack([_header, channels_ui, mo.md("Select a channel.")]))
    _marks = ", ".join("?" * len(channels_ui.value))
    CHANNELS_SQL = f"""SELECT wave, floor(x / ?) * ? AS x_bucket, avg(y) AS y_mean
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
      AND wave IN ({_marks})
      AND x >= ? AND x < ?
    GROUP BY wave, x_bucket
    ORDER BY wave, x_bucket"""
    _width = (ZOOM_X1 - ZOOM_X0) / buckets_ui.value
    _df, _seconds = timed_q(
        CHANNELS_SQL,
        [
            _width,
            _width,
            EXPERIMENT,
            shot_ui.value,
            stage_ui.value,
            version_ui.value,
            *channels_ui.value,
            ZOOM_X0,
            ZOOM_X1,
        ],
    )
    _y_title = "y"
    if norm_ui.value:
        _df["y_mean"] = _df.groupby("wave")["y_mean"].transform(
            lambda s: (s - s.mean()) / (s.std(ddof=0) or 1.0)
        )
        _y_title = "y (z-score)"
    _chart = (
        alt.Chart(_df)
        .mark_line()
        .encode(
            x=alt.X("x_bucket:Q", title="x (ms)"),
            y=alt.Y("y_mean:Q", title=_y_title),
            color=alt.Color("wave:N", legend=alt.Legend(orient="bottom")),
            tooltip=["wave", "x_bucket", "y_mean"],
        )
        .properties(width="container", height=300)
        .interactive()
    )
    mo.vstack(
        [
            _header,
            mo.hstack([channels_ui, norm_ui], justify="start"),
            _chart,
            mo.md(
                f"*Window {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms · "
                f"{len(channels_ui.value)} waves · {_seconds * 1000:.0f} ms.*"
            ),
            sql_block(CHANNELS_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    ZOOM_X0,
    ZOOM_X1,
    alt,
    buckets_ui,
    config,
    mo,
    pin,
    pinned,
    shots_ui,
    sql_block,
    stage_ui,
    timed_q,
    wave_ui,
):
    _header = mo.md(
        "## Compare the same wave in several shots\n\n"
        "This is **one** range query. A `VALUES` table pins one version for each shot."
    )
    _shots = [s for s in shots_ui.value if (s, stage_ui.value) in pinned]
    mo.stop(not _shots, mo.vstack([_header, shots_ui, mo.md("Select a shot.")]))
    _values = ", ".join(["(?, ?)"] * len(_shots))
    _marks = ", ".join("?" * len(_shots))
    SHOTS_SQL = f"""WITH pins(shot, data_version) AS (VALUES {_values})
    SELECT w.shot, floor(w.x / ?) * ? AS x_bucket, avg(w.y) AS y_mean
    FROM {config.TABLE_NAME} w
    JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
    WHERE w.experiment = ? AND w.stage = ? AND w.wave = ?
      AND w.shot IN ({_marks})
      AND w.x >= ? AND w.x < ?
    GROUP BY w.shot, x_bucket
    ORDER BY w.shot, x_bucket"""
    _width = (ZOOM_X1 - ZOOM_X0) / buckets_ui.value
    _pairs = [v for s in _shots for v in (s, pin(s, stage_ui.value))]
    _df, _seconds = timed_q(
        SHOTS_SQL,
        [
            *_pairs,
            _width,
            _width,
            EXPERIMENT,
            stage_ui.value,
            wave_ui.value,
            *_shots,
            ZOOM_X0,
            ZOOM_X1,
        ],
    )
    _chart = (
        alt.Chart(_df)
        .mark_line()
        .encode(
            x=alt.X("x_bucket:Q", title="x (ms)"),
            y=alt.Y("y_mean:Q", title="y (mean of bucket)"),
            color=alt.Color("shot:N", legend=alt.Legend(orient="bottom")),
            tooltip=["shot", "x_bucket", "y_mean"],
        )
        .properties(width="container", height=300)
        .interactive()
    )
    mo.vstack(
        [
            _header,
            shots_ui,
            _chart,
            mo.md(
                f"*`{wave_ui.value}` · window {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms · "
                f"{len(_shots)} shots · {_seconds * 1000:.0f} ms.*"
            ),
            sql_block(SHOTS_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    ZOOM_X0,
    ZOOM_X1,
    alt,
    mo,
    shot_ui,
    sql_block,
    stage_ui,
    timed_q,
    version_ui,
):
    AUC_SQL = "SELECT wave, auc FROM wave_auc(?, ?, ?, ?, ?, ?)\n-- experiment, shot, stage, data_version, x_min, x_max\nORDER BY wave"
    _df, _seconds = timed_q(
        AUC_SQL,
        [EXPERIMENT, shot_ui.value, stage_ui.value, version_ui.value, ZOOM_X0, ZOOM_X1],
    )
    _chart = (
        alt.Chart(_df)
        .mark_bar()
        .encode(
            x=alt.X("wave:N", title=None, sort=None, axis=alt.Axis(labelAngle=-40)),
            y=alt.Y("auc:Q", title="area under curve"),
            tooltip=["wave", "auc"],
        )
        .properties(width="container", height=240)
    )
    mo.vstack(
        [
            mo.md(
                "## Reduction for each wave: `wave_auc`\n\n"
                "This is a catalog table function. It uses a window function "
                "`PARTITION BY wave ORDER BY x`. The partition columns and `data_version` are "
                "**required** arguments. You cannot mix versions by mistake.\n\n"
                f"*Stage `{stage_ui.value}` · shot {shot_ui.value} · "
                f"window {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms · {_seconds * 1000:.0f} ms.*"
            ),
            _chart,
            sql_block(AUC_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    ZOOM_X0,
    ZOOM_X1,
    alt,
    buckets_ui,
    config,
    mo,
    shot_ui,
    sql_block,
    stage_ui,
    timed_q,
    version_ui,
    wave_ui,
):
    SLOPE_SQL = f"""SELECT
        floor(x / ?) * ? AS x_bucket,
        avg(y) AS y_mean,
        regr_slope(y, x) AS slope
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
      AND x >= ? AND x < ?
    GROUP BY x_bucket
    ORDER BY x_bucket"""
    _width = (ZOOM_X1 - ZOOM_X0) / buckets_ui.value
    _df, _seconds = timed_q(
        SLOPE_SQL,
        [
            _width,
            _width,
            EXPERIMENT,
            shot_ui.value,
            stage_ui.value,
            version_ui.value,
            wave_ui.value,
            ZOOM_X0,
            ZOOM_X1,
        ],
    )
    _x = alt.X("x_bucket:Q", title="x (ms)", scale=alt.Scale(domain=[ZOOM_X0, ZOOM_X1]))
    _signal = (
        alt.Chart(_df)
        .mark_line()
        .encode(x=_x, y=alt.Y("y_mean:Q", title="y (mean of bucket)"))
        .properties(width="container", height=110)
    )
    _slope = (
        alt.Chart(_df)
        .mark_line(color="#d62728")
        .encode(
            x=_x,
            y=alt.Y("slope:Q", title="dy/dt (y per ms)"),
            tooltip=["x_bucket", "y_mean", "slope"],
        )
        .properties(width="container", height=110)
    )
    mo.vstack(
        [
            mo.md(
                "## Rate of change: dy/dt of the signal\n\n"
                "`regr_slope(y, x)` fits a line through the samples of each bucket. "
                "The result is the **temporal derivative dy/dt** in y per ms, because `x` is time. "
                "It is not a spatial gradient: it is not the steepness of a density or "
                "potential profile (dn/dr). Edges and ramps are peaks in the slope. "
                "Do not use `lead(y) - y` for each sample: it amplifies noise. "
                "A bucket with fewer than 2 samples gives no slope.\n\n"
                f"*`{wave_ui.value}` · window {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms · "
                f"{_seconds * 1000:.0f} ms.*"
            ),
            _signal,
            _slope,
            sql_block(SLOPE_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Across shots: start with ONE range query

    A range of shots is **one bounded query**. The filter `shot BETWEEN ? AND ?` prunes it.
    Pin the version of each shot with a small `VALUES` table.

    - There is one round trip and one plan. DuckDB uses all cores for all shots.
    - **Use this method first.** Use a batch only if the query is too slow (next slides).

    **Next slide:** select a reduction and a range of shots. **Click a cell** to look at that wave.
    """)
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    alt,
    config,
    metric_ui,
    mo,
    pin,
    pinned,
    shot_range_ui,
    sql_block,
    stage_ui,
    timed_q,
):
    _stage = stage_ui.value
    _lo, _hi = shot_range_ui.value
    _shots = [s for s in range(int(_lo), int(_hi) + 1) if (s, _stage) in pinned]
    mo.stop(
        not _shots,
        mo.vstack([mo.hstack([stage_ui, metric_ui]), shot_range_ui, mo.md("No shot in range.")]),
    )
    _values = ", ".join(["(?, ?)"] * len(_shots))
    RANGE_SQL = f"""WITH pins(shot, data_version) AS (VALUES {_values})
    SELECT w.shot, w.wave, {metric_ui.value} AS value
    FROM {config.TABLE_NAME} w
    JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
    WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ?
    GROUP BY w.shot, w.wave"""
    _params = [v for s in _shots for v in (s, pin(s, _stage))]
    _params += [EXPERIMENT, _stage, min(_shots), max(_shots)]
    heat_df, RANGE_SECONDS = timed_q(RANGE_SQL, _params)
    heat_df["channel"] = heat_df["wave"].str.split("/").str[-1]

    _cell = alt.selection_point(name="cell", fields=["shot", "wave"])
    _heat = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("shot:O", title="shot"),
            y=alt.Y("channel:N", title=None),
            color=alt.Color("value:Q", scale=alt.Scale(scheme="viridis"), title=metric_ui.selected_key),
            tooltip=["shot", "wave", "value"],
        )
        .add_params(_cell)
        .properties(width="container", height=max(120, 28 * heat_df["wave"].nunique()))
    )
    heat_chart = mo.ui.altair_chart(_heat)

    mo.vstack(
        [
            mo.md("### Live: one range query for many shots"),
            mo.hstack([stage_ui, metric_ui], justify="start"),
            shot_range_ui,
            heat_chart,
            mo.md(
                f"*Stage `{_stage}`, shots {min(_shots)} to {max(_shots)}: "
                f"**{len(heat_df)} rows** in **{RANGE_SECONDS:.2f} s**. "
                "At demo scale the query is fast. Do not use a batch here.*"
            ),
            sql_block(RANGE_SQL),
        ]
    )
    return RANGE_SECONDS, heat_chart, heat_df


@app.cell(hide_code=True)
def _(
    DURATION,
    EXPERIMENT,
    alt,
    config,
    heat_chart,
    heat_df,
    mo,
    pin,
    sql_block,
    stage_ui,
    timed_q,
):
    _sel = heat_chart.value
    mo.stop(
        not (0 < len(_sel) < len(heat_df)),
        mo.md("### Drill down\n\n*Click a cell in the heatmap to look at that wave.*"),
    )
    _shot, _wave = int(_sel["shot"].iloc[0]), _sel["wave"].iloc[0]
    _width = DURATION / 500
    DRILL_SQL = f"""SELECT floor(x / ?) * ? AS x_bucket, min(y) AS y_min, max(y) AS y_max
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
    GROUP BY x_bucket
    ORDER BY x_bucket"""
    _df, _seconds = timed_q(
        DRILL_SQL, [_width, _width, EXPERIMENT, _shot, stage_ui.value, pin(_shot, stage_ui.value), _wave]
    )
    _chart = (
        alt.Chart(_df)
        .mark_area(opacity=0.5)
        .encode(x=alt.X("x_bucket:Q", title="x (ms)"), y=alt.Y("y_min:Q", title="y"), y2="y_max:Q")
        .properties(width="container", height=220)
        .interactive()
    )
    mo.vstack(
        [
            mo.md(f"### Drill down: `{_wave}` in shot {_shot} ({_seconds * 1000:.0f} ms)"),
            _chart,
            sql_block(DRILL_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell
def _(mo):
    run_ui = mo.ui.run_button(label="Run the batch")
    return (run_ui,)


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    RANGE_SECONDS,
    REDUCTIONS_DIR,
    SHOTS,
    config,
    mo,
    pin,
    pinned,
    q,
    run_ui,
    sql_block,
    stage_ui,
    time,
):
    REDUCE_SQL = f"""SELECT wave,
           sqrt(avg(y * y)) AS rms,
           max(abs(y))      AS peak,
           count(*)         AS n_samples
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ?
    GROUP BY wave"""

    BATCH_STAGE = stage_ui.value
    _shots = [s for s in SHOTS if (s, BATCH_STAGE) in pinned]
    mo.stop(
        not run_ui.value,
        mo.vstack(
            [
                mo.md(
                    f"Press the button to reduce **{len(_shots)} shots** of `{BATCH_STAGE}` "
                    "(the stage from the controls above)."
                ),
                run_ui,
                sql_block(REDUCE_SQL),
            ]
        ),
    )

    _t0 = time.perf_counter()
    for _f in REDUCTIONS_DIR.glob("*.parquet"):
        _f.unlink()
    for _shot in _shots:
        _out = REDUCTIONS_DIR / f"shot={_shot}.parquet"
        q(
            f"COPY (SELECT {_shot} AS shot, * FROM ({REDUCE_SQL})) TO '{_out}' (FORMAT parquet)",
            [EXPERIMENT, _shot, BATCH_STAGE, pin(_shot, BATCH_STAGE)],
        )
    BATCH_SECONDS = time.perf_counter() - _t0

    mo.vstack(
        [
            run_ui,
            mo.md(
                f"The batch reduced **{len(_shots)} shots** in **{BATCH_SECONDS:.1f} s**. "
                f"The single range query took **{RANGE_SECONDS:.2f} s**. At demo scale, "
                "each query in the loop adds overhead, so the loop is not faster. "
                "A batch is useful when the range query is slow or does not fit in memory.\n\n"
                f"Output: `{REDUCTIONS_DIR}/shot=<n>.parquet`."
            ),
            sql_block(REDUCE_SQL),
        ]
    )
    return (BATCH_STAGE,)


@app.cell(hide_code=True)
def _(BATCH_STAGE, REDUCTIONS_DIR, alt, duckdb, mo):
    # Query the small result set again. It has a few rows for each shot.
    _files = sorted(REDUCTIONS_DIR.glob("shot=*.parquet"))
    mo.stop(not _files, mo.md("Run the batch on the previous slide first."))
    _res = (
        duckdb.connect()
        .execute(f"SELECT * FROM read_parquet('{REDUCTIONS_DIR}/shot=*.parquet') ORDER BY shot, wave")
        .df()
    )
    _res["channel"] = _res["wave"].str.split("/").str[-1]
    _rms = (
        alt.Chart(_res)
        .mark_line(point=True)
        .encode(
            x=alt.X("shot:O"),
            y=alt.Y("rms:Q", title="RMS"),
            color=alt.Color("channel:N", legend=alt.Legend(orient="bottom", columns=4)),
            tooltip=["shot", "wave", "rms"],
        )
        .properties(width=420, height=260, title=f"RMS per channel · {BATCH_STAGE}")
    )
    _peak = (
        alt.Chart(_res)
        .mark_rect()
        .encode(
            x=alt.X("shot:O"),
            y=alt.Y("channel:N", title=None),
            color=alt.Color("peak:Q", scale=alt.Scale(scheme="viridis")),
            tooltip=["shot", "wave", "peak"],
        )
        .properties(width=420, height=260, title="peak |y| (shot × channel)")
    )
    mo.vstack(
        [
            mo.md(f"### Result: {len(_res)} rows from a lake of millions of samples"),
            _rms | _peak,
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    ZOOM_X0,
    ZOOM_X1,
    alt,
    config,
    mo,
    pin,
    shot_ui,
    sql_block,
    timed_q,
):
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
    _df, _seconds = timed_q(
        ASOF_SQL,
        [
            EXPERIMENT, shot_ui.value, pin(shot_ui.value, "thomson"), ZOOM_X0, ZOOM_X1,
            EXPERIMENT, shot_ui.value, pin(shot_ui.value, "mirnov"), ZOOM_X0, ZOOM_X1,
        ],
    )  # fmt: skip
    _x = alt.X("x:Q", title="x (ms)")
    _dense = alt.Chart(_df).mark_line().encode(x=_x, y=alt.Y("b_dot:Q", title="mirnov y"))
    _sparse = (
        alt.Chart(_df)
        .mark_line(color="#c0392b", point=True)
        .encode(x=_x, y=alt.Y("te:Q", title="thomson y"))
    )
    _chart = (
        alt.layer(_dense, _sparse)
        .resolve_scale(y="independent")
        .properties(width="container", height=260)
    )
    mo.vstack(
        [
            mo.md(
                "## Stages with different sample rates\n\n"
                "Mirnov runs at kHz to MHz. Thomson is pulsed and slow. Their `x` values "
                "almost never match, so **do not join on `x` with equality**. Use `ASOF JOIN` "
                "from the sparse side to the dense side. Each side pins its **own** "
                "`data_version`.\n\n"
                f"*Shot {shot_ui.value} · window {ZOOM_X0:.1f} to {ZOOM_X1:.1f} ms · "
                f"{len(_df)} matched points · {_seconds * 1000:.0f} ms.*"
            ),
            _chart,
            sql_block(ASOF_SQL),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    ZOOM_X0,
    ZOOM_X1,
    alt,
    mo,
    pd,
    row_group_funnel,
    shot_ui,
    stage_ui,
    version_ui,
    wave_ui,
):
    _steps = row_group_funnel(
        shot_ui.value, stage_ui.value, version_ui.value, wave_ui.value, ZOOM_X0, ZOOM_X1
    )
    _df = pd.DataFrame(_steps, columns=["filter", "row_groups"])
    _chart = (
        alt.Chart(_df)
        .mark_bar()
        .encode(
            y=alt.Y("filter:N", sort=None, title=None),
            x=alt.X("row_groups:Q", scale=alt.Scale(type="symlog"), title="row groups that could be read"),
            tooltip=["filter", "row_groups"],
        )
        .properties(width="container", height=200)
    )
    mo.vstack(
        [
            mo.md(
                "## What did the query read?\n\n"
                "Each filter prunes row groups. The `x` and `wave` filters work **only** if the "
                "column is bare in the predicate (rule 3). A cast or a function makes the last "
                "bar as long as the previous bar.\n\n"
                "The bars follow the stage, shot, version, wave and window that you select. "
                "At production scale, the first bar is about 5 × 10⁷."
            ),
            _chart,
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    EXPERIMENT,
    Path,
    SHOTS,
    STAGES,
    config,
    json,
    list_waves,
    os,
    pin,
    re,
    subprocess,
    tempfile,
):
    MODEL = os.environ.get("LAKE_CLAUDE_MODEL")  # default: the model of Claude Code
    _rules_file = Path(__file__).resolve().parent.parent / "QUERYING.md"
    _rules = (
        _rules_file.read_text()
        if _rules_file.exists()
        else "Pin experiment, stage, shot and one data_version. Select only x, y and wave. "
        "Reduce in SQL. Keep predicates bare."
    )

    def _lake_context() -> str:
        """Facts that Claude needs: table, columns, and the pinned version of each shot."""
        pins = []
        for shot in SHOTS:
            for stage in STAGES:
                try:
                    pins.append(f"shot={shot} stage={stage} data_version={pin(shot, stage)}")
                except KeyError:
                    continue
        waves = []
        for stage in STAGES:
            try:
                names = list_waves(SHOTS[0], stage, pin(SHOTS[0], stage))
            except KeyError:
                continue
            waves.append(f"stage={stage}: {len(names)} waves, for example {names[:4]}")
        return (
            f"Table: {config.TABLE_NAME}(experiment VARCHAR, shot INTEGER, stage VARCHAR, "
            "wave VARCHAR, data_version VARCHAR, x DOUBLE, y DOUBLE). x is time in ms.\n"
            f"experiment: '{EXPERIMENT}'\n"
            "Pinned data_version for each (shot, stage):\n"
            + "\n".join(pins[:200])
            + f"\nWave names in shot {SHOTS[0]}:\n"
            + "\n".join(waves)
        )

    _context = f"# Lake context\n{_lake_context()}\n\n# QUERYING.md\n{_rules}"
    _BASE_RULES = (
        "You write one read-only DuckDB SQL query for the physics wave lake. "
        "Follow the rules in QUERYING.md (below) exactly. Write literal values in the "
        "SQL, not `?` parameters. Use only the experiment, shots, stages and data_version "
        "values in the lake context. Return a SELECT or WITH statement only. "
        "If the question needs a full scan, write the closest bounded query and say what "
        "you changed. Write the explanation in short, simple sentences."
    )

    def ask_claude(extra_instructions: str, question: str, schema: dict) -> dict:
        """Run `claude -p` with the lake context. Return the structured answer."""
        # `claude -p` uses the Claude Code login. An API key in the environment would
        # replace it, so remove the key variables for this call.
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
        }
        cmd = [
            "claude",
            "-p",
            "--output-format", "json",
            "--json-schema", json.dumps(schema),
            "--system-prompt", f"{_BASE_RULES} {extra_instructions}\n\n{_context}",
            "--tools", "",
            "--disable-slash-commands",
            "--no-session-persistence",
        ]  # fmt: skip
        if MODEL:
            cmd += ["--model", MODEL]
        # A temporary working directory keeps the CLAUDE.md files of this repo out of the prompt.
        proc = subprocess.run(
            cmd,
            input=question,
            capture_output=True,
            text=True,
            timeout=180,
            cwd=tempfile.gettempdir(),
            env=env,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
        reply = json.loads(proc.stdout)
        if reply.get("is_error") or not reply.get("structured_output"):
            raise RuntimeError(str(reply.get("result") or "Claude gave no structured answer."))
        return reply["structured_output"]

    def check_sql(sql: str) -> list[tuple[str, bool]]:
        """Text check of the rules in QUERYING.md. It does not replace EXPLAIN ANALYZE."""
        plain = re.sub(r"--[^\n]*", "", sql)
        return [
            ("starts with SELECT or WITH", bool(re.match(r"\s*(select|with)\b", plain, re.I))),
            ("pins `experiment`", bool(re.search(r"\bexperiment\s*=", plain, re.I))),
            ("pins `stage`", bool(re.search(r"\bstage\s*=", plain, re.I))),
            ("bounds `shot`", bool(re.search(r"\bshot\s*(=|in\b|between\b)", plain, re.I))),
            ("pins `data_version`", bool(re.search(r"\bdata_version\s*=", plain, re.I))),
            ("does not use `SELECT *`", not re.search(r"\bselect\s+\*", plain, re.I)),
            (
                "keeps partition and sort columns bare",
                not re.search(
                    r"\b(lower|upper|cast|round|trim)\s*\(\s*(\w+\.)?(shot|stage|x)\b"
                    r"|\bshot\s*%",
                    plain,
                    re.I,
                ),
            ),
        ]

    return MODEL, ask_claude, check_sql


@app.cell
def _(mo):
    ask_ui = mo.ui.text_area(
        value="RMS and peak |y| of each thomson channel in shot 1.",
        label="Question",
        rows=3,
        full_width=True,
    )
    ask_run_ui = mo.ui.run_button(label="Ask Claude for SQL")
    return ask_run_ui, ask_ui


@app.cell(hide_code=True)
def _(MODEL, ask_claude, ask_run_ui, ask_ui, mo):
    generated_sql = None
    _header = mo.md(
        "## Generate a query with Claude\n\n"
        "Ask a question in plain language. Claude gets `QUERYING.md` and the pinned "
        "versions of this lake. It writes the SQL into the **playground below**. "
        "The rule checker and your own review come before the query runs. "
        f"Model: `{MODEL or 'default of Claude Code'}`. The notebook runs `claude -p`, "
        "so it uses your Claude Code login. No API key is necessary."
    )
    mo.stop(not ask_run_ui.value, mo.vstack([_header, ask_ui, ask_run_ui]))

    try:
        _answer = ask_claude(
            "",
            ask_ui.value,
            {
                "type": "object",
                "properties": {"sql": {"type": "string"}, "explanation": {"type": "string"}},
                "required": ["sql", "explanation"],
                "additionalProperties": False,
            },
        )
        generated_sql = _answer["sql"].strip()
        _result = mo.vstack(
            [
                mo.md(_answer["explanation"]),
                mo.md(f"```sql\n{generated_sql}\n```"),
                mo.callout(
                    mo.md("The SQL is in the playground below. Review it, then run it."),
                    kind="info",
                ),
            ]
        )
    except Exception as exc:
        _result = mo.callout(mo.md(f"**`claude -p` failed**\n\n```\n{exc}\n```"), kind="danger")
    mo.vstack([_header, ask_ui, ask_run_ui, _result])
    return (generated_sql,)


@app.cell
def _(EXPERIMENT, SHOTS, STAGES, config, generated_sql, mo, pin):
    _stage = "thomson" if "thomson" in STAGES else STAGES[0]
    _shot = SHOTS[0]
    sql_ui = mo.ui.code_editor(
        value=generated_sql
        or (
            "SELECT wave, count(*) AS n, avg(y) AS y_mean, max(abs(y)) AS peak\n"
            f"FROM {config.TABLE_NAME}\n"
            f"WHERE experiment = '{EXPERIMENT}' AND shot = {_shot} AND stage = '{_stage}'\n"
            f"  AND data_version = '{pin(_shot, _stage)}'\n"
            "GROUP BY wave\n"
            "ORDER BY wave"
        ),
        language="sql",
        min_height=180,
        label="SQL",
    )
    play_run_ui = mo.ui.run_button(label="Run the query")
    return play_run_ui, sql_ui


@app.cell(hide_code=True)
def _(check_sql, mo, play_run_ui, q, sql_ui, time):
    _sql = sql_ui.value
    _checks = check_sql(_sql)
    _ok = all(passed for _, passed in _checks)
    _report = mo.md(
        "\n".join(f"- {'✅' if passed else '❌'} {label}" for label, passed in _checks)
    )
    _header = mo.md(
        "## SQL playground with a rule checker\n\n"
        "Edit the query. The checker is a simple text check of the rules in `QUERYING.md`. "
        "It does not replace `EXPLAIN ANALYZE`. The query runs only if all checks pass. "
        "The result keeps at most 5,000 rows."
    )
    mo.stop(
        not (play_run_ui.value and _ok),
        mo.vstack([_header, sql_ui, _report, play_run_ui]),
    )

    _body = _sql.strip().rstrip(";")
    try:
        _t0 = time.perf_counter()
        _df = q(f"SELECT * FROM ({_body}) AS user_query LIMIT 5000")
        _result = mo.vstack(
            [
                mo.md(f"**{len(_df):,} rows** in {(time.perf_counter() - _t0) * 1000:.0f} ms."),
                mo.ui.table(_df, selection=None),
            ]
        )
    except Exception as exc:
        _result = mo.callout(mo.md(f"**Query error**\n\n```\n{exc}\n```"), kind="danger")
    mo.vstack([_header, sql_ui, _report, play_run_ui, _result])
    return


@app.cell
def _(mo):
    plot_ask_ui = mo.ui.text_area(
        value="Plot the min and max of each mirnov channel in shot 1 against time, "
        "as 400 time buckets.",
        label="Plot request",
        rows=3,
        full_width=True,
    )
    plot_ask_run_ui = mo.ui.run_button(label="Ask Claude for a plot")
    return plot_ask_run_ui, plot_ask_ui


@app.cell(hide_code=True)
def _(alt, ask_claude, check_sql, mo, pd, plot_ask_run_ui, plot_ask_ui, q):
    _header = mo.md(
        "## Generate a plot with Claude\n\n"
        "Claude returns two things: a SQL query that reduces the data, and a chart "
        "description (mark, x, y, color). The notebook checks the SQL with the same rules "
        "as the playground. If all checks pass, it runs the query and draws the chart. "
        "Claude does not write plotting code, so the notebook runs no code from the model."
    )
    mo.stop(
        not plot_ask_run_ui.value, mo.vstack([_header, plot_ask_ui, plot_ask_run_ui])
    )

    _schema = {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "explanation": {"type": "string"},
            "mark": {"type": "string", "enum": ["line", "point", "bar", "area"]},
            "x": {"type": "string"},
            "y": {"type": "string"},
            "color": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["sql", "explanation", "mark", "x", "y", "color", "title"],
        "additionalProperties": False,
    }
    _instructions = (
        "The result is plotted. Reduce the data in SQL first: use min/max decimation in "
        "time buckets, at most 2000 points for each series and at most 5000 rows in total. "
        "Name the output columns clearly. In the chart description, `x`, `y` and `color` "
        "must be output column names of your query. Use an empty string for `color` if "
        "you do not need it. Do not use `SELECT *`."
    )

    try:
        _answer = ask_claude(_instructions, plot_ask_ui.value, _schema)
        _sql = _answer["sql"].strip().rstrip(";")
        _checks = check_sql(_sql)
        _report = mo.md(
            "\n".join(f"- {'✅' if ok else '❌'} {label}" for label, ok in _checks)
        )
        _sql_view = mo.accordion({"SQL from Claude": mo.md(f"```sql\n{_sql}\n```")})
        _parts = [mo.md(_answer["explanation"]), _sql_view, _report]
        if not all(ok for _, ok in _checks):
            _parts.append(
                mo.callout(
                    mo.md("The SQL failed a rule check, so the notebook did not run it."),
                    kind="warn",
                )
            )
        else:
            _df = q(f"SELECT * FROM ({_sql}) AS plot_query LIMIT 5000")
            _fields = [_answer["x"], _answer["y"]] + ([_answer["color"]] if _answer["color"] else [])
            _missing = [f for f in _fields if f not in _df.columns]
            if _missing:
                raise ValueError(
                    f"The chart uses columns that the query does not return: {_missing}. "
                    f"Columns: {list(_df.columns)}"
                )

            def _field_type(col: str) -> str:
                return "quantitative" if pd.api.types.is_numeric_dtype(_df[col]) else "nominal"

            _mark = {"line": "mark_line", "point": "mark_point", "bar": "mark_bar", "area": "mark_area"}[
                _answer["mark"]
            ]
            _encodings = {
                "x": alt.X(_answer["x"], type=_field_type(_answer["x"])),
                "y": alt.Y(_answer["y"], type=_field_type(_answer["y"])),
            }
            if _answer["color"]:
                _encodings["color"] = alt.Color(_answer["color"], type=_field_type(_answer["color"]))
            _chart = (
                getattr(alt.Chart(_df), _mark)()
                .encode(**_encodings)
                .properties(title=_answer["title"], width=700, height=350)
                .interactive()
            )
            _parts.append(mo.md(f"**{len(_df):,} rows**"))
            if len(_df) == 5000:
                _parts.append(
                    mo.callout(
                        mo.md("The result reached the 5,000 row cap. The plot can be incomplete."),
                        kind="warn",
                    )
                )
            _parts.append(mo.ui.altair_chart(_chart))
        _result = mo.vstack(_parts)
    except Exception as exc:
        _result = mo.callout(mo.md(f"**Plot request failed**\n\n```\n{exc}\n```"), kind="danger")
    mo.vstack([_header, plot_ask_ui, plot_ask_run_ui, _result])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell(hide_code=True)
def _(lake, mo):
    # The demo lake has a DuckDB connection. The real catalog does not.
    _footnote = (
        "*To run this notebook on a real catalog:*\n    "
        '`LAKE_CATALOG="postgres:dbname=ducklake_catalog host=localhost user=ducklake" '
        "marimo run notebooks/querying_the_lake.py`"
        if lake.connection is not None
        else ""
    )
    mo.md(f"""
    ## Summary

    1. **Select the access path.** Metadata: Postgres. One wave: HTTP. Analysis: SQL.
    2. **Pin the partition and the version.** Use `experiment`, `shot`, `stage` and one `data_version`.
    3. **Reduce in SQL.** Decimate for plots. Aggregate for statistics.
    4. **Use one range query for many shots.** Use a per-shot batch only if the query is too slow.
    5. **Check what the query read.** Use `EXPLAIN ANALYZE` and the row-group counts from the manifest.

    Full rules: `QUERYING.md`. Design background: `README.md`.

    {_footnote}
    """)
    return


if __name__ == "__main__":
    app.run()

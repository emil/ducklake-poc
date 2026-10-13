import marimo

__generated_with = "0.25.1"
app = marimo.App(
    width="medium",
    app_title="Spectrometer impurities across shots",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Spectrometer impurities across shots

    **Use case:** Find how the impurity line intensities of the spectrometer change from shot to
    shot. Then find which other stages and waves move with them.

    1. **Impurity level for each shot.** One reduced value for each shot and each line.
    2. **Correlation across shots.** Each impurity line against each wave of the other stages.
    3. **One pair in detail.** A scatter plot of the strongest pair.
    4. **Correlation in time, inside one shot.** A lagged correlation of two waves with
       different sample rates.

    Each query follows `QUERYING.md`: one pinned `data_version` for each `(shot, stage)`,
    partition filters, and reduction in SQL. The notebook never pulls raw samples.
    """)
    return


@app.cell
def _():
    import os

    import altair as alt
    import marimo as mo
    import numpy as np
    import pandas as pd

    from ducklake_poc import config, synthetic
    from ducklake_poc.correlation import cross_correlation_matrix, lagged_correlation
    from ducklake_poc.notebook_lake import open_lake, pins_cte

    _ = alt.data_transformers.disable_max_rows()
    return (
        alt,
        config,
        cross_correlation_matrix,
        lagged_correlation,
        mo,
        open_lake,
        os,
        pd,
        pins_cte,
        synthetic,
    )


@app.cell
def _(mo, open_lake, os, pd):
    # Set LAKE_CATALOG to use the real catalog. Without it, the notebook builds a small
    # synthetic lake with the production generator and compactor.
    CATALOG = os.environ.get("LAKE_CATALOG")
    lake = open_lake(
        CATALOG,
        lake_url=os.environ.get("LAKE_URL"),
        experiment=os.environ.get("LAKE_EXPERIMENT"),
        on_ambiguous=os.environ.get("LAKE_VERSION_POLICY", "first"),  # pyright: ignore[reportArgumentType]
        build_context=lambda: mo.status.spinner(title="Building the demo lake (first run only)"),
    )
    SPECTRO_STAGE = "spectrometer"
    HAS_SPECTRO = SPECTRO_STAGE in lake.stages
    if not lake.shots:
        _out = mo.md("**The lake has no data.** Run `ducklake-ingest` first.")
    else:
        _out = [
            mo.md(
                f"**Data source:** {lake.mode}  \n"
                f"**Shots:** {lake.shots[0]}-{lake.shots[-1]} ({len(lake.shots)} shots)  \n"
                f"**Stages:** {', '.join(f'`{s}`' for s in lake.stages)}"
            )
        ]
        if lake.ambiguous:
            _out.append(
                mo.callout(
                    mo.md(
                        f"**{len(lake.ambiguous)} of {len(lake.pinned)} (shot, stage) pairs have "
                        "several `data_version`s.** The catalog has no timestamp for them, and "
                        "a version has no order. This notebook pins the **first version by "
                        "name**. This is a demo shortcut. For real work, ask which version to "
                        "use. Set `LAKE_VERSION_POLICY=error` to refuse instead."
                    ),
                    kind="warn",
                )
            )
        _pins = pd.DataFrame(
            [(s, st, v) for (s, st), v in sorted(lake.pinned.items())],
            columns=["shot", "stage", "data_version"],
        )
        _out.append(mo.accordion({"Pinned versions": mo.ui.table(_pins, selection=None)}))
        _out = mo.vstack(_out)
    _out
    return HAS_SPECTRO, SPECTRO_STAGE, lake


@app.cell
def _(HAS_SPECTRO, SPECTRO_STAGE, lake, mo, synthetic):
    mo.stop(
        not HAS_SPECTRO,
        mo.callout(
            mo.md(f"The lake has no `{SPECTRO_STAGE}` stage. Ingest it with "
                  f"`ducklake-ingest --shots N --stage {SPECTRO_STAGE}`."),
            kind="danger",
        ),
    )
    _lines = lake.list_waves(lake.shots[0], SPECTRO_STAGE)
    _others = [s for s in lake.stages if s != SPECTRO_STAGE]
    # Mirnov has about 2 M samples in each wave. Leave it off by default.
    _default_others = [s for s in _others if s != "mirnov"]

    shot_range_ui = mo.ui.range_slider(
        start=lake.shots[0],
        stop=lake.shots[-1],
        step=1,
        # A large lake: start with the first 20 shots. Widen the range on purpose.
        value=[lake.shots[0], lake.shots[min(len(lake.shots), 20) - 1]],
        label="shots",
        full_width=True,
    )
    window_ui = mo.ui.range_slider(
        start=0,
        stop=synthetic.DURATION_MS,
        step=5,
        value=[100, 400],
        label="time window (ms)",
        full_width=True,
    )
    lines_ui = mo.ui.multiselect(
        options=_lines,
        value=[w for w in _lines if "CIII" in w or "OII" in w] or _lines[:2],
        label="impurity lines",
    )
    reference_ui = mo.ui.dropdown(
        options=["(none)", *_lines],
        value=next((w for w in _lines if "Halpha" in w), "(none)"),
        label="reference line for the ratio",
    )
    metric_ui = mo.ui.dropdown(
        options={
            "mean": "avg(w.y)",
            "maximum": "max(w.y)",
            "95th percentile": "quantile_cont(w.y, 0.95)",
            "standard deviation": "stddev_samp(w.y)",
        },
        value="mean",
        label="reduction for each shot",
    )
    stages_ui = mo.ui.multiselect(
        options=_others, value=_default_others, label="stages to correlate with"
    )
    method_ui = mo.ui.dropdown(
        options=["pearson", "spearman"], value="pearson", label="correlation"
    )
    mo.vstack(
        [
            mo.md("## Controls"),
            shot_range_ui,
            window_ui,
            mo.hstack([lines_ui, reference_ui, metric_ui], justify="start", gap=2),
            mo.hstack([stages_ui, method_ui], justify="start", gap=2),
        ]
    )
    return (
        lines_ui,
        method_ui,
        metric_ui,
        reference_ui,
        shot_range_ui,
        stages_ui,
        window_ui,
    )


@app.cell
def _(config, lake, mo, pd, pins_cte):
    def per_shot_reduction(
        stage: str,
        shots: tuple[int, int],
        window: tuple[float, float],
        metric_sql: str,
        waves: list[str] | None = None,
    ) -> tuple[pd.DataFrame, str]:
        """Reduce each (shot, wave) of one stage to one number.

        One range query with one pinned data_version for each shot (QUERYING.md,
        "Cross-shot work"). Return the DataFrame and the SQL text.
        """
        present = [s for s in range(shots[0], shots[1] + 1) if (s, stage) in lake.pinned]
        if not present:
            return pd.DataFrame(columns=["shot", "wave", "value", "n"]), ""
        cte, params = pins_cte(present, stage, lake.pin)
        params += [lake.experiment, stage, min(present), max(present), window[0], window[1]]
        wave_filter = ""
        if waves:
            wave_filter = f"\n      AND w.wave IN ({', '.join('?' for _ in waves)})"
            params += waves
        sql = f"""WITH {cte}
    SELECT w.shot, w.wave, {metric_sql} AS value, count(*) AS n
    FROM {config.TABLE_NAME} w
    JOIN pins p ON w.shot = p.shot AND w.data_version = p.data_version
    WHERE w.experiment = ? AND w.stage = ? AND w.shot BETWEEN ? AND ?
          AND w.x >= ? AND w.x < ?{wave_filter}
    GROUP BY w.shot, w.wave
    ORDER BY w.shot, w.wave"""
        return lake.q(sql, params), sql

    def sql_block(sql: str):
        return mo.accordion({"SQL": mo.md(f"```sql\n{sql}\n```")})

    return per_shot_reduction, sql_block


@app.cell(hide_code=True)
def _(
    SPECTRO_STAGE,
    alt,
    lines_ui,
    metric_ui,
    mo,
    per_shot_reduction,
    reference_ui,
    shot_range_ui,
    sql_block,
    window_ui,
):
    mo.stop(not lines_ui.value, mo.md("Select at least one impurity line."))
    _shots = tuple(shot_range_ui.value)
    _window = tuple(window_ui.value)
    _ref = reference_ui.value
    _waves = list(lines_ui.value) + ([_ref] if _ref != "(none)" and _ref not in lines_ui.value else [])
    impurity_long, _sql = per_shot_reduction(
        SPECTRO_STAGE, _shots, _window, metric_ui.value, _waves
    )
    mo.stop(impurity_long.empty, mo.md("No spectrometer data for these shots."))

    # One column for each line, one row for each shot.
    impurity_wide = impurity_long.pivot(index="shot", columns="wave", values="value")
    _shown = impurity_long[impurity_long["wave"].isin(lines_ui.value)]
    _level = (
        alt.Chart(_shown)
        .mark_line(point=True)
        .encode(
            x=alt.X("shot:O", title="shot"),
            y=alt.Y("value:Q", title=f"{metric_ui.selected_key} of the line intensity"),
            color=alt.Color("wave:N", title="line"),
            tooltip=["shot", "wave", alt.Tooltip("value:Q", format=".4g")],
        )
        .properties(title="Impurity level for each shot", width=650, height=280)
    )
    _parts = [mo.md("## 1. Impurity level for each shot"), _level]
    if _ref != "(none)" and _ref in impurity_wide.columns:
        _ratio = (
            impurity_wide[list(lines_ui.value)]
            .div(impurity_wide[_ref], axis=0)
            .reset_index()
            .melt("shot", var_name="wave", value_name="ratio")
        )
        _parts.append(
            alt.Chart(_ratio)
            .mark_line(point=True)
            .encode(
                x=alt.X("shot:O"),
                y=alt.Y("ratio:Q", title=f"line / {_ref}"),
                color=alt.Color("wave:N", title="line"),
                tooltip=["shot", "wave", alt.Tooltip("ratio:Q", format=".4g")],
            )
            .properties(
                title=f"Ratio to the reference line (a proxy: the intensities are not calibrated)",
                width=650,
                height=220,
            )
        )
    _parts.append(sql_block(_sql))
    mo.vstack(_parts)
    return (impurity_wide,)


@app.cell(hide_code=True)
def _(
    SPECTRO_STAGE,
    metric_ui,
    mo,
    pd,
    per_shot_reduction,
    shot_range_ui,
    sql_block,
    stages_ui,
    window_ui,
):
    mo.stop(not stages_ui.value, mo.md("Select at least one stage to correlate with."))
    _shots = tuple(shot_range_ui.value)
    _window = tuple(window_ui.value)
    _frames = []
    _sqls = []
    with mo.status.spinner(title="Reducing the other stages (one query for each stage)"):
        for _stage in stages_ui.value:
            if _stage == SPECTRO_STAGE:
                continue
            _df, _sql = per_shot_reduction(_stage, _shots, _window, metric_ui.value)
            if not _df.empty:
                _df["stage"] = _stage
                _frames.append(_df)
                _sqls.append(_sql)
    mo.stop(not _frames, mo.md("The selected stages have no data in these shots."))
    others_long = pd.concat(_frames, ignore_index=True)
    others_wide = others_long.pivot(index="shot", columns="wave", values="value")
    mo.vstack(
        [
            mo.md(
                f"Reduced {others_long['wave'].nunique()} waves of "
                f"{len(stages_ui.value)} stages to {len(others_long):,} values. "
                f"`{metric_ui.selected_key}` over the window "
                f"{_window[0]:.0f}-{_window[1]:.0f} ms."
            ),
            sql_block(_sqls[0] if len(_sqls) == 1 else "\n\n-- next stage\n\n".join(_sqls)),
        ]
    )
    return others_long, others_wide


@app.cell(hide_code=True)
def _(
    alt,
    cross_correlation_matrix,
    impurity_wide,
    lines_ui,
    method_ui,
    mo,
    others_long,
    others_wide,
):
    _left = impurity_wide[[w for w in lines_ui.value if w in impurity_wide.columns]]
    corr = cross_correlation_matrix(_left, others_wide, method=method_ui.value)
    _n = len(_left.index.intersection(others_wide.index))
    _stage_of = others_long.drop_duplicates("wave").set_index("wave")["stage"]
    _long = (
        corr.rename_axis(index="wave", columns="line")
        .stack(future_stack=True)
        .rename("r")
        .reset_index()
        .assign(stage=lambda d: d["wave"].map(_stage_of))
    )
    _heat = (
        alt.Chart(_long)
        .mark_rect()
        .encode(
            x=alt.X("line:N", title="impurity line"),
            y=alt.Y("wave:N", title="wave", sort=list(corr.index)),
            color=alt.Color(
                "r:Q",
                scale=alt.Scale(scheme="redblue", domain=[-1, 1]),
                title=f"{method_ui.value} r",
            ),
            tooltip=["line", "wave", "stage", alt.Tooltip("r:Q", format=".2f")],
        )
        .properties(
            title=f"Correlation across {_n} shots",
            width=max(160, 110 * len(corr.columns)),
            height=max(200, 18 * len(corr.index)),
        )
    )
    _warn = (
        mo.callout(
            mo.md(f"Only {_n} shots are in the correlation. A few shots give large |r| by "
                  "chance. Use more shots before you draw a conclusion."),
            kind="warn",
        )
        if _n < 20
        else mo.md("")
    )
    ranked = (
        _long.dropna(subset=["r"])
        .assign(abs_r=lambda d: d["r"].abs())
        .sort_values("abs_r", ascending=False)
        .drop(columns="abs_r")
        .reset_index(drop=True)
    )
    mo.vstack(
        [
            mo.md("## 2. Correlation across shots"),
            mo.md(
                "Each cell is the correlation of one impurity line with one wave of another "
                "stage. The data points are the shots. Each shot has its own pinned "
                "`data_version` for each stage."
            ),
            _warn,
            _heat,
            mo.md("**Strongest pairs**"),
            mo.ui.table(ranked.head(15), selection=None, format_mapping={"r": "{:.3f}"}),
        ]
    )
    return (ranked,)


@app.cell
def _(mo, ranked):
    mo.stop(ranked.empty, mo.md("There are no correlations to show."))
    _pairs = {
        f"{r.line}  vs  {r.wave}  (r = {r.r:.2f})": (r.line, r.wave)
        for r in ranked.head(25).itertuples()
    }
    pair_ui = mo.ui.dropdown(options=_pairs, value=next(iter(_pairs)), label="pair")
    pair_ui
    return (pair_ui,)


@app.cell(hide_code=True)
def _(alt, impurity_wide, mo, others_wide, pair_ui, pd):
    _line, _wave = pair_ui.value
    _data = (
        pd.concat([impurity_wide[_line].rename("x"), others_wide[_wave].rename("y")], axis=1)
        .dropna()
        .reset_index()
    )
    _base = alt.Chart(_data).encode(
        x=alt.X("x:Q", title=_line, scale=alt.Scale(zero=False)),
        y=alt.Y("y:Q", title=_wave, scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(filled=True, size=70).encode(
        tooltip=["shot", alt.Tooltip("x:Q", format=".4g"), alt.Tooltip("y:Q", format=".4g")]
    )
    _labels = _base.mark_text(dx=8, dy=-6, fontSize=10).encode(text="shot:O")
    _fit = _base.transform_regression("x", "y").mark_line(color="gray", strokeDash=[4, 3])
    mo.vstack(
        [
            mo.md("## 3. One pair in detail"),
            (_points + _labels + _fit).properties(width=520, height=360),
            mo.md("Look for one shot that drives the whole correlation. Then check that shot "
                  "in section 4."),
        ]
    )
    return


@app.cell
def _(SPECTRO_STAGE, lake, lines_ui, mo, stages_ui):
    _shots = [s for s in lake.shots if (s, SPECTRO_STAGE) in lake.pinned]
    detail_shot_ui = mo.ui.dropdown(
        options={str(s): s for s in _shots}, value=str(_shots[0]), label="shot"
    )
    detail_line_ui = mo.ui.dropdown(
        options=list(lines_ui.value) or lake.list_waves(_shots[0], SPECTRO_STAGE),
        value=(list(lines_ui.value) or lake.list_waves(_shots[0], SPECTRO_STAGE))[0],
        label="impurity line",
    )
    _stage_options = list(stages_ui.value) or [s for s in lake.stages if s != SPECTRO_STAGE]
    detail_stage_ui = mo.ui.dropdown(
        options=_stage_options,
        value="bolometer" if "bolometer" in _stage_options else _stage_options[0],
        label="other stage",
    )
    bucket_ui = mo.ui.slider(
        start=1, stop=20, step=1, value=2, label="bucket width (ms)", full_width=True
    )
    lag_ui = mo.ui.slider(
        start=1, stop=50, step=1, value=15, label="largest lag (buckets)", full_width=True
    )
    return bucket_ui, detail_line_ui, detail_shot_ui, detail_stage_ui, lag_ui


@app.cell
def _(detail_shot_ui, detail_stage_ui, lake, mo):
    _waves = lake.list_waves(detail_shot_ui.value, detail_stage_ui.value)
    mo.stop(not _waves, mo.md("This stage has no waves in this shot."))
    detail_wave_ui = mo.ui.dropdown(options=_waves, value=_waves[0], label="other wave")
    return (detail_wave_ui,)


@app.cell(hide_code=True)
def _(
    SPECTRO_STAGE,
    alt,
    bucket_ui,
    config,
    detail_line_ui,
    detail_shot_ui,
    detail_stage_ui,
    detail_wave_ui,
    lag_ui,
    lagged_correlation,
    lake,
    mo,
    sql_block,
    window_ui,
):
    _shot = int(detail_shot_ui.value)
    _x0, _x1 = (float(v) for v in window_ui.value)
    _width = float(bucket_ui.value)

    # Both waves go to one common time grid. The sample rates are different, so the
    # join is on the bucket number, never on `x` (QUERYING.md, "Comparing waves ...").
    _sql = f"""SELECT floor((x - ?) / ?) AS bucket, avg(y) AS y
    FROM {config.TABLE_NAME}
    WHERE experiment = ? AND shot = ? AND stage = ? AND data_version = ? AND wave = ?
          AND x >= ? AND x < ?
    GROUP BY bucket
    ORDER BY bucket"""

    def _series(stage: str, wave: str):
        return lake.q(
            _sql,
            [_x0, _width, lake.experiment, _shot, stage, lake.pin(_shot, stage), wave, _x0, _x1],
        )

    _a = _series(SPECTRO_STAGE, detail_line_ui.value).rename(columns={"y": "line"})
    _b = _series(detail_stage_ui.value, detail_wave_ui.value).rename(columns={"y": "other"})
    _joined = _a.merge(_b, on="bucket", how="inner").sort_values("bucket")
    mo.stop(
        len(_joined) < 10,
        mo.callout(
            mo.md(f"Only {len(_joined)} buckets have both waves. Use a wider bucket or window."),
            kind="warn",
        ),
    )
    _joined["t_ms"] = _x0 + (_joined["bucket"] + 0.5) * _width
    for _col in ("line", "other"):
        _joined[f"{_col}_z"] = (_joined[_col] - _joined[_col].mean()) / (_joined[_col].std() or 1.0)

    _lag = lagged_correlation(
        _joined["line"].to_numpy(), _joined["other"].to_numpy(), int(lag_ui.value)
    )
    _lag["lag_ms"] = _lag["lag"] * _width
    _best = _lag.loc[_lag["r"].abs().idxmax()] if _lag["r"].notna().any() else None

    _series_chart = (
        alt.Chart(_joined)
        .transform_fold(["line_z", "other_z"], as_=["signal", "z"])
        .mark_line()
        .encode(
            x=alt.X("t_ms:Q", title="time (ms)"),
            y=alt.Y("z:Q", title="z-score"),
            color=alt.Color("signal:N", title="signal"),
        )
        .properties(
            title=f"Shot {_shot}: {detail_line_ui.value} and {detail_wave_ui.value}",
            width=650,
            height=220,
        )
    )
    _lag_chart = (
        alt.Chart(_lag.dropna())
        .mark_bar()
        .encode(
            x=alt.X("lag_ms:Q", title="lag (ms). A positive lag means the other wave follows."),
            y=alt.Y("r:Q", title="r", scale=alt.Scale(domain=[-1, 1])),
            tooltip=["lag", "lag_ms", alt.Tooltip("r:Q", format=".3f")],
        )
        .properties(width=650, height=200)
    )
    _summary = (
        mo.md(
            f"Largest |r| = **{_best['r']:.2f}** at a lag of **{_best['lag_ms']:.0f} ms** "
            f"({len(_joined)} buckets of {_width:.0f} ms)."
        )
        if _best is not None
        else mo.md("No lag has a valid correlation.")
    )
    mo.vstack(
        [
            mo.md("## 4. Correlation in time, inside one shot"),
            mo.hstack([detail_shot_ui, detail_line_ui, detail_stage_ui, detail_wave_ui],
                      justify="start", gap=2),
            mo.hstack([bucket_ui, lag_ui], gap=2),
            _series_chart,
            _lag_chart,
            _summary,
            sql_block(_sql),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Limits of this analysis

    - **Demo data is synthetic.** The generator makes each wave independently. In the demo
      lake, correlations between waves are only noise. The correlation tools are real. The
      physics result is not.
    - **A correlation is not a cause.** Two waves can move together because the plasma
      current or the heating power changed both of them.
    - **Few shots give large chance correlations.** The notebook shows a warning below 20 shots.
    - **Each stage has its own `data_version`.** After a recalibration, the shots of one
      stage can have different versions. Pin the versions on purpose. Ask the owner of the
      diagnostic which version to use.
    - **Line intensities are not calibrated.** The ratio to a reference line is a proxy for the
      impurity fraction. It is not a concentration.
    - **High-rate stages cost more.** `mirnov` has about 2 M samples in each wave. Leave it off,
      or narrow the shot range and the time window first.

    *To use the real catalog:*
    `LAKE_CATALOG="postgres:dbname=ducklake_catalog host=localhost user=ducklake" marimo run notebooks/spectrometer_impurities.py`
    """)
    return


if __name__ == "__main__":
    app.run()

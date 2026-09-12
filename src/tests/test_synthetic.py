from __future__ import annotations

import numpy as np

from ducklake_poc.synthetic import (
    DIAGNOSTICS,
    DURATION_MS,
    STAGE_PROFILE,
    _generate_version_ids,
    _stable_seed,
    generate_big_wave,
    generate_stage,
    wave_to_sample_table,
)


def test_stage_profile_covers_every_diagnostic_group() -> None:
    assert set(STAGE_PROFILE) == set(DIAGNOSTICS)


def test_stable_seed_is_deterministic_across_calls() -> None:
    a = _stable_seed(1, "mirnov", "mirnov/probe_01", "a1b2c3d")
    b = _stable_seed(1, "mirnov", "mirnov/probe_01", "a1b2c3d")
    assert a == b


def test_stable_seed_differs_for_different_inputs() -> None:
    a = _stable_seed(1, "mirnov")
    b = _stable_seed(1, "thomson")
    assert a != b


def test_generate_stage_is_deterministic() -> None:
    """Two independent generator runs for the same (shot, stage) must
    produce bit-identical output -- this is what local_demo.py and the
    notebook rely on to be reproducible across separate process runs."""
    a = list(generate_stage(shot=1, stage="rogowski"))
    b = list(generate_stage(shot=1, stage="rogowski"))

    assert len(a) == len(b)
    for rec_a, rec_b in zip(a, b, strict=True):
        assert rec_a.wave == rec_b.wave
        assert rec_a.data_version == rec_b.data_version
        assert np.array_equal(rec_a.x, rec_b.x)
        assert np.array_equal(rec_a.y, rec_b.y)


def test_generate_stage_yields_every_channel_times_every_version() -> None:
    stage = "thomson"
    version_ids = _generate_version_ids(shot=1, stage=stage)
    records = list(generate_stage(shot=1, stage=stage))

    assert len(records) == len(DIAGNOSTICS[stage]) * len(version_ids)
    seen = {(r.wave, r.data_version) for r in records}
    expected = {(w, v) for w in DIAGNOSTICS[stage] for v in version_ids}
    assert seen == expected


def test_generate_stage_recalibration_shared_across_channels_in_a_stage() -> None:
    """A calibration change reprocesses a whole diagnostic group at once,
    so every channel in a stage should see the exact same set of
    data_version hashes."""
    records = list(generate_stage(shot=42, stage="mirnov"))
    versions_by_wave: dict[str, set[str]] = {}
    for r in records:
        versions_by_wave.setdefault(r.wave, set()).add(r.data_version)

    version_sets = list(versions_by_wave.values())
    assert all(v == version_sets[0] for v in version_sets)


def test_generate_stage_sample_count_matches_rate_times_duration() -> None:
    # rogowski has no "big wave" chance, so every channel is exactly
    # rate_khz * DURATION_MS samples.
    rate_khz, big_p, _big_khz = STAGE_PROFILE["rogowski"]
    assert big_p == 0.0
    expected_n = round(rate_khz * DURATION_MS)

    for rec in generate_stage(shot=1, stage="rogowski"):
        assert len(rec.x) == expected_n
        assert len(rec.y) == expected_n


def test_x_spans_full_duration_in_milliseconds() -> None:
    rec = next(generate_stage(shot=1, stage="rogowski"))
    assert rec.x[0] == 0.0
    assert rec.x[-1] == DURATION_MS


def test_generate_big_wave_exact_sample_count() -> None:
    rec = generate_big_wave(shot=1, stage="mirnov", wave="mirnov/probe_01", n_samples=12345)
    assert len(rec.x) == 12345
    assert len(rec.y) == 12345
    assert rec.x.min() == 0.0
    assert rec.x.max() == DURATION_MS


def test_different_data_versions_produce_different_signals() -> None:
    """The calibration perturbation should actually vary the signal
    between data_versions -- otherwise two "different" versions would be
    silently identical data, defeating the point of the column."""
    v1 = generate_big_wave(1, "mirnov", "mirnov/probe_01", 1000, data_version="aaaaaaa")
    v2 = generate_big_wave(1, "mirnov", "mirnov/probe_01", 1000, data_version="bbbbbbb")
    assert not np.array_equal(v1.y, v2.y)


def test_wave_to_sample_table_schema_and_row_count() -> None:
    rec = generate_big_wave(1, "mirnov", "mirnov/probe_01", 500, data_version="a1b2c3d")
    table = wave_to_sample_table(rec)

    assert table.num_rows == 500
    # experiment/shot/stage are deliberately NOT physical columns --
    # they're hive-partition keys encoded only in the directory path
    # (see synthetic.py's wave_to_sample_table docstring for why:
    # avoiding github.com/duckdb/ducklake/issues/791).
    assert set(table.schema.names) == {"wave", "data_version", "x", "y"}
    assert table.column("wave").to_pylist() == ["mirnov/probe_01"] * 500
    assert table.column("data_version").to_pylist() == ["a1b2c3d"] * 500

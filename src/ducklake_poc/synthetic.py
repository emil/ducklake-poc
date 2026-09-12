"""Generate synthetic tokamak diagnostic wave data as PER-SAMPLE ROWS:

    experiment    VARCHAR   -- experimental campaign, e.g. "campaign-2026a"
                               (a shot belongs to exactly one campaign)
    shot          INTEGER   -- experiment/discharge id
    stage         VARCHAR   -- diagnostic group, e.g. "mirnov", "thomson"
    wave          VARCHAR   -- channel/signal produced by that diagnostic,
                               e.g. "thomson_te/A", "mirnov/probe_03"
    data_version  VARCHAR   -- git-commit-hash-style id of the calibration
                               constants + processing code that produced
                               this copy of the wave (see below)
    x             DOUBLE    -- elapsed time in MILLISECONDS since the shot's
                               start (the absolute start time itself lives
                               in a separate per-shot metadata record --
                               e.g. `started_at` -- which isn't modeled
                               here since it's not relevant to the
                               storage/query design)
    y             DOUBLE    -- one amplitude sample

`stage` and `wave` are dictionary-encoded (see parquet_encoding.py) --
`stage` is a handful of distinct diagnostic groups, `wave` is at most a
few dozen distinct channels across a whole shot.

`data_version` models recalibration: scientists periodically revise a
diagnostic's calibration and rerun the processing pipeline against the
same raw capture, producing a new, independent full copy of every channel
in that diagnostic group for that shot. Old versions are kept, not
overwritten -- so the same (shot, stage, wave) can legitimately have
several complete, independent sets of samples, one per data_version.
data_version is modeled as a short git-style hash (e.g. "a1b2c3d")
rather than a sequential integer, since in a real system it would
identify the exact processing code + calibration constants used --
there's no inherent chronological ordering in the hash value itself
(a real system wanting "give me the latest version" would need a
timestamp, likely from the same unmodeled per-shot metadata record
that holds `started_at`). A calibration change applies to a whole
diagnostic group at once (you don't recalibrate a single Mirnov probe
in isolation), so the set of data_version hashes is drawn once per
(shot, stage) and applied to every channel in that stage.

The diagnostic groups and their channel sets below are a representative
(not exhaustive) slice of real tokamak instrumentation, grouped the way
they'd actually be grouped operationally: magnetic sensors, optical/laser
diagnostics, and radiation/particle sensors. Sample-count profiles per
stage are deliberately realistic in *shape*, not just size: each stage
has a sample rate in kHz (== samples per ms, since 1 kHz = 1 sample/ms)
applied over a shared shot duration, and Mirnov coils are the stage
realistically capable of a multi-MHz digitization rate -- a genuine
physical reason for the heavy-tailed wave-size distribution the design
conversation started from, not an arbitrary "rare big wave" knob. Thomson
scattering, by contrast, is a pulsed spatial-profile measurement at a
comparatively low repetition rate.
"""

from __future__ import annotations

import random
import zlib
from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np
import pyarrow as pa


def _stable_seed(*parts) -> int:
    """Deterministic seed across processes/runs -- Python's built-in
    hash() is randomized per-process for strings (PYTHONHASHSEED), which
    would make the random draws in generate_stage non-reproducible
    between runs. crc32 is stable."""
    return zlib.crc32("|".join(str(p) for p in parts).encode())


def _short_hash(*parts) -> str:
    """A 7-hex-char, git-short-hash-looking, deterministic id."""
    return format(_stable_seed(*parts) & 0xFFFFFFF, "07x")


# Representative discharge length -- real tokamak shots range from tens
# of ms to several seconds; 500ms is a plausible mid-range pulse.
DURATION_MS = 500.0

DEFAULT_EXPERIMENT = "campaign-2026a"

# stage -> channel (wave) names it produces
DIAGNOSTICS: dict[str, list[str]] = {
    # Magnetic sensors (shape, position, current)
    "rogowski": ["rogowski/coil_1", "rogowski/coil_2"],
    "mirnov": [f"mirnov/probe_{i:02d}" for i in range(1, 9)],
    "flux_loops": ["flux_loops/poloidal_A", "flux_loops/saddle_B"],
    "diamagnetic_loop": ["diamagnetic_loop/main"],
    # Optical & laser diagnostics (density & temperature)
    "thomson": ["thomson_te/A", "thomson_te/B", "thomson_ne/A", "thomson_ne/B"],
    "tip": ["tip/density_ch1", "tip/faraday_ch1"],
    "ece": [f"ece/ch{i:02d}" for i in range(1, 9)],
    # Radiation & particle sensors
    "bolometer": [f"bolometer/array_{i:02d}" for i in range(1, 9)],
    "cxrs": ["cxrs/channel_1", "cxrs/channel_2"],
    # Machine protection & exhaust
    "langmuir_probe": [f"langmuir_probe/probe_{i}" for i in range(1, 5)],
}

# stage -> (typical_rate_khz, big_wave_probability, big_rate_khz)
# rate is in kHz, which is exactly samples/ms, so n_samples = rate * DURATION_MS.
# mirnov's big case (4 MHz = 4000 kHz) over a 500ms shot gives exactly
# 2,000,000 samples -- a genuinely plausible high-frequency MHD digitization
# rate, not just a round number picked for the demo.
STAGE_PROFILE: dict[str, tuple[float, float, float]] = {
    "rogowski": (10.0, 0.0, 0.0),
    "mirnov": (16.0, 0.05, 4_000.0),
    "flux_loops": (10.0, 0.0, 0.0),
    "diamagnetic_loop": (6.0, 0.0, 0.0),
    "thomson": (1.0, 0.0, 0.0),
    "tip": (40.0, 0.0, 0.0),
    "ece": (30.0, 0.0, 0.0),
    "bolometer": (20.0, 0.0, 0.0),
    "cxrs": (8.0, 0.0, 0.0),
    "langmuir_probe": (24.0, 0.0, 0.0),
}

# Most (shot, stage) combinations only get processed once; a recalibration
# + pipeline rerun producing a 2nd or 3rd data_version is the exception.
DATA_VERSION_1_PROB = 0.85
DATA_VERSION_2_PROB = 0.12  # cumulative: 0.85 + 0.12 = 0.97
# remaining 0.03 -> 3 versions


def _generate_version_ids(shot: int, stage: str) -> list[str]:
    """Every data_version hash for this (shot, stage) -- a calibration
    change reprocesses the whole diagnostic group at once, so all its
    channels share the same set of versions."""
    rng = random.Random(_stable_seed(shot, stage, "n_versions"))
    r = rng.random()
    if r < DATA_VERSION_1_PROB:
        n = 1
    elif r < DATA_VERSION_1_PROB + DATA_VERSION_2_PROB:
        n = 2
    else:
        n = 3
    return [_short_hash(shot, stage, "version", i) for i in range(n)]


@dataclass
class WaveRecord:
    experiment: str
    shot: int
    stage: str
    wave: str
    data_version: str
    x: np.ndarray
    y: np.ndarray


def _make_wave(
    experiment: str, shot: int, stage: str, wave: str, data_version: str, n: int, rng: random.Random
) -> WaveRecord:
    t = np.linspace(0.0, DURATION_MS, n, dtype=np.float64)
    freq = rng.uniform(1.0, 40.0)
    phase = rng.uniform(0.0, 2 * np.pi)
    seed = _stable_seed(experiment, shot, stage, wave, data_version)
    noise = np.random.default_rng(seed).normal(0.0, 0.05, n)
    # A recalibration nudges gain/offset slightly rather than reshaping the
    # signal entirely. The perturbation is derived from a hash of the
    # data_version string itself (not an ordinal index -- data_version
    # hashes have no inherent order), so it's deterministic per version
    # but doesn't assume anything about version sequencing.
    version_frac = (_stable_seed(data_version) % 1000) / 1000.0  # in [0, 1)
    gain = 1.0 + 0.02 * version_frac
    offset = 0.01 * version_frac
    y = gain * np.sin(2 * np.pi * freq * (t / DURATION_MS) + phase) + offset + noise
    return WaveRecord(
        experiment=experiment,
        shot=shot,
        stage=stage,
        wave=wave,
        data_version=data_version,
        x=t,
        y=y,
    )


def generate_stage(
    shot: int,
    stage: str,
    experiment: str = DEFAULT_EXPERIMENT,
    seed: int | None = None,
    n_samples: int | None = None,
) -> Iterator[WaveRecord]:
    """Yield one WaveRecord per (channel, data_version) that `stage`
    actually produced (per DIAGNOSTICS and _generate_version_ids), sized
    per STAGE_PROFILE unless n_samples overrides it. Sample count is held
    constant across a wave's data_versions (the raw capture length doesn't
    change on recalibration -- only the processed y values do)."""
    if stage not in DIAGNOSTICS:
        raise ValueError(f"unknown stage {stage!r}; known stages: {sorted(DIAGNOSTICS)}")

    typical_khz, big_p, big_khz = STAGE_PROFILE[stage]
    rng = random.Random(seed if seed is not None else _stable_seed(shot, stage))
    version_ids = _generate_version_ids(shot, stage)

    for wave in DIAGNOSTICS[stage]:
        if n_samples is not None:
            n = n_samples
        else:
            rate_khz = big_khz if (big_p > 0 and rng.random() < big_p) else typical_khz
            n = round(rate_khz * DURATION_MS)
        for data_version in version_ids:
            yield _make_wave(experiment, shot, stage, wave, data_version, n, rng)


def generate_big_wave(
    shot: int,
    stage: str,
    wave: str,
    n_samples: int,
    data_version: str = "0000000",
    experiment: str = DEFAULT_EXPERIMENT,
    seed: int | None = None,
) -> WaveRecord:
    """Explicitly generate one oversized wave for testing the multi-row-group
    / multi-file / narrow-window-query path (e.g. a specific mirnov probe)."""
    rng = random.Random(
        seed if seed is not None else _stable_seed(experiment, shot, stage, wave, data_version)
    )
    return _make_wave(experiment, shot, stage, wave, data_version, n_samples, rng)


def wave_to_sample_table(rec: WaveRecord) -> pa.Table:
    """One row PER SAMPLE for this wave. Deliberately does NOT include
    experiment/shot/stage as physical columns -- those are hive-partition
    keys, encoded only in the directory path (see ingest.py/compact.py),
    not duplicated in the file's own data. This matters specifically for
    `ducklake_add_data_files`: DuckLake (as of writing) has a known bug
    where a partition column that's ALSO present in the file's own data
    causes "invalid partition value for the table configuration" errors
    (github.com/duckdb/ducklake/issues/791) -- keeping these columns
    path-only avoids that entirely, which is what the underlying
    hive-partition support (github.com/duckdb/ducklake/pull/252) was
    actually built for ("columns that are not present in the files
    themselves but only in the file path").

    When queried through the DuckLake table (which still declares all 7
    columns and has them SET PARTITIONED BY), or read locally via
    `read_parquet(glob)` (DuckDB auto-detects hive-style directories and
    reconstructs the partition columns), experiment/shot/stage still
    appear as ordinary columns -- they're just not stored redundantly in
    every file's own data.
    """
    n = len(rec.x)
    return pa.table(
        {
            "wave": pa.array([rec.wave] * n, type=pa.string()),
            "data_version": pa.array([rec.data_version] * n, type=pa.string()),
            "x": pa.array(rec.x, type=pa.float64()),
            "y": pa.array(rec.y, type=pa.float64()),
        }
    )

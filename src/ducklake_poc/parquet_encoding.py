"""Single source of truth for how we write Parquet files -- used by both
the raw per-wave ingestion path and the compaction path, so encoding
choices don't drift between the two.

  - DICTIONARY_COLUMNS: low-cardinality string/categorical columns benefit
    from dictionary encoding (repeated values become small integer codes
    into a shared dictionary page). `wave` and `data_version` are the
    remaining string columns actually stored in the file -- `experiment`
    and `stage` are hive-partition keys encoded only in the directory
    path (see synthetic.py's `wave_to_sample_table` for why they're
    deliberately NOT duplicated as physical columns), so there's nothing
    to dictionary-encode for those here. `data_version` is constant
    within any given compacted file (compact.py makes it a hard
    partition boundary at the FILE level), so its dictionary is a single
    entry; `wave` stays genuinely low-cardinality (at most a few dozen
    distinct channels across a whole shot). This is pyarrow's default for
    string columns already, but we set it explicitly so it's a documented
    decision rather than an accident of the default.
  - BYTE_STREAM_SPLIT_COLUMNS: floating-point sample columns. Ordinary
    byte layout interleaves each double's 8 bytes (sign/exponent/mantissa
    for one sample, then the next), which compresses poorly when the
    mantissa bits are noisy. BYTE_STREAM_SPLIT instead groups all of
    byte-0 together, all of byte-1 together, etc. across the column --
    the sign/exponent bytes (which repeat a lot for smoothly-varying
    physical signals) compress far better once they're not interleaved
    with high-entropy mantissa bytes. Verified empirically on synthetic
    wave-shaped data: roughly 2x smaller than plain ZSTD on its own for
    noisy sine-wave-like signals (see conversation/testing history).
  - Dictionary and BYTE_STREAM_SPLIT are mutually exclusive per column in
    Parquet, so BYTE_STREAM_SPLIT columns must NOT also be in
    DICTIONARY_COLUMNS (and vice versa) -- pyarrow will raise if they are.
  - ZSTD level 2: a deliberately modest compression level. Level 2 is
    close to ZSTD's default and fast; going much higher (e.g. 15-19)
    buys a few more percent of size reduction at meaningfully more CPU
    time per write, which matters more here than in a one-shot batch job
    given ingestion happens continuously during the busiest 15-minute
    experiment windows. Tune based on actual measured ingestion latency
    vs. storage cost trade-offs in production.
"""

from __future__ import annotations

import pyarrow.parquet as pq

DICTIONARY_COLUMNS = ["wave", "data_version"]
BYTE_STREAM_SPLIT_COLUMNS = ["x", "y"]
COMPRESSION = "zstd"
COMPRESSION_LEVEL = 2

assert not (set(DICTIONARY_COLUMNS) & set(BYTE_STREAM_SPLIT_COLUMNS)), (
    "a column can't be both dictionary-encoded and BYTE_STREAM_SPLIT-encoded"
)


def open_parquet_writer(path: str, schema) -> pq.ParquetWriter:
    """Open a ParquetWriter with the shared encoding policy applied to
    whichever of DICTIONARY_COLUMNS / BYTE_STREAM_SPLIT_COLUMNS are
    actually present in this schema (so the same helper works for schemas
    that don't include every column, e.g. a manifest/test file)."""
    schema_names = set(schema.names)
    dict_cols = [c for c in DICTIONARY_COLUMNS if c in schema_names]
    bss_cols = [c for c in BYTE_STREAM_SPLIT_COLUMNS if c in schema_names]

    return pq.ParquetWriter(
        path,
        schema,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
        # pyarrow's type stub declares use_dictionary as bool-only, but at
        # runtime it also accepts a list of column names (verified against
        # the installed pyarrow: only the listed columns get dictionary
        # encoding, others get PLAIN) -- this is a stub gap, not a real
        # type error.
        use_dictionary=dict_cols if dict_cols else False,  # pyright: ignore[reportArgumentType]
        column_encoding={c: "BYTE_STREAM_SPLIT" for c in bss_cols} or None,
    )

"""Small HTTP API in front of the wave_manifest lookup: given a semantic
query (experiment/shot/stage/wave/data_version, optionally an x-range),
look up the matching row group in Postgres and redirect the client into
nginx's `/range-proxy/` location (see nginx/nginx.conf), which serves
exactly that byte range with Content-Type set to parquet -- all without
this Flask process ever touching the actual sample bytes itself.

Request flow:

    client -> GET /wave?...            (this server)
           <- 302 Location: nginx/range-proxy/lake/<file>?r=bytes=A-B
    client -> GET that Location        (nginx)
           <- 206 Partial Content, Content-Type: application/vnd.apache.parquet

Two HTTP round trips from the client's point of view, but nginx -- not
this Flask process -- streams the actual file bytes, which is the whole
point: the manifest lookup and the byte transfer are decoupled, and the
byte transfer scales the same way any static file server does.

Caveat worth being explicit about: a redirect can only point at ONE URL.
When every matched row group lives in the SAME physical file AND their
row_group_ids form one unbroken run (no gaps), that's still fine --
compact.py guarantees a single (wave, data_version) never splits across
files regardless of target_file_size_bytes (see there), and its row
groups are written back-to-back with no gaps in between (verified:
consecutive row groups' byte ranges touch exactly, `byte_end ==
next.byte_start`) -- so `min(byte_start)` to `max(byte_end)` across the
matches is a single valid, contiguous range, and this endpoint redirects
to that combined span in one hop.

If either condition fails -- matches span multiple files, OR (this
should be structurally impossible given compact.py's guarantees, but is
checked anyway rather than assumed) the matched row_group_ids have a gap
-- there's no single byte range that correctly represents the request.
A gap specifically matters: the "extra" bytes between two non-adjacent
matched row groups are a COMPLETE row group belonging to some other wave
or packed group, not padding -- silently including them would mean
serving another wave's actual sample data to a caller who didn't ask for
it, not a merely-inefficient overfetch. So this endpoint refuses to guess
in either case and returns 300 Multiple Choices with a JSON body listing
every match instead; at that point, use fetch_wave.py's approach (fetch
each match individually).

Also worth being explicit about: the returned bytes are a *row group's*
raw bytes, not a standalone valid .parquet file (no footer) -- same
caveat as everywhere else in this repo that does raw byte-range fetches.
See fetch_wave.py's docstring for the fuller version of this caveat and
the "validated read" alternative (RangeHTTPFile + pyarrow) if a
consumer needs a spec-compliant read instead.

Usage:
    python -m ducklake_poc.api_server
    curl -Li "http://localhost:5001/wave?experiment=campaign-2026a&shot=999&stage=mirnov&wave=mirnov/probe_01&data_version=a1b2c3d&x_min=250&x_max=251"
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from flask import Flask, jsonify, redirect, request
from flask.typing import ResponseReturnValue

from . import config, fetch_wave
from .manifest import clean_relative_path

app = Flask(__name__)


@dataclass
class CombinedRange:
    file_path: str
    byte_start: int
    byte_length: int
    row_group_ids: list[int]
    total_rows: int
    x_min: float
    x_max: float


def plan_response(rows: list[tuple]) -> CombinedRange | None:
    """Decide whether `rows` (fetch_wave.lookup()'s result) can be served
    as one combined byte-range redirect. Returns None if not -- either
    because the matches span multiple files, or because their
    row_group_ids have a gap (which would mean the combined range
    includes a complete, unrelated row group's bytes -- not a harmless
    overfetch, since that row group holds another wave's or packed
    group's actual sample data). Pulled out of the Flask view so it can
    be unit tested with synthetic rows, without a database or Flask.
    """
    file_paths = {r[0] for r in rows}
    if len(file_paths) > 1:
        return None

    row_group_ids = sorted(r[2] for r in rows)
    is_contiguous = row_group_ids == list(range(row_group_ids[0], row_group_ids[-1] + 1))
    if not is_contiguous:
        return None

    return CombinedRange(
        file_path=rows[0][0],
        byte_start=min(r[9] for r in rows),
        byte_length=max(r[9] + r[10] for r in rows) - min(r[9] for r in rows),
        row_group_ids=row_group_ids,
        total_rows=sum(r[3] for r in rows),
        x_min=min(r[7] for r in rows),
        x_max=max(r[8] for r in rows),
    )


def _row_group_url(file_path: str, byte_start: int, byte_length: int) -> str:
    """Translate a manifest row's LAKE_DATA_DIR-relative file path + byte
    range into the nginx range-proxy URL that will serve exactly those
    bytes -- using a clean (no `key=`) path, since the on-disk
    hive-partition directory naming is an implementation detail nginx's
    rewrite rules translate back to (see nginx/nginx.conf). Deliberately
    relative (not resolved against a local LAKE_DATA_DIR) so this works
    regardless of which machine/container produced the manifest row --
    see manifest.relative_to_lake_dir."""
    clean_rel = clean_relative_path(Path(file_path))
    byte_end = byte_start + byte_length - 1
    range_arg = quote(f"bytes={byte_start}-{byte_end}", safe="=")
    return f"{config.NGINX_HOST_URL}/range-proxy/lake/{clean_rel}?r={range_arg}"


def _matches_payload(rows: list[tuple]) -> list[dict]:
    return [
        {
            "file_url": r[1],
            "row_group_id": r[2],
            "row_group_rows": r[3],
            "x_min": r[7],
            "x_max": r[8],
            "byte_length": r[10],
        }
        for r in rows
    ]


@app.route("/wave")
def wave() -> ResponseReturnValue:
    experiment = request.args.get("experiment")
    shot = request.args.get("shot", type=int)
    stage = request.args.get("stage")
    wave_name = request.args.get("wave")
    data_version = request.args.get("data_version")
    x_min = request.args.get("x_min", type=float)
    x_max = request.args.get("x_max", type=float)

    missing = [
        name
        for name, val in [
            ("experiment", experiment),
            ("shot", shot),
            ("stage", stage),
            ("wave", wave_name),
            ("data_version", data_version),
        ]
        if val is None
    ]
    if missing:
        return jsonify(error=f"missing required query params: {', '.join(missing)}"), 400
    if (x_min is None) != (x_max is None):
        return jsonify(error="x_min and x_max must be given together"), 400

    assert experiment is not None
    assert shot is not None
    assert stage is not None
    assert wave_name is not None
    assert data_version is not None

    try:
        rows = fetch_wave.lookup(experiment, shot, stage, wave_name, data_version, x_min, x_max)
    except SystemExit as e:
        return jsonify(error=str(e)), 404

    plan = plan_response(rows)
    if plan is None:
        return jsonify(
            error=(
                "query matched row groups that can't be served as one byte range "
                "(they span multiple files, or aren't contiguous within one file). "
                "Narrow x_min/x_max, or fetch each match individually (see "
                "fetch_wave.py)."
            ),
            matches=_matches_payload(rows),
        ), 300

    target = _row_group_url(plan.file_path, plan.byte_start, plan.byte_length)
    resp = redirect(target, code=302)
    resp.headers["X-Row-Group-Ids"] = ",".join(map(str, plan.row_group_ids))
    resp.headers["X-Row-Group-Rows"] = str(plan.total_rows)
    resp.headers["X-Row-Group-X-Range"] = f"{plan.x_min},{plan.x_max}"
    return resp


def main() -> None:
    app.run(host=config.API_HOST, port=config.API_PORT)


if __name__ == "__main__":
    main()

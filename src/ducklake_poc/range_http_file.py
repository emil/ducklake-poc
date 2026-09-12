"""A minimal seekable, read-only file-like object backed by HTTP Range
requests. This is what lets pyarrow open a remote parquet file and read
just one row group -- it issues exactly two small HTTP GETs (one for the
footer, one for the target row group's bytes) instead of downloading the
whole file. No extra dependency (fsspec/aiohttp) needed -- just `requests`.
"""

from __future__ import annotations

import requests


class RangeHTTPFile:
    def __init__(self, url: str, session: requests.Session | None = None):
        self.url = url
        self.session = session or requests.Session()
        self._pos = 0
        self._size: int | None = None
        self.closed = False
        self.request_log: list[tuple[int, int]] = []  # (start, end) of each GET issued

    @property
    def size(self) -> int:
        if self._size is None:
            resp = self.session.head(self.url)
            resp.raise_for_status()
            self._size = int(resp.headers["Content-Length"])
        return self._size

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self.size + offset
        else:
            raise ValueError(f"unsupported whence={whence}")
        return self._pos

    def read(self, n: int = -1) -> bytes:
        start = self._pos
        end = self.size - 1 if n is None or n < 0 else min(self._pos + n - 1, self.size - 1)
        if start > end:
            return b""

        headers = {"Range": f"bytes={start}-{end}"}
        resp = self.session.get(self.url, headers=headers)
        resp.raise_for_status()
        self.request_log.append((start, end))

        data = resp.content
        self._pos += len(data)
        return data

    def close(self) -> None:
        self.closed = True

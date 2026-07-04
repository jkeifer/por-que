from __future__ import annotations

from io import SEEK_CUR, SEEK_END, SEEK_SET


class BufferedRangeReader:
    """A synchronous ``ReadableSeekable`` over an in-memory byte span.

    Parquet tells us the byte span of a structure (the footer metadata, a
    column index, an offset index) before we parse it. That lets us fetch the
    whole span in a single ``read`` and then parse it from memory, instead of
    issuing many tiny reads through a (possibly remote, cached) file. Parsing
    from memory is dramatically faster, but the parsers record absolute file
    offsets as teaching fields, so this reader preserves them: it holds the
    bytes for the span beginning at absolute file offset ``offset`` and
    reports every position in absolute file coordinates.

    Args:
        data: The bytes of the span.
        offset: Absolute file offset at which ``data`` begins.
        filesize: Total size of the underlying file, so that ``SEEK_END``
            resolves exactly as it would against the real file.
    """

    def __init__(self, data: bytes, offset: int, filesize: int) -> None:
        self._data = data
        self._offset = offset
        self._filesize = filesize
        # Position is tracked in absolute file coordinates.
        self._pos = offset

    def read(self, size: int | None = None, /) -> bytes:
        start = self._pos - self._offset
        end = len(self._data) if size is None else start + size

        if start < 0 or end > len(self._data):
            raise ValueError(
                f'Read of {size} bytes at file offset {self._pos} falls '
                f'outside the buffered range '
                f'[{self._offset}, {self._offset + len(self._data)})',
            )

        chunk = self._data[start:end]
        self._pos += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = SEEK_SET, /) -> int:
        if whence == SEEK_SET:
            self._pos = offset
        elif whence == SEEK_CUR:
            self._pos += offset
        elif whence == SEEK_END:
            self._pos = self._filesize + offset
        else:
            raise ValueError(f'Invalid whence: {whence}')
        return self._pos

    def tell(self) -> int:
        return self._pos

    def close(self) -> None:
        pass

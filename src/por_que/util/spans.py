"""Speculative byte-span fetching for structures of unknown size.

Thrift structures don't encode their own length, so when Parquet doesn't tell
us a structure's size up front (page headers, page indexes without a recorded
``*_length``) we can't fetch an exact span before parsing. Instead we guess:
fetch a small span, parse it synchronously from memory, and if the parser runs
off the end of the buffer (``BufferExhaustedError``) fetch a bigger span and try
again. This keeps all I/O in one place while the parsers stay synchronous.
"""

from __future__ import annotations

from collections.abc import Callable

from por_que.exceptions import BufferExhaustedError
from por_que.protocols import AsyncReadableSeekable

DEFAULT_INITIAL_SPAN = 64 * 1024
DEFAULT_MAX_SPAN = 16 * 1024 * 1024


async def read_thrift_span[T](
    reader: AsyncReadableSeekable,
    offset: int,
    parse_fn: Callable[[memoryview, int], T],
    initial_size: int = DEFAULT_INITIAL_SPAN,
    max_size: int = DEFAULT_MAX_SPAN,
) -> T:
    """Fetch a byte span of unknown length and parse it, growing on demand.

    Args:
        reader: Async reader to fetch bytes from.
        offset: Absolute file offset where the structure begins.
        parse_fn: Synchronous function of ``(data, offset)`` that parses the
            structure from the in-memory span.
        initial_size: First span size to try.
        max_size: Give up (re-raise ``BufferExhaustedError``) beyond this size.

    Returns:
        Whatever ``parse_fn`` returns.
    """
    size = initial_size
    while True:
        reader.seek(offset)
        data = await reader.read(size)
        try:
            return parse_fn(memoryview(data), offset)
        except BufferExhaustedError:
            if len(data) < size or size >= max_size:
                # A short read means the file itself ended, and hitting
                # max_size means growing further is unreasonable: either way
                # the structure really is truncated or malformed.
                raise
            size = min(size * 2, max_size)

"""Speculative byte-span fetching for structures of unknown size.

Thrift compact structures don't encode their own length — the only way to
learn a struct's total size is to parse it all the way to its STOP marker. So
when Parquet doesn't record a structure's size up front (page headers, page
indexes without a recorded ``*_length``) some speculation is irreducible: we
fetch an initial span, parse it synchronously from memory, and if the parser
runs off the end of the buffer (``BufferExhaustedError``) we fetch *more*
bytes and try again.

The retry never re-reads bytes it already has. The exception carries the
exact shortfall for the primitive that tripped it, so a big length-prefixed
value (a large binary statistic, say) is fetched in one jump instead of
doubling toward it; a geometric floor on top of that hint supplies
speculative headroom for whatever fields follow. Growth is bounded only by
the end of the file — the worst case for a structure that overflows the
initial span is a bounded amount of re-parsing (roughly 2x), and the only
hard failure is genuine truncation.

This keeps all I/O in one place while the parsers stay synchronous.
"""

from __future__ import annotations

import logging

from collections.abc import Callable

from por_que.exceptions import BufferExhaustedError
from por_que.protocols import AsyncReadableSeekable

logger = logging.getLogger(__name__)

DEFAULT_INITIAL_SPAN = 64 * 1024


async def read_thrift_span[T](
    reader: AsyncReadableSeekable,
    offset: int,
    parse_fn: Callable[[memoryview, int], T],
    initial_size: int = DEFAULT_INITIAL_SPAN,
) -> T:
    """Fetch a byte span of unknown length and parse it, growing on demand.

    Fetches ``initial_size`` bytes at ``offset`` and hands them to
    ``parse_fn``. If the parse runs off the end of the span
    (``BufferExhaustedError``), only the missing tail is fetched — never the
    bytes already in hand — and the parse retries from scratch. Each fetch
    grows the buffer by at least the exception's exact ``needed`` shortfall
    (which jumps straight past large length-prefixed values) and at least the
    current buffer size (geometric headroom for the fields that follow, since
    the shortfall hint only covers the one primitive that tripped).

    Growth stops only at end of file: if a fetch comes back short, the file
    has no more bytes to give, and a parse that still exhausts the buffer
    means the structure really is truncated — the ``BufferExhaustedError``
    propagates.

    Args:
        reader: Async reader to fetch bytes from.
        offset: Absolute file offset where the structure begins.
        parse_fn: Synchronous function of ``(data, offset)`` that parses the
            structure from the in-memory span.
        initial_size: First span size to try.

    Returns:
        Whatever ``parse_fn`` returns.

    Raises:
        BufferExhaustedError: The structure runs past the end of the file.
    """
    reader.seek(offset)
    data = await reader.read(initial_size)
    at_eof = len(data) < initial_size

    while True:
        try:
            return parse_fn(memoryview(data), offset)
        except BufferExhaustedError as exc:
            if at_eof:
                # The file has no more bytes to give; the structure really
                # is truncated.
                raise
            grow = max(exc.needed, len(data))
            logger.debug(
                'Span at offset %d exhausted: have %d bytes, parser needs '
                '%d more; fetching %d more bytes.',
                offset,
                len(data),
                exc.needed,
                grow,
            )
            reader.seek(offset + len(data))
            chunk = await reader.read(grow)
            at_eof = len(chunk) < grow
            # Accumulate into a fresh bytes object rather than resizing a
            # bytearray in place: the caught exception's traceback can keep
            # frames alive that still hold memoryview exports over the old
            # buffer, and resizing a bytearray with live exports raises
            # BufferError.
            data = data + chunk

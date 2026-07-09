import struct

from collections.abc import Iterator
from contextlib import contextmanager


class PorQueError(Exception):
    """Base exception for all por-que errors."""


class ParquetFormatError(PorQueError, ValueError):
    """Invalid Parquet file format."""


class ParquetMagicError(ParquetFormatError):
    """Invalid Parquet magic bytes."""


class ParquetCorruptedError(ParquetFormatError):
    """Parquet file appears corrupted or truncated."""


class InvalidStringLengthError(ParquetFormatError):
    """Invalid string length in Thrift data."""


class ThriftParsingError(ParquetFormatError):
    """Error parsing Thrift data structures."""


class ParquetDataError(ParquetFormatError):
    """Error deserializing Parquet data values."""


class CodecUnavailableError(ParquetDataError):
    """A compression codec's optional dependency is not installed.

    Raised instead of a generic :class:`ParquetDataError` so consumers can
    distinguish "install the package (or accept the limitation, e.g. under
    pyodide)" from genuinely corrupt data, without parsing the message.

    Attributes:
        codec: Name of the affected codec (e.g. ``'BROTLI'``, ``'LZO'``,
            ``'ZSTD'``).
    """

    def __init__(self, message: str, codec: str) -> None:
        super().__init__(message)
        self.codec = codec


class BufferExhaustedError(PorQueError):
    """A synchronous parse ran past the end of its in-memory buffer.

    The thrift parser works over a fixed byte span. When the span is a
    speculative fetch (we did not know the structure's size up front), a read
    past its end means "fetch more bytes and try again" rather than a genuine
    format error. Callers catch this to grow the span; see
    ``por_que.util.spans.read_thrift_span``.

    Attributes:
        needed: Exact number of bytes missing beyond the end of the buffer
            for the read that failed. This is a lower bound on how much more
            of the file the *whole* structure needs — it only accounts for
            the one primitive that tripped, not anything after it.
    """

    def __init__(self, message: str, needed: int = 0) -> None:
        super().__init__(message)
        self.needed = needed


class ParquetNetworkError(PorQueError, IOError):
    """Network-related error while fetching Parquet data."""


class ParquetUrlError(PorQueError, ValueError):
    """Invalid URL for Parquet file."""


# Low-level exceptions the thrift/struct machinery raises deep inside a parse
# when handed corrupt or truncated bytes. ``struct.error`` is its own class;
# ``UnicodeDecodeError`` is a ``ValueError``, as are the ``ValueError``s raised
# by enum lookups (e.g. ``PageType(999)``) and pydantic validation, so catching
# ``ValueError`` covers all of those in one go.
_LOW_LEVEL_PARSE_ERRORS = (IndexError, KeyError, ValueError, struct.error)


@contextmanager
def parse_context(what: str) -> Iterator[None]:
    """Turn a low-level parse failure into a helpful ``ParquetFormatError``.

    Students feed this library truncated downloads, non-parquet files, and
    corrupted bytes. Wrapping a public entry point's parse in this context
    manager guarantees the failure surfaces as a ``ParquetFormatError`` naming
    what was being parsed, instead of a raw ``IndexError``/``struct.error``/
    ``KeyError``/``UnicodeDecodeError``/``BufferExhaustedError`` traceback from
    deep in a parser.

    Args:
        what: Human-readable description of what is being parsed, ideally with
            an offset or size (e.g. ``"file metadata at offset 197"``).
    """
    try:
        yield
    except ParquetFormatError:
        # Already the helpful, typed error we want; leave it untouched.
        raise
    except BufferExhaustedError as err:
        raise ParquetCorruptedError(
            f'File appears truncated while reading {what}: {err}',
        ) from err
    except _LOW_LEVEL_PARSE_ERRORS as err:
        raise ParquetCorruptedError(
            f'Failed to parse {what}: {type(err).__name__}: {err}',
        ) from err

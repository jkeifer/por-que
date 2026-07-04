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


class BufferExhaustedError(PorQueError):
    """A synchronous parse ran past the end of its in-memory buffer.

    The thrift parser works over a fixed byte span. When the span is a
    speculative fetch (we did not know the structure's size up front), a read
    past its end means "fetch a bigger span and try again" rather than a
    genuine format error. Callers catch this to grow the span; see
    ``por_que.util.spans.read_thrift_span``.
    """


class ParquetNetworkError(PorQueError, IOError):
    """Network-related error while fetching Parquet data."""


class ParquetUrlError(PorQueError, ValueError):
    """Invalid URL for Parquet file."""

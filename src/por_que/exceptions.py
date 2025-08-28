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


class ParquetNetworkError(PorQueError, IOError):
    """Network-related error while fetching Parquet data."""


class ParquetUrlError(PorQueError, ValueError):
    """Invalid URL for Parquet file."""

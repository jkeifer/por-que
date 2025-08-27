from enum import IntEnum


class Type(IntEnum):
    """Parquet data types."""

    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7


class Compression(IntEnum):
    """Parquet compression algorithms."""

    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    BROTLI = 4
    LZ4 = 5
    ZSTD = 6


class Repetition(IntEnum):
    """Parquet schema repetition types."""

    REQUIRED = 0
    OPTIONAL = 1
    REPEATED = 2


class Encoding(IntEnum):
    """Parquet encoding types."""

    PLAIN = 0
    PLAIN_DICTIONARY = 2
    RLE = 3
    BIT_PACKED = 4
    DELTA_BINARY_PACKED = 5
    DELTA_LENGTH_BYTE_ARRAY = 6
    DELTA_BYTE_ARRAY = 7
    RLE_DICTIONARY = 8
    BYTE_STREAM_SPLIT = 9

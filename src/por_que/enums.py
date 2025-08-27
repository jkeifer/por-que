from enum import IntEnum


class Type(IntEnum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7


class Compression(IntEnum):
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    BROTLI = 4
    LZ4 = 5
    ZSTD = 6


class Repetition(IntEnum):
    REQUIRED = 0
    OPTIONAL = 1
    REPEATED = 2


class Encoding(IntEnum):
    PLAIN = 0
    PLAIN_DICTIONARY = 2
    RLE = 3
    BIT_PACKED = 4
    DELTA_BINARY_PACKED = 5
    DELTA_LENGTH_BYTE_ARRAY = 6
    DELTA_BYTE_ARRAY = 7
    RLE_DICTIONARY = 8
    BYTE_STREAM_SPLIT = 9


# Thrift Compact Protocol field types
class ThriftFieldType(IntEnum):
    STOP = 0
    BOOL_TRUE = 1
    BOOL_FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    SET = 10
    MAP = 11
    STRUCT = 12


# Field IDs for Parquet Thrift structures
class SchemaElementFieldId(IntEnum):
    TYPE = 1
    TYPE_LENGTH = 2
    REPETITION_TYPE = 3
    NAME = 4
    NUM_CHILDREN = 5
    CONVERTED_TYPE = 6


class ColumnMetadataFieldId(IntEnum):
    TYPE = 1
    ENCODINGS = 2
    PATH_IN_SCHEMA = 3
    CODEC = 4
    NUM_VALUES = 5
    TOTAL_UNCOMPRESSED_SIZE = 6
    TOTAL_COMPRESSED_SIZE = 7
    KEY_VALUE_METADATA = 8
    DATA_PAGE_OFFSET = 9
    INDEX_PAGE_OFFSET = 10
    DICTIONARY_PAGE_OFFSET = 11


class ColumnChunkFieldId(IntEnum):
    FILE_PATH = 1
    FILE_OFFSET = 2
    META_DATA = 3


class RowGroupFieldId(IntEnum):
    COLUMNS = 1
    TOTAL_BYTE_SIZE = 2
    NUM_ROWS = 3


class FileMetadataFieldId(IntEnum):
    VERSION = 1
    SCHEMA = 2
    NUM_ROWS = 3
    ROW_GROUPS = 4
    KEY_VALUE_METADATA = 5
    CREATED_BY = 6


class KeyValueFieldId(IntEnum):
    KEY = 1
    VALUE = 2

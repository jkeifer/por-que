from enum import IntEnum


# Field IDs for Parquet Thrift structures
class SchemaElementFieldId(IntEnum):
    """Field IDs for SchemaElement struct in Parquet metadata."""

    TYPE = 1
    TYPE_LENGTH = 2
    REPETITION_TYPE = 3
    NAME = 4
    NUM_CHILDREN = 5
    CONVERTED_TYPE = 6


class ColumnMetadataFieldId(IntEnum):
    """Field IDs for ColumnMetaData struct in Parquet metadata."""

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
    STATISTICS = 12


class ColumnChunkFieldId(IntEnum):
    """Field IDs for ColumnChunk struct in Parquet metadata."""

    FILE_PATH = 1
    FILE_OFFSET = 2
    META_DATA = 3


class RowGroupFieldId(IntEnum):
    """Field IDs for RowGroup struct in Parquet metadata."""

    COLUMNS = 1
    TOTAL_BYTE_SIZE = 2
    NUM_ROWS = 3


class FileMetadataFieldId(IntEnum):
    """Field IDs for FileMetaData struct in Parquet metadata."""

    VERSION = 1
    SCHEMA = 2
    NUM_ROWS = 3
    ROW_GROUPS = 4
    KEY_VALUE_METADATA = 5
    CREATED_BY = 6


class KeyValueFieldId(IntEnum):
    """Field IDs for KeyValue struct in Parquet metadata."""

    KEY = 1
    VALUE = 2


class StatisticsFieldId(IntEnum):
    """Field IDs for Statistics struct in Parquet metadata."""

    MIN_VALUE = 1
    MAX_VALUE = 2
    NULL_COUNT = 3
    DISTINCT_COUNT = 4
    MAX_VALUE_DELTA = 5
    MIN_VALUE_DELTA = 6

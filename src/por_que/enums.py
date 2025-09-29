from enum import IntEnum, StrEnum, auto


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


class ConvertedType(IntEnum):
    """Legacy Parquet logical types (converted_type field)."""

    UTF8 = 0
    MAP = 1
    MAP_KEY_VALUE = 2
    LIST = 3
    ENUM = 4
    DECIMAL = 5
    DATE = 6
    TIME_MILLIS = 7
    TIME_MICROS = 8
    TIMESTAMP_MILLIS = 9
    TIMESTAMP_MICROS = 10
    UINT_8 = 11
    UINT_16 = 12
    UINT_32 = 13
    UINT_64 = 14
    INT_8 = 15
    INT_16 = 16
    INT_32 = 17
    INT_64 = 18
    JSON = 19
    BSON = 20
    INTERVAL = 21


class GroupConvertedType(IntEnum):
    MAP = 1
    MAP_KEY_VALUE = 2
    LIST = 3


class ColumnConvertedType(IntEnum):
    UTF8 = 0
    ENUM = 4
    DECIMAL = 5
    DATE = 6
    TIME_MILLIS = 7
    TIME_MICROS = 8
    TIMESTAMP_MILLIS = 9
    TIMESTAMP_MICROS = 10
    UINT_8 = 11
    UINT_16 = 12
    UINT_32 = 13
    UINT_64 = 14
    INT_8 = 15
    INT_16 = 16
    INT_32 = 17
    INT_64 = 18
    JSON = 19
    BSON = 20
    INTERVAL = 21


class PageType(IntEnum):
    """Parquet page types."""

    DATA_PAGE = 0
    INDEX_PAGE = 1
    DICTIONARY_PAGE = 2
    DATA_PAGE_V2 = 3


class LogicalType(IntEnum):
    """Parquet logical types (logicalType field)."""

    STRING = 1
    MAP = 2
    LIST = 3
    ENUM = 4
    DECIMAL = 5
    DATE = 6
    TIME = 7
    TIMESTAMP = 8
    INTEGER = 10
    UNKNOWN = 11
    JSON = 12
    BSON = 13
    UUID = 14
    FLOAT16 = 15
    VARIANT = 16
    GEOMETRY = 17
    GEOGRAPHY = 18


class GroupLogicalType(IntEnum):
    MAP = 2
    LIST = 3


class ColumnLogicalType(IntEnum):
    STRING = 1
    ENUM = 4
    DECIMAL = 5
    DATE = 6
    TIME = 7
    TIMESTAMP = 8
    INTEGER = 10
    UNKNOWN = 11
    JSON = 12
    BSON = 13
    UUID = 14
    FLOAT16 = 15
    VARIANT = 16
    GEOMETRY = 17
    GEOGRAPHY = 18


class TimeUnit(IntEnum):
    """Time units for TIME and TIMESTAMP logical types."""

    MILLIS = 1
    MICROS = 2
    NANOS = 3


class BoundaryOrder(IntEnum):
    """Ordering of min/max values in Page Index."""

    UNORDERED = 0
    ASCENDING = 1
    DESCENDING = 2


class SchemaElementType(StrEnum):
    """Internal type to classify schema elements."""

    ROOT = auto()
    GROUP = auto()
    COLUMN = auto()


class GeospatialType(IntEnum):
    """WKB (Well-Known Binary) type integer IDs for geospatial data."""

    # Standard 2D types
    GEOMETRY = 0
    POINT = 1
    LINESTRING = 2
    POLYGON = 3
    MULTIPOINT = 4
    MULTILINESTRING = 5
    MULTIPOLYGON = 6
    GEOMETRYCOLLECTION = 7
    CIRCULARSTRING = 8
    COMPOUNDCURVE = 9
    CURVEPOLYGON = 10
    MULTICURVE = 11
    MULTISURFACE = 12
    CURVE = 13
    SURFACE = 14
    POLYHEDRALSURFACE = 15
    TIN = 16

    # Z-coordinate types (3D)
    GEOMETRY_Z = 1000
    POINT_Z = 1001
    LINESTRING_Z = 1002
    POLYGON_Z = 1003
    MULTIPOINT_Z = 1004
    MULTILINESTRING_Z = 1005
    MULTIPOLYGON_Z = 1006
    GEOMETRYCOLLECTION_Z = 1007
    CIRCULARSTRING_Z = 1008
    COMPOUNDCURVE_Z = 1009
    CURVEPOLYGON_Z = 1010
    MULTICURVE_Z = 1011
    MULTISURFACE_Z = 1012
    CURVE_Z = 1013
    SURFACE_Z = 1014
    POLYHEDRALSURFACE_Z = 1015
    TIN_Z = 1016

    # M-coordinate types (measured)
    GEOMETRY_M = 2000
    POINT_M = 2001
    LINESTRING_M = 2002
    POLYGON_M = 2003
    MULTIPOINT_M = 2004
    MULTILINESTRING_M = 2005
    MULTIPOLYGON_M = 2006
    GEOMETRYCOLLECTION_M = 2007
    CIRCULARSTRING_M = 2008
    COMPOUNDCURVE_M = 2009
    CURVEPOLYGON_M = 2010
    MULTICURVE_M = 2011
    MULTISURFACE_M = 2012
    CURVE_M = 2013
    SURFACE_M = 2014
    POLYHEDRALSURFACE_M = 2015
    TIN_M = 2016

    # ZM-coordinate types (3D + measured)
    GEOMETRY_ZM = 3000
    POINT_ZM = 3001
    LINESTRING_ZM = 3002
    POLYGON_ZM = 3003
    MULTIPOINT_ZM = 3004
    MULTILINESTRING_ZM = 3005
    MULTIPOLYGON_ZM = 3006
    GEOMETRYCOLLECTION_ZM = 3007
    CIRCULARSTRING_ZM = 3008
    COMPOUNDCURVE_ZM = 3009
    CURVEPOLYGON_ZM = 3010
    MULTICURVE_ZM = 3011
    MULTISURFACE_ZM = 3012
    CURVE_ZM = 3013
    SURFACE_ZM = 3014
    POLYHEDRALSURFACE_ZM = 3015
    TIN_ZM = 3016

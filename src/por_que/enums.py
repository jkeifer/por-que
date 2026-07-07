# The Parquet enums/unions below mirror the upstream Thrift IDL:
#   apache/parquet-format : src/main/thrift/parquet.thrift
# Pinned to commit c4b3ef2f79e1c6ae7418f5cb7b17dcc067b1f7ab (2026-06-08).
# To review upstream changes, diff that path from this commit to master:
#   https://github.com/apache/parquet-format/commits/master/src/main/thrift/parquet.thrift
# Note: not everything here comes from the IDL -- the internal/derived enums
# and the pydantic (de)serialization helpers below have no Thrift counterpart.
from dataclasses import dataclass
from enum import IntEnum, StrEnum, auto
from typing import Annotated

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema


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
    LZ4_RAW = 7  # Added in format 2.9


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


class EdgeInterpolationAlgorithm(IntEnum):
    """Edge interpolation algorithm for the GEOGRAPHY logical type.

    Added in format 2.11 alongside the geospatial (GEOMETRY/GEOGRAPHY) types.
    """

    SPHERICAL = 0
    VINCENTY = 1
    THOMAS = 2
    ANDOYER = 3
    KARNEY = 4


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


class ProgressPhase(StrEnum):
    """Phases reported by progress callbacks."""

    METADATA_READ = 'metadata-read'
    METADATA_PARSE = 'metadata-parse'
    COLUMN_CHUNKS = 'column-chunks'


class ListSemantics(StrEnum):
    """Semantic interpretation for repeated field handling."""

    MODERN_LIST = 'modern_list'  # 3-level LIST logical type semantics
    LEGACY_REPEATED = 'legacy_repeated'  # Legacy repeated group semantics


# ---------------------------------------------------------------------------
# Serialize IntEnums by NAME so JSON dumps are self-describing.
#
# Parquet's thrift wire format identifies these enums by integer code, so the
# models still parse and store real IntEnum members. Only the JSON boundary
# changes: dumps emit the member name (``"SNAPPY"`` not ``1``) and the
# serialization JSON Schema reports a string enum. Validation stays lenient --
# an enum member, its name, or its raw int all load -- so old numeric dumps
# keep working. Python-mode ``model_dump()`` still yields enum members.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnumByName:
    """Annotation making an IntEnum field (de)serialize by member name."""

    enum: type[IntEnum]

    def __get_pydantic_core_schema__(
        self,
        source: type,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        enum_cls = self.enum
        members = {member.name: member for member in enum_cls}

        def validate(value: object) -> IntEnum:
            if isinstance(value, enum_cls):
                return value
            if isinstance(value, str):
                try:
                    return members[value]
                except KeyError:
                    raise ValueError(
                        f'{value!r} is not a valid {enum_cls.__name__} name',
                    ) from None
            if isinstance(value, int):
                return enum_cls(value)
            raise ValueError(
                f'cannot coerce {value!r} to {enum_cls.__name__}',
            )

        return core_schema.no_info_before_validator_function(
            validate,
            handler(source),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda member: member.name,
                return_schema=core_schema.str_schema(),
                when_used='json',
            ),
        )

    def __get_pydantic_json_schema__(
        self,
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        return {'type': 'string', 'enum': [member.name for member in self.enum]}


@dataclass(frozen=True)
class LiteralEnumByName:
    """Like :class:`EnumByName`, but for a single-member ``Literal`` field.

    Used on the discriminator fields of the logical-type and page unions, whose
    per-variant type is ``Literal[SomeEnum.MEMBER]``.
    """

    member: IntEnum

    def __get_pydantic_core_schema__(
        self,
        source: type,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        enum_cls = type(self.member)

        def validate(value: object) -> object:
            if isinstance(value, str):
                try:
                    return enum_cls[value]
                except KeyError:
                    raise ValueError(
                        f'{value!r} is not a valid {enum_cls.__name__} name',
                    ) from None
            return value

        return core_schema.no_info_before_validator_function(
            validate,
            handler(source),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda member: member.name,
                return_schema=core_schema.str_schema(),
                when_used='json',
            ),
        )

    def __get_pydantic_json_schema__(
        self,
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        return {'type': 'string', 'const': self.member.name}


TypeName = Annotated[Type, EnumByName(Type)]
CompressionName = Annotated[Compression, EnumByName(Compression)]
RepetitionName = Annotated[Repetition, EnumByName(Repetition)]
EncodingName = Annotated[Encoding, EnumByName(Encoding)]
ConvertedTypeName = Annotated[ConvertedType, EnumByName(ConvertedType)]
PageTypeName = Annotated[PageType, EnumByName(PageType)]
TimeUnitName = Annotated[TimeUnit, EnumByName(TimeUnit)]
BoundaryOrderName = Annotated[BoundaryOrder, EnumByName(BoundaryOrder)]
EdgeInterpolationAlgorithmName = Annotated[
    EdgeInterpolationAlgorithm,
    EnumByName(EdgeInterpolationAlgorithm),
]
GeospatialTypeName = Annotated[GeospatialType, EnumByName(GeospatialType)]

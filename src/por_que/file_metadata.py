from __future__ import annotations

import struct
import warnings

from collections.abc import Callable, Sequence
from functools import cached_property
from io import SEEK_END
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    Discriminator,
    Field,
    PrivateAttr,
    computed_field,
    model_validator,
)

from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import (
    BoundaryOrder,
    ColumnConvertedType,
    ColumnLogicalType,
    Compression,
    ConvertedType,
    Encoding,
    GeospatialType,
    GroupConvertedType,
    GroupLogicalType,
    ListSemantics,
    LogicalType,
    PageType,
    Repetition,
    SchemaElementType,
    TimeUnit,
    Type,
)
from .exceptions import ParquetFormatError
from .protocols import AsyncReadableSeekable, ReadableSeekable
from .util.async_adapter import ensure_async_reader
from .util.spans import read_thrift_span


class CompressionStats(BaseModel, frozen=True):
    """Compression statistics for data."""

    total_compressed: int
    total_uncompressed: int

    @computed_field
    @cached_property
    def ratio(self) -> float:
        """Compression ratio (compressed/uncompressed)."""
        return (
            self.total_compressed / self.total_uncompressed
            if self.total_uncompressed > 0
            else 0.0
        )

    @computed_field
    @cached_property
    def space_saved_percent(self) -> float:
        """Percentage of space saved by compression."""
        return (1 - self.ratio) * 100 if self.total_uncompressed > 0 else 0.0

    @computed_field
    @cached_property
    def compressed_mb(self) -> float:
        """Compressed size in MB."""
        return self.total_compressed / (1024 * 1024)

    @computed_field
    @cached_property
    def uncompressed_mb(self) -> float:
        """Uncompressed size in MB."""
        return self.total_uncompressed / (1024 * 1024)


class KeyValueMetadata(BaseModel, frozen=True):
    """Key-value metadata pair with byte range information."""

    start_offset: int
    byte_length: int
    key: str
    value: str


class LogicalTypeInfo(BaseModel, frozen=True):
    """Base class for logical type information."""

    logical_type: LogicalType


class StringTypeInfo(LogicalTypeInfo, frozen=True):
    """String logical type."""

    logical_type: Literal[LogicalType.STRING] = LogicalType.STRING


class IntTypeInfo(LogicalTypeInfo, frozen=True):
    """Integer logical type with bit width and signedness."""

    logical_type: Literal[LogicalType.INTEGER] = LogicalType.INTEGER
    bit_width: int = 32
    is_signed: bool = True


class DecimalTypeInfo(LogicalTypeInfo, frozen=True):
    """Decimal logical type with scale and precision."""

    logical_type: Literal[LogicalType.DECIMAL] = LogicalType.DECIMAL
    scale: int = 0
    precision: int = 10


class TimeTypeInfo(LogicalTypeInfo, frozen=True):
    """Time logical type with unit and UTC adjustment."""

    logical_type: Literal[LogicalType.TIME] = LogicalType.TIME
    is_adjusted_to_utc: bool = True
    unit: TimeUnit = TimeUnit.MILLIS


class TimestampTypeInfo(LogicalTypeInfo, frozen=True):
    """Timestamp logical type with unit and UTC adjustment."""

    logical_type: Literal[LogicalType.TIMESTAMP] = LogicalType.TIMESTAMP
    is_adjusted_to_utc: bool = True
    unit: TimeUnit = TimeUnit.MILLIS


class DateTypeInfo(LogicalTypeInfo, frozen=True):
    """Date logical type."""

    logical_type: Literal[LogicalType.DATE] = LogicalType.DATE


class EnumTypeInfo(LogicalTypeInfo, frozen=True):
    """Enum logical type."""

    logical_type: Literal[LogicalType.ENUM] = LogicalType.ENUM


class JsonTypeInfo(LogicalTypeInfo, frozen=True):
    """JSON logical type."""

    logical_type: Literal[LogicalType.JSON] = LogicalType.JSON


class BsonTypeInfo(LogicalTypeInfo, frozen=True):
    """BSON logical type."""

    logical_type: Literal[LogicalType.BSON] = LogicalType.BSON


class UuidTypeInfo(LogicalTypeInfo, frozen=True):
    """UUID logical type."""

    logical_type: Literal[LogicalType.UUID] = LogicalType.UUID


class Float16TypeInfo(LogicalTypeInfo, frozen=True):
    """Float16 logical type."""

    logical_type: Literal[LogicalType.FLOAT16] = LogicalType.FLOAT16


class MapTypeInfo(LogicalTypeInfo, frozen=True):
    """Map logical type."""

    logical_type: Literal[LogicalType.MAP] = LogicalType.MAP


class ListTypeInfo(LogicalTypeInfo, frozen=True):
    """List logical type."""

    logical_type: Literal[LogicalType.LIST] = LogicalType.LIST


class VariantTypeInfo(LogicalTypeInfo, frozen=True):
    """Variant logical type."""

    logical_type: Literal[LogicalType.VARIANT] = LogicalType.VARIANT


class GeometryTypeInfo(LogicalTypeInfo, frozen=True):
    """Geometry logical type."""

    logical_type: Literal[LogicalType.GEOMETRY] = LogicalType.GEOMETRY


class GeographyTypeInfo(LogicalTypeInfo, frozen=True):
    """Geography logical type."""

    logical_type: Literal[LogicalType.GEOGRAPHY] = LogicalType.GEOGRAPHY


class UnknownTypeInfo(LogicalTypeInfo, frozen=True):
    """Unknown logical type."""

    logical_type: Literal[LogicalType.UNKNOWN] = LogicalType.UNKNOWN


LogicalTypeInfoUnion = (
    StringTypeInfo
    | IntTypeInfo
    | DecimalTypeInfo
    | TimeTypeInfo
    | TimestampTypeInfo
    | DateTypeInfo
    | EnumTypeInfo
    | JsonTypeInfo
    | BsonTypeInfo
    | UuidTypeInfo
    | Float16TypeInfo
    | MapTypeInfo
    | ListTypeInfo
    | VariantTypeInfo
    | GeometryTypeInfo
    | GeographyTypeInfo
    | UnknownTypeInfo
)

LogicalTypeInfoDiscriminated = Annotated[
    LogicalTypeInfoUnion,
    Discriminator('logical_type'),
]


CONVERTED_TYPE_TO_LOGICAL_TYPE: dict[
    ConvertedType,
    Callable[
        [int | None, int | None],
        LogicalTypeInfo,
    ],
] = {
    ConvertedType.UTF8: lambda _, __: StringTypeInfo(),
    ConvertedType.MAP: lambda _, __: MapTypeInfo(),
    ConvertedType.LIST: lambda _, __: ListTypeInfo(),
    ConvertedType.ENUM: lambda _, __: EnumTypeInfo(),
    ConvertedType.DATE: lambda _, __: DateTypeInfo(),
    ConvertedType.JSON: lambda _, __: JsonTypeInfo(),
    ConvertedType.BSON: lambda _, __: BsonTypeInfo(),
    ConvertedType.TIME_MILLIS: lambda _, __: TimeTypeInfo(unit=TimeUnit.MILLIS),
    ConvertedType.TIME_MICROS: lambda _, __: TimeTypeInfo(unit=TimeUnit.MICROS),
    ConvertedType.TIMESTAMP_MILLIS: (
        lambda _, __: TimestampTypeInfo(unit=TimeUnit.MILLIS)
    ),
    ConvertedType.TIMESTAMP_MICROS: (
        lambda _, __: TimestampTypeInfo(unit=TimeUnit.MICROS)
    ),
    ConvertedType.INT_8: lambda _, __: IntTypeInfo(bit_width=8, is_signed=True),
    ConvertedType.INT_16: lambda _, __: IntTypeInfo(bit_width=16, is_signed=True),
    ConvertedType.INT_32: lambda _, __: IntTypeInfo(bit_width=32, is_signed=True),
    ConvertedType.INT_64: lambda _, __: IntTypeInfo(bit_width=64, is_signed=True),
    ConvertedType.UINT_8: lambda _, __: IntTypeInfo(bit_width=8, is_signed=False),
    ConvertedType.UINT_16: lambda _, __: IntTypeInfo(bit_width=16, is_signed=False),
    ConvertedType.UINT_32: lambda _, __: IntTypeInfo(bit_width=32, is_signed=False),
    ConvertedType.UINT_64: lambda _, __: IntTypeInfo(bit_width=64, is_signed=False),
    ConvertedType.DECIMAL: (
        lambda scale, precision: DecimalTypeInfo(
            scale=scale or 0,
            precision=precision or 10,
        )
    ),
}


class SchemaElement(BaseModel, frozen=True):
    element_type: SchemaElementType
    name: str
    full_path: str
    start_offset: int
    byte_length: int
    # These levels are calculated during schema parsing based on the full path
    definition_level: int = 0
    repetition_level: int = 0

    def _repr_extra(self) -> list[str]:
        return []

    def __repr__(self) -> str:
        extra = self._repr_extra()
        extra_str = f': {" ".join(extra)}' if extra else None
        return f'{self.element_type}({self.name}{extra_str})'

    def get_logical_type(self) -> LogicalTypeInfo | None:
        """Get the logical type, prioritizing logical_type field over converted_type."""
        return self._logical_type_info

    @cached_property
    def _logical_type_info(self) -> LogicalTypeInfo | None:
        logical_type = getattr(self, 'logical_type', None)

        if logical_type is not None:
            return logical_type

        # Fallback to converting converted_type to logical equivalent
        return self._converted_type_to_logical_type(
            getattr(self, 'converted_type', None),
            getattr(self, 'scale', None),
            getattr(self, 'precision', None),
        )

    @staticmethod
    def _converted_type_to_logical_type(
        converted_type: ConvertedType | None,
        scale: int | None = None,
        precision: int | None = None,
    ) -> LogicalTypeInfo | None:
        """Convert a ConvertedType to a LogicalTypeInfo for backward compatibility."""
        if converted_type is None:
            return None

        return CONVERTED_TYPE_TO_LOGICAL_TYPE.get(
            converted_type,
            lambda _, __: None,
        )(scale, precision)

    @staticmethod
    def new(
        start_offset: int,
        byte_length: int,
        name: str | None = None,
        type: Type | None = None,
        type_length: int | None = None,
        repetition: Repetition | None = None,
        num_children: int | None = None,
        converted_type: ConvertedType | None = None,
        scale: int | None = None,
        precision: int | None = None,
        field_id: int | None = None,
        logical_type: LogicalTypeInfoUnion | None = None,
        **kwargs,
    ) -> SchemaRoot | SchemaGroup | SchemaLeaf:
        # Check type compatibility for column/leaf element
        is_column_converted_type = (
            converted_type is None or converted_type in ColumnConvertedType
        )
        is_column_logical_type = (
            logical_type is None or logical_type.logical_type in ColumnLogicalType
        )

        if (
            name
            and num_children is None
            and repetition is not None
            and type is not None
            and is_column_converted_type
            and is_column_logical_type
        ):
            return SchemaLeaf(
                name=name,
                type=type,
                type_length=type_length,
                repetition=repetition,
                converted_type=converted_type,
                scale=scale,
                precision=precision,
                field_id=field_id,
                logical_type=logical_type,
                start_offset=start_offset,
                byte_length=byte_length,
                # Levels will be calculated later during schema tree building
                definition_level=0,
                repetition_level=0,
                **kwargs,
            )

        # Root element could look essentially like any other group,
        # but with no repetition, which is required for other types
        if (
            name
            and converted_type is None
            and num_children is not None
            and type is None
            and logical_type is None
            and repetition is None
        ):
            return SchemaRoot(
                name=name,
                num_children=num_children,
                start_offset=start_offset,
                byte_length=byte_length,
                **kwargs,
            )

        # Root element check: should have no repetition, but some writers
        # incorrectly set repetition=REQUIRED on root elements, so we also
        # check the name
        if (
            name == 'schema'
            and num_children is not None
            and converted_type is None
            and logical_type is None
            and type is None
            and repetition == Repetition.REQUIRED
        ):
            warnings.warn(
                'Schema element appears to be root, but has invalid '
                'attrs. Warily assuming it is root... Schema element: '
                f"name='{name}', type='{type}', type_length='{type_length}', "
                f"repetition='{repetition}', num_children='{num_children}', "
                f"converted_type='{converted_type}",
                stacklevel=1,
            )
            return SchemaRoot(
                name=name,
                num_children=num_children,
                start_offset=start_offset,
                byte_length=byte_length,
                **kwargs,
            )

        # Check type compatibility for group element
        is_group_converted_type = (
            converted_type is None or converted_type in GroupConvertedType
        )
        is_group_logical_type = (
            logical_type is None or logical_type.logical_type in GroupLogicalType
        )

        if (
            name
            and is_group_converted_type
            and num_children is not None
            and repetition is not None
            and type is None
            and is_group_logical_type
        ):
            return SchemaGroup(
                name=name,
                repetition=repetition,
                num_children=num_children,
                converted_type=converted_type,
                start_offset=start_offset,
                byte_length=byte_length,
                field_id=field_id,
                logical_type=logical_type,
                **kwargs,
            )

        raise ValueError(
            'Could not resolve schema element type for args: '
            f"name='{name}', type='{type}', type_length='{type_length}', "
            f"repetition='{repetition}', num_children='{num_children}', "
            f"converted_type='{converted_type}",
        )


class BaseSchemaGroup(SchemaElement, frozen=True):
    element_type: SchemaElementType = SchemaElementType.GROUP
    num_children: int
    children: dict[str, SchemaGroup | SchemaLeaf] = Field(default_factory=dict)

    def count_leaf_columns(self) -> int:
        """Count all columns (leaves) in this schema element and its children."""
        return sum(
            child.count_leaf_columns() if isinstance(child, BaseSchemaGroup) else 1
            for child in self.children.values()
        )

    def find_element(self, path: str | list[str]) -> SchemaElement:
        """Finds a descendant schema element by its dotted path."""
        not_found = ValueError(f"Schema element for path '{path}' not found")

        if isinstance(path, str):
            path = path.split('.')

        child_name = path.pop(0)
        try:
            child = self.children[child_name]
        except KeyError:
            raise not_found from None

        if path and isinstance(child, BaseSchemaGroup):
            return child.find_element(path)

        if path:
            raise not_found

        return child

    def add_element(
        self,
        element: SchemaGroup | SchemaLeaf,
        path: str | list[str] | None = None,
    ) -> None:
        """Add an element to the schema at the specified dotted path."""
        if path is None:
            path = [element.name]
        elif isinstance(path, str):
            path = path.split('.')

        child_name = path.pop(0)

        if len(path) == 0:
            # Direct child
            self.children[child_name] = element
            return

        # Nested path - find parent group
        try:
            group = self.children[child_name]
        except KeyError:
            raise ValueError('Parent group not found') from None

        if not isinstance(group, BaseSchemaGroup):
            raise ValueError(
                'Found parent group, but it is not a group!',
            ) from None

        group.add_element(element, path)

    def __repr__(self) -> str:
        result = super().__repr__()
        for child in self.children.values():
            result += f'  {child}'
        return result


class SchemaRoot(BaseSchemaGroup, frozen=True):
    element_type: SchemaElementType = SchemaElementType.ROOT
    repetition: Repetition = Repetition.REQUIRED


class SchemaGroup(BaseSchemaGroup, frozen=True):
    repetition: Repetition
    converted_type: ConvertedType | None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
        ]


class SchemaLeaf(SchemaElement, frozen=True):
    element_type: SchemaElementType = SchemaElementType.COLUMN
    type: Type
    repetition: Repetition
    converted_type: ConvertedType | None
    type_length: int | None = None
    scale: int | None = None
    precision: int | None = None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None
    # Semantic interpretation for repeated field handling
    # (set during tree reconstruction)
    list_semantics: ListSemantics | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
            self.type.name,
        ]

    def bytes_to_physical_type(self, value: bytes) -> Any:
        from .parsers.physical_types import parse_bytes

        return parse_bytes(
            value,
            self.type,
        )

    def physical_to_logical_type(self, value: Any) -> Any:
        from .parsers.logical_types import convert_single_value

        if value is None:
            return None

        return convert_single_value(
            value,
            self.type,
            self.get_logical_type(),
        )

    def bytes_to_logical_type(self, value: bytes) -> Any:
        """Convert raw bytes (e.g. statistics values) to the logical value."""
        return self.physical_to_logical_type(self.bytes_to_physical_type(value))


class SchemaLinked(BaseModel, frozen=True):
    """Mixin for models that reference their column's ``SchemaLeaf``.

    The reference is serialized as a string key (``schema_path``) and the
    resolved leaf object is held in a private attribute that is linked at
    parse time via :meth:`_link`. Deserializing a model does not restore the
    link; callers must re-link against a schema root.

    Subclasses expose ``schema_path`` either as a serialized field or as a
    property (e.g. ``ColumnMetadata`` reuses its existing ``path_in_schema``).
    """

    _schema_element: SchemaLeaf | None = PrivateAttr(default=None)

    if TYPE_CHECKING:
        # Provided by subclasses as a serialized field or a property.
        @property
        def schema_path(self) -> str: ...

    @property
    def schema_element(self) -> SchemaLeaf:
        if self._schema_element is None:
            raise ValueError(
                f'This {type(self).__name__} is not linked to its schema '
                'element. Parse via FileMetadata.from_reader / '
                'ParquetFile.from_reader, or link it against a schema root '
                'before accessing schema_element.',
            )
        return self._schema_element

    def link(self, leaf: SchemaLeaf) -> Self:
        """Link this model to its schema leaf, validating the path matches."""
        if leaf.full_path != self.schema_path:
            raise ValueError(
                f'Cannot link {type(self).__name__} with schema_path '
                f'{self.schema_path!r} to schema leaf with full_path '
                f'{leaf.full_path!r}',
            )
        # These models are frozen, so bypass the normal setattr guard to
        # populate the private reference.
        object.__setattr__(self, '_schema_element', leaf)
        return self

    # Parsers link at construction time via this alias; kept private since
    # it's an implementation detail of the parse path rather than public API.
    _link = link


class ColumnStatistics(
    SchemaLinked,
    frozen=True,
    ser_json_bytes='base64',
    val_json_bytes='base64',
):
    min_: bytes | None = Field(None, alias='min')
    max_: bytes | None = Field(None, alias='max')
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: bytes | None = None
    max_value: bytes | None = None
    is_min_value_exact: bool | None = None
    is_max_value_exact: bool | None = None
    schema_path: str

    def _converted_value(
        self,
        value: bytes | None,
        deprecated_value: bytes | None,
    ) -> Any:
        if value is None:
            value = deprecated_value

        if value is None:
            return None

        return self.schema_element.bytes_to_logical_type(value)

    @property
    def converted_min_value(self) -> Any:
        return self._converted_value(self.min_value, self.min_)

    @property
    def converted_max_value(self) -> Any:
        return self._converted_value(self.max_value, self.max_)


class SizeStatistics(BaseModel, frozen=True):
    """Size statistics for BYTE_ARRAY columns."""

    unencoded_byte_array_data_bytes: int | None = None
    repetition_level_histogram: list[int] | None = None
    definition_level_histogram: list[int] | None = None


class BoundingBox(BaseModel, frozen=True):
    """Bounding box for GEOMETRY or GEOGRAPHY types."""

    xmin: float
    xmax: float
    ymin: float
    ymax: float
    zmin: float | None = None
    zmax: float | None = None
    mmin: float | None = None
    mmax: float | None = None


class GeospatialStatistics(BaseModel, frozen=True):
    """Statistics specific to Geometry and Geography logical types."""

    bbox: BoundingBox | None = None
    geospatial_types: list[GeospatialType] | None = None


class PageEncodingStats(BaseModel, frozen=True):
    """Statistics of a given page type and encoding."""

    page_type: PageType
    encoding: Encoding
    count: int


class PageLocation(BaseModel, frozen=True):
    """Location information for a page within a column chunk."""

    offset: int  # File offset of the page
    compressed_page_size: int  # Compressed size of the page
    first_row_index: int  # First row index of the page


class OffsetIndex(BaseModel, frozen=True):
    """Index containing page locations and sizes for efficient seeking."""

    start_offset: int
    byte_length: int
    page_locations: list[PageLocation]
    unencoded_byte_array_data_bytes: list[int] | None = None

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        start_offset: int,
        length: int | None = None,
    ) -> Self:
        """Parse Page Index data from file location.

        When ``length`` is known the exact span is fetched in one read;
        otherwise a speculative span is fetched and grown as needed.
        """
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        def parse(data: memoryview, offset: int) -> tuple[dict[str, Any], int]:
            parser = ThriftCompactParser(data, offset)
            props = PageIndexParser(parser).read_offset_index()
            return props, parser.pos - offset

        if length is not None:
            reader.seek(start_offset)
            data = await reader.read(length)
            props, byte_length = parse(memoryview(data), start_offset)
        else:
            props, byte_length = await read_thrift_span(reader, start_offset, parse)

        return cls(
            start_offset=start_offset,
            byte_length=byte_length,
            **props,
        )


class ColumnIndex(
    SchemaLinked,
    frozen=True,
    ser_json_bytes='base64',
    val_json_bytes='base64',
):
    """Index containing min/max statistics and null information for pages."""

    start_offset: int
    byte_length: int
    null_pages: list[bool]  # Which pages are all null
    min_values: list[bytes]  # Raw min values for each page
    max_values: list[bytes]  # Raw max values for each page
    boundary_order: BoundaryOrder  # Whether min/max values are ordered
    null_counts: list[int] | None = None  # Null count per page
    repetition_level_histograms: list[int] | None = None
    definition_level_histograms: list[int] | None = None
    schema_path: str

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        start_offset: int,
        schema_element: SchemaLeaf,
        length: int | None = None,
    ) -> Self:
        """Parse Page Index data from file location.

        When ``length`` is known the exact span is fetched in one read;
        otherwise a speculative span is fetched and grown as needed.
        """
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        def parse(data: memoryview, offset: int) -> tuple[dict[str, Any], int]:
            parser = ThriftCompactParser(data, offset)
            props = PageIndexParser(parser).read_column_index()
            return props, parser.pos - offset

        if length is not None:
            reader.seek(start_offset)
            data = await reader.read(length)
            props, byte_length = parse(memoryview(data), start_offset)
        else:
            props, byte_length = await read_thrift_span(reader, start_offset, parse)

        return cls(
            start_offset=start_offset,
            byte_length=byte_length,
            schema_path=schema_element.full_path,
            **props,
        )._link(schema_element)

    def _converted_values(self, values: list[bytes]) -> list[Any]:
        # min/max bytes for all-null pages are meaningless placeholders
        return [
            None if null_page else self.schema_element.bytes_to_logical_type(value)
            for value, null_page in zip(values, self.null_pages, strict=True)
        ]

    @property
    def converted_min_values(self) -> list[Any]:
        return self._converted_values(self.min_values)

    @property
    def converted_max_values(self) -> list[Any]:
        return self._converted_values(self.max_values)


class ColumnMetadata(SchemaLinked, frozen=True):
    """Detailed metadata about column chunk content and encoding."""

    start_offset: int
    byte_length: int
    type: Type
    encodings: list[Encoding]
    path_in_schema: str
    codec: Compression
    num_values: int
    total_uncompressed_size: int
    total_compressed_size: int
    data_page_offset: int
    index_page_offset: int | None = None
    dictionary_page_offset: int | None = None
    statistics: ColumnStatistics | None = None
    encoding_stats: list[PageEncodingStats] | None = None
    bloom_filter_offset: int | None = None
    bloom_filter_length: int | None = None
    size_statistics: SizeStatistics | None = None
    geospatial_statistics: GeospatialStatistics | None = None

    @property
    def schema_path(self) -> str:
        """The schema key for this column, reusing ``path_in_schema``."""
        return self.path_in_schema


class ColumnChunk(BaseModel, frozen=True):
    """File-level organization of column chunk."""

    file_offset: int
    metadata: ColumnMetadata
    file_path: str | None = None
    offset_index_offset: int | None = None
    offset_index_length: int | None = None
    column_index_offset: int | None = None
    column_index_length: int | None = None

    # Property accessors for flattened API access
    # We maintain the nested ColumnMetadata structure to stay consistent with
    # the actual Parquet metadata model, but provide these accessors for a
    # more logical and convenient API experience.
    @property
    def type(self) -> Type:
        return self.metadata.type

    @property
    def encodings(self) -> list[Encoding]:
        return self.metadata.encodings

    @property
    def path_in_schema(self) -> str:
        return self.metadata.path_in_schema

    @property
    def schema_element(self) -> SchemaLeaf:
        return self.metadata.schema_element

    @property
    def codec(self) -> Compression:
        return self.metadata.codec

    @property
    def num_values(self) -> int:
        return self.metadata.num_values

    @property
    def total_uncompressed_size(self) -> int:
        return self.metadata.total_uncompressed_size

    @property
    def total_compressed_size(self) -> int:
        return self.metadata.total_compressed_size

    @property
    def data_page_offset(self) -> int:
        return self.metadata.data_page_offset

    @property
    def index_page_offset(self) -> int | None:
        return self.metadata.index_page_offset

    @property
    def dictionary_page_offset(self) -> int | None:
        return self.metadata.dictionary_page_offset

    @property
    def statistics(self) -> ColumnStatistics | None:
        return self.metadata.statistics

    @property
    def bloom_filter_offset(self) -> int | None:
        return self.metadata.bloom_filter_offset

    @property
    def bloom_filter_length(self) -> int | None:
        return self.metadata.bloom_filter_length

    @property
    def size_statistics(self) -> SizeStatistics | None:
        return self.metadata.size_statistics

    @property
    def geospatial_statistics(self) -> GeospatialStatistics | None:
        return self.metadata.geospatial_statistics


class SortingColumn(BaseModel, frozen=True):
    column_idx: int
    descending: bool
    nulls_first: bool


class RowGroup(BaseModel, frozen=True):
    """Logical representation of row group metadata."""

    start_offset: int
    byte_length: int
    column_chunks: dict[str, ColumnChunk]
    total_byte_size: int
    row_count: int
    sorting_columns: list[SortingColumn] | None = None
    file_offset: int | None = None
    total_compressed_size: int | None = None
    ordinal: int | None = None

    @computed_field
    @cached_property
    def compression_stats(self) -> CompressionStats:
        total_compressed = 0
        total_uncompressed = 0
        for col in self.column_chunks.values():
            total_compressed += col.total_compressed_size
            total_uncompressed += col.total_uncompressed_size

        return CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

    @cached_property
    def column_names(self) -> list[str]:
        return list(self.column_chunks.keys())

    @cached_property
    def column_count(self) -> int:
        return len(self.column_chunks)


type RowGroups = list[RowGroup]


class FileMetadata(BaseModel, frozen=True):
    """Logical representation of file metadata."""

    version: int
    schema_root: SchemaRoot
    row_groups: RowGroups
    created_by: str | None = None
    key_value_metadata: list[KeyValueMetadata] = Field(default_factory=list)
    start_offset: int
    total_byte_size: int

    @model_validator(mode='after')
    def _relink_schema_references(self) -> Self:
        """Re-link column metadata/statistics to their schema leaves.

        Parsers link these references at construction time, but a
        ``FileMetadata`` built from a dict or JSON (e.g. via
        ``model_validate``) starts out with unlinked models. This walks
        the physical structure - row groups, then each row group's
        column chunks - and resolves every chunk's schema leaf by path,
        showing how physical structures correspond to schema leaves
        keyed by path. Re-linking an already-linked model (the parse
        path) is harmless, so we don't special-case it.
        """
        for row_group in self.row_groups:
            for chunk in row_group.column_chunks.values():
                path = chunk.metadata.path_in_schema
                leaf = self.schema_root.find_element(path)
                if not isinstance(leaf, SchemaLeaf):
                    raise ValueError(
                        f'Column chunk path {path!r} does not resolve to a schema leaf',
                    )
                chunk.metadata.link(leaf)
                if chunk.metadata.statistics is not None:
                    chunk.metadata.statistics.link(leaf)
        return self

    @classmethod
    async def from_reader(
        cls,
        reader: ReadableSeekable | AsyncReadableSeekable,
        columns: Sequence[str] | None = None,
    ) -> Self:
        """Parse file metadata from a reader.

        Args:
            reader: The file to read metadata from.
            columns: Optional projection of full dotted ``path_in_schema``
                strings. When provided, each row group's ``column_chunks``
                dict contains only the selected columns; everything else about
                the parse (schema tree, key-value metadata, row-group scalar
                fields) is unchanged. Unselected chunks are simply absent, so
                computed aggregates like ``compression_stats`` reflect only the
                selected columns. Unknown column names match nothing (no
                error); ``columns=[]`` selects no chunks while ``columns=None``
                selects all. This gives a large memory and CPU reduction when
                only a few columns of a wide file are needed.
        """
        from .parsers.parquet.metadata import MetadataParser

        reader = ensure_async_reader(reader)
        reader.seek(-FOOTER_SIZE, SEEK_END)
        footer_start = reader.tell()
        footer_bytes = await reader.read(FOOTER_SIZE)
        magic_footer = footer_bytes[4:8]

        if magic_footer != PARQUET_MAGIC:
            raise ParquetFormatError(
                'Invalid magic footer: expected '
                f'{PARQUET_MAGIC!r}, got {magic_footer!r}',
            )

        metadata_size = struct.unpack('<I', footer_bytes[:4])[0]
        metadata_start = footer_start - metadata_size

        # Parquet tells us the exact metadata span, so fetch it in a single
        # read and parse from memory. This avoids thousands of tiny reads back
        # through the (possibly remote, cached) file. The parser is told the
        # absolute offset where the span begins, so the recorded teaching
        # fields are identical to a direct parse.
        reader.seek(metadata_start)
        metadata_bytes = await reader.read(metadata_size)

        return cls(
            start_offset=metadata_start,
            total_byte_size=metadata_size,
            **MetadataParser(metadata_bytes, metadata_start).parse(columns=columns),
        )

    @computed_field
    @cached_property
    def compression_stats(self) -> CompressionStats:
        """Calculate overall file statistics."""
        total_compressed = 0
        total_uncompressed = 0
        for rg in self.row_groups:
            total_compressed += rg.compression_stats.total_compressed
            total_uncompressed += rg.compression_stats.total_uncompressed

        return CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

    @computed_field
    @cached_property
    def column_count(self) -> int:
        return self.schema_root.count_leaf_columns()

    @computed_field
    @cached_property
    def row_count(self) -> int:
        return sum(rg.row_count for rg in self.row_groups)

    @computed_field
    @cached_property
    def row_group_count(self) -> int:
        return len(self.row_groups)

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

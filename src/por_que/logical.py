from __future__ import annotations

import warnings

from dataclasses import asdict, dataclass, field
from typing import Self

from .enums import (
    BoundaryOrder,
    ColumnConvertedType,
    ColumnLogicalType,
    Compression,
    ConvertedType,
    Encoding,
    GroupConvertedType,
    GroupLogicalType,
    LogicalType,
    Repetition,
    SchemaElementType,
    TimeUnit,
    Type,
)


@dataclass(frozen=True)
class CompressionStats:
    """Compression statistics for data."""

    total_compressed: int
    total_uncompressed: int

    @property
    def ratio(self) -> float:
        """Compression ratio (compressed/uncompressed)."""
        return (
            self.total_compressed / self.total_uncompressed
            if self.total_uncompressed > 0
            else 0.0
        )

    @property
    def space_saved_percent(self) -> float:
        """Percentage of space saved by compression."""
        return (1 - self.ratio) * 100 if self.total_uncompressed > 0 else 0.0

    @property
    def compressed_mb(self) -> float:
        """Compressed size in MB."""
        return self.total_compressed / (1024 * 1024)

    @property
    def uncompressed_mb(self) -> float:
        """Uncompressed size in MB."""
        return self.total_uncompressed / (1024 * 1024)


@dataclass(frozen=True)
class LogicalTypeInfo:
    """Base class for logical type information."""

    logical_type: LogicalType


@dataclass(frozen=True)
class StringTypeInfo(LogicalTypeInfo):
    """String logical type."""

    logical_type: LogicalType = LogicalType.STRING


@dataclass(frozen=True)
class IntTypeInfo(LogicalTypeInfo):
    """Integer logical type with bit width and signedness."""

    logical_type: LogicalType = LogicalType.INTEGER
    bit_width: int = 32
    is_signed: bool = True


@dataclass(frozen=True)
class DecimalTypeInfo(LogicalTypeInfo):
    """Decimal logical type with scale and precision."""

    logical_type: LogicalType = LogicalType.DECIMAL
    scale: int = 0
    precision: int = 10


@dataclass(frozen=True)
class TimeTypeInfo(LogicalTypeInfo):
    """Time logical type with unit and UTC adjustment."""

    logical_type: LogicalType = LogicalType.TIME
    is_adjusted_to_utc: bool = False
    unit: TimeUnit = TimeUnit.MILLIS


@dataclass(frozen=True)
class TimestampTypeInfo(LogicalTypeInfo):
    """Timestamp logical type with unit and UTC adjustment."""

    logical_type: LogicalType = LogicalType.TIMESTAMP
    is_adjusted_to_utc: bool = False
    unit: TimeUnit = TimeUnit.MILLIS


@dataclass(frozen=True)
class DateTypeInfo(LogicalTypeInfo):
    """Date logical type."""

    logical_type: LogicalType = LogicalType.DATE


@dataclass(frozen=True)
class EnumTypeInfo(LogicalTypeInfo):
    """Enum logical type."""

    logical_type: LogicalType = LogicalType.ENUM


@dataclass(frozen=True)
class JsonTypeInfo(LogicalTypeInfo):
    """JSON logical type."""

    logical_type: LogicalType = LogicalType.JSON


@dataclass(frozen=True)
class BsonTypeInfo(LogicalTypeInfo):
    """BSON logical type."""

    logical_type: LogicalType = LogicalType.BSON


@dataclass(frozen=True)
class UuidTypeInfo(LogicalTypeInfo):
    """UUID logical type."""

    logical_type: LogicalType = LogicalType.UUID


@dataclass(frozen=True)
class Float16TypeInfo(LogicalTypeInfo):
    """Float16 logical type."""

    logical_type: LogicalType = LogicalType.FLOAT16


@dataclass(frozen=True)
class MapTypeInfo(LogicalTypeInfo):
    """Map logical type."""

    logical_type: LogicalType = LogicalType.MAP


@dataclass(frozen=True)
class ListTypeInfo(LogicalTypeInfo):
    """List logical type."""

    logical_type: LogicalType = LogicalType.LIST


@dataclass(frozen=True)
class VariantTypeInfo(LogicalTypeInfo):
    """Variant logical type."""

    logical_type: LogicalType = LogicalType.VARIANT


@dataclass(frozen=True)
class GeometryTypeInfo(LogicalTypeInfo):
    """Geometry logical type."""

    logical_type: LogicalType = LogicalType.GEOMETRY


@dataclass(frozen=True)
class GeographyTypeInfo(LogicalTypeInfo):
    """Geography logical type."""

    logical_type: LogicalType = LogicalType.GEOGRAPHY


@dataclass(frozen=True)
class UnknownTypeInfo(LogicalTypeInfo):
    """Unknown logical type."""

    logical_type: LogicalType = LogicalType.UNKNOWN


@dataclass(frozen=True, kw_only=True)
class SchemaElement:
    element_type: SchemaElementType
    name: str

    def _repr_extra(self) -> list[str]:
        return []

    def __repr__(self) -> str:
        extra = self._repr_extra()
        extra_str = f': {" ".join(extra)}' if extra else None
        return f'{self.element_type}({self.name}{extra_str})'

    def get_logical_type(self) -> LogicalTypeInfo | None:
        """Get the logical type, prioritizing logical_type field over converted_type."""
        if hasattr(self, 'logical_type') and self.logical_type is not None:
            return self.logical_type

        # Fallback to converting converted_type to logical equivalent
        if hasattr(self, 'converted_type'):
            return self._converted_type_to_logical_type(
                self.converted_type,
                getattr(self, 'scale', None),
                getattr(self, 'precision', None),
            )

        return None

    @staticmethod
    def _converted_type_to_logical_type(
        converted_type: ConvertedType | None,
        scale: int | None = None,
        precision: int | None = None,
    ) -> LogicalTypeInfo | None:
        """Convert a ConvertedType to a LogicalTypeInfo for backward compatibility."""
        if converted_type is None:
            return None

        mapping = {
            ConvertedType.UTF8: StringTypeInfo(),
            ConvertedType.MAP: MapTypeInfo(),
            ConvertedType.LIST: ListTypeInfo(),
            ConvertedType.ENUM: EnumTypeInfo(),
            ConvertedType.DATE: DateTypeInfo(),
            ConvertedType.JSON: JsonTypeInfo(),
            ConvertedType.BSON: BsonTypeInfo(),
            ConvertedType.TIME_MILLIS: TimeTypeInfo(unit=TimeUnit.MILLIS),
            ConvertedType.TIME_MICROS: TimeTypeInfo(unit=TimeUnit.MICROS),
            ConvertedType.TIMESTAMP_MILLIS: TimestampTypeInfo(unit=TimeUnit.MILLIS),
            ConvertedType.TIMESTAMP_MICROS: TimestampTypeInfo(unit=TimeUnit.MICROS),
            ConvertedType.INT_8: IntTypeInfo(bit_width=8, is_signed=True),
            ConvertedType.INT_16: IntTypeInfo(bit_width=16, is_signed=True),
            ConvertedType.INT_32: IntTypeInfo(bit_width=32, is_signed=True),
            ConvertedType.INT_64: IntTypeInfo(bit_width=64, is_signed=True),
            ConvertedType.UINT_8: IntTypeInfo(bit_width=8, is_signed=False),
            ConvertedType.UINT_16: IntTypeInfo(bit_width=16, is_signed=False),
            ConvertedType.UINT_32: IntTypeInfo(bit_width=32, is_signed=False),
            ConvertedType.UINT_64: IntTypeInfo(bit_width=64, is_signed=False),
        }

        # Special handling for DECIMAL which uses scale and precision
        if converted_type == ConvertedType.DECIMAL:
            return DecimalTypeInfo(
                scale=scale or 0,
                precision=precision or 10,
            )

        return mapping.get(converted_type)

    @staticmethod
    def new(
        name: str | None,
        type: Type | None,
        type_length: int | None,
        repetition: Repetition | None,
        num_children: int | None,
        converted_type: ConvertedType | None,
        scale: int | None = None,
        precision: int | None = None,
        field_id: int | None = None,
        logical_type: LogicalTypeInfo | None = None,
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
                field_id=field_id,
                logical_type=logical_type,
            )

        raise ValueError(
            'Could not resolve schema element type for args: '
            f"name='{name}', type='{type}', type_length='{type_length}', "
            f"repetition='{repetition}', num_children='{num_children}', "
            f"converted_type='{converted_type}",
        )


@dataclass(frozen=True, kw_only=True)
class BaseSchemaGroup(SchemaElement):
    element_type: SchemaElementType = SchemaElementType.GROUP
    num_children: int
    children: dict[str, SchemaGroup | SchemaLeaf] = field(default_factory=dict)

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


@dataclass(frozen=True, kw_only=True)
class SchemaRoot(BaseSchemaGroup):
    element_type: SchemaElementType = SchemaElementType.ROOT


@dataclass(frozen=True, kw_only=True)
class SchemaGroup(BaseSchemaGroup):
    repetition: Repetition
    converted_type: ConvertedType | None
    field_id: int | None = None
    logical_type: LogicalTypeInfo | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
        ]


@dataclass(frozen=True, kw_only=True)
class SchemaLeaf(SchemaElement):
    element_type: SchemaElementType = SchemaElementType.COLUMN
    type: Type
    repetition: Repetition
    converted_type: ConvertedType | None
    type_length: int | None = None
    scale: int | None = None
    precision: int | None = None
    field_id: int | None = None
    logical_type: LogicalTypeInfo | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
            self.type.name,
        ]


@dataclass(frozen=True)
class ColumnStatistics:
    min_value: str | int | float | bool | None = None
    max_value: str | int | float | bool | None = None
    null_count: int | None = None
    distinct_count: int | None = None


@dataclass(frozen=True)
class PageLocation:
    """Location information for a page within a column chunk."""

    offset: int  # File offset of the page
    compressed_page_size: int  # Compressed size of the page
    first_row_index: int  # First row index of the page


@dataclass(frozen=True)
class OffsetIndex:
    """Index containing page locations and sizes for efficient seeking."""

    page_locations: list[PageLocation]
    unencoded_byte_array_data_bytes: list[int] | None = None


@dataclass(frozen=True)
class ColumnIndex:
    """Index containing min/max statistics and null information for pages."""

    null_pages: list[bool]  # Which pages are all null
    min_values: list[bytes]  # Raw min values for each page
    max_values: list[bytes]  # Raw max values for each page
    boundary_order: BoundaryOrder  # Whether min/max values are ordered
    null_counts: list[int] | None = None  # Null count per page
    repetition_level_histograms: list[int] | None = None
    definition_level_histograms: list[int] | None = None


@dataclass(frozen=True)
class ColumnMetadata:
    """Detailed metadata about column chunk content and encoding."""

    type: Type
    encodings: list[Encoding]
    path_in_schema: str
    codec: Compression
    num_values: int
    total_uncompressed_size: int
    total_compressed_size: int
    data_page_offset: int
    dictionary_page_offset: int | None = None
    index_page_offset: int | None = None
    statistics: ColumnStatistics | None = None
    # Page Index fields (new in Parquet 2.5+)
    column_index_offset: int | None = None
    column_index_length: int | None = None
    column_index: ColumnIndex | None = None
    offset_index: OffsetIndex | None = None


@dataclass(frozen=True)
class ColumnChunk:
    """File-level organization of column chunk."""

    file_offset: int
    metadata: ColumnMetadata
    file_path: str | None

    @classmethod
    def new(
        cls,
        file_offset: int | None,
        metadata: ColumnMetadata | None,
        file_path: str | None,
    ) -> Self:
        if file_offset is None:
            raise ValueError('file_offset cannot be None')

        if metadata is None:
            raise ValueError('metadata cannot be None')

        return cls(
            file_offset=file_offset,
            metadata=metadata,
            file_path=file_path,
        )

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
    def dictionary_page_offset(self) -> int | None:
        return self.metadata.dictionary_page_offset

    @property
    def index_page_offset(self) -> int | None:
        return self.metadata.index_page_offset

    @property
    def statistics(self) -> ColumnStatistics | None:
        return self.metadata.statistics

    @property
    def column_index_offset(self) -> int | None:
        return self.metadata.column_index_offset

    @property
    def column_index_length(self) -> int | None:
        return self.metadata.column_index_length

    @property
    def column_index(self) -> ColumnIndex | None:
        return self.metadata.column_index

    @property
    def offset_index(self) -> OffsetIndex | None:
        return self.metadata.offset_index


@dataclass(frozen=True)
class RowGroup:
    """Logical representation of row group metadata."""

    column_chunks: dict[str, ColumnChunk]
    total_byte_size: int
    row_count: int
    compression_stats: CompressionStats = field(init=False)

    def __post_init__(self) -> None:
        total_compressed = 0
        total_uncompressed = 0
        for col in self.column_chunks.values():
            total_compressed += col.total_compressed_size
            total_uncompressed += col.total_uncompressed_size

        object.__setattr__(
            self,
            'compression_stats',
            CompressionStats(
                total_compressed=total_compressed,
                total_uncompressed=total_uncompressed,
            ),
        )

    @property
    def column_names(self) -> list[str]:
        return list(self.column_chunks.keys())

    @property
    def column_count(self) -> int:
        return len(self.column_chunks)


@dataclass(frozen=True)
class FileMetadata:
    """Logical representation of file metadata."""

    version: int
    schema: SchemaRoot
    row_groups: list[RowGroup]
    created_by: str | None = None
    key_value_metadata: dict[str, str] = field(default_factory=dict)
    compression_stats: CompressionStats = field(init=False)
    column_count: int = field(init=False)
    row_count: int = field(init=False)
    row_group_count: int = field(init=False)

    def __post_init__(self) -> None:
        """Calculate overall file statistics."""
        total_compressed = 0
        total_uncompressed = 0
        for rg in self.row_groups:
            total_compressed += rg.compression_stats.total_compressed
            total_uncompressed += rg.compression_stats.total_uncompressed

        object.__setattr__(self, 'column_count', self.schema.count_leaf_columns())
        object.__setattr__(
            self,
            'row_count',
            sum(rg.row_count for rg in self.row_groups),
        )
        object.__setattr__(self, 'row_group_count', len(self.row_groups))

        object.__setattr__(
            self,
            'compression_stats',
            CompressionStats(
                total_compressed=total_compressed,
                total_uncompressed=total_uncompressed,
            ),
        )

    def to_dict(self) -> dict:
        return asdict(self)

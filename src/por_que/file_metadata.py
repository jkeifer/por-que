from __future__ import annotations

import warnings

from functools import cached_property
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    ValidationInfo,
    computed_field,
    model_validator,
)

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
from .util.models import get_item_or_attr


class CompressionStats(BaseModel):
    """Compression statistics for data."""

    model_config = ConfigDict(frozen=True)

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


class KeyValueMetadata(BaseModel):
    """Key-value metadata pair with byte range information."""

    model_config = ConfigDict(frozen=True)

    start_offset: int
    byte_length: int
    key: str
    value: str


class LogicalTypeInfo(BaseModel):
    """Base class for logical type information."""

    model_config = ConfigDict(frozen=True)

    logical_type: LogicalType


class StringTypeInfo(LogicalTypeInfo):
    """String logical type."""

    logical_type: Literal[LogicalType.STRING] = LogicalType.STRING


class IntTypeInfo(LogicalTypeInfo):
    """Integer logical type with bit width and signedness."""

    logical_type: Literal[LogicalType.INTEGER] = LogicalType.INTEGER
    bit_width: int = 32
    is_signed: bool = True


class DecimalTypeInfo(LogicalTypeInfo):
    """Decimal logical type with scale and precision."""

    logical_type: Literal[LogicalType.DECIMAL] = LogicalType.DECIMAL
    scale: int = 0
    precision: int = 10


class TimeTypeInfo(LogicalTypeInfo):
    """Time logical type with unit and UTC adjustment."""

    logical_type: Literal[LogicalType.TIME] = LogicalType.TIME
    is_adjusted_to_utc: bool = False
    unit: TimeUnit = TimeUnit.MILLIS


class TimestampTypeInfo(LogicalTypeInfo):
    """Timestamp logical type with unit and UTC adjustment."""

    logical_type: Literal[LogicalType.TIMESTAMP] = LogicalType.TIMESTAMP
    is_adjusted_to_utc: bool = False
    unit: TimeUnit = TimeUnit.MILLIS


class DateTypeInfo(LogicalTypeInfo):
    """Date logical type."""

    logical_type: Literal[LogicalType.DATE] = LogicalType.DATE


class EnumTypeInfo(LogicalTypeInfo):
    """Enum logical type."""

    logical_type: Literal[LogicalType.ENUM] = LogicalType.ENUM


class JsonTypeInfo(LogicalTypeInfo):
    """JSON logical type."""

    logical_type: Literal[LogicalType.JSON] = LogicalType.JSON


class BsonTypeInfo(LogicalTypeInfo):
    """BSON logical type."""

    logical_type: Literal[LogicalType.BSON] = LogicalType.BSON


class UuidTypeInfo(LogicalTypeInfo):
    """UUID logical type."""

    logical_type: Literal[LogicalType.UUID] = LogicalType.UUID


class Float16TypeInfo(LogicalTypeInfo):
    """Float16 logical type."""

    logical_type: Literal[LogicalType.FLOAT16] = LogicalType.FLOAT16


class MapTypeInfo(LogicalTypeInfo):
    """Map logical type."""

    logical_type: Literal[LogicalType.MAP] = LogicalType.MAP


class ListTypeInfo(LogicalTypeInfo):
    """List logical type."""

    logical_type: Literal[LogicalType.LIST] = LogicalType.LIST


class VariantTypeInfo(LogicalTypeInfo):
    """Variant logical type."""

    logical_type: Literal[LogicalType.VARIANT] = LogicalType.VARIANT


class GeometryTypeInfo(LogicalTypeInfo):
    """Geometry logical type."""

    logical_type: Literal[LogicalType.GEOMETRY] = LogicalType.GEOMETRY


class GeographyTypeInfo(LogicalTypeInfo):
    """Geography logical type."""

    logical_type: Literal[LogicalType.GEOGRAPHY] = LogicalType.GEOGRAPHY


class UnknownTypeInfo(LogicalTypeInfo):
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


CONVERTED_TYPE_TO_LOGICAL_TYPE: dict[ConvertedType, LogicalTypeInfo] = {
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


class SchemaElement(BaseModel):
    model_config = ConfigDict(frozen=True)

    element_type: SchemaElementType
    name: str
    start_offset: int
    byte_length: int

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

        # Special handling for DECIMAL which uses scale and precision
        if converted_type == ConvertedType.DECIMAL:
            return DecimalTypeInfo(
                scale=scale or 0,
                precision=precision or 10,
            )

        return CONVERTED_TYPE_TO_LOGICAL_TYPE.get(converted_type)

    @staticmethod
    def new(
        start_offset: int,
        byte_length: int,
        name: str | None,
        type: Type | None,
        type_length: int | None,
        repetition: Repetition | None,
        num_children: int | None,
        converted_type: ConvertedType | None,
        scale: int | None = None,
        precision: int | None = None,
        field_id: int | None = None,
        logical_type: LogicalTypeInfoUnion | None = None,
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
            )

        raise ValueError(
            'Could not resolve schema element type for args: '
            f"name='{name}', type='{type}', type_length='{type_length}', "
            f"repetition='{repetition}', num_children='{num_children}', "
            f"converted_type='{converted_type}",
        )


class BaseSchemaGroup(SchemaElement):
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


class SchemaRoot(BaseSchemaGroup):
    element_type: SchemaElementType = SchemaElementType.ROOT


class SchemaGroup(BaseSchemaGroup):
    repetition: Repetition
    converted_type: ConvertedType | None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
        ]


class SchemaLeaf(SchemaElement):
    element_type: SchemaElementType = SchemaElementType.COLUMN
    type: Type
    repetition: Repetition
    converted_type: ConvertedType | None
    type_length: int | None = None
    scale: int | None = None
    precision: int | None = None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None

    @property
    def definition_level(self) -> int:
        """Calculate the definition level for this schema element.

        For non-nested schemas:
        - REQUIRED columns: 0 (never null)
        - OPTIONAL columns: 1 (can be null or present)
        - REPEATED columns: 1 (can be null/empty or present)

        For nested schemas, this would need to traverse the parent chain.
        """
        if self.repetition == Repetition.REQUIRED:
            return 0
        # OPTIONAL or REPEATED
        return 1

    @property
    def repetition_level(self) -> int:
        """Calculate the repetition level for this schema element.

        For non-nested schemas:
        - Non-REPEATED columns: 0
        - REPEATED columns: 1

        For nested schemas, this would need to traverse the parent chain.
        """
        if self.repetition == Repetition.REPEATED:
            return 1
        return 0

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
            self.type.name,
        ]


class ColumnStatistics(BaseModel):
    model_config = ConfigDict(frozen=True)

    min_value: str | int | float | bool | None = None
    max_value: str | int | float | bool | None = None
    null_count: int | None = None
    distinct_count: int | None = None


class PageLocation(BaseModel):
    """Location information for a page within a column chunk."""

    model_config = ConfigDict(frozen=True)

    offset: int  # File offset of the page
    compressed_page_size: int  # Compressed size of the page
    first_row_index: int  # First row index of the page


class OffsetIndex(BaseModel):
    """Index containing page locations and sizes for efficient seeking."""

    model_config = ConfigDict(frozen=True)

    page_locations: list[PageLocation]
    unencoded_byte_array_data_bytes: list[int] | None = None


class ColumnIndex(BaseModel):
    """Index containing min/max statistics and null information for pages."""

    model_config = ConfigDict(frozen=True)

    null_pages: list[bool]  # Which pages are all null
    min_values: list[bytes]  # Raw min values for each page
    max_values: list[bytes]  # Raw max values for each page
    boundary_order: BoundaryOrder  # Whether min/max values are ordered
    null_counts: list[int] | None = None  # Null count per page
    repetition_level_histograms: list[int] | None = None
    definition_level_histograms: list[int] | None = None


class ColumnMetadata(BaseModel):
    """Detailed metadata about column chunk content and encoding."""

    model_config = ConfigDict(frozen=True)

    type: Type
    encodings: list[Encoding]
    path_in_schema: str
    schema_element: SchemaLeaf = Field(exclude=True)
    codec: Compression
    num_values: int
    total_uncompressed_size: int
    total_compressed_size: int
    data_page_offset: int
    start_offset: int
    byte_length: int
    dictionary_page_offset: int | None = None
    index_page_offset: int | None = None
    statistics: ColumnStatistics | None = None
    # Page Index fields (new in Parquet 2.5+)
    column_index_offset: int | None = None
    column_index_length: int | None = None
    column_index: ColumnIndex | None = None
    offset_index: OffsetIndex | None = None


class ColumnChunk(BaseModel):
    """File-level organization of column chunk."""

    model_config = ConfigDict(frozen=True)

    file_offset: int
    metadata: ColumnMetadata
    file_path: str | None

    @model_validator(mode='before')
    @classmethod
    def inject_schema_element_from_context(cls, data: Any, info: ValidationInfo):
        """Inject schema element from context if not provided."""
        if not isinstance(data, dict) or not info.context:
            return data

        if 'schema_element' in data or 'schema_root' not in info.context:
            return data

        schema_root = info.context['schema_root']

        try:
            path = data['metadata']['path_in_schema']
        except KeyError:
            return data

        schema_element = schema_root.find_element(path)
        if schema_element and isinstance(schema_element, SchemaLeaf):
            data = {**data, 'schema_element': schema_element}

        return data

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

    @cached_property
    def type(self) -> Type:
        return self.metadata.type

    @cached_property
    def encodings(self) -> list[Encoding]:
        return self.metadata.encodings

    @cached_property
    def path_in_schema(self) -> str:
        return self.metadata.path_in_schema

    @cached_property
    def schema_element(self) -> SchemaLeaf:
        return self.metadata.schema_element

    @cached_property
    def codec(self) -> Compression:
        return self.metadata.codec

    @cached_property
    def num_values(self) -> int:
        return self.metadata.num_values

    @cached_property
    def total_uncompressed_size(self) -> int:
        return self.metadata.total_uncompressed_size

    @cached_property
    def total_compressed_size(self) -> int:
        return self.metadata.total_compressed_size

    @cached_property
    def data_page_offset(self) -> int:
        return self.metadata.data_page_offset

    @cached_property
    def dictionary_page_offset(self) -> int | None:
        return self.metadata.dictionary_page_offset

    @cached_property
    def index_page_offset(self) -> int | None:
        return self.metadata.index_page_offset

    @cached_property
    def statistics(self) -> ColumnStatistics | None:
        return self.metadata.statistics

    @cached_property
    def column_index_offset(self) -> int | None:
        return self.metadata.column_index_offset

    @cached_property
    def column_index_length(self) -> int | None:
        return self.metadata.column_index_length

    @cached_property
    def column_index(self) -> ColumnIndex | None:
        return self.metadata.column_index

    @cached_property
    def offset_index(self) -> OffsetIndex | None:
        return self.metadata.offset_index


class RowGroup(BaseModel):
    """Logical representation of row group metadata."""

    model_config = ConfigDict(frozen=True)

    column_chunks: dict[str, ColumnChunk]
    total_byte_size: int
    row_count: int
    start_offset: int
    byte_length: int

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


class FileMetadata(BaseModel):
    """Logical representation of file metadata."""

    model_config = ConfigDict(frozen=True)

    version: int
    schema_root: SchemaRoot
    row_groups: RowGroups
    created_by: str | None = None
    key_value_metadata: list[KeyValueMetadata] = Field(default_factory=list)

    @model_validator(mode='before')
    @classmethod
    def inject_schema_references(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        try:
            schema_root = data['schema_root']
            row_groups: RowGroups | list[dict] = data['row_groups']
        except KeyError:
            return data

        if not isinstance(schema_root, SchemaRoot):
            schema_root = SchemaRoot(**schema_root)
            data['schema_root'] = schema_root

        updated_row_groups: list[RowGroup | dict] = []
        for row_group in row_groups:
            updated = False
            try:
                column_chunks: dict[str, ColumnChunk | dict] = get_item_or_attr(
                    row_group,
                    'column_chunks',
                )
            except ValueError:
                return data

            updated_column_chunks: dict[str, ColumnChunk | dict] = {}
            for column_chunk in column_chunks.values():
                try:
                    metadata: ColumnMetadata | dict[str, Any] = get_item_or_attr(
                        column_chunk,
                        'metadata',
                    )
                    path: str = get_item_or_attr(
                        metadata,
                        'path_in_schema',
                    )
                except ValueError:
                    return data

                # Find and inject the logical metadata reference
                schema_element = schema_root.find_element(path)
                if (
                    hasattr(metadata, 'schema_element')
                    and metadata.schema_element is schema_element
                ):
                    updated_column_chunks[path] = column_chunk
                    continue

                updated = True
                _chunk = (
                    column_chunk
                    if isinstance(column_chunk, dict)
                    else column_chunk.__dict__
                )
                _meta = metadata if isinstance(metadata, dict) else metadata.__dict__
                _meta['schema_element'] = schema_element
                _chunk['metadata'] = _meta
                updated_column_chunks[path] = _chunk

            if not updated:
                updated_row_groups.append(row_group)
                continue

            _rg = row_group if isinstance(row_group, dict) else row_group.__dict__
            _rg['column_chunks'] = updated_column_chunks
            updated_row_groups.append(_rg)

        return {**data, 'row_groups': updated_row_groups}

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

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Self

from .enums import (
    ColumnConvertedType,
    Compression,
    ConvertedType,
    Encoding,
    GroupConvertedType,
    Repetition,
    SchemaElementType,
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

    @staticmethod
    def new(
        name: str | None,
        type: Type | None,
        type_length: int | None,
        repetition: Repetition | None,
        num_children: int | None,
        converted_type: ConvertedType | None,
    ) -> SchemaRoot | SchemaGroup | SchemaLeaf:
        if (
            name
            and num_children is None
            and repetition is not None
            and type is not None
            and (converted_type is None or converted_type in ColumnConvertedType)
        ):
            return SchemaLeaf(
                name=name,
                type=type,
                type_length=type_length,
                repetition=repetition,
                converted_type=converted_type,
            )

        if (
            name
            and (converted_type is None or converted_type in GroupConvertedType)
            and num_children is not None
            and repetition is not None
            and type is None
        ):
            return SchemaGroup(
                name=name,
                repetition=repetition,
                num_children=num_children,
                converted_type=converted_type,
            )

        if (
            name
            and converted_type is None
            and num_children is not None
            and repetition is None
            and type is None
        ):
            return SchemaRoot(
                name=name,
                num_children=num_children,
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

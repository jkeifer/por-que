from __future__ import annotations

import struct

from collections.abc import Sequence
from functools import cached_property
from io import SEEK_END
from typing import Self

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    model_validator,
)

from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import (
    Compression,
    Encoding,
    Type,
)
from .exceptions import ParquetFormatError
from .protocols import AsyncReadableSeekable, ReadableSeekable

# ---------------------------------------------------------------------------
# Backwards-compatible re-exports.
#
# The schema-element and statistics/index model families were split out into
# ``por_que.schema`` and ``por_que.statistics``. Re-export them here so that
# existing ``from por_que.file_metadata import X`` imports keep working.
# Internal code should import from the canonical modules directly.
# ---------------------------------------------------------------------------
from .schema import (  # noqa: F401
    CONVERTED_TYPE_TO_LOGICAL_TYPE,
    BaseSchemaGroup,
    BsonTypeInfo,
    DateTypeInfo,
    DecimalTypeInfo,
    EnumTypeInfo,
    Float16TypeInfo,
    GeographyTypeInfo,
    GeometryTypeInfo,
    IntTypeInfo,
    JsonTypeInfo,
    ListTypeInfo,
    LogicalTypeInfo,
    LogicalTypeInfoDiscriminated,
    LogicalTypeInfoUnion,
    MapTypeInfo,
    SchemaElement,
    SchemaGroup,
    SchemaLeaf,
    SchemaLinked,
    SchemaRoot,
    StringTypeInfo,
    TimestampTypeInfo,
    TimeTypeInfo,
    UnknownTypeInfo,
    UuidTypeInfo,
    VariantTypeInfo,
)
from .statistics import (  # noqa: F401
    BoundingBox,
    ColumnIndex,
    ColumnStatistics,
    GeospatialStatistics,
    OffsetIndex,
    PageEncodingStats,
    PageLocation,
    SizeStatistics,
)
from .util.async_adapter import ensure_async_reader


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

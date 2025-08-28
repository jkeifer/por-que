import os
import struct

from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import (
    Compression,
    ConvertedType,
    Encoding,
    Repetition,
    Type,
)
from .exceptions import ParquetMagicError
from .stats import CompressionStats, FileStats, RowGroupStats
from .util import http


@dataclass
class SchemaElement:
    name: str
    type: Type | None = None
    repetition: Repetition | None = None
    num_children: int = 0
    converted_type: ConvertedType | None = None
    type_length: int | None = None
    children: dict[str, 'SchemaElement'] = field(default_factory=dict)

    def is_group(self) -> bool:
        return self.type is None

    def get_logical_type(self, path_in_schema: str) -> ConvertedType | None:
        """Look up logical type by dotted path from this element."""
        parts = path_in_schema.split('.')
        current = self

        for part in parts:
            if part not in current.children:
                raise ValueError(f"Schema path '{path_in_schema}' not found in schema")
            current = current.children[part]

        return current.converted_type if not current.is_group() else None

    def add_element(self, element: 'SchemaElement', path: str) -> None:
        """Add an element to the schema at the specified dotted path."""
        if '.' not in path:
            # Direct child
            self.children[path] = element
        else:
            # Nested path - find or create parent groups
            parts = path.split('.')
            element_name = parts[-1]

            # Navigate to parent, creating groups as needed
            current = self
            for part in parts[:-1]:
                if part not in current.children:
                    # Create intermediate group
                    group = SchemaElement(name=part)
                    current.children[part] = group
                current = current.children[part]

            # Add the element to the final parent
            current.children[element_name] = element

    def __repr__(self) -> str:
        return self._repr_recursive(0)

    def _repr_recursive(self, indent: int) -> str:
        """Recursively build string representation of schema tree."""
        spaces = '  ' * indent
        if self.is_group():
            result = f'{spaces}Group({self.name})'
            if self.children:
                result += ' {\n'
                for child in self.children.values():
                    result += child._repr_recursive(indent + 1) + '\n'
                result += spaces + '}'
            return result
        rep = f' {self.repetition.name}' if self.repetition else ''
        logical = f' [{self.converted_type.name}]' if self.converted_type else ''
        type_name = self.type.name if self.type else 'UNKNOWN'
        type_name = 'UNKNOWN' if self.type is None else self.type.name
        return f'{spaces}Column({self.name}: {type_name}{rep}{logical})'


@dataclass
class ColumnStatistics:
    """Column statistics for predicate pushdown."""

    min_value: str | int | float | bool | None = None
    max_value: str | int | float | bool | None = None
    null_count: int | None = None
    distinct_count: int | None = None


@dataclass
class ColumnMetadata:
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


@dataclass
class ColumnChunk:
    file_offset: int
    meta_data: ColumnMetadata | None = None
    file_path: str | None = None


@dataclass
class RowGroup:
    columns: list[ColumnChunk]
    total_byte_size: int
    num_rows: int

    def column_names(self) -> list[str]:
        return [col.meta_data.path_in_schema for col in self.columns if col.meta_data]

    def get_stats(self) -> RowGroupStats:
        """Calculate statistics for this row group."""
        total_compressed = 0
        total_uncompressed = 0

        for col in self.columns:
            if col.meta_data:
                total_compressed += col.meta_data.total_compressed_size
                total_uncompressed += col.meta_data.total_uncompressed_size

        compression = CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

        return RowGroupStats(
            num_rows=self.num_rows,
            num_columns=len(self.columns),
            total_byte_size=self.total_byte_size,
            compression=compression,
        )


@dataclass
class FileMetadata:
    version: int
    schema: SchemaElement
    num_rows: int
    row_groups: list[RowGroup]
    created_by: str | None = None
    key_value_metadata: dict[str, str] = field(default_factory=dict)

    def get_stats(self) -> FileStats:
        """Calculate overall file statistics."""
        total_compressed = 0
        total_uncompressed = 0
        total_columns = 0
        min_rows = float('inf')
        max_rows = 0

        row_group_stats = []

        for rg in self.row_groups:
            rg_stats = rg.get_stats()
            row_group_stats.append(rg_stats)

            total_compressed += rg_stats.compression.total_compressed
            total_uncompressed += rg_stats.compression.total_uncompressed
            total_columns += rg_stats.num_columns
            min_rows = min(min_rows, rg_stats.num_rows)
            max_rows = max(max_rows, rg_stats.num_rows)

        # Handle case with no row groups
        if not self.row_groups:
            min_rows = 0

        compression = CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

        return FileStats(
            version=self.version,
            created_by=self.created_by,
            total_rows=self.num_rows,
            num_row_groups=len(self.row_groups),
            total_columns=total_columns,
            min_rows_per_group=int(min_rows),
            max_rows_per_group=max_rows,
            compression=compression,
            row_group_stats=row_group_stats,
        )

    @staticmethod
    def _validate_parquet_magic(magic: bytes) -> None:
        """Validate that magic bytes match Parquet format."""
        if magic != PARQUET_MAGIC:
            raise ParquetMagicError(
                'Invalid Parquet magic bytes: '
                f'expected {PARQUET_MAGIC!r}, got {magic!r}',
            )

    @classmethod
    def from_file(cls, file_path: Path | str) -> 'FileMetadata':
        file_path = Path(file_path)
        with file_path.open('rb') as f:
            return cls.from_buffer(f)

    @classmethod
    def from_url(cls, url: str) -> 'FileMetadata':
        file_size = http.get_length(url)

        footer_bytes = http.get_bytes(url, file_size - FOOTER_SIZE, file_size)
        footer_length = struct.unpack('<I', footer_bytes[:4])[0]
        magic = footer_bytes[4:8]

        cls._validate_parquet_magic(magic)

        return cls.from_bytes(
            http.get_bytes(
                url,
                file_size - FOOTER_SIZE - footer_length,
                file_size - FOOTER_SIZE,
            ),
        )

    @classmethod
    def from_buffer(cls, buffer: BinaryIO) -> 'FileMetadata':
        buffer.seek(-FOOTER_SIZE, os.SEEK_END)
        footer_data = buffer.read(FOOTER_SIZE)
        footer_length = struct.unpack('<I', footer_data[:4])[0]
        magic = footer_data[4:8]

        cls._validate_parquet_magic(magic)

        buffer.seek(-(FOOTER_SIZE + footer_length), os.SEEK_END)
        footer_bytes = buffer.read(footer_length)

        return cls.from_bytes(footer_bytes)

    @classmethod
    def from_bytes(cls, footer_bytes: bytes) -> 'FileMetadata':
        from .readers import MetadataReader

        reader = MetadataReader(footer_bytes)
        return reader()

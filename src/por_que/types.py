import os
import struct

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import (
    Compression,
    Encoding,
    Repetition,
    Type,
)
from .util import http


@dataclass
class SchemaElement:
    name: str
    type: Type | None = None
    repetition: Repetition | None = None
    num_children: int = 0
    converted_type: int | None = None
    type_length: int | None = None

    def is_group(self) -> bool:
        return self.type is None

    def __repr__(self):
        if self.is_group():
            return f'Group({self.name}, children={self.num_children})'
        rep = f' {self.repetition.name}' if self.repetition else ''
        return f'Column({self.name}: {self.type.name if self.type else "group"}{rep})'


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


@dataclass
class FileMetadata:
    version: int
    schema: list[SchemaElement]
    num_rows: int
    row_groups: list[RowGroup]
    created_by: str | None = None
    key_value_metadata: dict[str, str] | None = None

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

        if magic != PARQUET_MAGIC:
            raise ValueError(f'Not a Parquet file (magic: {magic!r})')

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

        if magic != PARQUET_MAGIC:
            raise ValueError(f'Not a Parquet file (magic: {magic!r})')

        buffer.seek(-(FOOTER_SIZE + footer_length), os.SEEK_END)
        footer_bytes = buffer.read(footer_length)

        return cls.from_bytes(footer_bytes)

    @classmethod
    def from_bytes(cls, footer_bytes: bytes) -> 'FileMetadata':
        from .readers import MetadataReader

        reader = MetadataReader(footer_bytes)
        return reader()

    # TODO: refine/refactor, remove noqa
    def summary(self) -> str:  # noqa: C901
        lines = [
            'Parquet File Metadata',
            '=' * 60,
            f'Version: {self.version}',
            f'Created by: {self.created_by or "unknown"}',
            f'Total rows: {self.num_rows:,}',
            f'Row groups: {len(self.row_groups)}',
            f'Schema elements: {len(self.schema)}',
        ]

        # Schema structure
        if self.schema:
            lines.append('\nSchema Structure:')
            lines.append('-' * 40)
            for i, element in enumerate(self.schema):
                if element.is_group():
                    lines.append(
                        f'  {i:2}: {element.name} '
                        f'(GROUP, {element.num_children} children)',
                    )
                else:
                    rep = f' {element.repetition.name}' if element.repetition else ''
                    type_name = element.type.name if element.type else 'unknown'
                    lines.append(f'  {i:2}: {element.name}: {type_name}{rep}')

        # Row groups summary
        if self.row_groups:
            lines.append('\nRow Groups Summary:')
            lines.append('-' * 40)
            total_compressed = 0
            total_uncompressed = 0

            for i, rg in enumerate(self.row_groups[:5]):
                lines.append(
                    f'  Group {i:2}: {rg.num_rows:,} rows, '
                    f'{len(rg.columns)} columns, '
                    f'{rg.total_byte_size:,} bytes',
                )

                # Add compression info if available
                for col in rg.columns:
                    if col.meta_data:
                        total_compressed += col.meta_data.total_compressed_size
                        total_uncompressed += col.meta_data.total_uncompressed_size

            if len(self.row_groups) > 5:
                lines.append(f'  ... and {len(self.row_groups) - 5} more groups')

            if total_uncompressed > 0:
                ratio = total_compressed / total_uncompressed
                lines.append(f'  Total compression ratio: {ratio:.3f}')

        # Key-value metadata
        if hasattr(self, 'key_value_metadata') and self.key_value_metadata:
            lines.append('\nKey-Value Metadata:')
            lines.append('-' * 40)
            for key, value in list(self.key_value_metadata.items())[
                :10
            ]:  # Show first 10
                if len(value) > 60:
                    value = value[:57] + '...'
                lines.append(f'  {key}: {value}')
            if len(self.key_value_metadata) > 10:
                lines.append(
                    f'  ... and {len(self.key_value_metadata) - 10} more entries',
                )

        return '\n'.join(lines)

    # TODO: refine/refactor, remove noqa
    def detailed_dump(self) -> str:  # noqa: C901
        """Generate a comprehensive dump of all metadata"""
        lines = [
            'PARQUET FILE DETAILED METADATA DUMP',
            '=' * 60,
            f'Version: {self.version}',
            f'Created by: {self.created_by or "unknown"}',
            f'Total rows: {self.num_rows:,}',
            f'Row groups: {len(self.row_groups)}',
            f'Schema elements: {len(self.schema)}',
            '',
        ]

        # Complete schema dump
        lines.append('SCHEMA STRUCTURE (all elements):')
        lines.append('=' * 60)
        for i, element in enumerate(self.schema):
            lines.append(f'Element {i:2}:')
            lines.append(f'  Name: {element.name}')
            if element.is_group():
                lines.append(f'  Type: GROUP ({element.num_children} children)')
            else:
                type_name = element.type.name if element.type else 'unknown'
                lines.append(f'  Type: {type_name}')
                if element.type_length:
                    lines.append(f'  Length: {element.type_length}')
            if element.repetition:
                lines.append(f'  Repetition: {element.repetition.name}')
            if element.converted_type:
                lines.append(f'  Converted type: {element.converted_type}')
            lines.append('')

        # Row groups detailed dump
        lines.append('ROW GROUPS DETAILED:')
        lines.append('=' * 60)
        for i, rg in enumerate(self.row_groups):
            lines.append(f'Row Group {i}:')
            lines.append(f'  Rows: {rg.num_rows:,}')
            lines.append(f'  Total byte size: {rg.total_byte_size:,}')
            lines.append(f'  Columns: {len(rg.columns)}')

            # Column details
            lines.append('  Column details:')
            for j, col in enumerate(rg.columns):
                lines.append(f'    Column {j}:')
                lines.append(f'      File offset: {col.file_offset:,}')
                if col.file_path:
                    lines.append(f'      File path: {col.file_path}')

                if col.meta_data:
                    meta = col.meta_data
                    lines.append(f'      Path in schema: {meta.path_in_schema}')
                    lines.append(f'      Type: {meta.type.name}')
                    lines.append(f'      Encodings: {[e.name for e in meta.encodings]}')
                    lines.append(f'      Codec: {meta.codec.name}')
                    lines.append(f'      Values: {meta.num_values:,}')
                    lines.append(
                        f'      Uncompressed: {meta.total_uncompressed_size:,} bytes',
                    )
                    lines.append(
                        f'      Compressed: {meta.total_compressed_size:,} bytes',
                    )
                    if meta.total_uncompressed_size > 0:
                        ratio = (
                            meta.total_compressed_size / meta.total_uncompressed_size
                        )
                        lines.append(f'      Compression ratio: {ratio:.3f}')
                    lines.append(f'      Data page offset: {meta.data_page_offset:,}')
                    if meta.dictionary_page_offset:
                        lines.append(
                            f'      Dictionary offset: {meta.dictionary_page_offset:,}',
                        )
                    if meta.index_page_offset:
                        lines.append(f'      Index offset: {meta.index_page_offset:,}')
            lines.append('')

            # Only show first 3 row groups in detail to avoid too much output
            if i >= 2:
                remaining = len(self.row_groups) - i - 1
                if remaining > 0:
                    lines.append(
                        f'... and {remaining} more row groups with similar structure',
                    )
                break

        # Key-value metadata
        if hasattr(self, 'key_value_metadata') and self.key_value_metadata:
            lines.append('KEY-VALUE METADATA:')
            lines.append('=' * 60)
            for key, value in self.key_value_metadata.items():
                lines.append(f'{key}: {value}')
            lines.append('')

        return '\n'.join(lines)

from __future__ import annotations

import json
import struct

from enum import StrEnum
from io import SEEK_END
from pathlib import Path
from typing import Any, Literal, Self, assert_never

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._version import get_version
from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import Compression
from .exceptions import ParquetFormatError
from .logical import (
    ColumnChunk,
    ColumnIndex,
    FileMetadata,
    OffsetIndex,
    SchemaRoot,
)
from .pages import (
    AnyDataPage,
    DataPageV1,
    DataPageV2,
    DictionaryPage,
    IndexPage,
    Page,
)
from .parsers.parquet.metadata import MetadataParser
from .protocols import ReadableSeekable


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


class PorQueMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    format_version: Literal[0] = 0
    por_que_version: str = get_version()


class PhysicalPageIndex(BaseModel):
    """Physical location and parsed content of Page Index data."""

    model_config = ConfigDict(frozen=True)

    column_index_offset: int
    column_index_length: int
    column_index: ColumnIndex
    offset_index: OffsetIndex

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        column_index_offset: int,
    ) -> Self:
        """Parse Page Index data from file location."""
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        reader.seek(column_index_offset)

        # Read buffer for index data (Page indexes are typically small)
        index_buffer = reader.read(65536)
        parser = ThriftCompactParser(index_buffer)
        page_index_parser = PageIndexParser(parser)

        # Parse ColumnIndex first
        column_index = page_index_parser.read_column_index()

        # Parse OffsetIndex second (should immediately follow)
        offset_index = page_index_parser.read_offset_index()

        return cls(
            column_index_offset=column_index_offset,
            column_index_length=parser.pos,
            column_index=column_index,
            offset_index=offset_index,
        )


class PhysicalColumnChunk(BaseModel):
    """A container for all the data for a single column within a row group."""

    model_config = ConfigDict(frozen=True)

    path_in_schema: str
    start_offset: int
    total_byte_size: int
    codec: Compression
    num_values: int
    data_pages: list[AnyDataPage]
    index_pages: list[IndexPage]
    dictionary_page: DictionaryPage | None
    metadata: ColumnChunk = Field(exclude=True)
    page_index: PhysicalPageIndex | None = None

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        chunk_metadata: ColumnChunk,
        schema_root: SchemaRoot,
    ) -> Self:
        """Parses all pages within a column chunk from a reader."""
        data_pages = []
        index_pages = []
        dictionary_page = None

        # The file_offset on the ColumnChunk struct can be misleading.
        # The actual start of the page data is the minimum of the page offsets.
        start_offset = chunk_metadata.data_page_offset
        if chunk_metadata.dictionary_page_offset is not None:
            start_offset = min(start_offset, chunk_metadata.dictionary_page_offset)

        current_offset = start_offset
        # The total_compressed_size is for all pages in the chunk.
        chunk_end_offset = start_offset + chunk_metadata.total_compressed_size

        # Read all pages sequentially within the column chunk's byte range
        while current_offset < chunk_end_offset:
            page = Page.from_reader(reader, current_offset, schema_root, chunk_metadata)

            # Sort pages by type
            if isinstance(page, DictionaryPage):
                if dictionary_page is not None:
                    raise ValueError('Multiple dictionary pages found in column chunk')
                dictionary_page = page
            elif isinstance(
                page,
                DataPageV1 | DataPageV2,
            ):
                data_pages.append(page)
            elif isinstance(page, IndexPage):
                index_pages.append(page)

            # Move to next page using the page size information
            current_offset = (
                page.start_offset + page.header_size + page.compressed_page_size
            )

        # Load Page Index if available
        page_index = None
        if chunk_metadata.column_index_offset is not None:
            page_index = PhysicalPageIndex.from_reader(
                reader,
                chunk_metadata.column_index_offset,
            )

        return cls(
            path_in_schema=chunk_metadata.path_in_schema,
            start_offset=start_offset,
            total_byte_size=chunk_metadata.total_compressed_size,
            codec=chunk_metadata.codec,
            num_values=chunk_metadata.num_values,
            data_pages=data_pages,
            index_pages=index_pages,
            dictionary_page=dictionary_page,
            metadata=chunk_metadata,
            page_index=page_index,
        )


class PhysicalMetadata(BaseModel):
    """The physical layout of the file metadata within the file."""

    model_config = ConfigDict(frozen=True)

    start_offset: int
    total_byte_size: int
    metadata: FileMetadata

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
    ) -> Self:
        reader.seek(-FOOTER_SIZE, SEEK_END)
        footer_start = reader.tell()
        footer_bytes = reader.read(FOOTER_SIZE)
        magic_footer = footer_bytes[4:8]

        if magic_footer != PARQUET_MAGIC:
            raise ParquetFormatError(
                'Invalid magic footer: expected '
                f'{PARQUET_MAGIC!r}, got {magic_footer!r}',
            )

        metadata_size = struct.unpack('<I', footer_bytes[:4])[0]

        # Read and parse metadata
        metadata_start = footer_start - metadata_size
        reader.seek(metadata_start)
        metadata_bytes = reader.read(metadata_size)
        metadata = MetadataParser(metadata_bytes).parse()

        return cls(
            start_offset=metadata_start,
            total_byte_size=metadata_size,
            metadata=metadata,
        )


class ParquetFile(BaseModel):
    """The root object representing the entire physical file structure."""

    model_config = ConfigDict(frozen=True)

    source: str
    filesize: int
    column_chunks: list[PhysicalColumnChunk]
    metadata: PhysicalMetadata
    magic_header: str = PARQUET_MAGIC.decode()
    magic_footer: str = PARQUET_MAGIC.decode()
    meta_info: PorQueMeta = Field(
        default_factory=PorQueMeta,
        alias='_meta',
        description='Metadata about the por-que serialization format',
    )

    @model_validator(mode='before')
    @classmethod
    def inject_metadata_references(cls, data: Any) -> Any:
        """Inject metadata references into column chunks during validation."""
        if not isinstance(data, dict):
            return data

        # If we already have a metadata field, use it to inject references
        if 'metadata' in data and 'column_chunks' in data:
            # First validate the metadata structure
            metadata_dict = data['metadata']
            if isinstance(metadata_dict, dict) and 'metadata' in metadata_dict:
                file_metadata_dict = metadata_dict['metadata']

                # Process each column chunk to add metadata reference
                updated_chunks = []
                for chunk_data in data['column_chunks']:
                    if isinstance(chunk_data, dict) and 'path_in_schema' in chunk_data:
                        # Find and inject the logical metadata reference
                        logical_chunk = cls._find_logical_chunk_from_dict(
                            file_metadata_dict,
                            chunk_data['path_in_schema'],
                        )
                        # Create new chunk data with metadata reference
                        chunk_with_metadata = {**chunk_data, 'metadata': logical_chunk}
                        updated_chunks.append(chunk_with_metadata)
                    else:
                        updated_chunks.append(chunk_data)

                # Update the data with injected metadata
                data = {**data, 'column_chunks': updated_chunks}

        return data

    @staticmethod
    def _find_logical_chunk_from_dict(
        file_metadata_dict: dict,
        path_in_schema: str,
    ) -> dict:
        """Find the logical ColumnChunk metadata dict for a given path."""
        if 'row_groups' not in file_metadata_dict:
            raise ValueError('No row_groups found in file metadata')

        for row_group_dict in file_metadata_dict['row_groups']:
            if 'column_chunks' in row_group_dict:
                column_chunks_dict = row_group_dict['column_chunks']
                if path_in_schema in column_chunks_dict:
                    return column_chunks_dict[path_in_schema]

        raise ValueError(f'Could not find logical metadata for column {path_in_schema}')

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        source: Path | str,
    ) -> Self:
        reader.seek(0, SEEK_END)
        filesize = reader.tell()

        if filesize < 12:
            raise ParquetFormatError('Parquet file is too small to be valid')

        phy_metadata = PhysicalMetadata.from_reader(reader)
        column_chunks = cls._parse_column_chunks(reader, phy_metadata.metadata)

        return cls(
            source=str(source),
            filesize=filesize,
            column_chunks=column_chunks,
            metadata=phy_metadata,
        )

    @classmethod
    def _parse_column_chunks(
        cls,
        file_obj: ReadableSeekable,
        metadata: FileMetadata,
    ) -> list[PhysicalColumnChunk]:
        column_chunks = []
        schema_root = metadata.schema_root

        # Iterate through all row groups and their column chunks
        for row_group_metadata in metadata.row_groups:
            for chunk_metadata in row_group_metadata.column_chunks.values():
                column_chunk = PhysicalColumnChunk.from_reader(
                    reader=file_obj,
                    chunk_metadata=chunk_metadata,
                    schema_root=schema_root,
                )
                column_chunks.append(column_chunk)

        return column_chunks

    def to_dict(self, target: AsdictTarget = AsdictTarget.DICT) -> dict[str, Any]:
        match target:
            case AsdictTarget.DICT:
                return self.model_dump()
            case AsdictTarget.JSON:
                return self.model_dump(mode='json')
            case _:
                assert_never(target)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        data = json.loads(json_str)
        return cls.from_dict(data)

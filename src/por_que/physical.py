from __future__ import annotations

import copy
import json
import struct

from dataclasses import asdict, dataclass
from enum import StrEnum
from io import SEEK_END
from pathlib import Path
from typing import Any, Literal, Self, assert_never

from . import logical
from ._version import get_version
from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import Compression
from .exceptions import ParquetFormatError
from .pages import AnyDataPage, DataPageV1, DataPageV2, DictionaryPage, IndexPage, Page
from .parsers.parquet.metadata import MetadataParser
from .protocols import ReadableSeekable
from .serialization import create_converter, structure_single_data_page


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


@dataclass(frozen=True)
class PhysicalPageIndex:
    """Physical location and parsed content of Page Index data."""

    column_index_offset: int
    column_index_length: int
    column_index: logical.ColumnIndex
    offset_index: logical.OffsetIndex

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


@dataclass(frozen=True)
class PhysicalColumnChunk:
    """A container for all the data for a single column within a row group."""

    path_in_schema: str
    start_offset: int
    total_byte_size: int
    codec: Compression
    num_values: int
    data_pages: list[AnyDataPage]
    index_pages: list[IndexPage]
    dictionary_page: DictionaryPage | None
    metadata: logical.ColumnChunk
    page_index: PhysicalPageIndex | None = None

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        chunk_metadata: logical.ColumnChunk,
        schema_root: logical.SchemaRoot,
    ) -> Self:
        """Parses all pages within a column chunk from a reader."""
        data_pages = []
        index_pages = []
        dictionary_page = None

        # The file_offset on the ColumnChunk struct can be misleading.
        # The actual start of the page data is the minimum of the specific page offsets.
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


@dataclass(frozen=True)
class PhysicalMetadata:
    """The physical layout of the file metadata within the file."""

    start_offset: int
    total_byte_size: int
    metadata: logical.FileMetadata

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


@dataclass(frozen=True)
class ParquetFile:
    """The root object representing the entire physical file structure."""

    source: str
    filesize: int
    column_chunks: list[PhysicalColumnChunk]
    metadata: PhysicalMetadata
    magic_header: str = PARQUET_MAGIC.decode()
    magic_footer: str = PARQUET_MAGIC.decode()

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
        metadata: logical.FileMetadata,
    ) -> list[PhysicalColumnChunk]:
        column_chunks = []
        schema_root = metadata.schema

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
                return asdict(self)
            case AsdictTarget.JSON:
                return create_converter().unstructure(self)
            case _:
                assert_never(target)

    def to_json(self, **kwargs) -> str:
        dump = self.to_dict(target=AsdictTarget.JSON)
        dump['_meta'] = asdict(PorQueMeta())
        return json.dumps(dump, **kwargs)

    @classmethod
    def from_dict(cls, data: dict[str, Any], deepcopy: bool = True) -> Self:
        # deepcopy is optional in case the external reference
        # will be discarded, e.g., our from_json method
        if deepcopy:
            # we want to deepcopy to ensure external modification don't
            # mess up the ParquetFile data references, and so that our
            # modifications don't mess something up externally
            data = copy.deepcopy(data)

        data.pop('_meta', None)

        converter = create_converter()

        # First deserialize the metadata structure
        metadata = converter.structure(data['metadata'], PhysicalMetadata)

        # Now deserialize column chunks with their proper metadata references
        column_chunks = []
        for chunk_data in data['column_chunks']:
            # Find the logical metadata for this chunk
            logical_chunk = cls._find_logical_chunk(
                metadata.metadata,
                chunk_data['path_in_schema'],
            )

            # Create the chunk with the proper metadata reference
            chunk = PhysicalColumnChunk(
                path_in_schema=chunk_data['path_in_schema'],
                start_offset=chunk_data['start_offset'],
                total_byte_size=chunk_data['total_byte_size'],
                codec=Compression(chunk_data['codec']),
                num_values=chunk_data['num_values'],
                data_pages=[
                    structure_single_data_page(converter, p)
                    for p in chunk_data['data_pages']
                    if isinstance(p, dict)
                ],
                index_pages=[
                    converter.structure(p, IndexPage) for p in chunk_data['index_pages']
                ],
                dictionary_page=converter.structure(
                    chunk_data['dictionary_page'],
                    DictionaryPage,
                )
                if chunk_data.get('dictionary_page')
                else None,
                metadata=logical_chunk,
                page_index=converter.structure(
                    chunk_data['page_index'],
                    PhysicalPageIndex,
                )
                if chunk_data.get('page_index')
                else None,
            )
            column_chunks.append(chunk)

        # Deserialize the rest of the fields
        return cls(
            source=data['source'],
            filesize=data['filesize'],
            column_chunks=column_chunks,
            metadata=metadata,
            magic_header=data.get('magic_header', PARQUET_MAGIC.decode()),
            magic_footer=data.get('magic_footer', PARQUET_MAGIC.decode()),
        )

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        data = json.loads(json_str)
        return cls.from_dict(data, deepcopy=False)

    @staticmethod
    def _find_logical_chunk(
        file_metadata: logical.FileMetadata,
        path_in_schema: str,
    ) -> logical.ColumnChunk:
        """Find the logical ColumnChunk metadata for a given path."""
        for row_group in file_metadata.row_groups:
            if path_in_schema in row_group.column_chunks:
                return row_group.column_chunks[path_in_schema]
        raise ValueError(f'Could not find logical metadata for column {path_in_schema}')


@dataclass(frozen=True)
class PorQueMeta:
    format_version: Literal[0] = 0
    por_que_version: str = get_version()

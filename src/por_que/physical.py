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
from .enums import Compression, Encoding, PageType
from .exceptions import ParquetFormatError
from .parsers.parquet.metadata import MetadataParser
from .parsers.parquet.page import PageParser
from .parsers.thrift.parser import ThriftCompactParser
from .protocols import ReadableSeekable
from .serialization import create_converter, structure_single_data_page


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


@dataclass(frozen=True)
class PageLayout:
    """Generic representation of a page, the smallest unit of storage."""

    page_type: PageType
    start_offset: int
    page_header_size: int
    compressed_page_size: int
    uncompressed_page_size: int
    crc: int | None

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        offset: int,
        schema_context: logical.SchemaElement,
    ) -> AnyPageLayout:
        """Factory method to parse and return the correct Page subtype."""
        reader.seek(offset)

        # Read page header using existing infrastructure
        # Read a reasonably sized buffer, assuming headers are not huge.
        # 8KB should be more than enough for any page header.
        header_buffer = reader.read(8192)
        parser = ThriftCompactParser(header_buffer)
        page_parser = PageParser(parser, schema_context)

        page_header = page_parser.read_page_header()
        header_size = parser.pos  # Size of the header we just read

        # Delegate to appropriate subclass based on page type
        match page_header.type:
            case PageType.DICTIONARY_PAGE:
                return DictionaryPageLayout._from_header(
                    page_header,
                    offset,
                    header_size,
                )
            case PageType.DATA_PAGE:
                return DataPageV1Layout._from_header(
                    page_header,
                    offset,
                    header_size,
                )
            case PageType.DATA_PAGE_V2:
                return DataPageV2Layout._from_header(
                    page_header,
                    offset,
                    header_size,
                )
            case PageType.INDEX_PAGE:
                return IndexPageLayout._from_header(
                    page_header,
                    offset,
                    header_size,
                )
            case _:
                raise ValueError(f'Unknown page type: {page_header.type}')


@dataclass(frozen=True)
class DictionaryPageLayout(PageLayout):
    """A page containing dictionary-encoded values."""

    num_values: int
    encoding: Encoding
    metadata: logical.DictionaryPageHeader

    @classmethod
    def _from_header(
        cls,
        page_header: logical.PageHeader,
        offset: int,
        header_size: int,
    ) -> Self:
        """Create DictionaryPageLayout from parsed page header."""
        dict_header = page_header.dictionary_page_header
        if dict_header is None:
            raise ValueError('Missing dictionary page header')

        return cls(
            page_type=page_header.type,
            start_offset=offset,
            page_header_size=header_size,
            compressed_page_size=page_header.compressed_page_size,
            uncompressed_page_size=page_header.uncompressed_page_size,
            crc=page_header.crc,
            num_values=dict_header.num_values,
            encoding=dict_header.encoding,
            metadata=dict_header,
        )


@dataclass(frozen=True)
class DataPageV1Layout(PageLayout):
    """A version 1 data page."""

    num_values: int
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding
    metadata: logical.DataPageHeader

    @classmethod
    def _from_header(
        cls,
        page_header: logical.PageHeader,
        offset: int,
        header_size: int,
    ) -> Self:
        """Create DataPageV1Layout from parsed page header."""
        data_header = page_header.data_page_header
        if data_header is None:
            raise ValueError('Missing data page header')

        return cls(
            page_type=page_header.type,
            start_offset=offset,
            page_header_size=header_size,
            compressed_page_size=page_header.compressed_page_size,
            uncompressed_page_size=page_header.uncompressed_page_size,
            crc=page_header.crc,
            num_values=data_header.num_values,
            encoding=data_header.encoding,
            definition_level_encoding=data_header.definition_level_encoding,
            repetition_level_encoding=data_header.repetition_level_encoding,
            metadata=data_header,
        )


@dataclass(frozen=True)
class DataPageV2Layout(PageLayout):
    """A version 2 data page."""

    num_values: int
    num_nulls: int
    num_rows: int
    encoding: Encoding
    definition_levels_byte_length: int
    repetition_levels_byte_length: int
    statistics: Any | None
    metadata: logical.DataPageHeaderV2

    @classmethod
    def _from_header(
        cls,
        page_header: logical.PageHeader,
        offset: int,
        header_size: int,
    ) -> Self:
        """Create DataPageV2Layout from parsed page header."""
        data_header_v2 = page_header.data_page_header_v2
        if data_header_v2 is None:
            raise ValueError('Missing data page header v2')

        return cls(
            page_type=page_header.type,
            start_offset=offset,
            page_header_size=header_size,
            compressed_page_size=page_header.compressed_page_size,
            uncompressed_page_size=page_header.uncompressed_page_size,
            crc=page_header.crc,
            num_values=data_header_v2.num_values,
            num_nulls=data_header_v2.num_nulls,
            num_rows=data_header_v2.num_rows,
            encoding=data_header_v2.encoding,
            definition_levels_byte_length=data_header_v2.definition_levels_byte_length,
            repetition_levels_byte_length=data_header_v2.repetition_levels_byte_length,
            statistics=None,  # TODO: Parse statistics if present
            metadata=data_header_v2,
        )


@dataclass(frozen=True)
class IndexPageLayout(PageLayout):
    """A page containing row group and offset statistics."""

    page_locations: Any | None

    @classmethod
    def _from_header(
        cls,
        page_header: logical.PageHeader,
        offset: int,
        header_size: int,
    ) -> Self:
        """Create IndexPageLayout from parsed page header."""
        return cls(
            page_type=page_header.type,
            start_offset=offset,
            page_header_size=header_size,
            compressed_page_size=page_header.compressed_page_size,
            uncompressed_page_size=page_header.uncompressed_page_size,
            crc=page_header.crc,
            page_locations=None,  # TODO: Parse page locations if needed
        )


AnyPageLayout = (
    DictionaryPageLayout | DataPageV1Layout | DataPageV2Layout | IndexPageLayout
)


@dataclass(frozen=True)
class PhysicalColumnChunk:
    """A container for all the data for a single column within a row group."""

    path_in_schema: str
    start_offset: int
    total_byte_size: int
    codec: Compression
    num_values: int
    data_pages: list[DataPageV1Layout | DataPageV2Layout]
    index_pages: list[IndexPageLayout]
    dictionary_page: DictionaryPageLayout | None
    metadata: logical.ColumnChunk

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        schema_context: logical.SchemaElement,
        chunk_metadata: logical.ColumnChunk,
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
            page = PageLayout.from_reader(reader, current_offset, schema_context)

            # Sort pages by type
            if isinstance(page, DictionaryPageLayout):
                if dictionary_page is not None:
                    raise ValueError('Multiple dictionary pages found in column chunk')
                dictionary_page = page
            elif isinstance(
                page,
                DataPageV1Layout | DataPageV2Layout,
            ):
                data_pages.append(page)
            elif isinstance(page, IndexPageLayout):
                index_pages.append(page)

            # Move to next page using the page size information
            current_offset = (
                page.start_offset + page.page_header_size + page.compressed_page_size
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
                # Use the helper method to find the schema element
                schema_element_for_chunk = schema_root.find_element(
                    chunk_metadata.path_in_schema,
                )

                column_chunk = PhysicalColumnChunk.from_reader(
                    reader=file_obj,
                    schema_context=schema_element_for_chunk,
                    chunk_metadata=chunk_metadata,
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
                    converter.structure(p, IndexPageLayout)
                    for p in chunk_data['index_pages']
                ],
                dictionary_page=converter.structure(
                    chunk_data['dictionary_page'],
                    DictionaryPageLayout,
                )
                if chunk_data.get('dictionary_page')
                else None,
                metadata=logical_chunk,
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

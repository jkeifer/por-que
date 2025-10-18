"""
Unified page models that combine logical content with physical layout information.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator

from .enums import Compression, Encoding, PageType, Type
from .file_metadata import (
    ColumnChunk,
    ColumnStatistics,
    SchemaLeaf,
    SchemaRoot,
)
from .parsers.page_content import (
    DataPageV1Parser,
    DataPageV2Parser,
    DictionaryPageParser,
    DictType,
    ValueTuple,
)
from .parsers.parquet.page import PageParser
from .parsers.thrift.parser import ThriftCompactParser
from .protocols import AsyncReadableSeekable


class Page(BaseModel, frozen=True):
    """
    Base class for all page types, containing both logical and physical
    information.
    """

    page_type: PageType
    start_offset: int
    header_size: int
    compressed_page_size: int
    uncompressed_page_size: int
    crc: int | None = None

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        offset: int,
        schema_root: SchemaRoot,
        chunk_metadata: ColumnChunk | None = None,
    ) -> AnyPage:
        """Factory method to parse and return the correct Page subtype."""
        reader.seek(offset)

        parser = ThriftCompactParser(reader, offset)

        column_type = None
        path_in_schema = None

        if chunk_metadata is not None:
            column_type = chunk_metadata.type
            path_in_schema = chunk_metadata.path_in_schema

        page_parser = PageParser(
            parser,
            schema_root,
            column_type,
            path_in_schema,
        )

        return await page_parser.read_page()


class DictionaryPage(Page, frozen=True):
    """A page containing dictionary-encoded values."""

    page_type: Literal[PageType.DICTIONARY_PAGE] = PageType.DICTIONARY_PAGE
    num_values: int
    encoding: Encoding
    is_sorted: bool = False

    async def parse_content(
        self,
        reader: AsyncReadableSeekable,
        physical_type: Type,
        compression_codec: Compression,
        schema_element,
    ) -> DictType:
        """Parse the dictionary page content into Python objects.

        Args:
            reader: File-like object to read from
            physical_type: Physical type of the dictionary values
            compression_codec: Compression codec used
            schema_element: Schema element with type information

        Returns:
            List of dictionary values as Python objects
        """
        parser = DictionaryPageParser()
        return await parser.parse_content(
            reader=reader,
            dictionary_page=self,
            physical_type=physical_type,
            compression_codec=compression_codec,
            schema_element=schema_element,
        )


class DataPageV1(Page, frozen=True):
    """A version 1 data page."""

    page_type: Literal[PageType.DATA_PAGE] = PageType.DATA_PAGE
    num_values: int
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding
    statistics: ColumnStatistics | None = None

    async def parse_content(
        self,
        reader: AsyncReadableSeekable,
        physical_type: Type,
        compression_codec: Compression,
        schema_element: SchemaLeaf,
        dictionary_values: list[Any] | None = None,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> Iterator[ValueTuple]:
        """Parse the data page content into Python objects.

        Args:
            reader: File-like object to read from
            physical_type: Physical type of the data values
            compression_codec: Compression codec used
            schema_element: Schema element for repetition/definition info
            dictionary_values: Dictionary values if dictionary-encoded

        Returns:
            List of data values as Python objects
        """
        return await DataPageV1Parser(
            reader=reader,
            data_page=self,
            physical_type=physical_type,
            compression_codec=compression_codec,
            schema_element=schema_element,
            dictionary_values=dictionary_values,
        ).parse(
            excluded_logical_columns=excluded_logical_columns,
        )


class DataPageV2(Page, frozen=True):
    """A version 2 data page."""

    page_type: Literal[PageType.DATA_PAGE_V2] = PageType.DATA_PAGE_V2
    num_values: int
    num_nulls: int
    num_rows: int
    encoding: Encoding
    definition_levels_byte_length: int
    repetition_levels_byte_length: int
    is_compressed: bool = True
    statistics: ColumnStatistics | None = None

    async def parse_content(
        self,
        reader: AsyncReadableSeekable,
        physical_type: Type,
        compression_codec: Compression,
        schema_element: SchemaLeaf,
        dictionary_values: list[Any] | None = None,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> Iterator[ValueTuple]:
        """Parse the data page content into Python objects.

        Args:
            reader: File-like object to read from
            physical_type: Physical type of the data values
            compression_codec: Compression codec used
            schema_element: Schema element for repetition/definition info
            dictionary_values: Dictionary values if dictionary-encoded

        Returns:
            List of data values as Python objects
        """
        return await DataPageV2Parser(
            reader=reader,
            data_page=self,
            physical_type=physical_type,
            compression_codec=compression_codec,
            schema_element=schema_element,
            dictionary_values=dictionary_values,
        ).parse(
            excluded_logical_columns=excluded_logical_columns,
        )


class IndexPage(Page, frozen=True):
    """A page containing row group and offset statistics."""

    page_type: Literal[PageType.INDEX_PAGE] = PageType.INDEX_PAGE


AnyPage = DictionaryPage | DataPageV1 | DataPageV2 | IndexPage
AnyDataPage = DataPageV1 | DataPageV2

AnyPageDiscriminated = Annotated[
    AnyPage,
    Discriminator('page_type'),
]

AnyDataPageDiscriminated = Annotated[
    AnyDataPage,
    Discriminator('page_type'),
]

"""
Unified page models that combine logical content with physical layout information.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator

from .enums import Encoding, PageType
from .file_metadata import (
    ColumnChunk,
    ColumnStatistics,
    SchemaRoot,
)
from .protocols import ReadableSeekable


class Page(BaseModel):
    """
    Base class for all page types, containing both logical and physical
    information.
    """

    model_config = ConfigDict(frozen=True)

    # Physical layout information (where in file, sizes)
    page_type: PageType
    start_offset: int
    header_size: int
    compressed_page_size: int
    uncompressed_page_size: int
    crc: int | None

    @classmethod
    def from_reader(
        cls,
        reader: ReadableSeekable,
        offset: int,
        schema_root: SchemaRoot,
        chunk_metadata: ColumnChunk | None = None,
    ) -> AnyPage:
        """Factory method to parse and return the correct Page subtype."""
        from .parsers.parquet.page import PageParser
        from .parsers.thrift.parser import ThriftCompactParser

        reader.seek(offset)

        # Parse page header directly from file
        parser = ThriftCompactParser(reader, offset)

        # Extract column type and path from metadata if available
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

        # PageParser will now directly return the appropriate Page subtype
        return page_parser.read_page(offset)


class DictionaryPage(Page):
    """A page containing dictionary-encoded values."""

    page_type: Literal[PageType.DICTIONARY_PAGE] = PageType.DICTIONARY_PAGE
    # Logical content from DictionaryPageHeader
    num_values: int
    encoding: Encoding
    is_sorted: bool = False


class DataPageV1(Page):
    """A version 1 data page."""

    page_type: Literal[PageType.DATA_PAGE] = PageType.DATA_PAGE
    # Logical content from DataPageHeader
    num_values: int
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding
    statistics: ColumnStatistics | None = None


class DataPageV2(Page):
    """A version 2 data page."""

    page_type: Literal[PageType.DATA_PAGE_V2] = PageType.DATA_PAGE_V2
    # Logical content from DataPageHeaderV2
    num_values: int
    num_nulls: int
    num_rows: int
    encoding: Encoding
    definition_levels_byte_length: int
    repetition_levels_byte_length: int
    is_compressed: bool
    statistics: ColumnStatistics | None = None


class IndexPage(Page):
    """A page containing row group and offset statistics."""

    page_type: Literal[PageType.INDEX_PAGE] = PageType.INDEX_PAGE
    # Logical content (minimal for now)
    page_locations: Any | None = None


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

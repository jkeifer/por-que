"""
Unified page models that combine logical content with physical layout information.

This module replaces the previous separation between PageHeader (logical) and
PageLayout (physical) classes with a single unified hierarchy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .enums import Encoding, PageType
from .protocols import ReadableSeekable

if TYPE_CHECKING:
    from . import logical


@dataclass(frozen=True)
class Page:
    """
    Base class for all page types, containing both logical and physical
    information.
    """

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
        schema_context: logical.SchemaElement,
    ) -> AnyPage:
        """Factory method to parse and return the correct Page subtype."""
        from .parsers.parquet.page import PageParser
        from .parsers.thrift.parser import ThriftCompactParser

        reader.seek(offset)

        # Read page header using existing infrastructure
        # Read a reasonably sized buffer, assuming headers are not huge.
        # 8KB should be more than enough for any page header.
        header_buffer = reader.read(8192)
        parser = ThriftCompactParser(header_buffer)
        page_parser = PageParser(parser, schema_context)

        # PageParser will now directly return the appropriate Page subtype
        return page_parser.read_page(offset)


@dataclass(frozen=True)
class DictionaryPage(Page):
    """A page containing dictionary-encoded values."""

    # Logical content from DictionaryPageHeader
    num_values: int
    encoding: Encoding
    is_sorted: bool = False


@dataclass(frozen=True)
class DataPageV1(Page):
    """A version 1 data page."""

    # Logical content from DataPageHeader
    num_values: int
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding
    statistics: Any | None = None  # Will be ColumnStatistics when implemented


@dataclass(frozen=True)
class DataPageV2(Page):
    """A version 2 data page."""

    # Logical content from DataPageHeaderV2
    num_values: int
    num_nulls: int
    num_rows: int
    encoding: Encoding
    definition_levels_byte_length: int
    repetition_levels_byte_length: int
    is_compressed: bool
    statistics: Any | None = None  # Will be ColumnStatistics when implemented


@dataclass(frozen=True)
class IndexPage(Page):
    """A page containing row group and offset statistics."""

    # Logical content (minimal for now)
    page_locations: Any | None = None


# Union type for all page types
AnyPage = DictionaryPage | DataPageV1 | DataPageV2 | IndexPage
AnyDataPage = DataPageV1 | DataPageV2

"""
Page-level parsing for Parquet data pages.

Teaching Points:
- Pages are the fundamental data organization unit within column chunks
- Different page types serve different purposes (data, dictionary, index)
- Page headers contain size and encoding information needed for decompression
- Page data follows the header and may be compressed
"""

import logging

from typing import TYPE_CHECKING

from por_que.enums import Encoding, PageType
from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftStructParser

from .base import BaseParser
from .enums import (
    DataPageHeaderFieldId,
    DataPageHeaderV2FieldId,
    DictionaryPageHeaderFieldId,
    PageHeaderFieldId,
)

if TYPE_CHECKING:
    from por_que.pages import AnyPage

logger = logging.getLogger(__name__)


class PageParser(BaseParser):
    """
    Parses individual page headers and manages page data reading.

    Teaching Points:
    - Page parsing is essential for columnar data access
    - Headers describe how to interpret the following data bytes
    - Different page types require different parsing strategies
    - Compression is handled at the page level, not column level
    """

    def __init__(self, parser, schema=None):
        """
        Initialize page parser.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for statistics parsing
        """
        super().__init__(parser)
        self.schema = schema

    def read_page(self, start_offset: int) -> AnyPage:  # noqa: C901
        """
        Read a complete Page directly from the stream.

        This method combines the previous PageHeader parsing logic with direct
        Page object creation, eliminating the intermediate logical.PageHeader step.

        Args:
            start_offset: The file offset where this page begins

        Returns:
            The appropriate Page subtype (DataPageV1, DataPageV2, DictionaryPage,
            IndexPage)
        """
        from por_que.pages import DataPageV1, DataPageV2, DictionaryPage, IndexPage

        header_start_offset = self.parser.pos
        struct_parser = ThriftStructParser(self.parser)

        # Basic page header fields
        page_type = PageType.DATA_PAGE  # default
        compressed_page_size = 0
        uncompressed_page_size = 0
        crc = None

        # Page-specific fields that we'll collect
        page_specific_data = {}

        logger.debug('Reading page at offset %d', start_offset)

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle struct types for specific page headers
            if field_type == ThriftFieldType.STRUCT:
                match field_id:
                    case PageHeaderFieldId.DATA_PAGE_HEADER:
                        page_specific_data.update(self.read_data_page_header())
                    case PageHeaderFieldId.DICTIONARY_PAGE_HEADER:
                        page_specific_data.update(
                            self.read_dictionary_page_header(),
                        )
                    case PageHeaderFieldId.DATA_PAGE_HEADER_V2:
                        page_specific_data.update(
                            self.read_data_page_header_v2(),
                        )
                    case PageHeaderFieldId.INDEX_PAGE_HEADER:
                        # Index pages are not implemented yet - skip
                        struct_parser.skip_field(field_type)
                    case _:
                        struct_parser.skip_field(field_type)
                continue

            # Handle primitive fields
            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case PageHeaderFieldId.TYPE:
                    page_type = PageType(value)
                case PageHeaderFieldId.UNCOMPRESSED_PAGE_SIZE:
                    uncompressed_page_size = value
                case PageHeaderFieldId.COMPRESSED_PAGE_SIZE:
                    compressed_page_size = value
                case PageHeaderFieldId.CRC:
                    crc = value

        header_end_offset = self.parser.pos
        header_size = header_end_offset - header_start_offset

        # Create appropriate page subtype based on page type
        page: AnyPage
        match page_type:
            case PageType.DICTIONARY_PAGE:
                page = DictionaryPage(
                    page_type=page_type,
                    start_offset=start_offset,
                    header_size=header_size,
                    compressed_page_size=compressed_page_size,
                    uncompressed_page_size=uncompressed_page_size,
                    crc=crc,
                    **page_specific_data,
                )
            case PageType.DATA_PAGE:
                page = DataPageV1(
                    page_type=page_type,
                    start_offset=start_offset,
                    header_size=header_size,
                    compressed_page_size=compressed_page_size,
                    uncompressed_page_size=uncompressed_page_size,
                    crc=crc,
                    **page_specific_data,
                )
            case PageType.DATA_PAGE_V2:
                page = DataPageV2(
                    page_type=page_type,
                    start_offset=start_offset,
                    header_size=header_size,
                    compressed_page_size=compressed_page_size,
                    uncompressed_page_size=uncompressed_page_size,
                    crc=crc,
                    **page_specific_data,
                )
            case PageType.INDEX_PAGE:
                page = IndexPage(
                    page_type=page_type,
                    start_offset=start_offset,
                    header_size=header_size,
                    compressed_page_size=compressed_page_size,
                    uncompressed_page_size=uncompressed_page_size,
                    crc=crc,
                    page_locations=None,  # Not implemented yet
                )
            case _:
                from por_que.exceptions import ParquetFormatError

                raise ParquetFormatError(f'Unknown page type: {page_type}')

        logger.debug(
            'Read page: type=%s, compressed=%d bytes, uncompressed=%d bytes',
            page_type.name,
            compressed_page_size,
            uncompressed_page_size,
        )

        return page

    def read_data_page_header(self) -> dict:
        """Read DataPageHeader fields and return as dict."""
        struct_parser = ThriftStructParser(self.parser)
        fields = {
            'num_values': 0,
            'encoding': Encoding.PLAIN,
            'definition_level_encoding': Encoding.PLAIN,
            'repetition_level_encoding': Encoding.PLAIN,
            'statistics': None,
        }

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle statistics struct if present
            if field_type == ThriftFieldType.STRUCT:
                if field_id == DataPageHeaderFieldId.STATISTICS:
                    # Statistics parsing requires column type and schema path
                    # For now, skip - this would need column context
                    struct_parser.skip_field(field_type)
                else:
                    struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case DataPageHeaderFieldId.NUM_VALUES:
                    fields['num_values'] = value
                case DataPageHeaderFieldId.ENCODING:
                    fields['encoding'] = Encoding(value)
                case DataPageHeaderFieldId.DEFINITION_LEVEL_ENCODING:
                    fields['definition_level_encoding'] = Encoding(value)
                case DataPageHeaderFieldId.REPETITION_LEVEL_ENCODING:
                    fields['repetition_level_encoding'] = Encoding(value)

        return fields

    def read_data_page_header_v2(self) -> dict:
        """Read DataPageHeaderV2 fields and return as dict."""
        struct_parser = ThriftStructParser(self.parser)
        fields = self._init_data_page_v2_fields()

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.STRUCT:
                self._handle_data_page_v2_struct(struct_parser, field_id)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            self._process_data_page_v2_field(fields, field_id, value)

        return fields

    def _init_data_page_v2_fields(self) -> dict:
        """Initialize default fields for DataPageHeaderV2."""
        return {
            'num_values': 0,
            'num_nulls': 0,
            'num_rows': 0,
            'encoding': Encoding.PLAIN,
            'definition_levels_byte_length': 0,
            'repetition_levels_byte_length': 0,
            'is_compressed': True,
            'statistics': None,
        }

    def _handle_data_page_v2_struct(self, struct_parser, field_id):
        """Handle struct fields in DataPageHeaderV2."""
        if field_id == DataPageHeaderV2FieldId.STATISTICS:
            # Skip statistics for now - needs column context
            # TODO: Parse statistics if present - this is the original TODO!
            struct_parser.skip_field(ThriftFieldType.STRUCT)
        else:
            struct_parser.skip_field(ThriftFieldType.STRUCT)

    def _process_data_page_v2_field(self, fields: dict, field_id: int, value):
        """Process a single field in DataPageHeaderV2."""
        match field_id:
            case DataPageHeaderV2FieldId.NUM_VALUES:
                fields['num_values'] = value
            case DataPageHeaderV2FieldId.NUM_NULLS:
                fields['num_nulls'] = value
            case DataPageHeaderV2FieldId.NUM_ROWS:
                fields['num_rows'] = value
            case DataPageHeaderV2FieldId.ENCODING:
                fields['encoding'] = Encoding(value)
            case DataPageHeaderV2FieldId.DEFINITION_LEVELS_BYTE_LENGTH:
                fields['definition_levels_byte_length'] = value
            case DataPageHeaderV2FieldId.REPETITION_LEVELS_BYTE_LENGTH:
                fields['repetition_levels_byte_length'] = value
            case DataPageHeaderV2FieldId.IS_COMPRESSED:
                fields['is_compressed'] = bool(value)

    def read_dictionary_page_header(self) -> dict:
        """Read DictionaryPageHeader fields and return as dict."""
        struct_parser = ThriftStructParser(self.parser)
        fields = {
            'num_values': 0,
            'encoding': Encoding.PLAIN,
            'is_sorted': False,
        }

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case DictionaryPageHeaderFieldId.NUM_VALUES:
                    fields['num_values'] = value
                case DictionaryPageHeaderFieldId.ENCODING:
                    fields['encoding'] = Encoding(value)
                case DictionaryPageHeaderFieldId.IS_SORTED:
                    fields['is_sorted'] = bool(value)

        return fields

    def read_page_data(self, page: AnyPage) -> bytes:
        """
        Read page data bytes following the header.

        Teaching Points:
        - Page data immediately follows the page header in the file
        - The data may be compressed according to column chunk settings
        - compressed_page_size tells us exactly how many bytes to read
        - Decompression happens later using the column's compression codec

        Args:
            page: Page containing size information

        Returns:
            Raw page data bytes (potentially compressed)
        """
        data_size = page.compressed_page_size
        logger.debug('Reading %d bytes of page data', data_size)

        data = self.parser.read(data_size)
        logger.debug('Read page data: %d bytes', len(data))

        return data

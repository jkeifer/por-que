"""
Page-level parsing for Parquet data pages.

Teaching Points:
- Pages are the fundamental data organization unit within column chunks
- Different page types serve different purposes (data, dictionary, index)
- Page headers contain size and encoding information needed for decompression
- Page data follows the header and may be compressed
"""

import logging

from por_que.enums import Encoding, PageType
from por_que.types import (
    DataPageHeader,
    DataPageHeaderV2,
    DictionaryPageHeader,
    PageHeader,
)

from ..thrift.enums import ThriftFieldType
from ..thrift.parser import ThriftStructParser
from .base import BaseParser
from .enums import (
    DataPageHeaderFieldId,
    DataPageHeaderV2FieldId,
    DictionaryPageHeaderFieldId,
    PageHeaderFieldId,
)

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

    def read_page_header(self) -> PageHeader:  # noqa: C901
        """
        Read a PageHeader struct from the stream.

        Teaching Points:
        - PageHeader is the first thing in every page
        - It tells us the page type and size information
        - The type field determines which specific header follows
        - Size information is crucial for seeking and memory allocation

        Returns:
            PageHeader with type and size information
        """
        struct_parser = ThriftStructParser(self.parser)
        header = PageHeader(
            type=PageType.DATA_PAGE,
            uncompressed_page_size=0,
            compressed_page_size=0,
        )
        logger.debug('Reading page header')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle struct types for specific page headers
            if field_type == ThriftFieldType.STRUCT:
                match field_id:
                    case PageHeaderFieldId.DATA_PAGE_HEADER:
                        header.data_page_header = self.read_data_page_header()
                    case PageHeaderFieldId.DICTIONARY_PAGE_HEADER:
                        header.dictionary_page_header = (
                            self.read_dictionary_page_header()
                        )
                    case PageHeaderFieldId.DATA_PAGE_HEADER_V2:
                        header.data_page_header_v2 = self.read_data_page_header_v2()
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
                    header.type = PageType(value)
                case PageHeaderFieldId.UNCOMPRESSED_PAGE_SIZE:
                    header.uncompressed_page_size = value
                case PageHeaderFieldId.COMPRESSED_PAGE_SIZE:
                    header.compressed_page_size = value
                case PageHeaderFieldId.CRC:
                    header.crc = value

        logger.debug(
            'Read page header: type=%s, compressed=%d bytes, uncompressed=%d bytes',
            header.type.name,
            header.compressed_page_size,
            header.uncompressed_page_size,
        )
        return header

    def read_data_page_header(self) -> DataPageHeader:
        """
        Read a DataPageHeader struct.

        Teaching Points:
        - Data pages contain the actual column values
        - Encoding determines how values are stored (PLAIN, DICTIONARY, etc.)
        - Definition/repetition level encodings handle nullability and nesting
        - Statistics provide optimization metadata for this specific page

        Returns:
            DataPageHeader with encoding and statistics information
        """
        struct_parser = ThriftStructParser(self.parser)
        header = DataPageHeader(
            num_values=0,
            encoding=Encoding.PLAIN,
            definition_level_encoding=Encoding.PLAIN,
            repetition_level_encoding=Encoding.PLAIN,
        )
        logger.debug('Reading data page header')

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
                    header.num_values = value
                case DataPageHeaderFieldId.ENCODING:
                    header.encoding = Encoding(value)
                case DataPageHeaderFieldId.DEFINITION_LEVEL_ENCODING:
                    header.definition_level_encoding = Encoding(value)
                case DataPageHeaderFieldId.REPETITION_LEVEL_ENCODING:
                    header.repetition_level_encoding = Encoding(value)

        logger.debug(
            'Read data page header: %d values, encoding=%s',
            header.num_values,
            header.encoding.name,
        )
        return header

    def read_data_page_header_v2(self) -> DataPageHeaderV2:  # noqa: C901
        """
        Read a DataPageHeaderV2 struct.

        Teaching Points:
        - V2 pages separate level data from value data for efficiency
        - Separate byte lengths allow selective reading of levels
        - num_nulls provides quick null count without value scanning
        - is_compressed applies only to values, not definition/repetition levels

        Returns:
            DataPageHeaderV2 with level information and compression settings
        """
        struct_parser = ThriftStructParser(self.parser)
        header = DataPageHeaderV2(
            num_values=0,
            num_nulls=0,
            num_rows=0,
            encoding=Encoding.PLAIN,
            definition_levels_byte_length=0,
            repetition_levels_byte_length=0,
            is_compressed=True,
        )
        logger.debug('Reading data page header v2')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle statistics struct if present
            if field_type == ThriftFieldType.STRUCT:
                if field_id == DataPageHeaderV2FieldId.STATISTICS:
                    # Skip statistics for now - needs column context
                    struct_parser.skip_field(field_type)
                else:
                    struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case DataPageHeaderV2FieldId.NUM_VALUES:
                    header.num_values = value
                case DataPageHeaderV2FieldId.NUM_NULLS:
                    header.num_nulls = value
                case DataPageHeaderV2FieldId.NUM_ROWS:
                    header.num_rows = value
                case DataPageHeaderV2FieldId.ENCODING:
                    header.encoding = Encoding(value)
                case DataPageHeaderV2FieldId.DEFINITION_LEVELS_BYTE_LENGTH:
                    header.definition_levels_byte_length = value
                case DataPageHeaderV2FieldId.REPETITION_LEVELS_BYTE_LENGTH:
                    header.repetition_levels_byte_length = value
                case DataPageHeaderV2FieldId.IS_COMPRESSED:
                    header.is_compressed = bool(value)

        logger.debug(
            'Read data page header v2: %d values (%d nulls), %d rows, encoding=%s',
            header.num_values,
            header.num_nulls,
            header.num_rows,
            header.encoding.name,
        )
        return header

    def read_dictionary_page_header(self) -> DictionaryPageHeader:
        """
        Read a DictionaryPageHeader struct.

        Teaching Points:
        - Dictionary pages define the mapping from indices to actual values
        - They must appear before any data pages that reference them
        - Dictionary encoding can significantly reduce storage for repeated values
        - Sorted dictionaries enable additional query optimizations

        Returns:
            DictionaryPageHeader with dictionary metadata
        """
        struct_parser = ThriftStructParser(self.parser)
        header = DictionaryPageHeader(
            num_values=0,
            encoding=Encoding.PLAIN,
        )
        logger.debug('Reading dictionary page header')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case DictionaryPageHeaderFieldId.NUM_VALUES:
                    header.num_values = value
                case DictionaryPageHeaderFieldId.ENCODING:
                    header.encoding = Encoding(value)
                case DictionaryPageHeaderFieldId.IS_SORTED:
                    header.is_sorted = bool(value)

        logger.debug(
            'Read dictionary page header: %d values, encoding=%s, sorted=%s',
            header.num_values,
            header.encoding.name,
            header.is_sorted,
        )
        return header

    def read_page_data(self, header: PageHeader) -> bytes:
        """
        Read page data bytes following the header.

        Teaching Points:
        - Page data immediately follows the page header in the file
        - The data may be compressed according to column chunk settings
        - compressed_page_size tells us exactly how many bytes to read
        - Decompression happens later using the column's compression codec

        Args:
            header: PageHeader containing size information

        Returns:
            Raw page data bytes (potentially compressed)
        """
        data_size = header.compressed_page_size
        logger.debug('Reading %d bytes of page data', data_size)

        data = self.parser.read(data_size)
        logger.debug('Read page data: %d bytes', len(data))

        return data

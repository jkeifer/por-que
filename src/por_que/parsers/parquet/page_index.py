"""
Page Index parsing for Parquet ColumnIndex and OffsetIndex structures.

Teaching Points:
- Page Index provides page-level statistics and location information
- ColumnIndex contains min/max values and null information per page
- OffsetIndex contains file locations and sizes for efficient seeking
- These structures enable efficient page skipping during queries
"""

import logging

from por_que.enums import BoundaryOrder
from por_que.file_metadata import ColumnIndex, OffsetIndex, PageLocation
from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftStructParser

from .base import BaseParser
from .enums import (
    ColumnIndexFieldId,
    OffsetIndexFieldId,
    PageLocationFieldId,
)

logger = logging.getLogger(__name__)


class PageIndexParser(BaseParser):
    """
    Parses Page Index structures (ColumnIndex and OffsetIndex).

    Teaching Points:
    - Page Index is separate from page headers - stored in file footer area
    - ColumnIndex and OffsetIndex work together for query optimization
    - These structures enable predicate pushdown and efficient row seeking
    - Min/max values are stored in raw binary format, need column type to decode
    """

    def read_page_location(self) -> PageLocation:
        """
        Read a PageLocation struct.

        Returns:
            PageLocation with file offset, size, and first row index
        """
        struct_parser = ThriftStructParser(self.parser)

        offset = 0
        compressed_page_size = 0
        first_row_index = 0

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case PageLocationFieldId.OFFSET:
                    offset = value
                case PageLocationFieldId.COMPRESSED_PAGE_SIZE:
                    compressed_page_size = value
                case PageLocationFieldId.FIRST_ROW_INDEX:
                    first_row_index = value

        return PageLocation(
            offset=offset,
            compressed_page_size=compressed_page_size,
            first_row_index=first_row_index,
        )

    def read_offset_index(self) -> OffsetIndex:
        """
        Read an OffsetIndex struct.

        Returns:
            OffsetIndex with page locations and optional byte array data
        """
        struct_parser = ThriftStructParser(self.parser)

        page_locations = []
        unencoded_byte_array_data_bytes = None

        logger.debug('Reading OffsetIndex')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.LIST:
                if field_id == OffsetIndexFieldId.PAGE_LOCATIONS:
                    page_locations = self.read_list(self.read_page_location)
                elif field_id == OffsetIndexFieldId.UNENCODED_BYTE_ARRAY_DATA_BYTES:
                    unencoded_byte_array_data_bytes = self.read_list(self.read_i64)
                else:
                    struct_parser.skip_field(field_type)
                continue

            # Skip any unexpected fields
            struct_parser.skip_field(field_type)

        logger.debug('Read OffsetIndex with %d page locations', len(page_locations))
        return OffsetIndex(
            page_locations=page_locations,
            unencoded_byte_array_data_bytes=unencoded_byte_array_data_bytes,
        )

    def read_column_index(self) -> ColumnIndex:  # noqa: C901
        """
        Read a ColumnIndex struct.

        Returns:
            ColumnIndex with page statistics and null information
        """
        struct_parser = ThriftStructParser(self.parser)

        null_pages = []
        min_values = []
        max_values = []
        boundary_order = BoundaryOrder.UNORDERED
        null_counts = None
        repetition_level_histograms = None
        definition_level_histograms = None

        logger.debug('Reading ColumnIndex')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.LIST:
                match field_id:
                    case ColumnIndexFieldId.NULL_PAGES:
                        null_pages = self.read_list(self.read_bool)
                    case ColumnIndexFieldId.MIN_VALUES:
                        min_values = self.read_list(self.read_bytes)
                    case ColumnIndexFieldId.MAX_VALUES:
                        max_values = self.read_list(self.read_bytes)
                    case ColumnIndexFieldId.NULL_COUNTS:
                        null_counts = self.read_list(self.read_i64)
                    case ColumnIndexFieldId.REPETITION_LEVEL_HISTOGRAMS:
                        repetition_level_histograms = self.read_list(self.read_i64)
                    case ColumnIndexFieldId.DEFINITION_LEVEL_HISTOGRAMS:
                        definition_level_histograms = self.read_list(self.read_i64)
                    case _:
                        struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case ColumnIndexFieldId.BOUNDARY_ORDER:
                    boundary_order = BoundaryOrder(value)

        logger.debug(
            'Read ColumnIndex with %d pages, boundary_order=%s',
            len(null_pages),
            boundary_order.name,
        )

        return ColumnIndex(
            null_pages=null_pages,
            min_values=min_values,
            max_values=max_values,
            boundary_order=boundary_order,
            null_counts=null_counts,
            repetition_level_histograms=repetition_level_histograms,
            definition_level_histograms=definition_level_histograms,
        )

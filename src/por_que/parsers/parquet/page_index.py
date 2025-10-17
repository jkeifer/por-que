"""
Page Index parsing for Parquet ColumnIndex and OffsetIndex structures.

Teaching Points:
- Page Index provides page-level statistics and location information
- ColumnIndex contains min/max values and null information per page
- OffsetIndex contains file locations and sizes for efficient seeking
- These structures enable efficient page skipping during queries
"""

import warnings

from typing import Any

from por_que.enums import BoundaryOrder
from por_que.file_metadata import ColumnIndex, OffsetIndex, PageLocation

from .base import BaseParser
from .enums import (
    ColumnIndexFieldId,
    OffsetIndexFieldId,
    PageLocationFieldId,
)


class PageIndexParser(BaseParser):
    """
    Parses Page Index structures (ColumnIndex and OffsetIndex).

    Teaching Points:
    - Page Index is separate from page headers - stored in file footer area
    - ColumnIndex and OffsetIndex work together for query optimization
    - These structures enable predicate pushdown and efficient row seeking
    - Min/max values are stored in raw binary format, need column type to decode
    """

    async def read_page_location(self) -> PageLocation:
        """
        Read a PageLocation struct using the new generic parser.

        Teaching Points:
        - Demonstrates the new streamlined parsing approach
        - All field parsing boilerplate is eliminated
        - Focus is on the data structure rather than parsing mechanics

        Returns:
            PageLocation with file offset, size, and first row index
        """
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case PageLocationFieldId.OFFSET:
                    props['offset'] = value
                case PageLocationFieldId.COMPRESSED_PAGE_SIZE:
                    props['compressed_page_size'] = value
                case PageLocationFieldId.FIRST_ROW_INDEX:
                    props['first_row_index'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown page location field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return PageLocation(**props)

    async def read_offset_index(self) -> OffsetIndex:
        """
        Read an OffsetIndex struct using the new generic parser.

        Returns:
            OffsetIndex with page locations and optional byte array data
        """
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case OffsetIndexFieldId.PAGE_LOCATIONS:
                    props['page_locations'] = [
                        await self.read_page_location() async for _ in value
                    ]
                case OffsetIndexFieldId.UNENCODED_BYTE_ARRAY_DATA_BYTES:
                    props['unencoded_byte_array_data_bytes'] = [
                        await self.read_i64() async for _ in value
                    ]
                case _:
                    warnings.warn(
                        f'Skipping unknown offset index field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return OffsetIndex(**props)

    async def read_column_index(self) -> ColumnIndex:
        """
        Read a ColumnIndex struct using the new generic parser.

        Returns:
            ColumnIndex with page statistics and null information
        """
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case ColumnIndexFieldId.BOUNDARY_ORDER:
                    props['boundary_order'] = BoundaryOrder(value)
                case ColumnIndexFieldId.NULL_PAGES:
                    props['null_pages'] = [await self.read_bool() async for _ in value]
                case ColumnIndexFieldId.MIN_VALUES:
                    props['min_values'] = [await self.read_bytes() async for _ in value]
                case ColumnIndexFieldId.MAX_VALUES:
                    props['max_values'] = [await self.read_bytes() async for _ in value]
                case ColumnIndexFieldId.NULL_COUNTS:
                    props['null_counts'] = [await self.read_i64() async for _ in value]
                case ColumnIndexFieldId.REPETITION_LEVEL_HISTOGRAMS:
                    props['repetition_level_histograms'] = [
                        await self.read_i64() async for _ in value
                    ]
                case ColumnIndexFieldId.DEFINITION_LEVEL_HISTOGRAMS:
                    props['definition_level_histograms'] = [
                        await self.read_i64() async for _ in value
                    ]
                case _:
                    warnings.warn(
                        f'Skipping unknown column index field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return ColumnIndex(**props)

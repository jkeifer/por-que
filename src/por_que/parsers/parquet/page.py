"""
Page-level parsing for Parquet data pages.

Teaching Points:
- Pages are the fundamental data organization unit within column chunks
- Different page types serve different purposes (data, dictionary, index)
- Page headers contain size and encoding information needed for decompression
- Page data follows the header and may be compressed
"""

from __future__ import annotations

import warnings

from typing import TYPE_CHECKING, Any

from por_que.enums import Encoding, PageType
from por_que.exceptions import ParquetFormatError
from por_que.file_metadata import ColumnStatistics, SchemaRoot

from .base import BaseParser
from .enums import (
    DataPageHeaderFieldId,
    DataPageHeaderV2FieldId,
    DictionaryPageHeaderFieldId,
    PageHeaderFieldId,
)
from .statistics import StatisticsParser

if TYPE_CHECKING:
    from por_que.pages import AnyPage


class PageParser(BaseParser):
    """
    Parses individual page headers and manages page data reading.

    Teaching Points:
    - Page parsing is essential for columnar data access
    - Headers describe how to interpret the following data bytes
    - Different page types require different parsing strategies
    - Compression is handled at the page level, not column level
    """

    def __init__(
        self,
        parser,
        schema_root: SchemaRoot,
        column_type,
        path_in_schema,
    ) -> None:
        """
        Initialize page parser.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for statistics parsing
            column_type: Physical type of the column for statistics parsing
            path_in_schema: Schema path for statistics parsing
        """
        super().__init__(parser)
        self.schema_root = schema_root
        self.column_type = column_type
        self.path_in_schema = path_in_schema

    def read_page(self) -> AnyPage:
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
        start_offset = self.parser.pos
        props: dict[str, Any] = {
            'start_offset': start_offset,
        }

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case PageHeaderFieldId.TYPE:
                    props['page_type'] = PageType(value)
                case PageHeaderFieldId.UNCOMPRESSED_PAGE_SIZE:
                    props['uncompressed_page_size'] = value
                case PageHeaderFieldId.COMPRESSED_PAGE_SIZE:
                    props['compressed_page_size'] = value
                case PageHeaderFieldId.CRC:
                    props['crc'] = value
                case PageHeaderFieldId.DATA_PAGE_HEADER:
                    props.update(self.read_data_page_header())
                case PageHeaderFieldId.DICTIONARY_PAGE_HEADER:
                    props.update(self.read_dictionary_page_header())
                case PageHeaderFieldId.DATA_PAGE_HEADER_V2:
                    props.update(self.read_data_page_header_v2())
                case PageHeaderFieldId.INDEX_PAGE_HEADER:
                    props.update(self.read_index_page_header())
                case _:
                    warnings.warn(
                        f'Skipping unknown page field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        header_end_offset = self.parser.pos
        props['header_size'] = header_end_offset - start_offset

        return self._make_page(**props)

    def _make_page(self, **kwargs) -> AnyPage:
        from por_que.pages import DataPageV1, DataPageV2, DictionaryPage, IndexPage

        page: AnyPage
        match kwargs.get('page_type'):
            case PageType.DICTIONARY_PAGE:
                page = DictionaryPage(**kwargs)
            case PageType.DATA_PAGE:
                page = DataPageV1(**kwargs)
            case PageType.DATA_PAGE_V2:
                page = DataPageV2(**kwargs)
            case PageType.INDEX_PAGE:
                page = IndexPage(**kwargs)
            case _ as unknown:
                raise ParquetFormatError(f'Unknown page type: {unknown}')

        return page

    def _handle_statistics(self) -> ColumnStatistics:
        return StatisticsParser(self.parser).read_statistics()

    def read_data_page_header(self) -> dict[str, Any]:
        """Read DataPageHeader fields and return as dict."""
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case DataPageHeaderFieldId.NUM_VALUES:
                    props['num_values'] = value
                case DataPageHeaderFieldId.ENCODING:
                    props['encoding'] = Encoding(value)
                case DataPageHeaderFieldId.DEFINITION_LEVEL_ENCODING:
                    props['definition_level_encoding'] = Encoding(value)
                case DataPageHeaderFieldId.REPETITION_LEVEL_ENCODING:
                    props['repetition_level_encoding'] = Encoding(value)
                case DataPageHeaderFieldId.STATISTICS:
                    props['statistics'] = self._handle_statistics()
                case _:
                    warnings.warn(
                        f'Skipping unknown data page v1 header field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return props

    def read_data_page_header_v2(self) -> dict[str, Any]:
        """Read DataPageHeaderV2 fields using the new generic parser."""
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case DataPageHeaderV2FieldId.NUM_VALUES:
                    props['num_values'] = value
                case DataPageHeaderV2FieldId.NUM_NULLS:
                    props['num_nulls'] = value
                case DataPageHeaderV2FieldId.NUM_ROWS:
                    props['num_rows'] = value
                case DataPageHeaderV2FieldId.ENCODING:
                    props['encoding'] = Encoding(value)
                case DataPageHeaderV2FieldId.DEFINITION_LEVELS_BYTE_LENGTH:
                    props['definition_levels_byte_length'] = value
                case DataPageHeaderV2FieldId.REPETITION_LEVELS_BYTE_LENGTH:
                    props['repetition_levels_byte_length'] = value
                case DataPageHeaderV2FieldId.IS_COMPRESSED:
                    props['is_compressed'] = bool(value)
                case DataPageHeaderV2FieldId.STATISTICS:
                    props['statistics'] = self._handle_statistics()
                case _:
                    warnings.warn(
                        f'Skipping unknown data page header v2 field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return props

    def read_dictionary_page_header(self) -> dict[str, Any]:
        """Read DictionaryPageHeader fields using the new generic parser."""
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case DictionaryPageHeaderFieldId.NUM_VALUES:
                    props['num_values'] = value
                case DictionaryPageHeaderFieldId.ENCODING:
                    props['encoding'] = Encoding(value)
                case DictionaryPageHeaderFieldId.IS_SORTED:
                    props['is_sorted'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown dictionary page header field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return props

    def read_index_page_header(self) -> dict[str, Any]:
        """
        Read IndexPageHeader fields using the new generic parser.

        Note: IndexPageHeader is currently empty in the Parquet specification
        (contains only a TODO comment), so this method doesn't expect any fields.
        """
        # IndexPageHeader is empty, but we still parse to consume the struct
        for field_id, _, _ in self.parse_struct_fields():
            warnings.warn(
                f'Skipping unknown dictionary page header field ID {field_id}',
                stacklevel=1,
            )

        return {}

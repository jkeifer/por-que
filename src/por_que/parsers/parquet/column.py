"""
Column metadata parsing for Parquet column chunks.

Teaching Points:
- Column chunks are the fundamental storage unit in Parquet row groups
- Each chunk contains metadata about compression, encoding, and page locations
- Statistics in column metadata enable query optimization
- Path in schema connects column chunks back to the logical schema structure
"""

from __future__ import annotations

import logging
import warnings

from typing import Any

from por_que.enums import Compression, Encoding, Type
from por_que.file_metadata import (
    ColumnChunk,
    ColumnMetadata,
    PageEncodingStats,
    SchemaRoot,
    SizeStatistics,
)
from por_que.pages import PageType

from .base import BaseParser
from .enums import (
    ColumnChunkFieldId,
    ColumnMetadataFieldId,
    PageEncodingStatsFieldId,
    SizeStatisticsFieldId,
)
from .geostats import GeoStatsParser
from .statistics import StatisticsParser

logger = logging.getLogger(__name__)


class ColumnParser(BaseParser):
    """
    Parses column chunk and column metadata structures.

    Teaching Points:
    - Column chunks represent a single column's data within a row group
    - Metadata includes compression codec, encoding methods, and data locations
    - File offsets enable selective reading of specific columns
    - Statistics provide query optimization without reading actual data
    """

    def __init__(self, parser, schema: SchemaRoot) -> None:
        """
        Initialize column parser with schema context for statistics.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for logical type resolution
        """
        super().__init__(parser)
        self.schema = schema

    async def read_column_chunk(self) -> ColumnChunk:
        """
        Read a ColumnChunk struct using the new generic parser.

        Teaching Points:
        - ColumnChunk is a container pointing to column data and metadata
        - file_path enables external file references (rarely used)
        - file_offset locates the column chunk within the file
        - metadata contains the detailed column information

        Returns:
            ColumnChunk with metadata and file location info
        """
        logger.debug('Reading column chunk')

        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case ColumnChunkFieldId.FILE_PATH:
                    props['file_path'] = value.decode('utf-8')
                case ColumnChunkFieldId.FILE_OFFSET:
                    props['file_offset'] = value
                case ColumnChunkFieldId.OFFSET_INDEX_OFFSET:
                    props['offset_index_offset'] = value
                case ColumnChunkFieldId.OFFSET_INDEX_LENGTH:
                    props['offset_index_length'] = value
                case ColumnChunkFieldId.COLUMN_INDEX_OFFSET:
                    props['column_index_offset'] = value
                case ColumnChunkFieldId.COLUMN_INDEX_LENGTH:
                    props['column_index_length'] = value
                case ColumnChunkFieldId.META_DATA:
                    props['metadata'] = await self.read_column_metadata()
                case _:
                    warnings.warn(
                        f'Skipping unknown column chunk field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return ColumnChunk(**props)

    async def read_column_metadata(self) -> ColumnMetadata:  # noqa: C901
        """
        Read ColumnMetaData struct using the new generic parser.

        Teaching Points:
        - Column metadata describes how data is stored and encoded
        - Physical type determines the primitive storage format
        - Encodings list shows compression/encoding methods applied
        - Page offsets enable direct seeking to data within the chunk
        - Statistics provide min/max values for query optimization

        Returns:
            ColumnMetadata with complete column information
        """
        start_offset = self.parser.pos
        props: dict[str, Any] = {
            'start_offset': start_offset,
        }

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case ColumnMetadataFieldId.TYPE:
                    props['type'] = Type(value)
                case ColumnMetadataFieldId.CODEC:
                    props['codec'] = Compression(value)
                case ColumnMetadataFieldId.NUM_VALUES:
                    props['num_values'] = value
                case ColumnMetadataFieldId.TOTAL_UNCOMPRESSED_SIZE:
                    props['total_uncompressed_size'] = value
                case ColumnMetadataFieldId.TOTAL_COMPRESSED_SIZE:
                    props['total_compressed_size'] = value
                case ColumnMetadataFieldId.DATA_PAGE_OFFSET:
                    props['data_page_offset'] = value
                case ColumnMetadataFieldId.INDEX_PAGE_OFFSET:
                    props['index_page_offset'] = value
                case ColumnMetadataFieldId.DICTIONARY_PAGE_OFFSET:
                    props['dictionary_page_offset'] = value
                case ColumnMetadataFieldId.ENCODINGS:
                    props['encodings'] = [
                        Encoding(await self.read_i32()) async for _ in value
                    ]
                case ColumnMetadataFieldId.PATH_IN_SCHEMA:
                    path_in_schema = '.'.join(
                        [await self.read_string() async for _ in value],
                    )
                    props['path_in_schema'] = path_in_schema
                    props['schema_element'] = self.schema.find_element(path_in_schema)
                case ColumnMetadataFieldId.STATISTICS:
                    props['statistics'] = await StatisticsParser(
                        self.parser,
                    ).read_statistics()
                case ColumnMetadataFieldId.ENCODING_STATS:
                    props['encoding_stats'] = [
                        await self._parse_page_encoding_stats() async for _ in value
                    ]
                case ColumnMetadataFieldId.BLOOM_FILTER_OFFSET:
                    props['bloom_filter_offset'] = value
                case ColumnMetadataFieldId.BLOOM_FILTER_LENGTH:
                    props['bloom_filter_length'] = value
                case ColumnMetadataFieldId.SIZE_STATISTICS:
                    props['size_statistics'] = await self._parse_size_statistics()
                case ColumnMetadataFieldId.GEOSPATIAL_STATISTICS:
                    props['geospatial_statistics'] = await GeoStatsParser(
                        self.parser,
                    ).read_geo_stats()
                case _:
                    warnings.warn(
                        f'Skipping unknown column metadata field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        end_offset = self.parser.pos
        props['byte_length'] = end_offset - start_offset

        return ColumnMetadata(**props)

    async def _parse_page_encoding_stats(self) -> PageEncodingStats:
        """Parse PageEncodingStats structs."""

        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case PageEncodingStatsFieldId.PAGE_TYPE:
                    props['page_type'] = PageType(value)
                case PageEncodingStatsFieldId.ENCODING:
                    props['encoding'] = Encoding(value)
                case PageEncodingStatsFieldId.COUNT:
                    props['count'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown page encodings stats field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return PageEncodingStats(**props)

    async def _parse_size_statistics(self) -> SizeStatistics:
        """Parse a SizeStatistics struct."""

        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case SizeStatisticsFieldId.UNENCODED_BYTE_ARRAY_DATA_BYTES:
                    props['unencoded_byte_array_data_bytes'] = value
                case SizeStatisticsFieldId.REPETITION_LEVEL_HISTOGRAM:
                    props['repetition_level_histogram'] = [
                        await self.read_i64() async for _ in value
                    ]
                case SizeStatisticsFieldId.DEFINITION_LEVEL_HISTOGRAM:
                    props['definition_level_histogram'] = [
                        await self.read_i64() async for _ in value
                    ]
                case _:
                    warnings.warn(
                        f'Skipping unknown size stats field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return SizeStatistics(**props)

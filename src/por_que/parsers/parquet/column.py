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

from collections.abc import Sequence
from typing import Any

from por_que.enums import Compression, Encoding, Type
from por_que.exceptions import ParquetFormatError
from por_que.file_metadata import (
    ColumnChunk,
    ColumnMetadata,
    ColumnStatistics,
    PageEncodingStats,
    SchemaLeaf,
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

    def __init__(
        self,
        parser,
        schema: SchemaRoot,
        columns: Sequence[str] | None = None,
    ) -> None:
        """
        Initialize column parser with schema context for statistics.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for logical type resolution
            columns: Optional projection of dotted ``path_in_schema`` strings.
                When given, chunks whose path is not selected are consumed but
                not built, and ``read_column_chunk`` returns ``None`` for them.
        """
        super().__init__(parser)
        self.schema = schema
        self.columns = columns

    def read_column_chunk(self) -> ColumnChunk | None:  # noqa: C901
        """
        Read a ColumnChunk struct using the new generic parser.

        Teaching Points:
        - ColumnChunk is a container pointing to column data and metadata
        - file_path enables external file references (rarely used)
        - file_offset locates the column chunk within the file
        - metadata contains the detailed column information

        Returns:
            ColumnChunk with metadata and file location info, or ``None`` when
            a column projection is active and this chunk is not selected. The
            struct is always consumed cleanly regardless.
        """
        logger.debug('Reading column chunk')

        props: dict[str, Any] = {}
        selected = True

        for field_id, field_type, value in self.parse_struct_fields():
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
                    metadata = self.read_column_metadata()
                    if metadata is None:
                        selected = False
                    else:
                        props['metadata'] = metadata
                case _:
                    warnings.warn(
                        f'Skipping unknown column chunk field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        if not selected:
            return None

        return ColumnChunk(**props)

    def read_column_metadata(self) -> ColumnMetadata | None:  # noqa: C901
        """
        Read ColumnMetaData struct using the new generic parser.

        Teaching Points:
        - Column metadata describes how data is stored and encoded
        - Physical type determines the primitive storage format
        - Encodings list shows compression/encoding methods applied
        - Page offsets enable direct seeking to data within the chunk
        - Statistics provide min/max values for query optimization

        When a column projection is active and this column's path is not
        selected, the remaining fields are consumed with the generic
        field-skipping machinery (no models built, no values decoded) and
        ``None`` is returned to signal the caller to omit the chunk.

        Returns:
            ColumnMetadata with complete column information, or ``None`` when
            not selected by an active column projection.
        """
        start_offset = self.parser.pos
        props: dict[str, Any] = {
            'start_offset': start_offset,
        }
        schema_element: SchemaLeaf | None = None
        skipping = False

        for field_id, field_type, value in self.parse_struct_fields():
            # Once we know this column is not selected, drain the rest of the
            # struct cleanly without building anything.
            if skipping:
                self.maybe_skip_field(field_type)
                continue

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
                    # Offset 0 means "no dictionary page" (0 is the file header)
                    props['dictionary_page_offset'] = value if value > 0 else None
                case ColumnMetadataFieldId.ENCODINGS:
                    props['encodings'] = [Encoding(self.read_i32()) for _ in value]
                case ColumnMetadataFieldId.PATH_IN_SCHEMA:
                    path_in_schema = '.'.join(
                        [self.read_string() for _ in value],
                    )
                    props['path_in_schema'] = path_in_schema
                    if self.columns is not None and path_in_schema not in self.columns:
                        # Not selected: skip the rest of the struct without
                        # resolving the schema element or building models.
                        skipping = True
                        continue
                    element = self.schema.find_element(path_in_schema)
                    if not isinstance(element, SchemaLeaf):
                        raise ParquetFormatError(
                            f'Schema element for column {path_in_schema!r} '
                            'is not a leaf',
                        )
                    schema_element = element
                case ColumnMetadataFieldId.STATISTICS:
                    if schema_element is None:
                        raise ParquetFormatError(
                            'STATISTICS field encountered before '
                            'PATH_IN_SCHEMA in column metadata',
                        )
                    props['statistics'] = ColumnStatistics(
                        schema_path=schema_element.full_path,
                        **(StatisticsParser(self.parser).read_statistics()),
                    )._link(schema_element)
                case ColumnMetadataFieldId.ENCODING_STATS:
                    props['encoding_stats'] = [
                        self._parse_page_encoding_stats() for _ in value
                    ]
                case ColumnMetadataFieldId.BLOOM_FILTER_OFFSET:
                    props['bloom_filter_offset'] = value
                case ColumnMetadataFieldId.BLOOM_FILTER_LENGTH:
                    props['bloom_filter_length'] = value
                case ColumnMetadataFieldId.SIZE_STATISTICS:
                    props['size_statistics'] = self._parse_size_statistics()
                case ColumnMetadataFieldId.GEOSPATIAL_STATISTICS:
                    props['geospatial_statistics'] = GeoStatsParser(
                        self.parser,
                    ).read_geo_stats()
                case _:
                    warnings.warn(
                        f'Skipping unknown column metadata field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        if skipping:
            # Column was not selected by the projection; the struct has been
            # consumed, so signal the caller to omit this chunk.
            return None

        end_offset = self.parser.pos
        props['byte_length'] = end_offset - start_offset

        if schema_element is None:
            raise ParquetFormatError(
                'Column metadata missing PATH_IN_SCHEMA field',
            )

        return ColumnMetadata(**props)._link(schema_element)

    def _parse_page_encoding_stats(self) -> PageEncodingStats:
        """Parse PageEncodingStats structs."""

        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
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
                    self.maybe_skip_field(field_type)

        return PageEncodingStats(**props)

    def _parse_size_statistics(self) -> SizeStatistics:
        """Parse a SizeStatistics struct."""

        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case SizeStatisticsFieldId.UNENCODED_BYTE_ARRAY_DATA_BYTES:
                    props['unencoded_byte_array_data_bytes'] = value
                case SizeStatisticsFieldId.REPETITION_LEVEL_HISTOGRAM:
                    props['repetition_level_histogram'] = [
                        self.read_i64() for _ in value
                    ]
                case SizeStatisticsFieldId.DEFINITION_LEVEL_HISTOGRAM:
                    props['definition_level_histogram'] = [
                        self.read_i64() for _ in value
                    ]
                case _:
                    warnings.warn(
                        f'Skipping unknown size stats field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return SizeStatistics(**props)

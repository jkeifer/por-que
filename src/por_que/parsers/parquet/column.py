"""
Column metadata parsing for Parquet column chunks.

Teaching Points:
- Column chunks are the fundamental storage unit in Parquet row groups
- Each chunk contains metadata about compression, encoding, and page locations
- Statistics in column metadata enable query optimization
- Path in schema connects column chunks back to the logical schema structure
"""

import logging

from por_que.enums import Compression, Encoding, Type
from por_que.types import ColumnChunk, ColumnMetadata, SchemaElement

from ..thrift.enums import ThriftFieldType
from ..thrift.parser import ThriftStructParser
from .base import BaseParser
from .enums import (
    ColumnChunkFieldId,
    ColumnMetadataFieldId,
)
from .statistics import RowGroupStatisticsParser

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

    def __init__(self, parser, schema: SchemaElement):
        """
        Initialize column parser with schema context for statistics.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for logical type resolution
        """
        super().__init__(parser)
        self.schema = schema

    def read_column_chunk(self) -> ColumnChunk:
        """
        Read a ColumnChunk struct.

        Teaching Points:
        - ColumnChunk is a container pointing to column data and metadata
        - file_path enables external file references (rarely used)
        - file_offset locates the column chunk within the file
        - meta_data contains the detailed column information

        Returns:
            ColumnChunk with metadata and file location info
        """
        struct_parser = ThriftStructParser(self.parser)
        chunk = ColumnChunk(file_offset=0)
        logger.debug('Reading column chunk')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.STRUCT:
                if field_id == ColumnChunkFieldId.META_DATA:
                    chunk.meta_data = self.read_column_metadata()
                else:
                    struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case ColumnChunkFieldId.FILE_PATH:
                    chunk.file_path = value.decode('utf-8')
                case ColumnChunkFieldId.FILE_OFFSET:
                    chunk.file_offset = value

        return chunk

    def read_column_metadata(self) -> ColumnMetadata:  # noqa: C901
        """
        Read ColumnMetaData struct.

        Teaching Points:
        - Column metadata describes how data is stored and encoded
        - Physical type determines the primitive storage format
        - Encodings list shows compression/encoding methods applied
        - Page offsets enable direct seeking to data within the chunk
        - Statistics provide min/max values for query optimization

        Returns:
            ColumnMetadata with complete column information
        """
        struct_parser = ThriftStructParser(self.parser)
        meta = ColumnMetadata(
            type=Type.BOOLEAN,
            encodings=[],
            path_in_schema='',
            codec=Compression.UNCOMPRESSED,
            num_values=0,
            total_uncompressed_size=0,
            total_compressed_size=0,
            data_page_offset=0,
        )

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle complex types explicitly
            if field_type == ThriftFieldType.LIST:
                if field_id == ColumnMetadataFieldId.ENCODINGS:
                    # Encodings list shows data transformation methods applied
                    encodings = self.read_list(self.read_i32)
                    for e in encodings:
                        meta.encodings.append(Encoding(e))
                elif field_id == ColumnMetadataFieldId.PATH_IN_SCHEMA:
                    # Path connects this column back to schema structure
                    path_list = self.read_list(self.read_string)
                    meta.path_in_schema = '.'.join(path_list)
                else:
                    struct_parser.skip_field(field_type)
                continue

            if field_type == ThriftFieldType.STRUCT:
                if field_id == ColumnMetadataFieldId.STATISTICS:
                    # Statistics enable predicate pushdown optimization
                    stats_parser = RowGroupStatisticsParser(self.parser, self.schema)
                    meta.statistics = stats_parser.read_statistics(
                        meta.type,
                        meta.path_in_schema,
                    )
                else:
                    struct_parser.skip_field(field_type)
                continue

            # Handle primitive types with the generic parser
            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case ColumnMetadataFieldId.TYPE:
                    meta.type = Type(value)
                case ColumnMetadataFieldId.CODEC:
                    # Compression codec (UNCOMPRESSED, SNAPPY, GZIP, etc.)
                    meta.codec = Compression(value)
                case ColumnMetadataFieldId.NUM_VALUES:
                    # Total number of values (excluding NULLs)
                    meta.num_values = value
                case ColumnMetadataFieldId.TOTAL_UNCOMPRESSED_SIZE:
                    # Raw data size before compression
                    meta.total_uncompressed_size = value
                case ColumnMetadataFieldId.TOTAL_COMPRESSED_SIZE:
                    # Data size after compression (actual bytes in file)
                    meta.total_compressed_size = value
                case ColumnMetadataFieldId.DATA_PAGE_OFFSET:
                    # File offset where data pages begin
                    meta.data_page_offset = value
                case ColumnMetadataFieldId.INDEX_PAGE_OFFSET:
                    # File offset for index pages (optional optimization)
                    meta.index_page_offset = value
                case ColumnMetadataFieldId.DICTIONARY_PAGE_OFFSET:
                    # File offset for dictionary page (if dictionary encoding used)
                    meta.dictionary_page_offset = value

        return meta

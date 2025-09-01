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
from por_que.logical import ColumnChunk, ColumnMetadata, SchemaRoot
from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftStructParser

from .base import BaseParser
from .enums import (
    ColumnChunkFieldId,
    ColumnMetadataFieldId,
)
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

    def __init__(self, parser, schema: SchemaRoot):
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
        - metadata contains the detailed column information

        Returns:
            ColumnChunk with metadata and file location info
        """
        struct_parser = ThriftStructParser(self.parser)

        # Collect values for frozen ColumnChunk construction
        file_offset: int | None = 0
        metadata: ColumnMetadata | None = None
        file_path: str | None = None

        logger.debug('Reading column chunk')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.STRUCT:
                if field_id == ColumnChunkFieldId.META_DATA:
                    metadata = self.read_column_metadata()
                else:
                    struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case ColumnChunkFieldId.FILE_PATH:
                    file_path = value.decode('utf-8')
                case ColumnChunkFieldId.FILE_OFFSET:
                    file_offset = value

        return ColumnChunk.new(
            file_offset=file_offset,
            metadata=metadata,
            file_path=file_path,
        )

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
        # Collect values for frozen ColumnMetadata construction
        type_val = Type.BOOLEAN
        encodings = []
        path_in_schema = ''
        codec = Compression.UNCOMPRESSED
        num_values = 0
        total_uncompressed_size = 0
        total_compressed_size = 0
        data_page_offset = 0
        dictionary_page_offset = None
        index_page_offset = None
        statistics = None
        column_index_offset = None

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # Handle complex types explicitly
            if field_type == ThriftFieldType.LIST:
                if field_id == ColumnMetadataFieldId.ENCODINGS:
                    # Encodings list shows data transformation methods applied
                    encoding_ints = self.read_list(self.read_i32)
                    encodings = [Encoding(e) for e in encoding_ints]
                elif field_id == ColumnMetadataFieldId.PATH_IN_SCHEMA:
                    # Path connects this column back to schema structure
                    path_list = self.read_list(self.read_string)
                    path_in_schema = '.'.join(path_list)
                else:
                    struct_parser.skip_field(field_type)
                continue

            if field_type == ThriftFieldType.STRUCT:
                if field_id == ColumnMetadataFieldId.STATISTICS:
                    # Statistics enable predicate pushdown optimization
                    stats_parser = StatisticsParser(self.parser, self.schema)
                    statistics = stats_parser.read_statistics(
                        type_val,
                        path_in_schema,
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
                    type_val = Type(value)
                case ColumnMetadataFieldId.CODEC:
                    # Compression codec (UNCOMPRESSED, SNAPPY, GZIP, etc.)
                    codec = Compression(value)
                case ColumnMetadataFieldId.NUM_VALUES:
                    # Total number of values (excluding NULLs)
                    num_values = value
                case ColumnMetadataFieldId.TOTAL_UNCOMPRESSED_SIZE:
                    # Raw data size before compression
                    total_uncompressed_size = value
                case ColumnMetadataFieldId.TOTAL_COMPRESSED_SIZE:
                    # Data size after compression (actual bytes in file)
                    total_compressed_size = value
                case ColumnMetadataFieldId.DATA_PAGE_OFFSET:
                    # File offset where data pages begin
                    data_page_offset = value
                case ColumnMetadataFieldId.INDEX_PAGE_OFFSET:
                    # File offset for index pages (optional optimization)
                    index_page_offset = value
                case ColumnMetadataFieldId.DICTIONARY_PAGE_OFFSET:
                    # File offset for dictionary page (if dictionary encoding used)
                    dictionary_page_offset = value
                case ColumnMetadataFieldId.COLUMN_INDEX_OFFSET:
                    # File offset for column index (Page Index feature)
                    column_index_offset = value

        # Construct the frozen ColumnMetadata
        return ColumnMetadata(
            type=type_val,
            encodings=encodings,
            path_in_schema=path_in_schema,
            codec=codec,
            num_values=num_values,
            total_uncompressed_size=total_uncompressed_size,
            total_compressed_size=total_compressed_size,
            data_page_offset=data_page_offset,
            dictionary_page_offset=dictionary_page_offset,
            index_page_offset=index_page_offset,
            statistics=statistics,
            column_index_offset=column_index_offset,
            column_index_length=None,  # Will be calculated when parsing indexes
            column_index=None,  # Will be populated when parsing indexes
            offset_index=None,  # Will be populated when parsing indexes
        )

"""
Row group parsing for Parquet file organization.

Teaching Points:
- Row groups are the primary unit of parallelization in Parquet
- Each row group contains a subset of rows across all columns
- Row group size balances memory usage vs I/O efficiency
- Column chunks within a row group enable selective column reading
"""

import logging

from por_que.logical import RowGroup, SchemaRoot
from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftStructParser

from .base import BaseParser
from .column import ColumnParser
from .enums import RowGroupFieldId

logger = logging.getLogger(__name__)


class RowGroupParser(BaseParser):
    """
    Parses row group metadata structures.

    Teaching Points:
    - Row groups partition the file horizontally (by rows)
    - Each row group is self-contained with its own column chunks
    - Row group size affects memory usage and query parallelization
    - Optimal size typically 128MB-1GB depending on use case
    """

    def __init__(self, parser, schema: SchemaRoot) -> None:
        """
        Initialize row group parser with schema context.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for column metadata parsing
        """
        super().__init__(parser)
        self.schema = schema

    def read_row_group(self) -> RowGroup:
        """
        Read a RowGroup struct.

        Teaching Points:
        - Row groups contain metadata about a horizontal slice of data
        - num_rows indicates how many records are in this row group
        - total_byte_size helps with memory planning and I/O optimization
        - columns list contains one ColumnChunk per column in the schema

        Returns:
            RowGroup with metadata and column chunk information
        """
        struct_parser = ThriftStructParser(self.parser)

        # Collect values for frozen RowGroup construction
        column_chunks_list = []
        total_byte_size = 0
        row_count = 0

        logger.debug('Reading row group')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_type == ThriftFieldType.LIST:
                if field_id == RowGroupFieldId.COLUMNS:
                    # Parse all column chunks in this row group
                    # Each column chunk contains data for one column across all rows
                    column_parser = ColumnParser(self.parser, self.schema)
                    column_chunks_list = self.read_list(column_parser.read_column_chunk)
                else:
                    struct_parser.skip_field(field_type)
                continue

            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case RowGroupFieldId.TOTAL_BYTE_SIZE:
                    # Total bytes for all column chunks in this row group
                    # Useful for memory estimation and I/O planning
                    total_byte_size = value
                case RowGroupFieldId.NUM_ROWS:
                    # Number of rows (records) in this row group
                    # Same across all columns in the row group
                    row_count = value

        # Convert column chunks list to dict keyed by path
        column_chunks = {
            chunk.metadata.path_in_schema: chunk for chunk in column_chunks_list
        }

        # Construct frozen RowGroup
        rg = RowGroup(
            column_chunks=column_chunks,
            total_byte_size=total_byte_size,
            row_count=row_count,
        )

        logger.debug(
            'Read row group with %d columns, %d rows, %d bytes',
            len(column_chunks_list),
            rg.row_count,
            rg.total_byte_size,
        )
        return rg

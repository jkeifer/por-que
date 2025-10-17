"""
Row group parsing for Parquet file organization.

Teaching Points:
- Row groups are the primary unit of parallelization in Parquet
- Each row group contains a subset of rows across all columns
- Row group size balances memory usage vs I/O efficiency
- Column chunks within a row group enable selective column reading
"""

import warnings

from typing import Any

from por_que.file_metadata import RowGroup, SchemaRoot, SortingColumn

from .base import BaseParser
from .column import ColumnParser
from .enums import RowGroupFieldId, SortingColumnFieldId


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

    async def read_row_group(self) -> RowGroup:
        """
        Read a RowGroup struct using the new generic parser.

        Teaching Points:
        - Row groups contain metadata about a horizontal slice of data
        - num_rows indicates how many records are in this row group
        - total_byte_size helps with memory planning and I/O optimization
        - columns list contains one ColumnChunk per column in the schema

        Returns:
            RowGroup with metadata and column chunk information
        """
        start_offset = self.parser.pos

        props: dict[str, Any] = {
            'start_offset': start_offset,
        }

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case RowGroupFieldId.TOTAL_BYTE_SIZE:
                    props['total_byte_size'] = value
                case RowGroupFieldId.NUM_ROWS:
                    props['row_count'] = value
                case RowGroupFieldId.COLUMNS:
                    column_parser = ColumnParser(self.parser, self.schema)
                    column_chunks_list = [
                        await column_parser.read_column_chunk() async for _ in value
                    ]
                    props['column_chunks'] = {
                        chunk.metadata.path_in_schema: chunk
                        for chunk in column_chunks_list
                    }
                case RowGroupFieldId.SORTING_COLUMNS:
                    props['sorting_columns'] = [
                        await self.parse_sorting_column() async for _ in value
                    ]
                case RowGroupFieldId.FILE_OFFSET:
                    props['file_offset'] = value
                case RowGroupFieldId.TOTAL_COMPRESSED_SIZE:
                    props['total_compressed_size'] = value
                case RowGroupFieldId.ORDINAL:
                    props['ordinal'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown row group field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        end_offset = self.parser.pos
        props['byte_length'] = end_offset - start_offset

        return RowGroup(**props)

    async def parse_sorting_column(self) -> SortingColumn:
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case SortingColumnFieldId.COLUMN_IDX:
                    props['column_idx'] = value
                case SortingColumnFieldId.DESCENDING:
                    props['descending'] = value
                case SortingColumnFieldId.NULLS_FIRST:
                    props['nulls_first'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown sorting column field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return SortingColumn(**props)

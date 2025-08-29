"""
RowGroupReader: Provides access to columns within a single row group.

Teaching Points:
- Row groups partition file data horizontally (by rows)
- Each row group contains data for all columns but only a subset of rows
- Row groups enable parallelization and memory management
- All columns in a row group have the same number of rows
"""

import logging

from por_que.protocols import ReadableSeekable
from por_que.types import RowGroup, SchemaElement

from .column_chunk import ColumnChunkReader

logger = logging.getLogger(__name__)


class RowGroupReader:
    """
    Provides lazy access to columns within a single row group.

    Teaching Points:
    - Row groups are the primary unit of parallel processing in Parquet
    - Each row group is self-contained with metadata about its columns
    - Selective column reading is possible without loading entire row group
    - Row group size represents a balance between memory usage and I/O efficiency
    """

    def __init__(
        self,
        file_obj: ReadableSeekable,
        row_group: RowGroup,
        schema: SchemaElement | None = None,
    ):
        """
        Initialize row group reader.

        Args:
            file_obj: File-like object for reading data
            row_group: Row group metadata with column chunk information
            schema: Root schema element for type information

        Teaching Points:
        - row_group contains metadata for all column chunks in this group
        - Each column chunk has offsets pointing to its pages in the file
        - Schema provides type information needed for value interpretation
        """
        self.file_obj = file_obj
        self.row_group = row_group
        self.schema = schema
        self._column_readers_cache: dict[
            str,
            ColumnChunkReader,
        ] = {}  # Cache ColumnChunkReader instances

        logger.debug(
            'RowGroupReader initialized: %d rows, %d columns',
            row_group.num_rows,
            len(row_group.columns),
        )

    def column(self, name: str) -> ColumnChunkReader:
        """
        Get a reader for a specific column in this row group.

        Args:
            name: Column name to read (case-sensitive)

        Returns:
            ColumnChunkReader for the specified column

        Raises:
            ValueError: If column name is not found in this row group

        Teaching Points:
        - Column access is lazy - we only create readers when requested
        - Column names come from the schema path (e.g., "user.address.street")
        - Each column chunk contains all data for that column in this row group
        - Caching prevents recreating readers for the same column
        """
        # Check cache first
        if name in self._column_readers_cache:
            logger.debug('Returning cached reader for column %s', name)
            return self._column_readers_cache[name]

        # Find column chunk with matching name
        for column_chunk in self.row_group.columns:
            if not column_chunk.meta_data:
                continue

            # Check if this column matches the requested name
            column_path = column_chunk.meta_data.path_in_schema
            column_name = column_path.split('.')[-1]  # Get last part of path

            if column_name == name or column_path == name:
                logger.debug('Found column %s at path %s', name, column_path)

                reader = ColumnChunkReader(
                    self.file_obj,
                    column_chunk,
                    self.schema,
                )

                # Cache for future use
                self._column_readers_cache[name] = reader
                return reader

        # Column not found
        available_columns = self.columns()
        raise ValueError(
            f'Column "{name}" not found in row group. '
            f'Available columns: {available_columns}',
        )

    def columns(self) -> list[str]:
        """
        Get list of all column names in this row group.

        Returns:
            List of column names available for reading

        Teaching Points:
        - Column names are derived from the schema path
        - For nested schemas, names may include parent paths
        - All row groups in a file have the same column structure
        - This enables schema discovery and column validation
        """
        columns = []
        for column_chunk in self.row_group.columns:
            if column_chunk.meta_data:
                column_path = column_chunk.meta_data.path_in_schema
                column_name = column_path.split('.')[-1]
                columns.append(column_name)

        logger.debug('Available columns: %s', columns)
        return columns

    def column_paths(self) -> list[str]:
        """
        Get list of all full column paths in this row group.

        Returns:
            List of full schema paths for all columns

        Teaching Points:
        - Full paths show the hierarchical structure of nested data
        - Paths use dot notation: "user.address.street"
        - Root-level columns have simple names without dots
        - Paths are essential for navigating complex nested schemas
        """
        paths = []
        for column_chunk in self.row_group.columns:
            if column_chunk.meta_data:
                paths.append(column_chunk.meta_data.path_in_schema)

        return paths

    def num_rows(self) -> int:
        """
        Get number of rows in this row group.

        Returns:
            Number of rows across all columns in this group

        Teaching Points:
        - All columns in a row group have the same number of rows
        - This is a fundamental invariant in Parquet's design
        - Row count helps with memory estimation and processing planning
        """
        return self.row_group.num_rows

    def num_columns(self) -> int:
        """
        Get number of columns in this row group.

        Returns:
            Number of columns in this row group

        Teaching Points:
        - Column count matches the schema structure
        - Nested schemas may have many leaf columns
        - Each column chunk represents one leaf column in the schema tree
        """
        return len(self.row_group.columns)

    def total_byte_size(self) -> int:
        """
        Get total byte size of all column chunks in this row group.

        Returns:
            Total compressed byte size of all data

        Teaching Points:
        - This size includes compression savings
        - Useful for I/O planning and memory estimation
        - Row group size affects parallelization strategies
        - Typical row groups are 128MB-1GB in size
        """
        return self.row_group.total_byte_size

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f'RowGroupReader('
            f'rows={self.row_group.num_rows}, '
            f'columns={len(self.row_group.columns)}, '
            f'size={self.row_group.total_byte_size} bytes)'
        )

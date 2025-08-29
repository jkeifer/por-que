"""
ParquetFile: Main entry point for reading Parquet files.

Teaching Points:
- ParquetFile is the user-facing interface for Parquet data access
- It provides both metadata inspection and lazy data reading
- File metadata is loaded eagerly, but data reading is lazy
- The design mirrors Parquet's physical structure for educational clarity
"""

import logging

from collections.abc import Iterator
from dataclasses import asdict
from typing import Any

from .protocols import ReadableSeekable
from .readers.row_group import RowGroupReader
from .types import FileMetadata

logger = logging.getLogger(__name__)


class ParquetFile:
    """
    Main interface for reading Parquet files.

    Teaching Points:
    - ParquetFile represents the entire file and provides organized access to its
      contents
    - Metadata is read immediately to understand file structure
    - Row group readers are created lazily for memory efficiency
    - This design demonstrates why Parquet stores metadata at the end (fast file
      opening)
    - The API mirrors the physical file structure: file -> row groups -> columns
      -> pages
    """

    def __init__(self, file_obj: ReadableSeekable):
        """
        Initialize Parquet file reader.

        Args:
            file_obj: File-like object implementing BinaryIO

        Teaching Points:
        - Metadata is read immediately from the file footer
        - This requires seeking to the end of the file (why metadata is at the end)
        - The file handle is expected to remain open for lazy data reading
        - Schema information is extracted from metadata for type resolution
        - Caller is responsible for file lifecycle management

        Raises:
            ValueError: If file is not a valid Parquet file
        """
        self._file_obj = file_obj
        logger.debug('Using provided file-like object')

        # Read metadata using new parser infrastructure
        self._metadata = self._read_metadata_from_file()
        logger.debug(
            'Loaded metadata: version %d, %d row groups, %d total rows',
            self._metadata.version,
            len(self._metadata.row_groups),
            self._metadata.num_rows,
        )

        # Cache for row group readers (created lazily)
        self._row_group_readers: dict[int, RowGroupReader] = {}

    def _read_metadata_from_file(self) -> FileMetadata:
        """
        Read file metadata using our parser infrastructure.

        Teaching Points:
        - This mirrors the logic in FileMetadata.from_buffer but uses our parsers
        - We read the footer to find metadata location, then parse with MetadataParser
        - This demonstrates the complete parsing pipeline from file bytes to metadata
        """
        import struct

        from .constants import FOOTER_SIZE, PARQUET_MAGIC
        from .exceptions import ParquetMagicError
        from .parsers.parquet.metadata import MetadataParser

        # Seek to end to read footer
        self._file_obj.seek(0, 2)  # Seek to end
        file_size = self._file_obj.tell()

        if file_size < FOOTER_SIZE:
            raise ValueError('File too small to be a valid Parquet file')

        # Read footer
        self._file_obj.seek(file_size - FOOTER_SIZE)
        footer_bytes = self._file_obj.read(FOOTER_SIZE)

        if len(footer_bytes) != FOOTER_SIZE:
            raise ValueError('Could not read complete footer')

        # Parse footer
        footer_length = struct.unpack('<I', footer_bytes[:4])[0]
        magic = footer_bytes[4:8]

        if magic != PARQUET_MAGIC:
            raise ParquetMagicError(
                'Invalid Parquet magic bytes: '
                f'expected {PARQUET_MAGIC!r}, got {magic!r}',
            )

        # Read metadata section
        metadata_start = file_size - FOOTER_SIZE - footer_length
        if metadata_start < 0:
            raise ValueError('Invalid metadata offset')

        self._file_obj.seek(metadata_start)
        metadata_bytes = self._file_obj.read(footer_length)

        if len(metadata_bytes) != footer_length:
            raise ValueError('Could not read complete metadata')

        # Parse metadata using our new parser
        parser = MetadataParser(metadata_bytes)
        return parser.parse()

    @property
    def metadata(self) -> FileMetadata:
        """
        Get file metadata.

        Returns:
            Complete file metadata including schema and row group information

        Teaching Points:
        - Metadata contains the file's schema, row group locations, and statistics
        - This information is stored in the file footer for fast access
        - Metadata enables selective reading without scanning the entire file
        """
        return self._metadata

    @property
    def row_groups(self) -> list[RowGroupReader]:
        """
        Get readers for all row groups.

        Returns:
            List of RowGroupReader instances for each row group

        Teaching Points:
        - Row groups are created lazily when first accessed
        - Each row group reader provides access to its columns
        - Row groups enable parallel processing of large files
        - This property creates all readers - use row_group(index) for single access
        """
        readers = []
        for i in range(len(self._metadata.row_groups)):
            readers.append(self.row_group(i))
        return readers

    def row_group(self, index: int) -> RowGroupReader:
        """
        Get reader for a specific row group.

        Args:
            index: Row group index (0-based)

        Returns:
            RowGroupReader for the specified row group

        Raises:
            IndexError: If row group index is invalid

        Teaching Points:
        - Row group readers are cached to avoid recreating them
        - Each row group is independent and can be processed separately
        - Index-based access enables parallel processing strategies
        """
        if index < 0 or index >= len(self._metadata.row_groups):
            raise IndexError(
                f'Row group index {index} out of range. '
                f'File has {len(self._metadata.row_groups)} row groups.',
            )

        # Check cache first
        if index not in self._row_group_readers:
            row_group = self._metadata.row_groups[index]
            reader = RowGroupReader(
                self._file_obj,
                row_group,
                self._metadata.schema,
            )
            self._row_group_readers[index] = reader
            logger.debug('Created reader for row group %d', index)

        return self._row_group_readers[index]

    def column(self, name: str) -> Iterator[Any]:
        """
        Read a column across all row groups.

        Args:
            name: Column name to read

        Yields:
            Values from the specified column across all row groups

        Teaching Points:
        - This convenience method reads a column from the entire file
        - Values are yielded from each row group in sequence
        - This demonstrates Parquet's columnar access pattern
        - Memory usage stays low by processing one row group at a time

        Raises:
            ValueError: If column name is not found
        """
        logger.debug(
            'Reading column %s across %d row groups',
            name,
            len(self._metadata.row_groups),
        )

        # Verify column exists by checking first row group
        if len(self._metadata.row_groups) > 0:
            first_rg = self.row_group(0)
            if name not in first_rg.columns():
                available = first_rg.columns()
                raise ValueError(
                    f'Column "{name}" not found. Available columns: {available}',
                )

        # Read column from each row group
        total_values = 0
        for i in range(len(self._metadata.row_groups)):
            row_group_reader = self.row_group(i)
            column_reader = row_group_reader.column(name)

            logger.debug('Reading column %s from row group %d', name, i)
            for value in column_reader.read():
                yield value
                total_values += 1

        logger.debug('Column %s reading complete: %d total values', name, total_values)

    def columns(self) -> list[str]:
        """
        Get list of all column names in the file.

        Returns:
            List of column names available for reading

        Teaching Points:
        - Column names are consistent across all row groups
        - This enables schema discovery and validation
        - Names come from the schema stored in file metadata
        """
        if len(self._metadata.row_groups) == 0:
            return []

        # All row groups have the same schema, so use the first one
        first_rg = self.row_group(0)
        return first_rg.columns()

    def num_rows(self) -> int:
        """
        Get total number of rows in the file.

        Returns:
            Total rows across all row groups

        Teaching Points:
        - This is the sum of rows across all row groups
        - Stored in file metadata for fast access without scanning
        - Useful for memory estimation and progress tracking
        """
        return self._metadata.num_rows

    def num_row_groups(self) -> int:
        """
        Get number of row groups in the file.

        Returns:
            Number of row groups

        Teaching Points:
        - Row group count affects parallelization strategies
        - More row groups enable finer-grained parallel processing
        - Fewer row groups reduce metadata overhead
        """
        return len(self._metadata.row_groups)

    def schema_string(self) -> str:
        """
        Get human-readable schema description.

        Returns:
            Formatted string describing the file schema

        Teaching Points:
        - Schema describes the structure of data in the file
        - Parquet schemas are hierarchical and support nesting
        - Schema is immutable for the entire file
        """
        # This could be enhanced to format the schema nicely
        # For now, return basic information
        root = self._metadata.schema
        return f'Schema: {root.name} ({len(root.children)} columns)'

    def to_dict(self) -> dict:
        return asdict(self._metadata)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f'ParquetFile('
            f'rows={self.num_rows()}, '
            f'row_groups={self.num_row_groups()}, '
            f'columns={len(self.columns())})'
        )

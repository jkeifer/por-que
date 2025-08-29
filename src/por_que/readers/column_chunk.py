"""
ColumnChunkReader: Reads all pages within a single column chunk.

Teaching Points:
- Column chunks contain all pages for one column within one row group
- Dictionary pages (if present) must be read before data pages
- Each page contains a portion of the column's values
- Lazy reading means we only read pages when .read() is called
"""

import logging

from collections.abc import Iterator
from typing import Any

from por_que.enums import PageType
from por_que.protocols import ReadableSeekable
from por_que.types import ColumnChunk, SchemaElement

from .page import PageReader

logger = logging.getLogger(__name__)


class ColumnChunkReader:
    """
    Reads all pages within a single column chunk.

    Teaching Points:
    - A column chunk represents one column's data within one row group
    - Column chunks may contain multiple pages for memory efficiency
    - Dictionary encoding requires reading dictionary pages before data pages
    - Page-by-page reading enables streaming large columns without huge memory usage
    - The column metadata tells us where to find each type of page
    """

    def __init__(
        self,
        file_obj: ReadableSeekable,
        column_chunk: ColumnChunk,
        schema: SchemaElement | None = None,
    ):
        """
        Initialize column chunk reader.

        Args:
            file_obj: File-like object for reading data
            column_chunk: Column chunk metadata with page offsets
            schema: Root schema element for type information

        Teaching Points:
        - column_chunk.meta_data contains all the page offset information
        - Different page types have different offsets (dictionary, data, index)
        - Schema context helps with statistics parsing and logical type handling
        """
        self.file_obj = file_obj
        self.column_chunk = column_chunk
        self.schema = schema

        if not column_chunk.meta_data:
            raise ValueError(
                f'Column chunk at offset {column_chunk.file_offset} has no metadata. '
                'This suggests corrupted or incomplete file metadata.',
            )

        self.column_metadata = column_chunk.meta_data
        self.dictionary_values = None  # Will be populated if dictionary page exists

        logger.debug(
            'ColumnChunkReader initialized for column %s with %d values',
            self.column_metadata.path_in_schema,
            self.column_metadata.num_values,
        )

    def read(self) -> Iterator[Any]:
        """
        Read all values from this column chunk.

        Teaching Points:
        - Column reading follows a specific order: dictionary first, then data pages
        - Dictionary pages (if present) define value mappings for data pages
        - Data pages contain the actual column values (or dictionary indices)
        - Large columns may have many data pages to manage memory usage

        Yields:
            For now: (page_header, raw_page_data) tuples from each page
            Future: Actual decoded column values

        Raises:
            ValueError: If page offsets are invalid or pages cannot be read
        """
        logger.debug(
            'Reading column chunk: %s (%d total values)',
            self.column_metadata.path_in_schema,
            self.column_metadata.num_values,
        )

        # Step 1: Read dictionary page if present
        if self.column_metadata.dictionary_page_offset is not None:
            logger.debug(
                'Reading dictionary page at offset %d',
                self.column_metadata.dictionary_page_offset,
            )

            dict_reader = PageReader(
                self.file_obj,
                self.column_metadata.dictionary_page_offset,
                self.column_metadata,
                self.schema,
            )

            for page_data in dict_reader.read():
                page_header, raw_data = page_data

                # Validate this is actually a dictionary page
                if page_header.type != PageType.DICTIONARY_PAGE:
                    raise ValueError(
                        f'Expected DICTIONARY_PAGE at offset '
                        f'{self.column_metadata.dictionary_page_offset}, '
                        f'got {page_header.type.name}. This indicates corrupted '
                        'metadata.',
                    )

                logger.debug(
                    'Read dictionary page: %d entries',
                    page_header.dictionary_page_header.num_values
                    if page_header.dictionary_page_header
                    else 0,
                )

                # For now, yield the dictionary page data
                # In Phase 3, we'll decode this into self.dictionary_values
                yield page_data

        # Step 2: Read all data pages
        # For now, we assume all data follows the dictionary page
        # In a complete implementation, we'd need to parse page headers
        # to find page boundaries
        logger.debug(
            'Reading data pages starting at offset %d',
            self.column_metadata.data_page_offset,
        )

        data_reader = PageReader(
            self.file_obj,
            self.column_metadata.data_page_offset,
            self.column_metadata,
            self.schema,
        )

        values_read = 0
        for page_data in data_reader.read():
            page_header, raw_data = page_data

            # Validate this is a data page
            if page_header.type not in (PageType.DATA_PAGE, PageType.DATA_PAGE_V2):
                raise ValueError(
                    f'Expected DATA_PAGE at offset '
                    f'{self.column_metadata.data_page_offset}, '
                    f'got {page_header.type.name}. This indicates corrupted metadata.',
                )

            # Track how many values we've seen
            if page_header.data_page_header:
                page_values = page_header.data_page_header.num_values
            elif page_header.data_page_header_v2:
                page_values = page_header.data_page_header_v2.num_values
            else:
                page_values = 0

            values_read += page_values
            logger.debug(
                'Read data page: %d values (%d total so far)',
                page_values,
                values_read,
            )

            yield page_data

            # For now, we only read the first data page since we can't
            # reliably find page boundaries without implementing full parsing
            # This will be fixed when we add proper page iteration
            break

        logger.debug(
            'Column chunk reading complete: %d values read',
            values_read,
        )

    def column_name(self) -> str:
        """
        Get the column name from schema path.

        Returns:
            Column name (last component of the schema path)

        Teaching Points:
        - path_in_schema is a dot-separated path like "user.address.street"
        - The column name is typically the last component
        - Full path is needed for nested schema navigation
        """
        return self.column_metadata.path_in_schema.split('.')[-1]

    def value_count(self) -> int:
        """
        Get total number of values in this column chunk.

        Returns:
            Total values across all pages in this column chunk

        Teaching Points:
        - num_values is the total across all data pages
        - This count excludes dictionary entries (those are not column values)
        - NULL values are included in this count (they still take space in encoding)
        """
        return self.column_metadata.num_values

    def is_compressed(self) -> bool:
        """
        Check if this column chunk uses compression.

        Returns:
            True if compression is applied to pages

        Teaching Points:
        - Compression is applied at the page level within column chunks
        - Different columns can use different compression algorithms
        - UNCOMPRESSED means no compression is applied
        """
        from por_que.enums import Compression

        return self.column_metadata.codec != Compression.UNCOMPRESSED

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f'ColumnChunkReader('
            f'column={self.column_metadata.path_in_schema}, '
            f'values={self.column_metadata.num_values}, '
            f'codec={self.column_metadata.codec.name})'
        )

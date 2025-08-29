"""
PageReader: Individual page reading and decoding.

Teaching Points:
- PageReader handles a single page within a column chunk
- It uses PageParser for low-level header and data parsing
- Page-level decompression and decoding happens here
- This is where raw bytes become usable values (in future phases)
"""

import logging

from collections.abc import Iterator
from typing import Any

from por_que.enums import Compression
from por_que.protocols import ReadableSeekable
from por_que.types import ColumnMetadata, PageHeader

from ..parsers.parquet.page import PageParser
from ..parsers.thrift.parser import ThriftCompactParser

logger = logging.getLogger(__name__)


class PageReader:
    """
    Reads and decodes a single page within a column chunk.

    Teaching Points:
    - A page is the smallest unit of data reading in Parquet
    - Each page has a header describing its type, size, and encoding
    - Page data may be compressed according to the column's compression codec
    - Dictionary pages must be read before data pages that reference them
    - The page structure enables streaming reads without loading entire columns
    """

    def __init__(
        self,
        file_obj: ReadableSeekable,
        page_offset: int,
        column_metadata: ColumnMetadata,
        schema=None,
    ):
        """
        Initialize page reader for a specific page.

        Args:
            file_obj: File-like object positioned for reading
            page_offset: Byte offset in file where page starts
            column_metadata: Column metadata for compression and type info
            schema: Schema element for statistics parsing (optional)

        Teaching Points:
        - page_offset tells us exactly where in the file this page begins
        - column_metadata provides compression codec and type information
        - File seeking allows random access to pages without reading everything
        """
        self.file_obj = file_obj
        self.page_offset = page_offset
        self.column_metadata = column_metadata
        self.schema = schema
        logger.debug(
            'PageReader initialized at offset %d for column %s',
            page_offset,
            column_metadata.path_in_schema,
        )

    def read(self) -> Iterator[Any]:
        """
        Read and decode all values from this page.

        Teaching Points:
        - Page reading is a two-step process: header then data
        - Header tells us page type, size, and how many values to expect
        - Data section contains the actual encoded values
        - In future phases, this will handle decompression and decoding

        Yields:
            For now: (page_header, raw_page_data) tuples
            Future: Actual decoded values from the page

        Raises:
            ValueError: If page header is invalid or data is corrupted
        """
        logger.debug('Reading page at offset %d', self.page_offset)

        # Seek to page start
        self.file_obj.seek(self.page_offset)

        # Read entire remaining data (we'll optimize this later)
        remaining_data = self.file_obj.read()
        if not remaining_data:
            logger.debug('No data at page offset %d', self.page_offset)
            return

        # Create parser for page data
        parser = ThriftCompactParser(remaining_data)
        page_parser = PageParser(parser, self.schema)

        # Read page header
        try:
            page_header = page_parser.read_page_header()
            logger.debug(
                'Read page header: type=%s, %d values, %d compressed bytes',
                page_header.type.name,
                self._get_page_value_count(page_header),
                page_header.compressed_page_size,
            )
        except Exception as e:
            raise ValueError(
                f'Failed to read page header at offset {self.page_offset}: {e}. '
                'This may indicate file corruption or incorrect metadata.',
            ) from e

        # Read page data
        try:
            page_data = page_parser.read_page_data(page_header)
            logger.debug('Read %d bytes of page data', len(page_data))
        except Exception as e:
            raise ValueError(
                f'Failed to read page data at offset {self.page_offset}: {e}. '
                'Page header indicated {page_header.compressed_page_size} bytes.',
            ) from e

        # For now, yield the header and raw data
        # In Phase 3, this will decompress and decode the actual values
        yield (page_header, page_data)

        logger.debug('Page reading complete')

    def _get_page_value_count(self, page_header: PageHeader) -> int:
        """
        Extract value count from page header based on page type.

        Teaching Points:
        - Different page types store value count in different header fields
        - Dictionary pages contain dictionary entries, not column values
        - Data pages contain the actual column values for this page

        Args:
            page_header: Page header with type-specific information

        Returns:
            Number of values in this page
        """
        if page_header.data_page_header:
            return page_header.data_page_header.num_values
        if page_header.data_page_header_v2:
            return page_header.data_page_header_v2.num_values
        if page_header.dictionary_page_header:
            return page_header.dictionary_page_header.num_values
        return 0

    def _should_decompress_page(self, page_data: bytes) -> bool:
        """
        Determine if page data needs decompression.

        Teaching Points:
        - Compression is applied at the page level, not column level
        - Uncompressed pages can be read directly
        - Compressed pages need decompression before value decoding
        - The column metadata specifies which compression codec to use

        Args:
            page_data: Raw page data bytes

        Returns:
            True if decompression is needed, False otherwise
        """
        return (
            self.column_metadata.codec != Compression.UNCOMPRESSED
            and len(page_data) > 0
        )

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f'PageReader(offset={self.page_offset}, '
            f'column={self.column_metadata.path_in_schema})'
        )

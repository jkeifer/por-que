"""
Dictionary page content parsing for Parquet files.

This module provides functionality to parse the actual content of dictionary pages,
converting the compressed binary data into meaningful Python objects.
"""

from __future__ import annotations

import logging

from io import BytesIO
from typing import TYPE_CHECKING, TypeVar

from por_que.enums import Compression, Encoding, Type
from por_que.exceptions import ParquetDataError
from por_que.parsers import physical_types
from por_que.protocols import ReadableSeekable

if TYPE_CHECKING:
    from por_que.pages import DictionaryPage

from . import compressors

logger = logging.getLogger(__name__)

T = TypeVar(
    'T',
    bool,
    int,
    float,
    bytes,
)
type DictType[T] = list[T]


class DictionaryPageParser:
    """Parser for dictionary page content."""

    def parse_content(
        self,
        reader: ReadableSeekable,
        dictionary_page: DictionaryPage,
        physical_type: Type,
        compression_codec: Compression,
        schema_element,
    ) -> DictType:
        """
        Parse dictionary page content into Python objects.

        Args:
            reader: File-like object to read from
            dictionary_page: DictionaryPage instance with header info
            physical_type: Physical type of the dictionary values
            compression_codec: Compression codec used

        Returns:
            List of dictionary values as Python objects
        """
        # Seek to content start (after header)
        content_start = dictionary_page.start_offset + dictionary_page.header_size
        reader.seek(content_start)

        # Read compressed content
        compressed_data = reader.read(dictionary_page.compressed_page_size)
        if len(compressed_data) != dictionary_page.compressed_page_size:
            raise ParquetDataError(
                f'Could not read expected {dictionary_page.compressed_page_size} bytes '
                f'of dictionary content, got {len(compressed_data)} bytes',
            )

        # Decompress content
        decompressed_data = self._decompress_data(compressed_data, compression_codec)

        # Verify decompressed size matches expected
        if len(decompressed_data) != dictionary_page.uncompressed_page_size:
            logger.warning(
                "Decompressed dictionary size %d doesn't match expected %d",
                len(decompressed_data),
                dictionary_page.uncompressed_page_size,
            )

        # Parse values according to type and encoding
        return self._parse_values(
            decompressed_data,
            dictionary_page.num_values,
            physical_type,
            dictionary_page.encoding,
            schema_element,
        )

    def _decompress_data(self, compressed_data: bytes, codec: Compression) -> bytes:
        """Decompress data using the specified compression codec."""
        match codec:
            case Compression.UNCOMPRESSED:
                return compressed_data
            case Compression.SNAPPY:
                return compressors.get_snappy().decompress(compressed_data)
            case Compression.GZIP:
                return compressors.get_gzip().decompress(compressed_data)
            case Compression.LZO:
                return compressors.get_lzo().decompress(compressed_data)
            case Compression.BROTLI:
                return compressors.get_brotli().decompress(compressed_data)
            case Compression.ZSTD:
                import io

                dctx = compressors.get_zstd().ZstdDecompressor()
                # Use streaming decompression for frames without content size
                input_stream = io.BytesIO(compressed_data)
                reader = dctx.stream_reader(input_stream)
                return reader.readall()
            case _:
                raise ParquetDataError(f'Unsupported compression codec: {codec}')

    def _parse_values(  # noqa: C901
        self,
        data: bytes,
        num_values: int,
        physical_type: Type,
        encoding: Encoding,
        schema_element,
    ) -> DictType:
        """Parse values from decompressed data."""
        if encoding not in (Encoding.PLAIN, Encoding.PLAIN_DICTIONARY):
            raise ParquetDataError(
                f'Dictionary encoding {encoding} not yet supported. '
                'Currently PLAIN and PLAIN_DICTIONARY encodings are supported.',
            )

        values: DictType = []
        stream = BytesIO(data)

        match physical_type:
            case Type.BOOLEAN:
                values = physical_types.parse_boolean_values(stream, num_values)
            case Type.INT32:
                values = physical_types.parse_int32_values(stream, num_values)
            case Type.INT64:
                values = physical_types.parse_int64_values(stream, num_values)
            case Type.INT96:
                values = physical_types.parse_int96_values(stream, num_values)
            case Type.FLOAT:
                values = physical_types.parse_float_values(stream, num_values)
            case Type.DOUBLE:
                values = physical_types.parse_double_values(stream, num_values)
            case Type.BYTE_ARRAY:
                values = physical_types.parse_byte_array_values(stream, num_values)
            case Type.FIXED_LEN_BYTE_ARRAY:
                # Get type length from schema element
                if (
                    not schema_element
                    or not hasattr(schema_element, 'type_length')
                    or schema_element.type_length is None
                ):
                    raise ParquetDataError(
                        'FIXED_LEN_BYTE_ARRAY requires type_length from schema element',
                    )
                values = physical_types.parse_fixed_len_byte_array_values(
                    stream,
                    num_values,
                    schema_element.type_length,
                )
            case _:
                raise ParquetDataError(f'Unsupported physical type: {physical_type}')

        if len(values) != num_values:
            logger.warning(
                'Parsed %d values but expected %d from dictionary header',
                len(values),
                num_values,
            )

        return values

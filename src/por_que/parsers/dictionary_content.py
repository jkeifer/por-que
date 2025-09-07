"""
Dictionary page content parsing for Parquet files.

This module provides functionality to parse the actual content of dictionary pages,
converting the compressed binary data into meaningful Python objects.
"""

import logging
import struct

from io import BytesIO

from por_que.enums import Compression, Encoding, Type
from por_que.exceptions import ParquetDataError
from por_que.protocols import ReadableSeekable

logger = logging.getLogger(__name__)


def get_brotli():
    try:
        import brotli
    except ImportError:
        raise ParquetDataError(
            'Brotli compression requires brotli package',
        ) from None
    return brotli


def get_gzip():
    import gzip

    return gzip


def get_lzo():
    try:
        import lzo
    except ImportError:
        raise ParquetDataError(
            'LZO compression requires python-lzo package',
        ) from None
    return lzo


def get_snappy():
    try:
        import snappy
    except ImportError:
        raise ParquetDataError(
            'Snappy compression requires python-snappy package',
        ) from None
    return snappy


def get_zstd():
    try:
        import zstandard
    except ImportError:
        raise ParquetDataError(
            'Zstandard compression requires zstandard package',
        ) from None
    return zstandard


type DictType = list[bool] | list[int] | list[float] | list[bytes]


class DictionaryPageParser:
    """Parser for dictionary page content."""

    def parse_content(
        self,
        reader: ReadableSeekable,
        dictionary_page,  # DictionaryPage - avoiding circular import
        physical_type: Type,
        compression_codec: Compression,
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
        )

    def _decompress_data(self, compressed_data: bytes, codec: Compression) -> bytes:
        """Decompress data using the specified compression codec."""
        match codec:
            case Compression.UNCOMPRESSED:
                return compressed_data
            case Compression.SNAPPY:
                return get_snappy().decompress(compressed_data)
            case Compression.GZIP:
                return get_gzip().decompress(compressed_data)
            case Compression.LZO:
                return get_lzo().decompress(compressed_data)
            case Compression.BROTLI:
                return get_brotli().decompress(compressed_data)
            case Compression.ZSTD:
                dctx = get_zstd().ZstdDecompressor()
                return dctx.decompress(compressed_data)
            case _:
                raise ParquetDataError(f'Unsupported compression codec: {codec}')

    def _parse_values(
        self,
        data: bytes,
        num_values: int,
        physical_type: Type,
        encoding: Encoding,
    ) -> DictType:
        """Parse values from decompressed data."""
        if encoding != Encoding.PLAIN:
            raise ParquetDataError(
                f'Dictionary encoding {encoding} not yet supported. '
                'Currently only PLAIN encoding is supported.',
            )

        values: DictType = []
        stream = BytesIO(data)

        match physical_type:
            case Type.BOOLEAN:
                values = self._parse_boolean_values(stream, num_values)
            case Type.INT32:
                values = self._parse_int32_values(stream, num_values)
            case Type.INT64:
                values = self._parse_int64_values(stream, num_values)
            case Type.FLOAT:
                values = self._parse_float_values(stream, num_values)
            case Type.DOUBLE:
                values = self._parse_double_values(stream, num_values)
            case Type.BYTE_ARRAY:
                values = self._parse_byte_array_values(stream, num_values)
            case Type.FIXED_LEN_BYTE_ARRAY:
                # Would need type_length from schema for this
                raise ParquetDataError(
                    'FIXED_LEN_BYTE_ARRAY dictionary parsing not yet implemented',
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

    def _parse_boolean_values(self, stream: BytesIO, num_values: int) -> list[bool]:
        """Parse boolean values from stream."""
        values = []
        # Booleans are packed in bits
        bytes_needed = (num_values + 7) // 8
        data = stream.read(bytes_needed)

        for bit_index in range(num_values):
            byte_index = bit_index // 8
            bit_offset = bit_index % 8
            bit_value = (data[byte_index] >> bit_offset) & 1
            values.append(bool(bit_value))

        return values

    def _parse_int32_values(self, stream: BytesIO, num_values: int) -> list[int]:
        """Parse INT32 values from stream."""
        values = []
        for _ in range(num_values):
            data = stream.read(4)
            if len(data) != 4:
                break
            value = struct.unpack('<i', data)[0]
            values.append(value)
        return values

    def _parse_int64_values(self, stream: BytesIO, num_values: int) -> list[int]:
        """Parse INT64 values from stream."""
        values = []
        for _ in range(num_values):
            data = stream.read(8)
            if len(data) != 8:
                break
            value = struct.unpack('<q', data)[0]
            values.append(value)
        return values

    def _parse_float_values(self, stream: BytesIO, num_values: int) -> list[float]:
        """Parse FLOAT values from stream."""
        values = []
        for _ in range(num_values):
            data = stream.read(4)
            if len(data) != 4:
                break
            value = struct.unpack('<f', data)[0]
            values.append(value)
        return values

    def _parse_double_values(self, stream: BytesIO, num_values: int) -> list[float]:
        """Parse DOUBLE values from stream."""
        values = []
        for _ in range(num_values):
            data = stream.read(8)
            if len(data) != 8:
                break
            value = struct.unpack('<d', data)[0]
            values.append(value)
        return values

    def _parse_byte_array_values(self, stream: BytesIO, num_values: int) -> list[bytes]:
        """Parse BYTE_ARRAY values from stream."""
        values = []
        for _ in range(num_values):
            # Read length prefix (4 bytes, little-endian)
            length_data = stream.read(4)
            if len(length_data) != 4:
                break
            length = struct.unpack('<I', length_data)[0]

            # Read the actual data
            data = stream.read(length)
            if len(data) != length:
                break
            values.append(data)

        return values

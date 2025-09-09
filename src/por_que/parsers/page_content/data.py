"""
Data page content parsing for Parquet files.

This module provides functionality to parse the actual content of data pages,
converting the compressed binary data into meaningful Python objects with
proper handling of repetition levels, definition levels, and various encodings.

This implementation is modeled after the parquet-mr reference library to ensure
correctness and robustness.
"""

from __future__ import annotations

import logging
import math
import struct

from collections.abc import Sequence
from io import BytesIO
from typing import TYPE_CHECKING, TypeVar, assert_never

from por_que.enums import Compression, Encoding, Type
from por_que.exceptions import ParquetDataError
from por_que.file_metadata import SchemaLeaf
from por_que.parsers import physical_types
from por_que.parsers.logical_types import convert_values_to_logical_types
from por_que.protocols import ReadableSeekable

if TYPE_CHECKING:
    from por_que.pages import DataPageV1, DataPageV2

from . import compressors
from .dictionary import DictType

logger = logging.getLogger(__name__)

T = TypeVar(
    'T',
    bool,
    int,
    float,
    bytes,
)
type PageDataType[T] = list[T | None]


class DataPageParser:
    """Parser for data page content."""

    def parse_content(
        self,
        reader: ReadableSeekable,
        data_page: DataPageV1 | DataPageV2,
        physical_type: Type,
        compression_codec: Compression,
        schema_element: SchemaLeaf,
        dictionary_values: DictType | None = None,
        apply_logical_types: bool = True,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> PageDataType:
        """
        Parse data page content into Python objects. This is the public entry point.
        """
        from por_que.pages import DataPageV1, DataPageV2

        content_start = data_page.start_offset + data_page.header_size
        reader.seek(content_start)

        compressed_data = reader.read(data_page.compressed_page_size)
        decompressed_data = self._decompress_data(compressed_data, compression_codec)

        if len(decompressed_data) != data_page.uncompressed_page_size:
            raise ParquetDataError(
                "Decompressed data page size doesn't match expected size",
            )

        repetition_levels: list[int] = []
        definition_levels: list[int] = []

        # This unified block correctly handles page formats and populates levels
        match data_page:
            case DataPageV1():
                stream = BytesIO(decompressed_data)
                if schema_element.repetition.name == 'REPEATED':
                    bit_width = (schema_element.repetition_level).bit_length()
                    repetition_levels = self._decode_rle_with_length_prefix(
                        stream,
                        bit_width,
                        data_page.num_values,
                    )
                if schema_element.repetition.name != 'REQUIRED':
                    bit_width = (schema_element.definition_level).bit_length()
                    definition_levels = self._decode_rle_with_length_prefix(
                        stream,
                        bit_width,
                        data_page.num_values,
                    )
                values_stream = stream
            case DataPageV2():
                stream = BytesIO(decompressed_data)
                if data_page.repetition_levels_byte_length > 0:
                    rep_level_bytes = stream.read(
                        data_page.repetition_levels_byte_length,
                    )
                    bit_width = (schema_element.repetition_level).bit_length()
                    repetition_levels = self._decode_rle_levels(
                        BytesIO(rep_level_bytes),
                        bit_width,
                        data_page.num_values,
                    )
                if data_page.definition_levels_byte_length > 0:
                    def_level_bytes = stream.read(
                        data_page.definition_levels_byte_length,
                    )
                    bit_width = (schema_element.definition_level).bit_length()
                    definition_levels = self._decode_rle_levels(
                        BytesIO(def_level_bytes),
                        bit_width,
                        data_page.num_values,
                    )
                values_stream = stream
            case _ as unreachable:
                assert_never(unreachable)

        # Main parsing logic
        all_values = self._read_and_reassemble_values(
            values_stream,
            data_page.encoding,
            physical_type,
            data_page.num_values,
            dictionary_values,
            definition_levels,
            repetition_levels,
            schema_element,
        )

        if apply_logical_types:
            all_values = convert_values_to_logical_types(
                all_values,
                physical_type,
                schema_element,
                excluded_logical_columns,
            )
        return all_values

    def _decompress_data(self, compressed_data: bytes, codec: Compression) -> bytes:
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

    def _read_and_reassemble_values(
        self,
        stream: BytesIO,
        encoding: Encoding,
        physical_type: Type,
        num_values: int,
        dictionary_values: DictType | None,
        definition_levels: list[int],
        repetition_levels: list[int],
        schema_element: SchemaLeaf,
    ) -> PageDataType:
        if not definition_levels and schema_element.repetition.name == 'REQUIRED':
            definition_levels = [schema_element.definition_level] * num_values

        num_non_null = sum(
            1 for level in definition_levels if level >= schema_element.definition_level
        )

        non_null_values: list = self._read_values(
            stream,
            encoding,
            physical_type,
            num_non_null,
            dictionary_values,
        )

        # Handle repeated values using repetition levels from page data
        if schema_element.repetition.name == 'REPEATED':
            return self._reconstruct_repeated_values(
                non_null_values,
                definition_levels,
                repetition_levels,
                schema_element,
            )

        # Handle simple (non-repeated) values
        all_values: PageDataType = []
        non_null_iter = iter(non_null_values)
        # The max_definition_level indicates a non-null value for this schema element.
        # Levels less than that indicate nulls at this level.
        for level in definition_levels:
            if level >= schema_element.definition_level:
                try:
                    all_values.append(next(non_null_iter))
                except StopIteration:
                    raise ParquetDataError(
                        'Fewer non-null values than specified by definition levels.',
                    ) from None
            else:
                all_values.append(None)
        return all_values

    def _reconstruct_repeated_values(
        self,
        non_null_values: list,
        definition_levels: list[int],
        repetition_levels: list[int],
        schema_element: SchemaLeaf,
    ) -> PageDataType:
        """Reconstruct repeated/list values using repetition and definition levels."""
        result: list = []
        current_list: list = []
        non_null_iter = iter(non_null_values)

        for i, (def_level, rep_level) in enumerate(
            zip(definition_levels, repetition_levels, strict=False),
        ):
            # If repetition level is 0, we're starting a new list (or record)
            if rep_level == 0 and i > 0:
                # End current list and start new one
                result.append(current_list if current_list else None)
                current_list = []

            # Add value to current list based on definition level
            if def_level >= schema_element.definition_level:
                # Non-null value
                try:
                    value = next(non_null_iter)
                    current_list.append(value)
                except StopIteration:
                    raise ParquetDataError(
                        'Fewer non-null values than specified by definition levels.',
                    ) from None
            elif def_level > 0:
                # Null value within the list
                current_list.append(None)
            # If def_level == 0, the entire list is null, so we don't add anything

        # Add the final list
        if current_list or (definition_levels and definition_levels[-1] > 0):
            result.append(current_list if current_list else None)
        elif not result:
            # Handle case where there are no values
            result.append(None)

        return result

    def _read_values(
        self,
        stream: BytesIO,
        encoding: Encoding,
        physical_type: Type,
        num_non_null: int,
        dictionary_values: DictType | None,
    ) -> list:
        """Selects the correct value decoder based on the encoding."""
        if num_non_null == 0:
            return []

        match encoding:
            case Encoding.PLAIN:
                return self._read_plain_values(stream, physical_type, num_non_null)
            case Encoding.PLAIN_DICTIONARY | Encoding.RLE_DICTIONARY:
                return self._read_dictionary_values(
                    stream,
                    num_non_null,
                    dictionary_values,
                )
            case Encoding.DELTA_BINARY_PACKED:
                return self._read_delta_binary_packed_values(
                    stream,
                    num_non_null,
                )
            case Encoding.DELTA_BYTE_ARRAY:
                return self._read_delta_byte_array_values(stream, num_non_null)
            case _:
                raise ParquetDataError(f'Encoding {encoding} not supported.')

    def _decode_rle_with_length_prefix(
        self,
        stream: BytesIO,
        bit_width: int,
        num_values: int,
    ) -> list[int]:
        length_bytes = stream.read(4)
        if len(length_bytes) != 4:
            raise ParquetDataError('Could not read 4-byte length prefix for RLE levels')
        data_length = struct.unpack('<I', length_bytes)[0]
        rle_data = stream.read(data_length)
        return self._decode_rle_levels(BytesIO(rle_data), bit_width, num_values)

    def _decode_rle_levels(
        self,
        stream: BytesIO,
        bit_width: int,
        num_expected: int,
    ) -> list[int]:
        values: list[int] = []
        while len(values) < num_expected:
            header = self._read_varint(stream)
            if header is None:
                break

            if (header & 1) == 0:
                # RLE run
                count = header >> 1
                value_bytes_to_read = (bit_width + 7) // 8
                value_bytes = stream.read(value_bytes_to_read)
                if len(value_bytes) < value_bytes_to_read:
                    raise ParquetDataError('Unexpected EOF in RLE run')
                value = int.from_bytes(value_bytes, 'little')
                num_to_add = min(count, num_expected - len(values))
                values.extend([value] * num_to_add)
            else:
                # Bit-packed run
                num_groups = header >> 1
                count = num_groups * 8
                num_bytes_to_read = math.ceil(count * bit_width / 8)
                packed_bytes = stream.read(num_bytes_to_read)
                packed_values = self._read_bit_packed_integers(
                    BytesIO(packed_bytes),
                    count,
                    bit_width,
                )
                num_to_add = min(len(packed_values), num_expected - len(values))
                values.extend(packed_values[:num_to_add])

        return values

    def _read_varint(self, stream: BytesIO) -> int | None:
        result, shift = 0, 0
        while True:
            byte_data = stream.read(1)
            if not byte_data:
                return None
            byte = byte_data[0]
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                return result
            shift += 7

    def _read_zigzag_varint(self, stream: BytesIO) -> int | None:
        value = self._read_varint(stream)
        if value is None:
            return None
        return (value >> 1) ^ (-(value & 1))

    def _read_bit_packed_integers(
        self,
        stream: BytesIO,
        count: int,
        bit_width: int,
    ) -> list[int]:
        """
        A robust bit-stream reader that treats input as continuous bits.
        Uses a 64-bit buffer to handle cross-byte boundaries correctly.
        """
        if bit_width == 0:
            return [0] * count

        values: list[int] = []
        buffer = 0  # 64-bit buffer to hold bits
        bits_in_buffer = 0  # Number of valid bits in buffer
        mask = (1 << bit_width) - 1

        # Read values one at a time
        for _ in range(count):
            # Ensure we have enough bits in the buffer
            while bits_in_buffer < bit_width:
                byte_data = stream.read(1)
                if not byte_data:
                    # End of stream - check if we have partial bits
                    if bits_in_buffer >= bit_width:
                        break
                    # Not enough bits for another value
                    return values

                # Shift byte into buffer at the high end
                buffer |= byte_data[0] << bits_in_buffer
                bits_in_buffer += 8

            # Extract value from low-order bits
            value = buffer & mask
            values.append(value)

            # Consume the bits we just read
            buffer >>= bit_width
            bits_in_buffer -= bit_width

        return values

    # --- Per-Encoding Value Readers ---

    def _read_plain_values(
        self,
        stream: BytesIO,
        physical_type: Type,
        num_values: int,
    ) -> list:
        match physical_type:
            case Type.BOOLEAN:
                return physical_types.parse_boolean_values(stream, num_values)
            case Type.INT32:
                return physical_types.parse_int32_values(stream, num_values)
            case Type.INT64:
                return physical_types.parse_int64_values(stream, num_values)
            case Type.INT96:
                return physical_types.parse_int96_values(stream, num_values)
            case Type.FLOAT:
                return physical_types.parse_float_values(stream, num_values)
            case Type.DOUBLE:
                return physical_types.parse_double_values(stream, num_values)
            case Type.BYTE_ARRAY:
                return physical_types.parse_byte_array_values(stream, num_values)
            case _:
                raise ParquetDataError(f'Unsupported physical type: {physical_type}')

    def _read_dictionary_values(
        self,
        stream: BytesIO,
        num_values: int,
        dictionary_values: DictType[T] | None,
    ) -> list[T]:
        if dictionary_values is None:
            raise ParquetDataError('Dictionary values required')
        bit_width_bytes = stream.read(1)
        if not bit_width_bytes:
            raise ParquetDataError('Could not read bit width for dictionary indices')
        bit_width = bit_width_bytes[0]
        indices = self._decode_rle_levels(stream, bit_width, num_values)
        return [dictionary_values[i] for i in indices]

    def _read_delta_binary_packed_values(  # noqa: C901
        self,
        stream: BytesIO,
        num_values: int,
    ) -> list[int]:
        if num_values == 0:
            return []

        block_size = self._read_varint(stream)
        num_mini_blocks = self._read_varint(stream)
        total_value_count = self._read_varint(stream)
        first_value = self._read_zigzag_varint(stream)

        if (
            block_size is None
            or num_mini_blocks is None
            or total_value_count is None
            or first_value is None
        ):
            raise ParquetDataError('Could not read DELTA_BINARY_PACKED header')

        if num_values > total_value_count:
            raise ParquetDataError(
                f'Requesting {num_values} but DBP block only contains '
                f'{total_value_count}',
            )

        values = [first_value]
        if num_values == 1:
            return values

        current_value = first_value
        values_read = 1

        if num_mini_blocks == 0:
            if num_values > 1:
                raise ParquetDataError(
                    'DBP num_mini_blocks is 0 but more than one value is expected',
                )
            return values

        values_per_mini_block = block_size // num_mini_blocks

        # Read complete blocks (might have more values than we need due to padding)
        total_values_in_block = 0
        while total_values_in_block < total_value_count:
            min_delta = self._read_zigzag_varint(stream)
            if min_delta is None:
                raise ParquetDataError('Could not read block min_delta')

            bit_widths = stream.read(num_mini_blocks)
            for _, bit_width in enumerate(bit_widths):
                # Always read complete mini-blocks to advance stream correctly
                if bit_width == 0:
                    # All zeros, no data to read
                    mini_block_values = [0] * values_per_mini_block
                else:
                    mini_block_values = self._read_bit_packed_integers(
                        stream,
                        values_per_mini_block,
                        bit_width,
                    )

                # Process values but only keep what we need
                for _, delta in enumerate(mini_block_values):
                    if values_read >= num_values:
                        # Continue reading to advance stream, but don't store values
                        continue
                    current_value += delta + min_delta
                    values.append(current_value)
                    values_read += 1

                total_values_in_block += values_per_mini_block
        return values

    def _read_delta_byte_array_values(
        self,
        stream: BytesIO,
        num_non_null_values: int,
    ) -> list[bytes]:
        # DELTA_BYTE_ARRAY format (from parquet-format spec):
        # 1. DELTA_BINARY_PACKED prefix lengths (for all values)
        # 2. DELTA_BINARY_PACKED suffix lengths (for all values)
        # 3. Concatenated suffix data

        # Read prefix lengths first (for ALL values, first should typically be 0)
        prefix_lengths = self._read_delta_binary_packed_values(
            stream,
            num_non_null_values,
        )

        # Read suffix lengths (NOT total lengths!)
        suffix_lengths = self._read_delta_binary_packed_values(
            stream,
            num_non_null_values,
        )

        non_null_values: list[bytes] = []
        previous_value = b''

        for i in range(num_non_null_values):
            suffix_length = suffix_lengths[i]
            prefix_length = prefix_lengths[i]

            # Validate lengths
            if suffix_length < 0:
                raise ParquetDataError(
                    f'Invalid negative suffix_length at index {i}: {suffix_length}',
                )

            if prefix_length < 0:
                raise ParquetDataError(
                    f'Invalid negative prefix_length at index {i}: {prefix_length}',
                )

            if prefix_length > len(previous_value):
                raise ParquetDataError(
                    f'Invalid prefix length at index {i}: '
                    f'prefix={prefix_length}, prev_len={len(previous_value)}',
                )

            # Read suffix bytes
            suffix = stream.read(suffix_length)
            if len(suffix) != suffix_length:
                raise ParquetDataError(f'EOF reading suffix of length {suffix_length}')

            # Reconstruct value: prefix from previous + new suffix
            if prefix_length > 0:
                prefix = previous_value[:prefix_length]
                current_value = prefix + suffix
            else:
                current_value = suffix

            non_null_values.append(current_value)
            previous_value = current_value

        return non_null_values

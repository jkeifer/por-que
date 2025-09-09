"""
Physical type parsing utilities for Parquet data.

This module provides shared functionality for parsing physical Parquet types
from binary streams, eliminating code duplication between data and dictionary
page parsers.
"""

import struct

from io import BytesIO

from por_que.exceptions import ParquetDataError


def parse_boolean_values(stream: BytesIO, num_values: int) -> list[bool]:
    """Parse boolean values from stream."""
    values = []
    # Booleans are packed in bits
    bytes_needed = (num_values + 7) // 8
    data = stream.read(bytes_needed)

    for bit_index in range(num_values):
        byte_index = bit_index // 8
        bit_offset = bit_index % 8
        if byte_index < len(data):
            bit_value = (data[byte_index] >> bit_offset) & 1
            values.append(bool(bit_value))

    return values


def parse_int32_values(stream: BytesIO, num_values: int) -> list[int]:
    """Parse INT32 values from stream."""
    values = []
    for _ in range(num_values):
        data = stream.read(4)
        if len(data) != 4:
            break
        value = struct.unpack('<i', data)[0]
        values.append(value)
    return values


def parse_int64_values(stream: BytesIO, num_values: int) -> list[int]:
    """Parse INT64 values from stream."""
    values = []
    for _ in range(num_values):
        data = stream.read(8)
        if len(data) != 8:
            break
        value = struct.unpack('<q', data)[0]
        values.append(value)
    return values


def parse_int96_values(stream: BytesIO, num_values: int) -> list[int]:
    """Parse INT96 values from stream (stored as 12-byte values)."""
    values = []
    for _ in range(num_values):
        data = stream.read(12)  # INT96 is 12 bytes
        if len(data) != 12:
            break
        # Convert 12-byte little-endian value to integer
        value = int.from_bytes(data, 'little')
        values.append(value)
    return values


def parse_float_values(stream: BytesIO, num_values: int) -> list[float]:
    """Parse FLOAT values from stream."""
    values = []
    for _ in range(num_values):
        data = stream.read(4)
        if len(data) != 4:
            break
        value = struct.unpack('<f', data)[0]
        values.append(value)
    return values


def parse_double_values(stream: BytesIO, num_values: int) -> list[float]:
    """Parse DOUBLE values from stream."""
    values = []
    for _ in range(num_values):
        data = stream.read(8)
        if len(data) != 8:
            break
        value = struct.unpack('<d', data)[0]
        values.append(value)
    return values


def parse_byte_array_values(stream: BytesIO, num_values: int) -> list[bytes]:
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


# Raw bytes parsing functions for statistics and other use cases


def parse_boolean_from_bytes(raw_bytes: bytes) -> bool:
    """Parse boolean value from raw bytes."""
    if len(raw_bytes) != 1:
        raise ParquetDataError(f'BOOLEAN value must be 1 byte, got {len(raw_bytes)}')
    return raw_bytes[0] != 0


def parse_int32_from_bytes(raw_bytes: bytes) -> int:
    """Parse INT32 value from raw bytes."""
    if len(raw_bytes) != 4:
        raise ParquetDataError(f'INT32 value must be 4 bytes, got {len(raw_bytes)}')
    return int.from_bytes(raw_bytes, byteorder='little', signed=True)


def parse_int64_from_bytes(raw_bytes: bytes) -> int:
    """Parse INT64 value from raw bytes."""
    if len(raw_bytes) != 8:
        raise ParquetDataError(f'INT64 value must be 8 bytes, got {len(raw_bytes)}')
    return int.from_bytes(raw_bytes, byteorder='little', signed=True)


def parse_int96_from_bytes(raw_bytes: bytes) -> int:
    """Parse INT96 value from raw bytes."""
    if len(raw_bytes) != 12:
        raise ParquetDataError(f'INT96 value must be 12 bytes, got {len(raw_bytes)}')
    return int.from_bytes(raw_bytes, byteorder='little')


def parse_float_from_bytes(raw_bytes: bytes) -> float:
    """Parse FLOAT value from raw bytes."""
    if len(raw_bytes) != 4:
        raise ParquetDataError(f'FLOAT value must be 4 bytes, got {len(raw_bytes)}')
    return struct.unpack('<f', raw_bytes)[0]


def parse_double_from_bytes(raw_bytes: bytes) -> float:
    """Parse DOUBLE value from raw bytes."""
    if len(raw_bytes) != 8:
        raise ParquetDataError(f'DOUBLE value must be 8 bytes, got {len(raw_bytes)}')
    return struct.unpack('<d', raw_bytes)[0]

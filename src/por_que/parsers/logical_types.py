"""
Logical type conversion utilities for Parquet data values.

This module provides functionality to convert physical Parquet values to their
logical representations based on logical type annotations.
"""

from __future__ import annotations

import datetime
import struct
import uuid

from decimal import Decimal
from typing import cast

from por_que.enums import LogicalType, TimeUnit, Type
from por_que.exceptions import ParquetDataError
from por_que.file_metadata import (
    DecimalTypeInfo,
    LogicalTypeInfo,
    TimestampTypeInfo,
)

JULIAN_DAY_UNIX_EPOCH = 2440588


def _scale_time_value_to_seconds(value: int, unit: TimeUnit) -> Decimal:
    """Convert time value to seconds based on unit using precise decimal arithmetic."""
    decimal_value = Decimal(value)
    match unit:
        case TimeUnit.MILLIS:
            return decimal_value / Decimal('1000')
        case TimeUnit.MICROS:
            return decimal_value / Decimal('1000000')
        case TimeUnit.NANOS:
            return decimal_value / Decimal('1000000000')
        case _:
            msg = f'Unknown time unit: {unit}'
            raise ParquetDataError(msg)


def convert_single_value(
    value,
    physical_type: Type,
    logical_type_info: LogicalTypeInfo | None,
):
    """
    Convert a single physical value to its logical representation.

    Args:
        value: Physical value to convert
        physical_type: Physical type of the value
        logical_type_info: Logical type information (can be None)

    Returns:
        Converted value
    """
    if value is None:
        return None

    # Handle nested lists (repeated values)
    if isinstance(value, list):
        return [
            convert_single_value(
                item,
                physical_type,
                logical_type_info,
            )
            for item in value
        ]

    match physical_type:
        case Type.BOOLEAN | Type.FLOAT | Type.DOUBLE:
            # These types don't have logical type conversions
            return value
        case Type.INT32:
            return _convert_int32_value(value, logical_type_info)
        case Type.INT64:
            return _convert_int64_value(value, logical_type_info)
        case Type.INT96:
            return _convert_int96_value(value, logical_type_info)
        case Type.BYTE_ARRAY:
            return _convert_byte_array_value(value, logical_type_info)
        case Type.FIXED_LEN_BYTE_ARRAY:
            return _convert_fixed_len_byte_array_value(
                value,
                logical_type_info,
            )
        case _:
            # Unknown physical type, return as-is
            return value


def _convert_int32_value(
    value: int,
    logical_type_info: LogicalTypeInfo | None,
) -> int | datetime.date | datetime.time:
    """Convert INT32 value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.DATE:
            # DATE stores days since Unix epoch (1970-01-01)
            epoch = datetime.date(1970, 1, 1)
            return epoch + datetime.timedelta(days=value)
        case LogicalType.TIME:
            # Convert TIME values to datetime.time objects to match PyArrow
            unit = getattr(logical_type_info, 'unit', TimeUnit.MILLIS)
            total_seconds = _scale_time_value_to_seconds(value, unit)

            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = int(total_seconds % 60)
            microseconds = int((total_seconds % 1) * 1000000)

            return datetime.time(hours, minutes, seconds, microseconds)
        case LogicalType.INTEGER:
            # Integer with specific bit width and signedness
            return value
        case _:
            return value


def _convert_int64_value(
    value: int,
    logical_type_info: LogicalTypeInfo | None,
) -> int | datetime.datetime:
    """Convert INT64 value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.TIMESTAMP:
            return _convert_timestamp_value(
                value,
                cast(TimestampTypeInfo, logical_type_info),
            )
        case LogicalType.INTEGER:
            # Integer with specific bit width and signedness
            return value
        case _:
            return value


def _convert_int96_value(
    value: int,
    logical_type_info: LogicalTypeInfo | None,
) -> datetime.datetime:
    """Convert INT96 value based on logical type."""
    # INT96 is typically used for legacy timestamp storage in Spark/Hive
    # Format: 12 bytes = 8 bytes nanoseconds + 4 bytes Julian day number
    return _convert_int96_timestamp(value)


def _convert_int96_timestamp(value: int) -> datetime.datetime:
    """Convert INT96 timestamp to datetime object.

    INT96 timestamps in Parquet store:
    - First 8 bytes: nanoseconds within the day (little-endian)
    - Last 4 bytes: Julian day number (little-endian)

    Args:
        value: INT96 value as a large integer

    Returns:
        datetime object representing the timestamp
    """
    # Convert the large integer back to 12 bytes
    val_bytes = value.to_bytes(12, 'little')

    # Extract nanoseconds (first 8 bytes) and Julian day (last 4 bytes)
    nanoseconds = int.from_bytes(val_bytes[:8], 'little')
    julian_day = int.from_bytes(val_bytes[8:], 'little')

    # Convert Julian day to Unix days
    unix_days = julian_day - JULIAN_DAY_UNIX_EPOCH

    # Calculate total seconds
    total_seconds = unix_days * 24 * 60 * 60 + nanoseconds / 1_000_000_000

    # Convert to naive datetime (no timezone info, treating as UTC)
    return datetime.datetime.fromtimestamp(
        total_seconds,
        tz=datetime.UTC,
    ).replace(tzinfo=None)


def _convert_timestamp_value(
    value: int,
    logical_type_info: TimestampTypeInfo,
) -> datetime.datetime:
    """Convert timestamp value to datetime object."""
    timestamp_seconds = _scale_time_value_to_seconds(value, logical_type_info.unit)
    seconds_float = float(timestamp_seconds)

    if logical_type_info.is_adjusted_to_utc:
        return datetime.datetime.fromtimestamp(
            seconds_float,
            tz=datetime.UTC,
        )
    return datetime.datetime.fromtimestamp(seconds_float)  # noqa: DTZ006


def _convert_byte_array_value(
    value: bytes,
    logical_type_info: LogicalTypeInfo | None,
) -> str | bytes:
    """Convert BYTE_ARRAY value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.STRING:
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError as e:
                raise ParquetDataError(
                    f'STRING logical type value could not be decoded as UTF-8: {e}',
                ) from e
        case LogicalType.JSON | LogicalType.BSON | LogicalType.ENUM:
            # These are typically UTF-8 encoded
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                # If it fails, return as bytes
                return value
        case _:
            # Other logical types, leave as bytes for now
            return value


def _convert_fixed_len_byte_array_value(
    value: bytes,
    logical_type_info: LogicalTypeInfo | None,
) -> str | bytes | Decimal | uuid.UUID | float:
    """Convert FIXED_LEN_BYTE_ARRAY value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.DECIMAL:
            return _convert_decimal_value(
                value,
                cast(DecimalTypeInfo, logical_type_info),
            )
        case LogicalType.UUID:
            return _convert_uuid_type(value)
        case LogicalType.FLOAT16:
            return _convert_float16(value)
        case _:
            return value


def _convert_decimal_value(
    value: bytes,
    logical_type_info: DecimalTypeInfo,
) -> Decimal:
    # precision is not used with python Decimal type
    scale = logical_type_info.scale
    # Convert bytes to integer (big-endian for Parquet DECIMAL)
    unscaled = Decimal(int.from_bytes(value, byteorder='big', signed=True))
    return unscaled / (Decimal(10) ** scale)


def _convert_uuid_type(
    value: bytes,
) -> uuid.UUID:
    return uuid.UUID(bytes=value)


def _convert_float16(
    value: bytes,
) -> float:
    # Little-endian half-precision float
    return struct.unpack('<e', value)[0]

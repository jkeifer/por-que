"""
Logical type conversion utilities for Parquet data values.

This module provides functionality to convert physical Parquet values to their
logical representations based on logical type annotations.
"""

from __future__ import annotations

from collections.abc import Sequence

from por_que.enums import LogicalType, TimeUnit, Type
from por_que.exceptions import ParquetDataError
from por_que.file_metadata import LogicalTypeInfo, SchemaLeaf


def convert_values_to_logical_types(
    values: list,
    physical_type: Type,
    schema_element: SchemaLeaf,
    excluded_columns: Sequence[str] | None = None,
    infer_strings: bool = True,
) -> list:
    """
    Convert physical values to their logical representations.

    Args:
        values: List of physical values to convert
        physical_type: Physical type of the values
        schema_element: Schema element containing logical type information
        excluded_columns: Sequence of column names to exclude from conversion
        infer_strings: Whether to infer BYTE_ARRAY as UTF-8 strings when no logical type

    Returns:
        List of converted values
    """
    # Skip conversion if column is in excluded set
    if excluded_columns and schema_element.name in excluded_columns:
        return values

    # Get logical type information using the existing SchemaElement method
    logical_type_info = schema_element.get_logical_type()

    return convert_values_with_logical_type(
        values,
        physical_type,
        logical_type_info,
        infer_strings,
    )


def convert_values_with_logical_type(
    values: list,
    physical_type: Type,
    logical_type_info: LogicalTypeInfo | None,
    infer_strings: bool = True,
) -> list:
    """
    Convert physical values using explicit logical type information.

    Args:
        values: List of physical values to convert
        physical_type: Physical type of the values
        logical_type_info: Logical type information (can be None)
        infer_strings: Whether to infer BYTE_ARRAY as UTF-8 strings when no logical type

    Returns:
        List of converted values
    """
    if not values:
        return values

    # Convert each value individually
    converted_values = []
    for value in values:
        converted_value = convert_single_value(
            value,
            physical_type,
            logical_type_info,
            infer_strings,
        )
        converted_values.append(converted_value)

    return converted_values


def convert_single_value(
    value,
    physical_type: Type,
    logical_type_info: LogicalTypeInfo | None,
    infer_strings: bool = True,
):
    """
    Convert a single physical value to its logical representation.

    Args:
        value: Physical value to convert
        physical_type: Physical type of the value
        logical_type_info: Logical type information (can be None)
        infer_strings: Whether to infer BYTE_ARRAY as UTF-8 strings when no logical type

    Returns:
        Converted value
    """
    if value is None:
        return None

    # Handle nested lists (repeated values)
    if isinstance(value, list):
        return [
            convert_single_value(item, physical_type, logical_type_info, infer_strings)
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
            return _convert_byte_array_value(value, logical_type_info, infer_strings)
        case Type.FIXED_LEN_BYTE_ARRAY:
            return _convert_fixed_len_byte_array_value(
                value,
                logical_type_info,
                infer_strings,
            )
        case _:
            # Unknown physical type, return as-is
            return value


def _convert_int32_value(value: int, logical_type_info: LogicalTypeInfo | None):
    """Convert INT32 value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.DATE:
            # DATE stores days since Unix epoch (1970-01-01)
            # For now, return as integer - could convert to date object
            return value
        case LogicalType.TIME:
            if (
                hasattr(logical_type_info, 'unit')
                and logical_type_info.unit == TimeUnit.MILLIS
            ):
                # TIME_MILLIS stores milliseconds since midnight
                # For now, return as integer - could convert to time object
                return value
            return value
        case LogicalType.INTEGER:
            # Integer with specific bit width and signedness
            return value
        case _:
            return value


def _convert_int64_value(value: int, logical_type_info: LogicalTypeInfo | None):
    """Convert INT64 value based on logical type."""
    if logical_type_info is None:
        return value

    match logical_type_info.logical_type:
        case LogicalType.TIMESTAMP:
            if (
                hasattr(logical_type_info, 'unit')
                and logical_type_info.unit == TimeUnit.MILLIS
            ):
                # TIMESTAMP_MILLIS stores milliseconds since Unix epoch
                # For now, return as integer - could convert to datetime object
                return value
            if (
                hasattr(logical_type_info, 'unit')
                and logical_type_info.unit == TimeUnit.MICROS
            ):
                # TIMESTAMP_MICROS stores microseconds since Unix epoch
                # For now, return as integer - could convert to datetime object
                return value
            return value
        case LogicalType.INTEGER:
            # Integer with specific bit width and signedness
            return value
        case _:
            return value


def _convert_int96_value(value: int, logical_type_info: LogicalTypeInfo | None):
    """Convert INT96 value based on logical type."""
    # INT96 is typically used for legacy timestamp storage
    # For now, return as integer - could implement timestamp conversion
    return value


def _convert_byte_array_value(
    value: bytes,
    logical_type_info: LogicalTypeInfo | None,
    infer_strings: bool = True,
) -> str | bytes:
    """Convert BYTE_ARRAY value based on logical type."""
    if isinstance(value, str):
        # Already converted somehow
        return value

    if not isinstance(value, bytes):
        return value

    # Check for explicit STRING logical type
    if (
        logical_type_info is not None
        and logical_type_info.logical_type == LogicalType.STRING
    ):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ParquetDataError(
                f'STRING logical type value could not be decoded as UTF-8: {e}',
            ) from e

    # Handle other logical types
    if logical_type_info is not None:
        match logical_type_info.logical_type:
            case LogicalType.JSON | LogicalType.BSON | LogicalType.ENUM:
                # These are typically UTF-8 encoded
                try:
                    return value.decode('utf-8')
                except UnicodeDecodeError:
                    # If it fails, return as bytes
                    return value
            case _:
                # Other logical types, leave as bytes for now
                pass

    # Default to UTF-8 for backward compatibility if infer_strings is True
    if infer_strings:
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # If UTF-8 decoding fails, return as bytes
            return value

    return value


def _convert_fixed_len_byte_array_value(
    value: bytes,
    logical_type_info: LogicalTypeInfo | None,
    infer_strings: bool = True,
) -> str | bytes:
    """Convert FIXED_LEN_BYTE_ARRAY value based on logical type."""
    if isinstance(value, str):
        # Already converted somehow
        return value

    if not isinstance(value, bytes):
        return value

    if logical_type_info is not None:
        match logical_type_info.logical_type:
            case LogicalType.DECIMAL:
                # For now, return hex representation of decimal
                # Full decimal parsing requires precision/scale handling
                return f'0x{value.hex()}'
            case LogicalType.UUID:
                # UUIDs are 16-byte fixed length
                if len(value) == 16:
                    # Could format as standard UUID string
                    return f'0x{value.hex()}'
                return value
            case _:
                pass

    # Default to UTF-8 for backward compatibility if infer_strings is True
    if infer_strings:
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # If UTF-8 decoding fails, return as bytes
            return value

    return value

"""
Column statistics parsing for Parquet.

Teaching Points:
- Column statistics enable query optimization through predicate pushdown
- Min/max values allow skipping data during query execution (row groups, pages)
- Statistics are stored in binary format and must be deserialized per logical type
- Null and distinct counts provide additional optimization opportunities
- Statistics can be present at multiple levels: row groups, pages, etc.
"""

import logging

from datetime import UTC, date, datetime, timedelta

from por_que.enums import LogicalType, TimeUnit, Type
from por_que.exceptions import ParquetDataError
from por_que.file_metadata import (
    ColumnStatistics,
    LogicalTypeInfo,
    SchemaRoot,
)
from por_que.parsers import physical_types
from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftStructParser

from .base import BaseParser
from .enums import StatisticsFieldId

logger = logging.getLogger(__name__)


class StatisticsParser(BaseParser):
    """
    Parses column statistics from Parquet metadata.

    Teaching Points:
    - Statistics are the key to Parquet's query performance
    - Min/max values stored in physical format, decoded per logical type
    - Statistics enable "predicate pushdown" - filtering before reading data
    - File-level, row group-level, and page-level statistics provide nested optimization
    """

    def __init__(self, parser, schema: SchemaRoot) -> None:
        """
        Initialize statistics parser with schema context.

        Args:
            parser: ThriftCompactParser for parsing
            schema: Root schema element for logical type lookup
        """
        super().__init__(parser)
        self.schema = schema

    def read_statistics(
        self,
        column_type: Type,
        path_in_schema: str,
    ) -> ColumnStatistics:
        """
        Read Statistics struct for predicate pushdown.

        Teaching Points:
        - Min/max values are stored in their physical byte representation
        - Logical types require special deserialization (dates, timestamps, etc.)
        - Statistics are optional - missing values indicate unavailable optimization
        - Delta values (rarely used) provide additional compression for ordered data

        Args:
            column_type: Physical type of the column (INT32, BYTE_ARRAY, etc.)
            path_in_schema: Dot-separated path to find logical type information

        Returns:
            ColumnStatistics with deserialized min/max values
        """
        struct_parser = ThriftStructParser(self.parser)
        logger.debug(
            'Reading statistics for %s column at path %s',
            column_type,
            path_in_schema,
        )
        min_value: str | int | float | bool | None = None
        max_value: str | int | float | bool | None = None
        null_count: int | None = None
        distinct_count: int | None = None

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # MIN_VALUE and MAX_VALUE are special cases, they are BINARY
            # but need custom deserialization based on logical type.
            if field_id in (StatisticsFieldId.MIN_VALUE, StatisticsFieldId.MAX_VALUE):
                raw_bytes = struct_parser.read_value(field_type)
                if raw_bytes is None:
                    continue

                deserialized = self._deserialize_value(
                    raw_bytes,
                    column_type,
                    path_in_schema,
                )
                if field_id == StatisticsFieldId.MIN_VALUE:
                    min_value = deserialized
                else:
                    max_value = deserialized
                continue

            # MAX_VALUE_DELTA and MIN_VALUE_DELTA are also special
            if field_id in (
                StatisticsFieldId.MAX_VALUE_DELTA,
                StatisticsFieldId.MIN_VALUE_DELTA,
            ):
                # Skip delta values - rarely used and complex
                # These provide additional compression for sorted data
                struct_parser.skip_field(field_type)
                continue

            # Handle all other primitive fields
            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case StatisticsFieldId.NULL_COUNT:
                    null_count = value
                case StatisticsFieldId.DISTINCT_COUNT:
                    distinct_count = value

        return ColumnStatistics(
            min_value=min_value,
            max_value=max_value,
            null_count=null_count,
            distinct_count=distinct_count,
        )

    def _deserialize_value(
        self,
        raw_bytes: bytes,
        column_type: Type,
        path_in_schema: str,
    ) -> str | int | float | bool | None:
        """
        Deserialize binary value based on Parquet physical type and logical type.

        Teaching Points:
        - Physical types define storage format (how bytes are interpreted)
        - Logical types define semantic meaning (dates, timestamps, strings)
        - Same physical type can represent multiple logical types
        - Conversion preserves the original data's intended meaning

        Args:
            raw_bytes: Binary representation from statistics
            column_type: Physical type (Type enum)
            path_in_schema: Path to field for logical type lookup

        Returns:
            Deserialized value in appropriate Python type

        Raises:
            ParquetDataError: If deserialization fails or type is unsupported
            ThriftParsingError: If schmea element does not have a converted type
        """
        if not raw_bytes:
            return None

        element = self.schema.find_element(path_in_schema)

        logical_type_info = element.get_logical_type()

        logger.debug(
            'Deserializing %d bytes for type %s (logical=%s) at path %s',
            len(raw_bytes),
            column_type,
            logical_type_info,
            path_in_schema,
        )

        match column_type:
            case Type.BOOLEAN:
                return self._deserialize_boolean(raw_bytes)
            case Type.INT32:
                return self._deserialize_int32_value(raw_bytes, logical_type_info)
            case Type.INT64:
                return self._deserialize_int64_value(raw_bytes, logical_type_info)
            case Type.FLOAT:
                return self._deserialize_float(raw_bytes)
            case Type.DOUBLE:
                return self._deserialize_double(raw_bytes)
            case Type.BYTE_ARRAY:
                return self._deserialize_byte_array(raw_bytes, logical_type_info)
            case Type.FIXED_LEN_BYTE_ARRAY:
                return self._deserialize_fixed_len_byte_array(
                    raw_bytes,
                    logical_type_info,
                )
            case _:
                raise ParquetDataError(f'Unsupported column type: {column_type}')

    def _deserialize_boolean(self, raw_bytes: bytes) -> bool:
        """Deserialize boolean value from single byte."""
        return physical_types.parse_boolean_from_bytes(raw_bytes)

    def _deserialize_int32_value(
        self,
        raw_bytes: bytes,
        logical_type_info: LogicalTypeInfo | None,
    ) -> str | int:
        """
        Deserialize INT32 with logical type handling.

        Teaching Points:
        - INT32 can represent regular integers, dates, or time values
        - DATE stores days since Unix epoch (1970-01-01)
        - TIME_MILLIS stores milliseconds since midnight
        - Logical type determines interpretation, not storage format
        """
        if logical_type_info is None:
            return self._deserialize_int32(raw_bytes)

        match logical_type_info.logical_type:
            case LogicalType.DATE:
                return self._deserialize_date(raw_bytes)
            case LogicalType.TIME:
                if (
                    hasattr(logical_type_info, 'unit')
                    and logical_type_info.unit == TimeUnit.MILLIS
                ):
                    return self._deserialize_time_millis(raw_bytes)
                return self._deserialize_int32(raw_bytes)
            case _:
                return self._deserialize_int32(raw_bytes)

    def _deserialize_int64_value(
        self,
        raw_bytes: bytes,
        logical_type_info: LogicalTypeInfo | None,
    ) -> str | int:
        """
        Deserialize INT64 with logical type handling.

        Teaching Points:
        - INT64 can represent large integers or timestamp values
        - TIMESTAMP_MILLIS stores milliseconds since Unix epoch
        - Different timestamp precisions use different logical types
        """
        if logical_type_info is None:
            return self._deserialize_int64(raw_bytes)

        match logical_type_info.logical_type:
            case LogicalType.TIMESTAMP:
                if (
                    hasattr(logical_type_info, 'unit')
                    and logical_type_info.unit == TimeUnit.MILLIS
                ):
                    return self._deserialize_timestamp_millis(raw_bytes)
                if (
                    hasattr(logical_type_info, 'unit')
                    and logical_type_info.unit == TimeUnit.MICROS
                ):
                    return self._deserialize_timestamp_micros(raw_bytes)
                return self._deserialize_int64(raw_bytes)
            case _:
                return self._deserialize_int64(raw_bytes)

    def _deserialize_float(self, raw_bytes: bytes) -> float:
        """Deserialize IEEE 754 single-precision float."""
        return physical_types.parse_float_from_bytes(raw_bytes)

    def _deserialize_double(self, raw_bytes: bytes) -> float:
        """Deserialize IEEE 754 double-precision float."""
        return physical_types.parse_double_from_bytes(raw_bytes)

    def _deserialize_byte_array(
        self,
        raw_bytes: bytes,
        logical_type_info: LogicalTypeInfo | None,
    ) -> str:
        """
        Deserialize BYTE_ARRAY based on logical type.

        Teaching Points:
        - BYTE_ARRAY is variable-length binary data
        - UTF8 logical type indicates text strings
        - Without logical type, assume UTF-8 for compatibility
        """
        if (
            logical_type_info is not None
            and logical_type_info.logical_type == LogicalType.STRING
        ):
            return raw_bytes.decode('utf-8')

        # Default to UTF-8 for backward compatibility
        try:
            return raw_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ParquetDataError(
                f'BYTE_ARRAY could not be decoded as UTF-8: {e}',
            ) from e

    def _deserialize_fixed_len_byte_array(
        self,
        raw_bytes: bytes,
        logical_type_info: LogicalTypeInfo | None,
    ) -> str:
        """
        Deserialize FIXED_LEN_BYTE_ARRAY based on logical type.

        Teaching Points:
        - FIXED_LEN_BYTE_ARRAY has predetermined length per schema
        - DECIMAL logical type encodes fixed-point numbers
        - Binary format varies by logical type
        """
        if (
            logical_type_info is not None
            and logical_type_info.logical_type == LogicalType.DECIMAL
        ):
            # For now, return hex representation of decimal
            # Full decimal parsing requires precision/scale from schema
            return f'0x{raw_bytes.hex()}'

        # Default to UTF-8 for backward compatibility
        try:
            return raw_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ParquetDataError(
                f'FIXED_LEN_BYTE_ARRAY could not be decoded as UTF-8: {e}',
            ) from e

    def _deserialize_date(self, raw_bytes: bytes) -> str:
        """
        Deserialize DATE logical type (INT32 days since epoch).

        Teaching Points:
        - Parquet DATE uses days since Unix epoch (1970-01-01)
        - More efficient than storing full datetime for date-only data
        - Allows easy date arithmetic and comparison
        """
        days = physical_types.parse_int32_from_bytes(raw_bytes)
        return str(date(1970, 1, 1) + timedelta(days=days))

    def _deserialize_time_millis(self, raw_bytes: bytes) -> str:
        """
        Deserialize TIME_MILLIS logical type (INT32 milliseconds since midnight).

        Teaching Points:
        - TIME_MILLIS stores time-of-day without date information
        - Millisecond precision sufficient for most applications
        - More efficient than full timestamp for time-only data
        """
        millis = physical_types.parse_int32_from_bytes(raw_bytes)
        hours, remainder = divmod(millis, 3600000)
        minutes, remainder = divmod(remainder, 60000)
        seconds, millis = divmod(remainder, 1000)
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}'

    def _deserialize_timestamp_millis(self, raw_bytes: bytes) -> str:
        """
        Deserialize TIMESTAMP_MILLIS logical type (INT64 millis since epoch).

        Teaching Points:
        - TIMESTAMP_MILLIS provides millisecond precision timestamps
        - Uses Unix epoch (1970-01-01 00:00:00 UTC) as reference
        - INT64 provides sufficient range for most timestamp use cases
        """
        millis = physical_types.parse_int64_from_bytes(raw_bytes)
        return str(datetime.fromtimestamp(millis / 1000, tz=UTC))

    def _deserialize_timestamp_micros(self, raw_bytes: bytes) -> str:
        """
        Deserialize TIMESTAMP_MICROS logical type (INT64 micros since epoch).

        Teaching Points:
        - TIMESTAMP_MICROS provides microsecond precision timestamps
        - Uses Unix epoch (1970-01-01 00:00:00 UTC) as reference
        - INT64 provides sufficient range for most timestamp use cases
        """
        micros = physical_types.parse_int64_from_bytes(raw_bytes)
        return str(datetime.fromtimestamp(micros / 1_000_000, tz=UTC))

    def _deserialize_int32(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT32 value (little-endian)."""
        return physical_types.parse_int32_from_bytes(raw_bytes)

    def _deserialize_int64(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT64 value (little-endian)."""
        return physical_types.parse_int64_from_bytes(raw_bytes)

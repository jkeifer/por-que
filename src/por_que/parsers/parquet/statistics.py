"""
Row group statistics parsing for Parquet column statistics.

Teaching Points:
- Column statistics enable query optimization through predicate pushdown
- Min/max values allow skipping entire row groups during query execution
- Statistics are stored in binary format and must be deserialized per logical type
- Null and distinct counts provide additional optimization opportunities
"""

import logging

from datetime import UTC, date, datetime, timedelta

from por_que.enums import ConvertedType, Type
from por_que.exceptions import ParquetDataError, ThriftParsingError
from por_que.types import ColumnStatistics, SchemaElement

from ..thrift.enums import ThriftFieldType
from ..thrift.parser import ThriftStructParser
from .base import BaseParser
from .enums import StatisticsFieldId

logger = logging.getLogger(__name__)


class RowGroupStatisticsParser(BaseParser):
    """
    Parses column statistics embedded in row group metadata.

    Teaching Points:
    - Statistics are the key to Parquet's query performance
    - Min/max values stored in physical format, decoded per logical type
    - Statistics enable "predicate pushdown" - filtering before reading data
    - File-level, row group-level, and page-level statistics provide nested optimization
    """

    def __init__(self, parser, schema: SchemaElement):
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
        stats = ColumnStatistics()
        logger.debug(
            'Reading statistics for %s column at path %s',
            column_type,
            path_in_schema,
        )

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
                    stats.min_value = deserialized
                else:
                    stats.max_value = deserialized
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
                    stats.null_count = value
                case StatisticsFieldId.DISTINCT_COUNT:
                    stats.distinct_count = value

        return stats

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
            ThriftParsingError: If schema is not available for logical type lookup
        """
        if not raw_bytes:
            return None

        if not self.schema:
            raise ThriftParsingError('Schema must be read before parsing statistics')

        converted_type = self.schema.get_logical_type(path_in_schema)
        logger.debug(
            'Deserializing %d bytes for type %s (logical=%s) at path %s',
            len(raw_bytes),
            column_type,
            converted_type,
            path_in_schema,
        )

        match column_type:
            case Type.BOOLEAN:
                return self._deserialize_boolean(raw_bytes)
            case Type.INT32:
                return self._deserialize_int32_value(raw_bytes, converted_type)
            case Type.INT64:
                return self._deserialize_int64_value(raw_bytes, converted_type)
            case Type.FLOAT:
                return self._deserialize_float(raw_bytes)
            case Type.DOUBLE:
                return self._deserialize_double(raw_bytes)
            case Type.BYTE_ARRAY:
                return self._deserialize_byte_array(raw_bytes, converted_type)
            case Type.FIXED_LEN_BYTE_ARRAY:
                return self._deserialize_fixed_len_byte_array(raw_bytes, converted_type)
            case _:
                raise ParquetDataError(f'Unsupported column type: {column_type}')

    def _deserialize_boolean(self, raw_bytes: bytes) -> bool:
        """Deserialize boolean value from single byte."""
        return raw_bytes[0] != 0

    def _deserialize_int32_value(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str | int:
        """
        Deserialize INT32 with logical type handling.

        Teaching Points:
        - INT32 can represent regular integers, dates, or time values
        - DATE stores days since Unix epoch (1970-01-01)
        - TIME_MILLIS stores milliseconds since midnight
        - Logical type determines interpretation, not storage format
        """
        match converted_type:
            case ConvertedType.DATE:
                return self._deserialize_date(raw_bytes)
            case ConvertedType.TIME_MILLIS:
                return self._deserialize_time_millis(raw_bytes)
            case _:
                return self._deserialize_int32(raw_bytes)

    def _deserialize_int64_value(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str | int:
        """
        Deserialize INT64 with logical type handling.

        Teaching Points:
        - INT64 can represent large integers or timestamp values
        - TIMESTAMP_MILLIS stores milliseconds since Unix epoch
        - Different timestamp precisions use different logical types
        """
        match converted_type:
            case ConvertedType.TIMESTAMP_MILLIS:
                return self._deserialize_timestamp_millis(raw_bytes)
            case _:
                return self._deserialize_int64(raw_bytes)

    def _deserialize_float(self, raw_bytes: bytes) -> float:
        """Deserialize IEEE 754 single-precision float."""
        import struct

        if len(raw_bytes) != 4:
            raise ParquetDataError(
                f'FLOAT value must be 4 bytes, got {len(raw_bytes)}',
            )
        return struct.unpack('<f', raw_bytes)[0]

    def _deserialize_double(self, raw_bytes: bytes) -> float:
        """Deserialize IEEE 754 double-precision float."""
        import struct

        if len(raw_bytes) != 8:
            raise ParquetDataError(
                f'DOUBLE value must be 8 bytes, got {len(raw_bytes)}',
            )
        return struct.unpack('<d', raw_bytes)[0]

    def _deserialize_byte_array(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str:
        """
        Deserialize BYTE_ARRAY based on logical type.

        Teaching Points:
        - BYTE_ARRAY is variable-length binary data
        - UTF8 logical type indicates text strings
        - Without logical type, assume UTF-8 for compatibility
        """
        if converted_type == ConvertedType.UTF8:
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
        converted_type: ConvertedType | None,
    ) -> str:
        """
        Deserialize FIXED_LEN_BYTE_ARRAY based on logical type.

        Teaching Points:
        - FIXED_LEN_BYTE_ARRAY has predetermined length per schema
        - DECIMAL logical type encodes fixed-point numbers
        - Binary format varies by logical type
        """
        if converted_type == ConvertedType.DECIMAL:
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
        if len(raw_bytes) != 4:
            raise ParquetDataError(f'DATE value must be 4 bytes, got {len(raw_bytes)}')

        days = int.from_bytes(raw_bytes, byteorder='little', signed=True)
        return str(date(1970, 1, 1) + timedelta(days=days))

    def _deserialize_time_millis(self, raw_bytes: bytes) -> str:
        """
        Deserialize TIME_MILLIS logical type (INT32 milliseconds since midnight).

        Teaching Points:
        - TIME_MILLIS stores time-of-day without date information
        - Millisecond precision sufficient for most applications
        - More efficient than full timestamp for time-only data
        """
        if len(raw_bytes) != 4:
            raise ParquetDataError(
                f'TIME_MILLIS value must be 4 bytes, got {len(raw_bytes)}',
            )

        millis = int.from_bytes(raw_bytes, byteorder='little', signed=True)
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
        if len(raw_bytes) != 8:
            raise ParquetDataError(
                f'TIMESTAMP_MILLIS value must be 8 bytes, got {len(raw_bytes)}',
            )

        millis = int.from_bytes(raw_bytes, byteorder='little', signed=True)
        return str(datetime.fromtimestamp(millis / 1000, tz=UTC))

    def _deserialize_int32(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT32 value (little-endian)."""
        if len(raw_bytes) != 4:
            raise ParquetDataError(f'INT32 value must be 4 bytes, got {len(raw_bytes)}')
        return int.from_bytes(raw_bytes, byteorder='little', signed=True)

    def _deserialize_int64(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT64 value (little-endian)."""
        if len(raw_bytes) != 8:
            raise ParquetDataError(f'INT64 value must be 8 bytes, got {len(raw_bytes)}')
        return int.from_bytes(raw_bytes, byteorder='little', signed=True)

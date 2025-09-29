"""
Column statistics parsing for Parquet.

Teaching Points:
- Column statistics enable query optimization through predicate pushdown
- Min/max values allow skipping data during query execution (row groups, pages)
- Statistics are stored in binary format and must be deserialized per logical type
- Null and distinct counts provide additional optimization opportunities
- Statistics can be present at multiple levels: row groups, pages, etc.
"""

import warnings

from typing import Any

from por_que.enums import Type
from por_que.file_metadata import (
    ColumnStatistics,
    LogicalTypeInfo,
)
from por_que.parsers.logical_types import convert_single_value
from por_que.parsers.physical_types import parse_bytes

from .base import BaseParser
from .enums import StatisticsFieldId


class StatisticsParser(BaseParser):
    """
    Parses column statistics from Parquet metadata.

    Teaching Points:
    - Statistics are the key to Parquet's query performance
    - Min/max values stored in physical format, decoded per logical type
    - Statistics enable "predicate pushdown" - filtering before reading data
    - File-level, row group-level, and page-level statistics provide nested optimization
    """

    def read_statistics(
        self,
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
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case StatisticsFieldId.MIN:
                    props['min'] = value
                case StatisticsFieldId.MAX:
                    props['max'] = value
                case StatisticsFieldId.NULL_COUNT:
                    props['null_count'] = value
                case StatisticsFieldId.DISTINCT_COUNT:
                    props['distinct_count'] = value
                case StatisticsFieldId.MIN_VALUE:
                    props['min_value'] = value
                case StatisticsFieldId.MAX_VALUE:
                    props['max_value'] = value
                case StatisticsFieldId.IS_MIN_VALUE_EXACT:
                    props['is_min_value_exact'] = value
                case StatisticsFieldId.IS_MAX_VALUE_EXACT:
                    props['is_max_value_exact'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown statistics field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return ColumnStatistics(**props)

    def _parse_value(
        self,
        value: bytes,
        physical_type: Type,
        logical_type_info: LogicalTypeInfo | None,
    ) -> Any:
        return convert_single_value(
            parse_bytes(value, physical_type),
            physical_type,
            logical_type_info,
        )

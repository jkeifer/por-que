from collections.abc import Iterator
from typing import Any

from por_que.parsers.thrift.enums import ThriftFieldType
from por_que.parsers.thrift.parser import ThriftCompactParser, ThriftStructParser


class BaseParser:
    """
    Base parser for all Thrift struct parsing in Parquet files.

    Teaching Points:
    - Parquet uses Apache Thrift's compact protocol for metadata serialization
    - The compact protocol uses variable-length encoding to save space
    - Field IDs allow schema evolution - fields can be added without breaking
      compatibility
    - The protocol includes type information for self-describing data structures
    """

    def __init__(self, parser: ThriftCompactParser) -> None:
        """
        Initialize parser with a Thrift compact protocol parser.

        Args:
            parser: ThriftCompactParser positioned at the start of a struct
        """
        self.parser = parser

    def read(self, length: int = 1) -> bytes:
        return self.parser.read(length)

    def read_varint(self) -> int:
        return self.parser.read_varint()

    def read_zigzag(self) -> int:
        return self.parser.read_zigzag()

    def read_bool(self) -> bool:
        return self.parser.read_bool()

    def read_i32(self) -> int:
        return self.parser.read_i32()

    def read_i64(self) -> int:
        return self.parser.read_i64()

    def read_string(self) -> str:
        return self.parser.read_string()

    def read_bytes(self) -> bytes:
        return self.parser.read_bytes()

    def maybe_skip_field(self, field_type: ThriftFieldType | int) -> None:
        if not isinstance(field_type, ThriftFieldType):
            field_type = ThriftFieldType(field_type)

        if field_type.is_complex:
            self.parser.skip_field(field_type)

    def parse_struct_fields(
        self,
    ) -> Iterator[tuple[int, int, Any]]:
        """
        Yield-based struct field parsing that preserves context and parsing state.

        Teaching Points:
        - Yields each field as encountered, preserving parsing position
        - Caller retains full context about which struct type is being parsed
        - Enables flexible per-field handling without complex orchestration
        - Avoids parsing state corruption between different field types

        Yields:
            Tuples of (field_id, field_type, value) where:
            - For simple types: value is the parsed primitive value
            - For complex types (LIST, STRUCT): value is None, caller handles parsing
        """
        struct_parser = ThriftStructParser(self.parser)

        while True:
            field_type, field_id = struct_parser.read_field_header()

            match field_type:
                case ThriftFieldType.STOP:
                    break
                case ThriftFieldType.LIST:
                    # For LIST, must read header and optional STOP
                    yield field_id, field_type, self.parser.yield_list_elements()
                case ThriftFieldType.STRUCT:
                    # For STRUCT, no header to read - just yield
                    yield field_id, field_type, None
                case _:
                    # Handle simple field types (primitives)
                    value = struct_parser.read_value(field_type)
                    yield field_id, field_type, value

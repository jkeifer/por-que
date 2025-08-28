from collections.abc import Callable
from typing import TypeVar

from ..thrift.constants import (
    THRIFT_FIELD_TYPE_MASK,
    THRIFT_SIZE_SHIFT,
    THRIFT_SPECIAL_LIST_SIZE,
)
from ..thrift.parser import ThriftCompactParser

T = TypeVar('T')


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

    def __init__(self, parser: ThriftCompactParser):
        """
        Initialize parser with a Thrift compact protocol parser.

        Args:
            parser: ThriftCompactParser positioned at the start of a struct
        """
        self.parser = parser

    def read_list(self, read_element_func: Callable[[], T]) -> list[T]:
        """
        Read a list of elements using Thrift compact protocol.

        Teaching Points:
        - Lists in Thrift encode element type and count in a header byte
        - Size field uses 4 bits, with special handling for sizes >= 15
        - This enables efficient storage of both small and large lists
        """
        header = int.from_bytes(self.read())
        size = header >> THRIFT_SIZE_SHIFT  # Size from upper 4 bits
        # TODO: determine if we need element type for anything
        _ = header & THRIFT_FIELD_TYPE_MASK  # Element type from lower 4 bits

        # If size == 15, read actual size from varint
        if size == THRIFT_SPECIAL_LIST_SIZE:
            size = self.read_varint()

        elements: list[T] = []
        for _ in range(size):
            if self.at_end():
                break
            elements.append(read_element_func())

        return elements

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

    def at_end(self) -> bool:
        return self.parser.at_end()

    def skip_field(self, field_type):
        """Skip a field of the given type in the Thrift stream."""
        # This will delegate to the ThriftStructParser's skip_field method
        # when used in the context of struct parsing
        pass

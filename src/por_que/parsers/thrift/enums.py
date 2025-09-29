from enum import IntEnum


# Thrift Compact Protocol field types
class ThriftFieldType(IntEnum):
    """
    Field type identifiers for Thrift compact protocol.

    Teaching Points:
    - Each type has a specific wire format and parsing method
    - BOOL values are encoded in the type itself (no separate data)
    - Variable-length types (BINARY, LIST, etc.) include length prefixes
    - Type information enables generic field skipping
    """

    STOP = 0
    BOOL_TRUE = 1
    BOOL_FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    SET = 10
    MAP = 11
    STRUCT = 12

    @property
    def is_complex(self) -> bool:
        return self in (self.LIST, self.SET, self.MAP, self.STRUCT)

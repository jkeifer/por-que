import logging
import struct

from typing import Any

from por_que.exceptions import InvalidStringLengthError

from .constants import (
    DEFAULT_STRING_ENCODING,
    THRIFT_FIELD_TYPE_MASK,
    THRIFT_MAP_TYPE_SHIFT,
    THRIFT_SIZE_SHIFT,
    THRIFT_SPECIAL_LIST_SIZE,
    THRIFT_VARINT_CONTINUE,
    THRIFT_VARINT_MASK,
)
from .enums import ThriftFieldType

logger = logging.getLogger(__name__)


class ThriftCompactParser:
    """
    Parser for Apache Thrift's compact binary protocol.

    Teaching Points:
    - Thrift compact protocol uses variable-length encoding to save space
    - Zigzag encoding allows negative numbers to be encoded efficiently
    - Field deltas enable sparse field IDs without wasting bytes
    """

    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def read(self, length: int = 1) -> bytes:
        data = self.data[self.pos : self.pos + length]
        self.pos += len(data)
        return data

    def read_varint(self) -> int:
        """
        Read variable-length integer from the stream.

        Teaching Points:
        - Varints save space for small numbers (most field IDs are small)
        - Continue bit indicates if more bytes follow
        - Little-endian 7-bit chunks with continuation bit
        """
        start_pos = self.pos
        result = 0
        shift = 0
        while self.pos < len(self.data):
            byte = int.from_bytes(self.read())
            result |= (byte & THRIFT_VARINT_MASK) << shift
            if (byte & THRIFT_VARINT_CONTINUE) == 0:
                break
            shift += 7
        logger.debug(
            'Read varint at pos %d: %d (%d bytes)',
            start_pos,
            result,
            self.pos - start_pos,
        )
        return result

    def read_zigzag(self) -> int:
        """
        Read zigzag-encoded signed integer.

        Teaching Points:
        - Zigzag encoding maps signed integers to unsigned ones
        - Small negative numbers (-1, -2) become small positive numbers (1, 3)
        - This makes varint encoding efficient for negative numbers too
        """
        n = self.read_varint()
        result = (n >> 1) ^ -(n & 1)
        logger.debug('Read zigzag: %d (from varint %d)', result, n)
        return result

    def read_bool(self) -> bool:
        return self.read() == 1

    def read_i32(self) -> int:
        return self.read_zigzag()

    def read_i64(self) -> int:
        return self.read_zigzag()

    def read_string(self) -> str:
        length = self.read_varint()
        logger.debug('Reading string of length %d at pos %d', length, self.pos)

        if length < 0 or self.pos + length > len(self.data):
            raise InvalidStringLengthError(
                f'Invalid string length {length} at position {self.pos}. '
                f'Length cannot be negative or exceed buffer bounds.',
            )

        result = self.read(length).decode(DEFAULT_STRING_ENCODING)
        logger.debug('Read string: %r', result)
        return result

    def read_bytes(self) -> bytes:
        length = self.read_varint()
        logger.debug('Reading %d bytes at pos %d', length, self.pos)
        result = self.read(length)
        hex_preview = result.hex()[:32] + ('...' if len(result) > 16 else '')
        logger.debug('Read %d bytes: %s', length, hex_preview)
        return result

    def skip(self, n: int) -> None:
        """Skip n bytes"""
        self.read(n)

    def at_end(self) -> bool:
        """Check if at end of data"""
        return self.pos >= len(self.data)


class ThriftStructParser:
    """
    Parser for a single Thrift struct - tracks field IDs internally.

    Teaching Points:
    - Struct parsing tracks the last field ID to enable delta encoding
    - Field deltas save space when field IDs are sequential
    - STOP field (0x00) indicates end of struct
    - Unknown fields can be skipped for forward compatibility
    """

    def __init__(self, parser: ThriftCompactParser) -> None:
        self.parser = parser
        self.last_field_id = 0

    def read_field_header(self) -> tuple[int, int]:
        """
        Read field header and return (field_type, field_id).

        Teaching Points:
        - Field headers encode both type and ID information
        - Field ID deltas save space for sequential fields
        - Type information enables generic field skipping
        """
        if self.parser.at_end():
            return ThriftFieldType.STOP, 0

        byte = int.from_bytes(self.parser.read())

        field_type = byte & THRIFT_FIELD_TYPE_MASK
        field_delta = byte >> 4

        if field_delta == 0:
            # Special case: STOP field is just 0x00, no zigzag varint to read
            if field_type == ThriftFieldType.STOP:
                field_delta = 0
            else:
                field_delta = self.parser.read_zigzag()

        self.last_field_id += field_delta
        logger.debug(
            'Read field header: type=%d, id=%d (delta=%d)',
            field_type,
            self.last_field_id,
            field_delta,
        )
        return field_type, self.last_field_id

    def read_value(self, field_type: int) -> Any:
        """Read a value of a given type from the stream."""
        match field_type:
            case ThriftFieldType.BOOL_TRUE:
                return True
            case ThriftFieldType.BOOL_FALSE:
                return False
            case ThriftFieldType.BYTE:
                return self.parser.read(1)
            case ThriftFieldType.I16 | ThriftFieldType.I32:
                return self.parser.read_i32()
            case ThriftFieldType.I64:
                return self.parser.read_i64()
            case ThriftFieldType.DOUBLE:
                return struct.unpack('<d', self.parser.read(8))[0]
            case ThriftFieldType.BINARY:
                return self.parser.read_bytes()
            case _:
                self.skip_field(field_type)
                return None

    def skip_field(self, field_type: int) -> None:  # noqa: C901
        """Skip a field based on its type"""
        if self.parser.at_end():
            return

        if (
            field_type == ThriftFieldType.BOOL_TRUE
            or field_type == ThriftFieldType.BOOL_FALSE
        ):
            # No data to skip
            return

        if field_type == ThriftFieldType.BYTE:
            self.parser.skip(1)
        elif field_type in [
            ThriftFieldType.I16,
            ThriftFieldType.I32,
            ThriftFieldType.I64,
        ]:
            self.parser.read_varint()
        elif field_type == ThriftFieldType.DOUBLE:
            self.parser.skip(8)
        elif field_type == ThriftFieldType.BINARY:
            length = self.parser.read_varint()
            self.parser.skip(length)
        elif field_type == ThriftFieldType.STRUCT:
            # Create a new struct parser for the nested struct
            nested = ThriftStructParser(self.parser)
            while True:
                ftype, _ = nested.read_field_header()
                if ftype == ThriftFieldType.STOP:
                    break
                nested.skip_field(ftype)
        elif field_type == ThriftFieldType.LIST:
            self.skip_list()
        elif field_type == ThriftFieldType.SET:
            # Same as list
            self.skip_list()
        elif field_type == ThriftFieldType.MAP:
            self.skip_map()

    def skip_list(self) -> None:
        """Skip a list/set"""
        if self.parser.at_end():
            return

        header = int.from_bytes(self.parser.read())
        size = header >> THRIFT_SIZE_SHIFT  # Size from upper 4 bits
        elem_type = header & THRIFT_FIELD_TYPE_MASK  # Element type from lower 4 bits

        # If size == 15, read actual size from varint
        if size == THRIFT_SPECIAL_LIST_SIZE:
            size = self.parser.read_varint()

        # Skip all elements
        skip_parser = ThriftStructParser(self.parser)
        for _ in range(size):
            if self.parser.at_end():
                break
            skip_parser.skip_field(elem_type)

    def skip_map(self) -> None:
        """Skip a map"""
        if self.parser.at_end():
            return

        # Maps always encode size as varint (unlike lists)
        size = self.parser.read_varint()

        if size > 0:
            types_byte = int.from_bytes(self.parser.read())
            key_type = (types_byte >> THRIFT_MAP_TYPE_SHIFT) & THRIFT_FIELD_TYPE_MASK
            val_type = types_byte & THRIFT_FIELD_TYPE_MASK

            skip_parser = ThriftStructParser(self.parser)
            for _ in range(size):
                if self.parser.at_end():
                    break
                skip_parser.skip_field(key_type)
                skip_parser.skip_field(val_type)

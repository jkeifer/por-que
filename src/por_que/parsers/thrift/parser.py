import logging
import struct

from collections.abc import Iterator
from typing import Any

from por_que.exceptions import (
    BufferExhaustedError,
    InvalidStringLengthError,
    ThriftParsingError,
)

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
    - Operates over an in-memory byte span. Decoding is pure computation with
      no I/O, so every read is a plain function call rather than an await.

    The parser holds a ``memoryview`` of the span plus the absolute file offset
    at which that span begins. It tracks an index into the buffer and reports
    ``pos``/``tell`` in absolute file coordinates, so the teaching fields the
    models record (``start_offset``, ``byte_length``) are identical to a parse
    that read straight from the file.
    """

    def __init__(self, data: bytes | memoryview, base_offset: int) -> None:
        self.data = memoryview(data)
        self.base_offset = base_offset
        # Index into the buffer; absolute position is base_offset + index.
        self.index = 0

    def read(self, length: int = 1) -> bytes:
        """Read bytes from the buffer.

        Raises:
            BufferExhaustedError: If the read runs past the end of the buffer. For a
                speculative span this means "fetch more and retry".
        """
        end = self.index + length
        if end > len(self.data):
            raise BufferExhaustedError(
                f'Read of {length} bytes at file offset {self.pos} runs past '
                f'the end of the {len(self.data)}-byte buffer.',
            )
        chunk = self.data[self.index : end]
        self.index = end
        return bytes(chunk)

    @property
    def pos(self) -> int:
        """Current absolute file position."""
        return self.base_offset + self.index

    def tell(self) -> int:
        """Current absolute file position."""
        return self.pos

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
        while True:
            byte_data = self.read(1)
            if not byte_data:
                break
            byte = int.from_bytes(byte_data)
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

        if length < 0:
            raise InvalidStringLengthError(
                f'Invalid string length {length} at position {self.pos}. '
                f'Length cannot be negative.',
            )

        data = self.read(length)
        if len(data) != length:
            raise InvalidStringLengthError(
                f'Could not read {length} bytes for string at position {self.pos}. '
                f'Only {len(data)} bytes available.',
            )

        result = data.decode(DEFAULT_STRING_ENCODING)
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
        end = self.index + n
        if end > len(self.data):
            raise BufferExhaustedError(
                f'Skip of {n} bytes at file offset {self.pos} runs past the '
                f'end of the {len(self.data)}-byte buffer.',
            )
        self.index = end

    def read_value(self, field_type: int) -> Any:
        """Read a value of a given type from the stream."""
        match field_type:
            case ThriftFieldType.BOOL_TRUE:
                return True
            case ThriftFieldType.BOOL_FALSE:
                return False
            case ThriftFieldType.BYTE:
                return self.read(1)
            case ThriftFieldType.I16 | ThriftFieldType.I32:
                return self.read_i32()
            case ThriftFieldType.I64:
                return self.read_i64()
            case ThriftFieldType.DOUBLE:
                return struct.unpack('<d', self.read(8))[0]
            case ThriftFieldType.BINARY:
                return self.read_bytes()
            case _:
                self.skip_field(field_type)
                return None

    def skip_field(self, field_type: int) -> None:  # noqa: C901
        match field_type:
            case ThriftFieldType.BOOL_TRUE | ThriftFieldType.BOOL_FALSE:
                # No data to skip
                return
            case ThriftFieldType.BYTE:
                self.skip(1)
            case ThriftFieldType.I16 | ThriftFieldType.I32 | ThriftFieldType.I64:
                self.read_varint()
            case ThriftFieldType.DOUBLE:
                self.skip(8)
            case ThriftFieldType.BINARY:
                self.skip(self.read_varint())
            case ThriftFieldType.STRUCT:
                nested = ThriftStructParser(self)
                while True:
                    ftype, _ = nested.read_field_header()
                    if ftype == ThriftFieldType.STOP:
                        break
                    self.skip_field(ftype)
            case ThriftFieldType.LIST | ThriftFieldType.SET:
                self.skip_list()
            case ThriftFieldType.MAP:
                self.skip_map()
            case _:
                raise ThriftParsingError(
                    f'Unknown thrift type: {field_type}',
                )

    def skip_list(self) -> None:
        """Skip a list/set"""
        for elem_type in self.yield_list_elements():
            self.skip_field(elem_type)

    def skip_map(self) -> None:
        """Skip a map"""
        # Maps always encode size as varint (unlike lists)
        size = self.read_varint()

        if size > 0:
            types_byte = int.from_bytes(self.read())
            key_type = (types_byte >> THRIFT_MAP_TYPE_SHIFT) & THRIFT_FIELD_TYPE_MASK
            val_type = types_byte & THRIFT_FIELD_TYPE_MASK

            for _ in range(size):
                self.skip_field(key_type)
                self.skip_field(val_type)

    def yield_list_elements(
        self,
    ) -> Iterator[int]:
        """
        Yield for each element in a Thrift compact protocol list.

        Teaching Points:
        - Lists in Thrift encode element type and count in a header byte
        - Size field uses 4 bits, with special handling for sizes >= 15
        - This enables efficient storage of both small and large lists
        """
        header: int = int.from_bytes(self.read())
        size: int = header >> THRIFT_SIZE_SHIFT
        elem_type = header & THRIFT_FIELD_TYPE_MASK

        # If size == 15, read actual size from varint
        if size == THRIFT_SPECIAL_LIST_SIZE:
            size = self.read_varint()

        for _ in range(size):
            yield elem_type


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
        - Structs always terminate with an explicit STOP byte
        """
        byte = int.from_bytes(self.parser.read(1))

        field_type = byte & THRIFT_FIELD_TYPE_MASK
        field_delta = byte >> 4

        # Special case: STOP field is just 0x00, no zigzag varint to read
        if field_type != ThriftFieldType.STOP and field_delta == 0:
            field_delta = self.parser.read_zigzag()

        self.last_field_id += field_delta
        logger.debug(
            'Read field header: type=%d, id=%d (delta=%d)',
            field_type,
            self.last_field_id,
            field_delta,
        )
        return field_type, self.last_field_id

    def peek_field_header(self) -> tuple[int, int]:
        index = self.parser.index
        last_field_id = self.last_field_id
        field_type, field_id = self.read_field_header()
        self.last_field_id = last_field_id
        self.parser.index = index
        return field_type, field_id

    def read_value(self, field_type: int) -> Any:
        return self.parser.read_value(field_type)

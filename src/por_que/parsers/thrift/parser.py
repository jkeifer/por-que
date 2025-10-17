import logging
import struct

from collections.abc import AsyncIterator
from typing import Any

from por_que.exceptions import InvalidStringLengthError, ThriftParsingError
from por_que.protocols import AsyncReadableSeekable

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
    - Operates directly on file-like objects for true file position tracking
    """

    def __init__(self, reader: AsyncReadableSeekable, start_offset: int) -> None:
        self.reader = reader
        # Ensure we're positioned at the start offset
        self.reader.seek(start_offset)

    async def read(self, length: int = 1) -> bytes:
        """Read bytes from the file."""
        data = await self.reader.read(length)
        if len(data) != length:
            raise ThriftParsingError(
                f'Unexpected end of file: expected {length} bytes, got {len(data)} '
                f'at position {self.pos - len(data)}. '
                'File appears to be truncated or malformed.',
            )
        return data

    @property
    def pos(self) -> int:
        """Current absolute file position."""
        return self.reader.tell()

    async def read_varint(self) -> int:
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
            byte_data = await self.read(1)
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

    async def read_zigzag(self) -> int:
        """
        Read zigzag-encoded signed integer.

        Teaching Points:
        - Zigzag encoding maps signed integers to unsigned ones
        - Small negative numbers (-1, -2) become small positive numbers (1, 3)
        - This makes varint encoding efficient for negative numbers too
        """
        n = await self.read_varint()
        result = (n >> 1) ^ -(n & 1)
        logger.debug('Read zigzag: %d (from varint %d)', result, n)
        return result

    async def read_bool(self) -> bool:
        return await self.read() == 1

    async def read_i32(self) -> int:
        return await self.read_zigzag()

    async def read_i64(self) -> int:
        return await self.read_zigzag()

    async def read_string(self) -> str:
        length = await self.read_varint()
        logger.debug('Reading string of length %d at pos %d', length, self.pos)

        if length < 0:
            raise InvalidStringLengthError(
                f'Invalid string length {length} at position {self.pos}. '
                f'Length cannot be negative.',
            )

        data = await self.read(length)
        if len(data) != length:
            raise InvalidStringLengthError(
                f'Could not read {length} bytes for string at position {self.pos}. '
                f'Only {len(data)} bytes available.',
            )

        result = data.decode(DEFAULT_STRING_ENCODING)
        logger.debug('Read string: %r', result)
        return result

    async def read_bytes(self) -> bytes:
        length = await self.read_varint()
        logger.debug('Reading %d bytes at pos %d', length, self.pos)
        result = await self.read(length)
        hex_preview = result.hex()[:32] + ('...' if len(result) > 16 else '')
        logger.debug('Read %d bytes: %s', length, hex_preview)
        return result

    def skip(self, n: int) -> None:
        """Skip n bytes"""
        self.reader.seek(n, 1)

    async def read_value(self, field_type: int) -> Any:
        """Read a value of a given type from the stream."""
        match field_type:
            case ThriftFieldType.BOOL_TRUE:
                return True
            case ThriftFieldType.BOOL_FALSE:
                return False
            case ThriftFieldType.BYTE:
                return await self.read(1)
            case ThriftFieldType.I16 | ThriftFieldType.I32:
                return await self.read_i32()
            case ThriftFieldType.I64:
                return await self.read_i64()
            case ThriftFieldType.DOUBLE:
                return struct.unpack('<d', await self.read(8))[0]
            case ThriftFieldType.BINARY:
                return await self.read_bytes()
            case _:
                await self.skip_field(field_type)
                return None

    async def skip_field(self, field_type: int) -> None:  # noqa: C901
        match field_type:
            case ThriftFieldType.BOOL_TRUE | ThriftFieldType.BOOL_FALSE:
                # No data to skip
                return
            case ThriftFieldType.BYTE:
                self.skip(1)
            case ThriftFieldType.I16 | ThriftFieldType.I32 | ThriftFieldType.I64:
                await self.read_varint()
            case ThriftFieldType.DOUBLE:
                self.skip(8)
            case ThriftFieldType.BINARY:
                self.skip(await self.read_varint())
            case ThriftFieldType.STRUCT:
                nested = ThriftStructParser(self)
                while True:
                    ftype, _ = await nested.read_field_header()
                    if ftype == ThriftFieldType.STOP:
                        break
                    await self.skip_field(ftype)
            case ThriftFieldType.LIST | ThriftFieldType.SET:
                await self.skip_list()
            case ThriftFieldType.MAP:
                await self.skip_map()
            case _:
                raise ThriftParsingError(
                    f'Unknown thrift type: {field_type}',
                )

    async def skip_list(self) -> None:
        """Skip a list/set"""
        async for elem_type in self.yield_list_elements():
            await self.skip_field(elem_type)

    async def skip_map(self) -> None:
        """Skip a map"""
        # Maps always encode size as varint (unlike lists)
        size = await self.read_varint()

        if size > 0:
            types_byte = int.from_bytes(await self.read())
            key_type = (types_byte >> THRIFT_MAP_TYPE_SHIFT) & THRIFT_FIELD_TYPE_MASK
            val_type = types_byte & THRIFT_FIELD_TYPE_MASK

            for _ in range(size):
                await self.skip_field(key_type)
                await self.skip_field(val_type)

    async def yield_list_elements(
        self,
    ) -> AsyncIterator[int]:
        """
        Yield for each element in a Thrift compact protocol list.

        Teaching Points:
        - Lists in Thrift encode element type and count in a header byte
        - Size field uses 4 bits, with special handling for sizes >= 15
        - This enables efficient storage of both small and large lists
        """
        header: int = int.from_bytes(await self.read())
        size: int = header >> THRIFT_SIZE_SHIFT
        elem_type = header & THRIFT_FIELD_TYPE_MASK

        # If size == 15, read actual size from varint
        if size == THRIFT_SPECIAL_LIST_SIZE:
            size = await self.read_varint()

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

    async def read_field_header(self) -> tuple[int, int]:
        """
        Read field header and return (field_type, field_id).

        Teaching Points:
        - Field headers encode both type and ID information
        - Field ID deltas save space for sequential fields
        - Type information enables generic field skipping
        - EOF at field header boundary indicates end of parsing
        """
        try:
            byte = int.from_bytes(await self.parser.read(1))
        except ThriftParsingError:
            # EOF at field header boundary is legitimate (end of struct/parsing)
            return ThriftFieldType.STOP, 0

        field_type = byte & THRIFT_FIELD_TYPE_MASK
        field_delta = byte >> 4

        # Special case: STOP field is just 0x00, no zigzag varint to read
        if field_type != ThriftFieldType.STOP and field_delta == 0:
            field_delta = await self.parser.read_zigzag()

        self.last_field_id += field_delta
        logger.debug(
            'Read field header: type=%d, id=%d (delta=%d)',
            field_type,
            self.last_field_id,
            field_delta,
        )
        return field_type, self.last_field_id

    async def peek_field_header(self) -> tuple[int, int]:
        pos = self.parser.pos
        last_field_id = self.last_field_id
        field_type, field_id = await self.read_field_header()
        self.last_field_id = last_field_id
        self.parser.reader.seek(pos)
        return field_type, field_id

    async def read_value(self, field_type: int) -> Any:
        return await self.parser.read_value(field_type)

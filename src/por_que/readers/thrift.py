from por_que.enums import (
    ThriftFieldType,
)


class ThriftCompactReader:
    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def read(self, length: int = 1) -> bytes:
        data = self.data[self.pos : self.pos + length]
        self.pos += len(data)
        return data

    def read_varint(self) -> int:
        result = 0
        shift = 0
        while self.pos < len(self.data):
            byte = int.from_bytes(self.read())
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return result

    def read_zigzag(self) -> int:
        n = self.read_varint()
        return (n >> 1) ^ -(n & 1)

    def read_bool(self) -> bool:
        return self.read() == 1

    def read_i32(self) -> int:
        return self.read_zigzag()

    def read_i64(self) -> int:
        return self.read_zigzag()

    def read_string(self) -> str:
        length = self.read_varint()

        if length < 0 or self.pos + length > len(self.data):
            raise ValueError(f'Invalid string length {length} at position {self.pos}')

        return self.read(length).decode('utf-8')

    def read_bytes(self) -> bytes:
        length = self.read_varint()
        return self.read(length)

    def skip(self, n: int) -> None:
        """Skip n bytes"""
        self.read(n)

    def at_end(self) -> bool:
        """Check if at end of data"""
        return self.pos >= len(self.data)


class ThriftStructReader:
    """Reader for a single Thrift struct - tracks field IDs internally"""

    def __init__(self, reader: ThriftCompactReader) -> None:
        self.reader = reader
        self.last_field_id = 0

    def read_field_header(self) -> tuple[int, int]:
        """Read field header and return (field_type, field_id)"""
        if self.reader.at_end():
            return ThriftFieldType.STOP, 0

        byte = int.from_bytes(self.reader.read())

        field_type = byte & 0x0F
        field_delta = byte >> 4

        if field_delta == 0:
            # Special case: STOP field is just 0x00, no zigzag varint to read
            if field_type == ThriftFieldType.STOP:
                field_delta = 0
            else:
                field_delta = self.reader.read_zigzag()

        self.last_field_id += field_delta
        return field_type, self.last_field_id

    def skip_field(self, field_type: int) -> None:  # noqa: C901
        """Skip a field based on its type"""
        if self.reader.at_end():
            return

        if (
            field_type == ThriftFieldType.BOOL_TRUE
            or field_type == ThriftFieldType.BOOL_FALSE
        ):
            # No data to skip
            return

        if field_type == ThriftFieldType.BYTE:
            self.reader.skip(1)
        elif field_type in [
            ThriftFieldType.I16,
            ThriftFieldType.I32,
            ThriftFieldType.I64,
        ]:
            self.reader.read_varint()
        elif field_type == ThriftFieldType.DOUBLE:
            self.reader.skip(8)
        elif field_type == ThriftFieldType.BINARY:
            length = self.reader.read_varint()
            self.reader.skip(length)
        elif field_type == ThriftFieldType.STRUCT:
            # Create a new struct reader for the nested struct
            nested = ThriftStructReader(self.reader)
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
        if self.reader.at_end():
            return

        header = int.from_bytes(self.reader.read())
        size = header >> 4  # Size from upper 4 bits
        elem_type = header & 0x0F  # Element type from lower 4 bits

        # If size == 15, read actual size from varint
        if size == 15:
            size = self.reader.read_varint()

        # Skip all elements
        skip_reader = ThriftStructReader(self.reader)
        for _ in range(size):
            if self.reader.at_end():
                break
            skip_reader.skip_field(elem_type)

    def skip_map(self) -> None:
        """Skip a map"""
        if self.reader.at_end():
            return

        self.reader.read()
        size = self.reader.read_varint()

        if size > 0:
            types_byte = int.from_bytes(self.reader.read())
            key_type = (types_byte >> 4) & 0x0F
            val_type = types_byte & 0x0F

            skip_reader = ThriftStructReader(self.reader)
            for _ in range(size):
                if self.reader.at_end():
                    break
                skip_reader.skip_field(key_type)
                skip_reader.skip_field(val_type)

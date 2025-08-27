from .enums import (
    ColumnChunkFieldId,
    ColumnMetadataFieldId,
    Compression,
    Encoding,
    FileMetadataFieldId,
    KeyValueFieldId,
    Repetition,
    RowGroupFieldId,
    SchemaElementFieldId,
    ThriftFieldType,
    Type,
)
from .types import (
    ColumnChunk,
    ColumnMetadata,
    FileMetadata,
    RowGroup,
    SchemaElement,
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
            byte = int(self.read())
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
        try:
            result = self.read(length).decode('utf-8')
            return result
        except UnicodeDecodeError as e:
            # This indicates position corruption - we're reading from wrong place
            raise ValueError(
                f'Position corruption: tried to read {length}-byte string at pos {self.pos}, got UTF-8 error: {e}',
            )

    def read_bytes(self) -> bytes:
        length = self.read_varint()
        result = self.read(length)
        return result

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

        byte = int(self.reader.read())

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

    def skip_field(self, field_type: int) -> None:
        """Skip a field based on its type"""
        if self.reader.at_end():
            return

        if (
            field_type == ThriftFieldType.BOOL_TRUE
            or field_type == ThriftFieldType.BOOL_FALSE
        ):
            pass  # No data to skip
        elif field_type == ThriftFieldType.BYTE:
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
            self.skip_list()  # Same as list
        elif field_type == ThriftFieldType.MAP:
            self.skip_map()

    def skip_list(self) -> None:
        """Skip a list/set"""
        if self.reader.at_end():
            return

        header = int(self.reader.read())
        size = header >> 4  # Size from upper 4 bits
        elem_type = header & 0x0F  # Type from lower 4 bits

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
            types_byte = int(self.reader.read())
            key_type = (types_byte >> 4) & 0x0F
            val_type = types_byte & 0x0F

            skip_reader = ThriftStructReader(self.reader)
            for _ in range(size):
                if self.reader.at_end():
                    break
                skip_reader.skip_field(key_type)
                skip_reader.skip_field(val_type)


class MetadataReader:
    def __init__(self, metadata: bytes) -> None:
        self.reader = ThriftCompactReader(metadata)

    def read_list(self, read_element_func) -> list:
        """Read a list of elements"""
        header = int(self.reader.read())
        size = header >> 4  # Size from upper 4 bits
        elem_type = header & 0x0F  # Type from lower 4 bits

        # If size == 15, read actual size from varint
        if size == 15:
            size = self.reader.read_varint()

        elements = []
        for _ in range(size):
            if self.reader.at_end():
                break
            elements.append(read_element_func(self.reader))

        return elements

    def read_schema_element(self) -> SchemaElement:
        """Read a SchemaElement struct"""
        struct_reader = ThriftStructReader(self.reader)
        element = SchemaElement(name='unknown')

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == SchemaElementFieldId.TYPE:
                element.type = Type(self.reader.read_i32())
            elif field_id == SchemaElementFieldId.TYPE_LENGTH:
                element.type_length = self.reader.read_i32()
            elif field_id == SchemaElementFieldId.REPETITION_TYPE:
                element.repetition = Repetition(
                    self.reader.read_i32(),
                )
            elif field_id == SchemaElementFieldId.NAME:
                element.name = self.reader.read_string()
            elif field_id == SchemaElementFieldId.NUM_CHILDREN:
                element.num_children = self.reader.read_i32()
            elif field_id == SchemaElementFieldId.CONVERTED_TYPE:
                element.converted_type = self.reader.read_i32()
            else:
                struct_reader.skip_field(field_type)

        return element

    def read_column_metadata(self) -> ColumnMetadata:
        """Read ColumnMetaData struct"""
        struct_reader = ThriftStructReader(self.reader)
        meta = ColumnMetadata(
            type=Type.BOOLEAN,
            encodings=[],
            path_in_schema='',
            codec=Compression.UNCOMPRESSED,
            num_values=0,
            total_uncompressed_size=0,
            total_compressed_size=0,
            data_page_offset=0,
        )

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == ColumnMetadataFieldId.TYPE:
                meta.type = Type(self.reader.read_i32())
            elif field_id == ColumnMetadataFieldId.ENCODINGS:
                encodings = self.read_list(
                    lambda r: r.read_i32(),
                )
                meta.encodings = []
                for e in encodings:
                    try:
                        meta.encodings.append(Encoding(e))
                    except ValueError:
                        print(f'Warning: Invalid encoding {e}, skipping')
                        # Use a default encoding or skip
                        meta.encodings.append(Encoding.PLAIN)
            elif field_id == ColumnMetadataFieldId.PATH_IN_SCHEMA:
                path_list = self.read_list(
                    lambda r: r.read_string(),
                )
                meta.path_in_schema = '.'.join(path_list)  # Join path components
            elif field_id == ColumnMetadataFieldId.CODEC:
                meta.codec = Compression(self.reader.read_i32())
            elif field_id == ColumnMetadataFieldId.NUM_VALUES:
                meta.num_values = self.reader.read_i64()
            elif field_id == ColumnMetadataFieldId.TOTAL_UNCOMPRESSED_SIZE:
                meta.total_uncompressed_size = self.reader.read_i64()
            elif field_id == ColumnMetadataFieldId.TOTAL_COMPRESSED_SIZE:
                meta.total_compressed_size = self.reader.read_i64()
            elif field_id == ColumnMetadataFieldId.DATA_PAGE_OFFSET:
                meta.data_page_offset = self.reader.read_i64()
            elif field_id == ColumnMetadataFieldId.INDEX_PAGE_OFFSET:
                meta.index_page_offset = self.reader.read_i64()
            elif field_id == ColumnMetadataFieldId.DICTIONARY_PAGE_OFFSET:
                meta.dictionary_page_offset = self.reader.read_i64()
            else:
                struct_reader.skip_field(field_type)

        return meta

    def read_column_chunk(self) -> ColumnChunk:
        """Read a ColumnChunk struct"""
        struct_reader = ThriftStructReader(self.reader)
        chunk = ColumnChunk(file_offset=0)

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == ColumnChunkFieldId.FILE_PATH:
                chunk.file_path = self.reader.read_string()
            elif field_id == ColumnChunkFieldId.FILE_OFFSET:
                chunk.file_offset = self.reader.read_i64()
            elif field_id == ColumnChunkFieldId.META_DATA:
                chunk.meta_data = self.read_column_metadata()
            else:
                struct_reader.skip_field(field_type)

        return chunk

    def read_row_group(self) -> RowGroup:
        """Read a RowGroup struct"""
        struct_reader = ThriftStructReader(self.reader)
        rg = RowGroup(columns=[], total_byte_size=0, num_rows=0)

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == RowGroupFieldId.COLUMNS:
                rg.columns = self.read_list(
                    self.read_column_chunk,
                )
            elif field_id == RowGroupFieldId.TOTAL_BYTE_SIZE:
                rg.total_byte_size = self.reader.read_i64()
            elif field_id == RowGroupFieldId.NUM_ROWS:
                rg.num_rows = self.reader.read_i64()
            else:
                struct_reader.skip_field(field_type)

        return rg

    def read_key_value(self) -> tuple[str, str]:
        """Read a KeyValue pair"""
        struct_reader = ThriftStructReader(self.reader)
        key = None
        value = None

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == KeyValueFieldId.KEY:
                key = self.reader.read_string()
            elif field_id == KeyValueFieldId.VALUE:
                value = self.reader.read_string()
            else:
                struct_reader.skip_field(field_type)

        if key is None or value is None:
            raise RuntimeError('Error parsing key/value pair')

        return key, value

    def _read_file_metadata(self) -> FileMetadata:
        """Read the FileMetaData struct"""
        struct_reader = ThriftStructReader(self.reader)
        metadata = FileMetadata(
            version=0,
            schema=[],
            num_rows=0,
            row_groups=[],
        )

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == FileMetadataFieldId.VERSION:
                metadata.version = self.reader.read_i32()
            elif field_id == FileMetadataFieldId.SCHEMA:
                metadata.schema = self.read_list(
                    self.read_schema_element,
                )
            elif field_id == FileMetadataFieldId.NUM_ROWS:
                metadata.num_rows = self.reader.read_i64()
            elif field_id == FileMetadataFieldId.ROW_GROUPS:
                metadata.row_groups = self.read_list(
                    self.read_row_group,
                )
            elif field_id == FileMetadataFieldId.KEY_VALUE_METADATA:
                kvs = self.read_list(self.read_key_value)
                metadata.key_value_metadata = {k: v for k, v in kvs if k}
            elif field_id == FileMetadataFieldId.CREATED_BY:
                metadata.created_by = self.reader.read_string()
            else:
                struct_reader.skip_field(field_type)

        return metadata

    def __call__(self) -> FileMetadata:
        return self._read_file_metadata()

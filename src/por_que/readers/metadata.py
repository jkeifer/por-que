from collections.abc import Callable
from typing import TypeVar

from por_que.enums import (
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
from por_que.types import (
    ColumnChunk,
    ColumnMetadata,
    FileMetadata,
    RowGroup,
    SchemaElement,
)

from .thrift import (
    ThriftCompactReader,
    ThriftStructReader,
)

T = TypeVar('T')


class MetadataReader:
    def __init__(self, metadata: bytes) -> None:
        self.reader = ThriftCompactReader(metadata)

    def read_list(self, read_element_func: Callable[[], T]) -> list[T]:
        """Read a list of elements"""
        header = int.from_bytes(self.read())
        size = header >> 4  # Size from upper 4 bits
        # TODO: determine if we need element type for anything
        _ = header & 0x0F  # Element type from lower 4 bits

        # If size == 15, read actual size from varint
        if size == 15:
            size = self.read_varint()

        elements: list[T] = []
        for _ in range(size):
            if self.at_end():
                break
            elements.append(read_element_func())

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
                element.type = Type(self.read_i32())
            elif field_id == SchemaElementFieldId.TYPE_LENGTH:
                element.type_length = self.read_i32()
            elif field_id == SchemaElementFieldId.REPETITION_TYPE:
                element.repetition = Repetition(
                    self.read_i32(),
                )
            elif field_id == SchemaElementFieldId.NAME:
                element.name = self.read_string()
            elif field_id == SchemaElementFieldId.NUM_CHILDREN:
                element.num_children = self.read_i32()
            elif field_id == SchemaElementFieldId.CONVERTED_TYPE:
                element.converted_type = self.read_i32()
            else:
                struct_reader.skip_field(field_type)

        return element

    def read_column_metadata(self) -> ColumnMetadata:  # noqa: C901
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
                meta.type = Type(self.read_i32())
            elif field_id == ColumnMetadataFieldId.ENCODINGS:
                encodings = self.read_list(self.read_i32)
                for e in encodings:
                    meta.encodings.append(Encoding(e))
            elif field_id == ColumnMetadataFieldId.PATH_IN_SCHEMA:
                path_list = self.read_list(self.read_string)
                meta.path_in_schema = '.'.join(path_list)
            elif field_id == ColumnMetadataFieldId.CODEC:
                meta.codec = Compression(self.read_i32())
            elif field_id == ColumnMetadataFieldId.NUM_VALUES:
                meta.num_values = self.read_i64()
            elif field_id == ColumnMetadataFieldId.TOTAL_UNCOMPRESSED_SIZE:
                meta.total_uncompressed_size = self.read_i64()
            elif field_id == ColumnMetadataFieldId.TOTAL_COMPRESSED_SIZE:
                meta.total_compressed_size = self.read_i64()
            elif field_id == ColumnMetadataFieldId.DATA_PAGE_OFFSET:
                meta.data_page_offset = self.read_i64()
            elif field_id == ColumnMetadataFieldId.INDEX_PAGE_OFFSET:
                meta.index_page_offset = self.read_i64()
            elif field_id == ColumnMetadataFieldId.DICTIONARY_PAGE_OFFSET:
                meta.dictionary_page_offset = self.read_i64()
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
                chunk.file_path = self.read_string()
            elif field_id == ColumnChunkFieldId.FILE_OFFSET:
                chunk.file_offset = self.read_i64()
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
                rg.total_byte_size = self.read_i64()
            elif field_id == RowGroupFieldId.NUM_ROWS:
                rg.num_rows = self.read_i64()
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
                key = self.read_string()
            elif field_id == KeyValueFieldId.VALUE:
                value = self.read_string()
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
                metadata.version = self.read_i32()
            elif field_id == FileMetadataFieldId.SCHEMA:
                metadata.schema = self.read_list(
                    self.read_schema_element,
                )
            elif field_id == FileMetadataFieldId.NUM_ROWS:
                metadata.num_rows = self.read_i64()
            elif field_id == FileMetadataFieldId.ROW_GROUPS:
                metadata.row_groups = self.read_list(
                    self.read_row_group,
                )
            elif field_id == FileMetadataFieldId.KEY_VALUE_METADATA:
                kvs = self.read_list(self.read_key_value)
                metadata.key_value_metadata = {k: v for k, v in kvs if k}
            elif field_id == FileMetadataFieldId.CREATED_BY:
                metadata.created_by = self.read_string()
            else:
                struct_reader.skip_field(field_type)

        return metadata

    def __call__(self) -> FileMetadata:
        return self._read_file_metadata()

    def read(self, length: int = 1) -> bytes:
        return self.reader.read(length)

    def read_varint(self) -> int:
        return self.reader.read_varint()

    def read_zigzag(self) -> int:
        return self.reader.read_zigzag()

    def read_bool(self) -> bool:
        return self.reader.read_bool()

    def read_i32(self) -> int:
        return self.reader.read_i32()

    def read_i64(self) -> int:
        return self.reader.read_i64()

    def read_string(self) -> str:
        return self.reader.read_string()

    def read_bytes(self) -> bytes:
        return self.reader.read_bytes()

    def at_end(self) -> bool:
        return self.reader.at_end()

from collections.abc import Callable
from datetime import UTC
from typing import TypeVar

from por_que.enums import (
    Compression,
    ConvertedType,
    Encoding,
    Repetition,
    Type,
)
from por_que.types import (
    ColumnChunk,
    ColumnMetadata,
    ColumnStatistics,
    FileMetadata,
    RowGroup,
    SchemaElement,
)

from ..exceptions import ParquetDataError, ThriftParsingError
from .constants import (
    DEFAULT_SCHEMA_NAME,
    THRIFT_FIELD_TYPE_MASK,
    THRIFT_SIZE_SHIFT,
    THRIFT_SPECIAL_LIST_SIZE,
)
from .enums import (
    ColumnChunkFieldId,
    ColumnMetadataFieldId,
    FileMetadataFieldId,
    KeyValueFieldId,
    RowGroupFieldId,
    SchemaElementFieldId,
    StatisticsFieldId,
    ThriftFieldType,
)
from .thrift import (
    ThriftCompactReader,
    ThriftStructReader,
)

T = TypeVar('T')


class MetadataReader:
    def __init__(self, metadata: bytes) -> None:
        self.reader = ThriftCompactReader(metadata)
        self.schema: SchemaElement | None = None

    def read_list(self, read_element_func: Callable[[], T]) -> list[T]:
        """Read a list of elements"""
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

    def read_schema_tree(self, elements_iter) -> SchemaElement:
        """Read and build nested schema tree recursively."""
        try:
            element = next(elements_iter)
        except StopIteration:
            raise ThriftParsingError('Unexpected end of schema elements') from None

        # If this element has children, recursively read them
        for _ in range(element.num_children):
            child = self.read_schema_tree(elements_iter)
            element.children[child.name] = child

        return element

    def read_schema_element(self) -> SchemaElement:
        """Read a single SchemaElement struct"""
        struct_reader = ThriftStructReader(self.reader)
        element = SchemaElement(name=DEFAULT_SCHEMA_NAME)

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
                element.converted_type = ConvertedType(self.read_i32())
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
            elif field_id == ColumnMetadataFieldId.STATISTICS:
                meta.statistics = self.read_statistics(meta.type, meta.path_in_schema)
            else:
                struct_reader.skip_field(field_type)

        return meta

    def read_statistics(
        self,
        column_type: Type,
        path_in_schema: str,
    ) -> ColumnStatistics:
        """Read Statistics struct for predicate pushdown."""
        struct_reader = ThriftStructReader(self.reader)
        stats = ColumnStatistics()

        while True:
            field_type, field_id = struct_reader.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            if field_id == StatisticsFieldId.MIN_VALUE:
                raw_bytes = self.read_bytes()
                stats.min_value = self._deserialize_value(
                    raw_bytes,
                    column_type,
                    path_in_schema,
                )
            elif field_id == StatisticsFieldId.MAX_VALUE:
                raw_bytes = self.read_bytes()
                stats.max_value = self._deserialize_value(
                    raw_bytes,
                    column_type,
                    path_in_schema,
                )
            elif field_id == StatisticsFieldId.NULL_COUNT:
                stats.null_count = self.read_i64()
            elif field_id == StatisticsFieldId.DISTINCT_COUNT:
                stats.distinct_count = self.read_i64()
            elif (
                field_id == StatisticsFieldId.MAX_VALUE_DELTA
                or field_id == StatisticsFieldId.MIN_VALUE_DELTA
            ):
                # Skip delta values - rarely used and complex
                self.read_bytes()
            else:
                struct_reader.skip_field(field_type)

        return stats

    def _deserialize_value(
        self,
        raw_bytes: bytes,
        column_type: Type,
        path_in_schema: str,
    ) -> str | int | float | bool | None:
        """Deserialize binary value based on Parquet physical type and logical type."""
        if not raw_bytes:
            return None

        if not self.schema:
            raise ThriftParsingError('Schema must be read before parsing statistics')

        converted_type = self.schema.get_logical_type(path_in_schema)

        match column_type:
            case Type.BOOLEAN:
                return self._deserialize_boolean(raw_bytes)
            case Type.INT32:
                return self._deserialize_int32_value(raw_bytes, converted_type)
            case Type.INT64:
                return self._deserialize_int64_value(raw_bytes, converted_type)
            case Type.FLOAT:
                return self._deserialize_float(raw_bytes)
            case Type.DOUBLE:
                return self._deserialize_double(raw_bytes)
            case Type.BYTE_ARRAY:
                return self._deserialize_byte_array(raw_bytes, converted_type)
            case Type.FIXED_LEN_BYTE_ARRAY:
                return self._deserialize_fixed_len_byte_array(raw_bytes, converted_type)
            case _:
                raise ParquetDataError(f'Unsupported column type: {column_type}')

    def _deserialize_boolean(self, raw_bytes: bytes) -> bool:
        """Deserialize boolean value."""
        return raw_bytes[0] != 0

    def _deserialize_int32_value(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str | int:
        """Deserialize INT32 with logical type handling."""
        match converted_type:
            case ConvertedType.DATE:
                return self._deserialize_date(raw_bytes)
            case ConvertedType.TIME_MILLIS:
                return self._deserialize_time_millis(raw_bytes)
            case _:
                return self._deserialize_int32(raw_bytes)

    def _deserialize_int64_value(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str | int:
        """Deserialize INT64 with logical type handling."""
        match converted_type:
            case ConvertedType.TIMESTAMP_MILLIS:
                return self._deserialize_timestamp_millis(raw_bytes)
            case _:
                return self._deserialize_int64(raw_bytes)

    def _deserialize_float(self, raw_bytes: bytes) -> float:
        """Deserialize FLOAT value."""
        import struct

        if len(raw_bytes) != 4:
            raise ParquetDataError(
                f'FLOAT value must be 4 bytes, got {len(raw_bytes)}',
            )
        return struct.unpack('<f', raw_bytes)[0]

    def _deserialize_double(self, raw_bytes: bytes) -> float:
        """Deserialize DOUBLE value."""
        import struct

        if len(raw_bytes) != 8:
            raise ParquetDataError(
                f'DOUBLE value must be 8 bytes, got {len(raw_bytes)}',
            )
        return struct.unpack('<d', raw_bytes)[0]

    def _deserialize_byte_array(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str:
        """Deserialize BYTE_ARRAY based on logical type."""
        if converted_type == ConvertedType.UTF8:
            return raw_bytes.decode('utf-8')

        # Default to UTF-8 for backward compatibility
        try:
            return raw_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ParquetDataError(
                f'BYTE_ARRAY could not be decoded as UTF-8: {e}',
            ) from e

    def _deserialize_fixed_len_byte_array(
        self,
        raw_bytes: bytes,
        converted_type: ConvertedType | None,
    ) -> str:
        """Deserialize FIXED_LEN_BYTE_ARRAY based on logical type."""
        if converted_type == ConvertedType.DECIMAL:
            # For now, return hex representation of decimal
            return f'0x{raw_bytes.hex()}'

        # Default to UTF-8 for backward compatibility
        try:
            return raw_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ParquetDataError(
                f'FIXED_LEN_BYTE_ARRAY could not be decoded as UTF-8: {e}',
            ) from e

    def _deserialize_date(self, raw_bytes: bytes) -> str:
        """Deserialize DATE logical type (INT32 days since epoch)."""
        if len(raw_bytes) != 4:
            raise ParquetDataError(f'DATE value must be 4 bytes, got {len(raw_bytes)}')

        days = int.from_bytes(raw_bytes, byteorder='little', signed=True)
        from datetime import date, timedelta

        return str(date(1970, 1, 1) + timedelta(days=days))

    def _deserialize_time_millis(self, raw_bytes: bytes) -> str:
        """Deserialize TIME_MILLIS logical type (INT32 milliseconds since midnight)."""
        if len(raw_bytes) != 4:
            raise ParquetDataError(
                f'TIME_MILLIS value must be 4 bytes, got {len(raw_bytes)}',
            )

        millis = int.from_bytes(raw_bytes, byteorder='little', signed=True)
        hours, remainder = divmod(millis, 3600000)
        minutes, remainder = divmod(remainder, 60000)
        seconds, millis = divmod(remainder, 1000)
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}'

    def _deserialize_timestamp_millis(self, raw_bytes: bytes) -> str:
        """Deserialize TIMESTAMP_MILLIS logical type (INT64 millis since epoch)."""
        if len(raw_bytes) != 8:
            raise ParquetDataError(
                f'TIMESTAMP_MILLIS value must be 8 bytes, got {len(raw_bytes)}',
            )

        millis = int.from_bytes(raw_bytes, byteorder='little', signed=True)
        from datetime import datetime

        return str(datetime.fromtimestamp(millis / 1000, tz=UTC))

    def _deserialize_int32(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT32 value."""
        if len(raw_bytes) != 4:
            raise ParquetDataError(f'INT32 value must be 4 bytes, got {len(raw_bytes)}')
        return int.from_bytes(raw_bytes, byteorder='little', signed=True)

    def _deserialize_int64(self, raw_bytes: bytes) -> int:
        """Deserialize regular INT64 value."""
        if len(raw_bytes) != 8:
            raise ParquetDataError(f'INT64 value must be 8 bytes, got {len(raw_bytes)}')
        return int.from_bytes(raw_bytes, byteorder='little', signed=True)

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
            raise ThriftParsingError(
                'Incomplete key/value pair: missing key or value field. '
                'This may indicate corrupted metadata.',
            )

        return key, value

    def _read_file_metadata(self) -> FileMetadata:
        """Read the FileMetaData struct"""
        struct_reader = ThriftStructReader(self.reader)
        metadata = FileMetadata(
            version=0,
            schema=SchemaElement(name='root'),
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
                schema_elements = self.read_list(self.read_schema_element)
                # Build nested tree structure
                elements_iter = iter(schema_elements)
                metadata.schema = self.read_schema_tree(elements_iter)
                self.schema = metadata.schema
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

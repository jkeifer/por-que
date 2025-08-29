from dataclasses import asdict, dataclass, field

from .enums import (
    Compression,
    ConvertedType,
    Encoding,
    PageType,
    Repetition,
    Type,
)
from .stats import CompressionStats, FileStats, RowGroupStats


@dataclass
class SchemaElement:
    name: str
    type: Type | None = None
    repetition: Repetition | None = None
    num_children: int = 0
    converted_type: ConvertedType | None = None
    type_length: int | None = None
    children: dict[str, 'SchemaElement'] = field(default_factory=dict)

    def is_group(self) -> bool:
        return self.type is None

    def get_logical_type(self, path_in_schema: str) -> ConvertedType | None:
        """Look up logical type by dotted path from this element."""
        parts = path_in_schema.split('.')
        current = self

        for part in parts:
            if part not in current.children:
                raise ValueError(f"Schema path '{path_in_schema}' not found in schema")
            current = current.children[part]

        return current.converted_type if not current.is_group() else None

    def add_element(self, element: 'SchemaElement', path: str) -> None:
        """Add an element to the schema at the specified dotted path."""
        if '.' not in path:
            # Direct child
            self.children[path] = element
        else:
            # Nested path - find or create parent groups
            parts = path.split('.')
            element_name = parts[-1]

            # Navigate to parent, creating groups as needed
            current = self
            for part in parts[:-1]:
                if part not in current.children:
                    # Create intermediate group
                    group = SchemaElement(name=part)
                    current.children[part] = group
                current = current.children[part]

            # Add the element to the final parent
            current.children[element_name] = element

    def __repr__(self) -> str:
        return self._repr_recursive(0)

    def _repr_recursive(self, indent: int) -> str:
        """Recursively build string representation of schema tree."""
        spaces = '  ' * indent
        if self.is_group():
            result = f'{spaces}Group({self.name})'
            if self.children:
                result += ' {\n'
                for child in self.children.values():
                    result += child._repr_recursive(indent + 1) + '\n'
                result += spaces + '}'
            return result
        rep = f' {self.repetition.name}' if self.repetition else ''
        logical = f' [{self.converted_type.name}]' if self.converted_type else ''
        type_name = self.type.name if self.type else 'UNKNOWN'
        type_name = 'UNKNOWN' if self.type is None else self.type.name
        return f'{spaces}Column({self.name}: {type_name}{rep}{logical})'

    def to_dict(self) -> dict:
        """Convert SchemaElement to dictionary."""
        return asdict(self)


@dataclass
class ColumnStatistics:
    """Column statistics for predicate pushdown."""

    min_value: str | int | float | bool | None = None
    max_value: str | int | float | bool | None = None
    null_count: int | None = None
    distinct_count: int | None = None


@dataclass
class ColumnMetadata:
    type: Type
    encodings: list[Encoding]
    path_in_schema: str
    codec: Compression
    num_values: int
    total_uncompressed_size: int
    total_compressed_size: int
    data_page_offset: int
    dictionary_page_offset: int | None = None
    index_page_offset: int | None = None
    statistics: ColumnStatistics | None = None


@dataclass
class ColumnChunk:
    file_offset: int
    meta_data: ColumnMetadata | None = None
    file_path: str | None = None


@dataclass
class RowGroup:
    columns: list[ColumnChunk]
    total_byte_size: int
    num_rows: int

    def column_names(self) -> list[str]:
        return [col.meta_data.path_in_schema for col in self.columns if col.meta_data]

    def get_stats(self) -> RowGroupStats:
        """Calculate statistics for this row group."""
        total_compressed = 0
        total_uncompressed = 0

        for col in self.columns:
            if col.meta_data:
                total_compressed += col.meta_data.total_compressed_size
                total_uncompressed += col.meta_data.total_uncompressed_size

        compression = CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

        return RowGroupStats(
            num_rows=self.num_rows,
            num_columns=len(self.columns),
            total_byte_size=self.total_byte_size,
            compression=compression,
        )


@dataclass
class FileMetadata:
    version: int
    schema: SchemaElement
    num_rows: int
    row_groups: list[RowGroup]
    created_by: str | None = None
    key_value_metadata: dict[str, str] = field(default_factory=dict)

    def get_stats(self) -> FileStats:
        """Calculate overall file statistics."""
        total_compressed = 0
        total_uncompressed = 0
        total_columns = 0
        min_rows = float('inf')
        max_rows = 0

        row_group_stats = []

        for rg in self.row_groups:
            rg_stats = rg.get_stats()
            row_group_stats.append(rg_stats)

            total_compressed += rg_stats.compression.total_compressed
            total_uncompressed += rg_stats.compression.total_uncompressed
            total_columns += rg_stats.num_columns
            min_rows = min(min_rows, rg_stats.num_rows)
            max_rows = max(max_rows, rg_stats.num_rows)

        # Handle case with no row groups
        if not self.row_groups:
            min_rows = 0

        compression = CompressionStats(
            total_compressed=total_compressed,
            total_uncompressed=total_uncompressed,
        )

        return FileStats(
            version=self.version,
            created_by=self.created_by,
            total_rows=self.num_rows,
            num_row_groups=len(self.row_groups),
            total_columns=total_columns,
            min_rows_per_group=int(min_rows),
            max_rows_per_group=max_rows,
            compression=compression,
            row_group_stats=row_group_stats,
        )

    def to_dict(self) -> dict:
        """Convert FileMetadata to dictionary."""
        return asdict(self)


# Page-level structures for Phase 2


@dataclass
class PageHeader:
    """
    Header information for all page types.

    Teaching Points:
    - Every page starts with a PageHeader containing type and size information
    - uncompressed_page_size tells us how much data after decompression
    - compressed_page_size is the actual bytes to read from file
    - CRC enables data integrity verification
    """

    type: 'PageType'
    uncompressed_page_size: int
    compressed_page_size: int
    crc: int | None = None
    data_page_header: 'DataPageHeader | None' = None
    dictionary_page_header: 'DictionaryPageHeader | None' = None
    data_page_header_v2: 'DataPageHeaderV2 | None' = None


@dataclass
class DataPageHeader:
    """
    Header for DATA_PAGE containing value and encoding information.

    Teaching Points:
    - num_values indicates how many actual values are stored in the page
    - encoding specifies how the values are encoded (PLAIN, DICTIONARY, etc.)
    - definition_level_encoding handles NULL value representation
    - repetition_level_encoding handles nested/repeated field structure
    """

    num_values: int
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding


@dataclass
class DataPageHeaderV2:
    """
    Header for DATA_PAGE_V2 with separate definition/repetition level sizes.

    Teaching Points:
    - V2 separates definition and repetition level data for efficiency
    - num_nulls provides quick NULL count without scanning all values
    - num_rows differs from num_values when dealing with repeated fields
    - is_compressed applies only to the value data, not levels
    """

    num_values: int
    num_nulls: int
    num_rows: int
    encoding: Encoding
    definition_levels_byte_length: int
    repetition_levels_byte_length: int
    is_compressed: bool


@dataclass
class DictionaryPageHeader:
    """
    Header for DICTIONARY_PAGE containing dictionary encoding information.

    Teaching Points:
    - Dictionary pages must appear before any data pages that reference them
    - num_values is the size of the dictionary (number of unique values)
    - encoding is typically PLAIN for dictionary values
    - The dictionary maps integer indices to actual values for compression
    - is_sorted indicates if dictionary values are in sorted order (optimization)
    """

    num_values: int
    encoding: Encoding
    is_sorted: bool | None = None

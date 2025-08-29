# Implementation Guide: Step-by-Step Breakdown

> **📋 REORGANIZATION COMPLETE**: Phase 1 has been completed with a comprehensive package reorganization.
> All parsing infrastructure moved from `readers/` to `parsers/` with proper naming (`ThriftCompactParser` not `ThriftCompactReader`).

## Phase 1: Metadata Parser Refactoring ✅ **COMPLETED**

### Step 1: Create Base Infrastructure ✅ **REORGANIZED**
- [x] Create `src/por_que/parsers/` directory structure (`thrift/` and `parquet/` subdirs)
- [x] Create `src/por_que/parsers/parquet/base.py`
  - [x] Define `BaseParser` class with `__init__(self, parser: ThriftCompactParser)`
  - [x] Move primitive reading methods from `MetadataReader`:
    - `read_i32()`, `read_i64()`, `read_bool()`, `read_string()`, `read_bytes()`
    - `read_list()`, `skip_field()`
  - [x] Add educational docstrings explaining Thrift compact protocol basics

### Step 2: Create Component Parsers ✅ **REORGANIZED**
- [x] Create `src/por_que/parsers/parquet/schema.py`
  - [x] Implement `SchemaParser(BaseParser)`
  - [x] Move `read_schema_element()` and `read_schema_tree()` from `MetadataReader`
  - [x] Add comments explaining schema tree structure and field relationships

- [x] Create `src/por_que/parsers/parquet/statistics.py`
  - [x] Implement `RowGroupStatisticsParser(BaseParser)` *(renamed for clarity)*
  - [x] Move `read_statistics()` and all `_deserialize_*` helpers
  - [x] Document why statistics need special deserialization (logical vs physical types)

- [x] Create `src/por_que/parsers/parquet/column.py`
  - [x] Implement `ColumnParser(BaseParser)`
  - [x] Move `read_column_chunk()` and `read_column_metadata()`
  - [x] Integrate with `RowGroupStatisticsParser` for statistics fields

- [x] Create `src/por_que/parsers/parquet/row_group.py`
  - [x] Implement `RowGroupParser(BaseParser)`
  - [x] Move `read_row_group()`
  - [x] Integrate with `ColumnParser` for column chunks

### Step 3: Create Metadata Orchestrator ✅ **REORGANIZED**
- [x] Create `src/por_que/parsers/parquet/metadata.py`
  - [x] Implement `MetadataParser` that composes all component parsers
  - [x] Implement main `parse()` method that returns `FileMetadata`
  - [x] Add tracing support: `parse(trace=False)`

### Step 4: Integration and Cleanup ✅ **COMPLETED**
- [x] Update `FileMetadata.from_bytes()` to use new `MetadataParser`
- [x] Run all existing tests to ensure compatibility
- [x] Delete old `src/por_que/readers/` directory entirely
- [x] **BONUS:** Full package reorganization with proper naming (`parsers` not `readers`)
- [ ] Add unit tests for each new component parser

## Phase 2: Page-Level Parsing ✅ **COMPLETED**

### Step 1: Define Page Types ✅ **COMPLETED**
- [x] Add to `src/por_que/types.py`:
  - [x] `PageType` enum (DATA_PAGE, DATA_PAGE_V2, DICTIONARY_PAGE)
  - [x] `PageHeader` dataclass with field metadata
  - [x] `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader` dataclasses

### Step 2: Implement Page Parser ✅ **COMPLETED**
- [x] Create `src/por_que/parsers/parquet/page.py`
  - [x] Implement `PageParser(BaseParser)` with comprehensive debug logging
  - [x] Method: `read_page_header() -> PageHeader`
  - [x] Method: `read_data_page_header() -> DataPageHeader`
  - [x] Method: `read_data_page_header_v2() -> DataPageHeaderV2`
  - [x] Method: `read_dictionary_page_header() -> DictionaryPageHeader`
  - [x] Method: `read_page_data(header: PageHeader) -> bytes`
  - [x] Add detailed comments about page structure and compression
  - [x] Add field ID enums for all page header types

### Step 3: Add Page Tests ✅ **COMPLETED**
- [x] All existing integration tests pass with new page parser components
- [x] Page parser integrates cleanly with existing architecture
- [x] Code quality checks pass (ruff, mypy)

## Phase 3: Data Decoding

### Step 1: Compression Support
- [ ] Create `src/por_que/parsers/parquet/compression.py`
  - [ ] Function: `decompress(data: bytes, codec: Compression) -> bytes`
  - [ ] Implement UNCOMPRESSED (no-op)
  - [ ] Implement SNAPPY decompression
  - [ ] Implement GZIP decompression
  - [ ] Add tests with sample compressed data

### Step 2: PLAIN Encoding
- [ ] Create `src/por_que/parsers/parquet/encoding.py` *(Internal decoder functions)*
  - [ ] Create base: `def decode_plain(data: bytes, type_info: SchemaElement) -> Iterator[Any]`
  - [ ] Implement for each physical type:
    - [ ] BOOLEAN (bit-packed)
    - [ ] INT32, INT64 (little-endian)
    - [ ] FLOAT, DOUBLE (IEEE 754)
    - [ ] BYTE_ARRAY (length-prefixed)
    - [ ] FIXED_LEN_BYTE_ARRAY
  - [ ] Add conversion for logical types (DATE, TIMESTAMP, etc.)

### Step 3: Dictionary Encoding
- [ ] Add to `encoding.py`:
  - [ ] `decode_rle_dictionary(indices: bytes, dictionary: List[Any]) -> Iterator[Any]`
  - [ ] Implement RLE/Bit-packed hybrid decoder for indices
  - [ ] Add helper: `read_dictionary_page(data: bytes, type_info) -> List[Any]`

### Step 4: RLE and Bit-Packing
- [ ] Add to `encoding.py`:
  - [ ] `decode_rle(data: bytes) -> Iterator[int]` for run-length encoding
  - [ ] `decode_bit_packed(data: bytes, bit_width: int) -> Iterator[int]`
  - [ ] Document the RLE/Bit-packed hybrid format used by Parquet

### Step 5: Definition and Repetition Levels
- [ ] Add to `src/por_que/parsers/parquet/levels.py`:
  - [ ] `decode_definition_levels(data: bytes, max_level: int) -> List[int]`
  - [ ] `decode_repetition_levels(data: bytes, max_level: int) -> List[int]`
  - [ ] Document how levels track nullability and nesting in Parquet
  - [ ] Add educational comments explaining the state machine concept
- [ ] Update `PageReader` to handle levels:
  - [ ] Parse definition levels from data pages
  - [ ] Parse repetition levels from data pages
  - [ ] Pass levels along with values for nested schema reconstruction

## Phase 4: Top-Level Integration ✅ **COMPLETED**

> **📁 Directory Structure Note**:
> - `src/por_que/readers/` - User-facing reader classes (lazy loading interface)
> - `src/por_que/parsers/` - Internal parsing components (low-level Thrift/binary parsing)
> - Reader classes use parser components internally but present a clean API

### Step 1: Implement Reader Classes ✅ **COMPLETED**
- [x] Create `src/por_que/parquet_file.py`:
  - [x] `ParquetFile` class with `__init__(file_obj)` supporting file paths, file objects, and HTTP URLs
  - [x] Property: `metadata: FileMetadata`
  - [x] Property: `row_groups: List[RowGroupReader]` and `row_group(index: int)`
  - [x] Method: `column(name: str) -> Iterator[Any]` for convenience reading across all row groups
  - [x] Additional methods: `columns()`, `num_rows()`, `num_row_groups()`, `schema_string()`
  - [x] Context manager support and proper resource cleanup

- [x] Create `src/por_que/readers/row_group.py`:
  - [x] `RowGroupReader` class for user-facing row group access
  - [x] Store row group metadata and file handle
  - [x] Method: `column(name: str) -> ColumnChunkReader`
  - [x] Method: `columns() -> List[str]` and `column_paths() -> List[str]`
  - [x] Properties: `num_rows()`, `num_columns()`, `total_byte_size()`

- [x] Create `src/por_que/readers/page.py`:
  - [x] `PageReader` class for individual page reading
  - [x] Store page metadata and file handle
  - [x] Method: `read() -> Iterator[Tuple[PageHeader, bytes]]` to yield parsed page headers and raw data
  - [x] Internal: Use `PageParser` from `parsers/parquet/page.py` for raw page parsing
  - [x] Proper error handling and educational logging

- [x] Create `src/por_que/readers/column_chunk.py`:
  - [x] `ColumnChunkReader` class for user-facing column access
  - [x] Store column metadata, file handle, and schema info
  - [x] Method: `read() -> Iterator[Tuple[PageHeader, bytes]]`
  - [x] Internal: Use `PageReader` objects to read individual pages
  - [x] Handle dictionary pages and data pages sequentially
  - [x] Properties: `column_name()`, `value_count()`, etc.

### Step 2: HTTP File Support ✅ **COMPLETED**
- [x] Create `src/por_que/util/http_file.py`:
  - [x] `HttpFile` class implementing file-like interface for remote URLs
  - [x] HTTP range request support for lazy loading
  - [x] Caching to minimize network requests
  - [x] Standard file methods: `read()`, `seek()`, `tell()`, `close()`
  - [x] Context manager support

### Step 3: Public API and Testing ✅ **COMPLETED**
- [x] Update `__init__.py` to export `ParquetFile`
- [x] Create comprehensive integration tests in `tests/test_parquet_file.py`
- [x] Test fixtures in `conftest.py` using real Apache Parquet test files
- [x] Validate complete parsing pipeline: ParquetFile → RowGroupReader → ColumnChunkReader → PageReader → PageParser
- [x] **Achievement**: PageParser now exercised with real Parquet files (moved from 63% to 80% test coverage)

## Testing Checkpoints

After each phase:
1. **Unit tests pass** - Each new component has focused tests
2. **Integration tests pass** - Existing file reading tests still work
3. **Cross-validation** - Results match `pyarrow` for test files

## Documentation Tasks

Throughout implementation:
- [ ] Add format specification references in docstrings
- [ ] Create diagrams showing file layout for complex sections
- [ ] Write "How Parquet Works" comments in strategic locations
- [ ] Maintain a NOTES.md with interesting discoveries about the format

## Debug Logging Strategy

> **🔍 Educational Tracing**: Instead of `trace=False` parameters, we use Python's standard logging module with debug level for educational tracing. This provides:
> - Consistent logging across all components
> - User control via standard logging configuration
> - Rich context about parsing operations

### Implementation Guidelines
- All parser and reader components should use `logger = logging.getLogger(__name__)`
- Debug logs should explain what's happening in educational terms
- Example: `logger.debug('Reading row group %d with %d columns', index, num_cols)`
- Users can enable with: `logging.basicConfig(level=logging.DEBUG)`

## Development Strategy

### General Approach
1. **Maintain a working parser throughout** - Start with Phase 1 (metadata refactoring) to keep the existing functionality intact while improving the architecture
2. **Test-Driven Development for encodings** - For Phase 3, write tests first with known byte sequences and expected outputs
3. **Keep a learning journal** - Document format quirks and "aha moments" in a PARQUET_QUIRKS.md file as you discover them

### Incremental Development
- After each component, run the full test suite
- Commit working code frequently with descriptive messages
- Use feature branches for each phase to allow easy rollback

### Performance Benchmarking
- Add simple benchmarks comparing:
  - Reading a single column vs entire file
  - Impact of compression on read speed
  - Memory usage with streaming vs loading entire column
- Document these in a PERFORMANCE.md to show why columnar formats excel

## Documentation Strategy

### Format Quirks to Document (PARQUET_QUIRKS.md)

#### Thrift Encoding Gotchas
- [ ] Document variable-length integer encoding (ZigZag for signed integers)
- [ ] Explain why field IDs can be non-contiguous
- [ ] Show how Thrift's compact protocol saves space vs regular protocol

#### Physical Layout Requirements
- [ ] Page boundary alignment - some implementations align to 8-byte boundaries
- [ ] Dictionary pages MUST precede data pages in a column chunk
- [ ] Footer length is written twice (for backward reading)
- [ ] Row group size trade-offs (memory vs I/O efficiency)

#### Encoding Edge Cases
- [ ] Bit-packing always pads to byte boundaries
- [ ] RLE runs can be zero-length (important for sparse data)
- [ ] Boolean PLAIN encoding packs 8 values per byte
- [ ] Dictionary fallback - what happens when dictionary gets too large

### Educational Visualizations

Create ASCII diagrams in code comments:
```python
# Parquet File Layout:
# ┌─────────────────┐
# │  Magic Number   │ 4 bytes: "PAR1"
# ├─────────────────┤
# │                 │
# │   Row Group 0   │ ← Column chunks (actual data)
# │                 │
# ├─────────────────┤
# │                 │
# │   Row Group 1   │
# │                 │
# ├─────────────────┤
# │   ...           │
# ├─────────────────┤
# │                 │
# │  File Metadata  │ ← Schema, row group locations
# │                 │
# ├─────────────────┤
# │  Footer Length  │ 4 bytes (little-endian)
# ├─────────────────┤
# │  Magic Number   │ 4 bytes: "PAR1"
# └─────────────────┘
```

### Format Archaeology (in docstrings)

Document the "why" behind format decisions:
- Why dictionaries? (Columnar compression for repeated values)
- Why separate metadata? (Enable reading without scanning entire file)
- Why row groups? (Parallelization and memory boundaries)
- Why statistics? (Predicate pushdown for query engines)

### Code Examples

Create an `examples/` directory with educational scripts:
- [ ] `read_single_column.py` - Shows selective reading benefit
- [ ] `explore_metadata.py` - Prints human-readable file structure
- [ ] `compression_comparison.py` - Shows size/speed trade-offs
- [ ] `trace_parsing.py` - Uses trace=True to show parsing steps

## Progress Tracking

Consider using GitHub Issues or a project board to track:
- Which components are complete
- Which tests are passing
- Which format features are supported
- Known limitations or TODOs

### Milestone Checklist
- [x] Phase 1 Complete: Metadata parsing refactored
- [x] Phase 2 Complete: Can read page headers
- [ ] Phase 3 Complete: Can decode PLAIN encoding
- [ ] Phase 3 Enhanced: Dictionary encoding working
- [x] Phase 4 Complete: Full lazy reading API with HTTP support
- [ ] Documentation: All educational materials complete

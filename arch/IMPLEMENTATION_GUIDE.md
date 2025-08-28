# Implementation Guide: Step-by-Step Breakdown

## Phase 1: Metadata Parser Refactoring

### Step 1: Create Base Infrastructure
- [ ] Create `src/por_que/readers/` directory
- [ ] Create `src/por_que/readers/base.py`
  - [ ] Define `BaseParser` class with `__init__(self, reader: ThriftCompactReader)`
  - [ ] Move primitive reading methods from `MetadataReader`:
    - `read_i32()`, `read_i64()`, `read_bool()`, `read_string()`, `read_bytes()`
    - `read_list()`, `skip_field()`
  - [ ] Add educational docstrings explaining Thrift compact protocol basics

### Step 2: Create Component Parsers
- [ ] Create `src/por_que/readers/schema.py`
  - [ ] Implement `SchemaParser(BaseParser)`
  - [ ] Move `read_schema_element()` and `read_schema_tree()` from `MetadataReader`
  - [ ]<boltEndCtag> Add comments explaining schema tree structure and field relationships

- [ ] Create `src/por_que/readers/statistics.py`
  - [ ] Implement `StatisticsParser(BaseParser)`
  - [ ] Move `read_statistics()` and all `_deserialize_*` helpers
  - [ ] Document why statistics need special deserialization (logical vs physical types)

- [ ] Create `src/por_que/readers/column.py`
  - [ ] Implement `ColumnParser(BaseParser)`
  - [ ] Move `read_column_chunk()` and `read_column_metadata()`
  - [ ] Integrate with `StatisticsParser` for statistics fields

- [ ] Create `src/por_que/readers/row_group.py`
  - [ ] Implement `RowGroupParser(BaseParser)`
  - [ ] Move `read_row_group()`
  - [ ] Integrate with `ColumnParser` for column chunks

### Step 3: Create Metadata Orchestrator
- [ ] Create `src/por_que/readers/metadata.py`
  - [ ] Implement `MetadataParser` that composes all component parsers
  - [ ] Implement main `parse()` method that returns `FileMetadata`
  - [ ] Add tracing support: `parse(trace=False)`

### Step 4: Integration and Cleanup
- [ ] Update `FileMetadata.from_bytes()` to use new `MetadataParser`
- [ ] Run all existing tests to ensure compatibility
- [ ] Delete old `src/por_que/readers/metadata.py` (the monolithic one)
- [ ] Add unit tests for each new component parser

## Phase 2: Page-Level Parsing

### Step 1: Define Page Types
- [ ] Add to `src/por_que/types.py`:
  - [ ] `PageType` enum (DATA_PAGE, DATA_PAGE_V2, DICTIONARY_PAGE)
  - [ ] `PageHeader` dataclass with field metadata
  - [ ] `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader` dataclasses

### Step 2: Implement Page Parser
- [ ] Create `src/por_que/readers/page.py`
  - [ ] Implement `PageParser(BaseParser)`
  - [ ] Method: `read_page_header() -> PageHeader`
  - [ ] Method: `read_page_data(header: PageHeader) -> bytes`
  - [ ] Add detailed comments about page structure and compression

### Step 3: Add Page Tests
- [ ] Create test files with known page structures
- [ ] Test reading different page types
- [ ] Test handling of compressed vs uncompressed pages

## Phase 3: Data Decoding

### Step 1: Compression Support
- [ ] Create `src/por_que/readers/compression.py`
  - [ ] Function: `decompress(data: bytes, codec: Compression) -> bytes`
  - [ ] Implement UNCOMPRESSED (no-op)
  - [ ] Implement SNAPPY decompression
  - [ ] Implement GZIP decompression
  - [ ] Add tests with sample compressed data

### Step 2: PLAIN Encoding
- [ ] Create `src/por_que/readers/encoding.py`
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

## Phase 4: Top-Level Integration

### Step 1: Implement Reader Classes
- [ ] Create `src/por_que/parquet_file.py`:
  - [ ] `ParquetFile` class with `__init__(file_obj, trace=False)`
  - [ ] Property: `metadata: FileMetadata`
  - [ ] Property: `row_groups: List[RowGroupReader]`
  - [ ] Method: `column(name: str) -> Iterator[Any]` for convenience

- [ ] Create `src/por_que/readers/row_group.py` (enhance):
  - [ ] `RowGroupReader` class
  - [ ] Store row group metadata and file handle
  - [ ] Method: `column(name: str) -> ColumnChunkReader`
  - [ ] Method: `columns() -> List[str]`

- [ ] Create `src/por_que/readers/column_chunk.py`:
  - [ ] `ColumnChunkReader` class
  - [ ] Store column metadata, file handle, and schema info
  - [ ] Method: `read(trace=False) -> Iterator[Any]`
  - [ ] Internal: Use `PageParser` to read pages
  - [ ] Internal: Handle dictionary pages vs data pages
  - [ ] Internal: Yield decoded values one page at a time

### Step 2: Implement File Parser
- [ ] Create `src/por_que/readers/file.py`:
  - [ ] `FileParser` class that creates `ParquetFile` instances
  - [ ] Read magic number and footer
  - [ ] Use `MetadataParser` for metadata
  - [ ] Create `RowGroupReader` instances

### Step 3: Public API
- [ ] Update `__init__.py` to export `ParquetFile`
- [ ] Create examples/ directory with usage examples
- [ ] Write integration tests using sample Parquet files

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
- [ ] Phase 1 Complete: Metadata parsing refactored
- [ ] Phase 2 Complete: Can read page headers
- [ ] Phase 3 Complete: Can decode PLAIN encoding
- [ ] Phase 3 Enhanced: Dictionary encoding working
- [ ] Phase 4 Complete: Full lazy reading API
- [ ] Documentation: All educational materials complete

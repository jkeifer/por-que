# Parquet Physical Parser: Architecture and Implementation Plan

## 1. Overview

This document outlines a new parsing strategy for `por-que`. The goal is to move from a purely metadata-driven model to a model that represents the **physical layout** of the Parquet file. This will enable deep analysis, byte-level verification, and advanced performance insights that are impossible to achieve by only reading the file's logical metadata summary.

This approach treats the Parquet file as a sequence of structures to be walked and parsed, rather than a collection of data to be read.

## 2. Core Philosophy: The Hybrid Walk

A purely sequential walk of a Parquet file is challenging because context from the schema (located at the end of the file) is required to correctly interpret structures at the beginning. To solve this, we will use a two-pass **"Hybrid Walk"** approach.

#### Pass 1: Metadata Bootstrap
The first pass quickly reads the end of the file to acquire the global context needed for the main parsing pass.
1.  **Read Footer:** Read the last 8 bytes of the file to get the `FileMetaData` length and the final `PAR1` magic bytes.
2.  **Read Metadata:** Seek to the `FileMetaData` offset and read the complete block.
3.  **Parse Metadata:** Use the Thrift parser to deserialize the `FileMetaData` object.
4.  **Extract Schema:** The primary goal of this pass is to extract the `SchemaElement` list, which is essential for interpreting column data.

#### Pass 2: Sequential Physical Walk & Memory Management
The second, main pass walks the file from the beginning to the end, identifying and parsing every physical component in the order it appears on disk. This pass is designed to be memory-efficient by consuming the file as an incremental stream of bytes and not loading the entire file into memory.

1.  **Magic Header:** Start at offset 0 and verify the `PAR1` magic bytes.
2.  **Identify and Parse Components:** Read the file sequentially, identifying the boundaries of each Row Group, Column Chunk, and Page.
    *   Only the necessary bytes for the immediate component being parsed (e.g., a page header, a compressed page) are read into memory.
    *   Once a component is parsed and its data is extracted into the high-level data model, the raw byte buffer used for its parsing is immediately discarded.
3.  **Enrich and Build:** As each component is parsed, use the schema information from Pass 1 to enrich the data model. For example, when parsing a column chunk, use the schema to determine its data type, repetition/definition levels, and path.
4.  **Verify:** As the walk progresses, compare the physical offsets and sizes of discovered components with the values stored in the `FileMetaData` from Pass 1. This acts as a powerful validation check.

## 3. High-Level Data Model

This model represents the physical file structure.

*   **`ParquetFile`**: The root object representing the entire file.
    *   `filepath`: Path to the file on disk.
    *   `filesize`: Total size in bytes.
    *   `magic_header`: The `PAR1` bytes at offset 0.
    *   `row_groups`: A list of `RowGroup` objects, in the order they appear in the file.
    *   `file_metadata`: The fully parsed `FileMetaData` object from Pass 1.
    *   `magic_footer`: The `PAR1` bytes at the end of the file.

*   **`RowGroup`**: A container for a horizontal slice of the data.
    *   `ordinal`: The 0-based physical order of this row group in the file.
    *   `start_offset`: The byte offset where the first column chunk of this row group begins.
    *   `total_byte_size`: The total size of all column chunks in this row group.
    *   `num_rows`: The number of rows in this group.
    *   `column_chunks`: A list of `ColumnChunk` objects, in physical order.

*   **`ColumnChunk`**: A container for all the data for a single column within a row group.
    *   `path_in_schema`: The dot-notation path of the column (e.g., `profile.email`).
    *   `start_offset`: The byte offset where the column chunk's data begins.
    *   `total_byte_size`: The total size of the column chunk on disk.
    *   `codec`: The compression codec used (e.g., `SNAPPY`, `GZIP`).
    *   `num_values`: The total number of values in the chunk.
    *   `dictionary_page`: An optional `DictionaryPage` object if dictionary encoding is used.
    *   `data_pages`: A list of `DataPage` objects.
    *   `index_pages`: A list of `IndexPage` objects.

*   **`Page`**: A generic representation of a page, the smallest unit of storage.
    *   `page_type`: An enum (`DATA_PAGE`, `DATA_PAGE_V2`, `DICTIONARY_PAGE`, `INDEX_PAGE`).
    *   `start_offset`: The byte offset where the page begins.
    *   `page_header_size`: The size of the page's Thrift header.
    *   `compressed_page_size`: The size of the page's data on disk.
    *   `uncompressed_page_size`: The size of the page's data after decompression.
    *   `crc`: An optional checksum for the page data.
    *   **Sub-types:**
        *   **`DictionaryPage`**: `num_values`, `encoding`.
        *   **`DataPageV1`**: `num_values`, `encoding`, `definition_level_encoding`, `repetition_level_encoding`.
        *   **`DataPageV2`**: `num_values`, `num_nulls`, `num_rows`, `encoding`, `definition_levels_byte_length`, `repetition_levels_byte_length`, `statistics`.
        *   **`IndexPage`**: `page_locations` (min/max stats and offsets for pages in the chunk).

## 4. Error Handling: Fail-Fast and Custom Exceptions

The parser will adopt a strict, "fail-fast" error handling strategy. The integrity of the file structure is paramount, and any deviation from the Parquet specification will be treated as a fatal error.

*   **Immediate Termination:** Parsing will terminate immediately upon encountering any of the following conditions:
    *   Invalid magic bytes at the start or end of the file.
    *   Failed CRC checksums on pages.
    *   Inability to deserialize a Thrift structure.
    *   Mismatches between physical offsets discovered during the walk and the offsets declared in the `FileMetaData`.
*   **Custom Exceptions:** All exceptions raised by the parser will be custom exception types. These will either be existing types from `por_que.exceptions` or new types derived from the `PorQueError` base class. This ensures that consumers of the parser can implement specific and robust error handling logic.

## 5. Implementation Plan

### Phase 1: Core Data Models & Bootstrap
- [ ] Define Python data classes for the entire physical hierarchy (`ParquetFile`, `RowGroup`, `ColumnChunk`, `Page`, etc.).
- [ ] Implement the "Metadata Bootstrap" pass.
- [ ] Create a function to read the file footer (last 8 bytes).
- [ ] Create a function to read and deserialize the `FileMetaData` Thrift object.
- [ ] Ensure the schema can be successfully extracted from the metadata.

### Phase 2: The Physical File Walk
- [ ] Implement the main file walk loop that reads from the `PAR1` header to the start of the `FileMetaData`.
- [ ] Implement logic to identify `ColumnChunk` boundaries. The `file_offset` in the `FileMetaData`'s `ColumnChunk` list provides the starting point for each chunk.
- [ ] Group the physically contiguous `ColumnChunk`s into `RowGroup` objects.

### Phase 3: Page-Level Parsing
- [ ] Implement a function to read a `PageHeader` Thrift object from any given offset.
- [ ] Based on the `PageHeader.type`, delegate to a specific page parsing function.
- [ ] Implement a parser for `DICTIONARY_PAGE`.
- [ ] Implement a parser for `DATA_PAGE`.
- [ ] Implement a parser for `DATA_PAGE_V2`.
- [ ] Implement a parser for `INDEX_PAGE`.
- [ ] Connect the parsed page objects back to their parent `ColumnChunk`.

### Phase 4: Enrichment and Verification
- [ ] Implement the enrichment logic that uses the schema (from Pass 1) to add context to the physical objects (from Pass 2/3).
- [ ] Add a verification step that compares the offsets and sizes from the `FileMetaData` with the actual discovered offsets and sizes.
- [ ] Log any discrepancies found during verification (e.g., "Warning: Metadata offset for column 'X' is 1234, but found at 1238").
- [ ] (Optional) Implement CRC-32C checksum validation for page data.

### Phase 5: Caching and CLI Integration
- [ ] Design a caching strategy to store the parsed `ParquetFile` object.
- [ ] The cache key should be based on the file path, file size, and last modification time to ensure freshness.
- [ ] Choose a serialization format for the cache (e.g., `pickle` is easy, `JSON` is more portable).
- [ ] Implement `save_to_cache` and `load_from_cache` functions.
- [ ] Refactor the CLI commands to use the new physical parser instead of the old approach.
- [ ] Ensure all existing CLI functionality works with the new, richer data model.

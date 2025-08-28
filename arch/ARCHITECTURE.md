# Architecture and Refactoring Plan

## 1. Architectural Vision and Educational Goals

The primary goal of this project is to create a pure-Python library that serves as a clear educational tool for understanding the **Parquet format itself**. This includes its physical layout, its performance characteristics, and its compositional structure.

To achieve this, we will implement a **component-based, lazy-loading "Reader" model**. This architecture was chosen because it is a direct reflection of the Parquet format's design, making it the most effective model for teaching.

*   **It directly mirrors the physical structure of the file.** The code will be a tangible map of the format. A user can navigate the tree of `Reader` objects (`ParquetFile` -> `RowGroupReader` -> `ColumnChunkReader`) and see how the format is composed.

*   **It teaches the *why* behind the format.** Parquet is designed for efficient, selective reading. Our lazy-loading approach, where data is only read from disk when a `.read()` method is called, demonstrates *why* the format has features like metadata footers and column chunk offsets.

*   **It presents "Honest Complexity."** The logic for decoding a data page is complex, but this complexity is essential to understanding Parquet. Our design isolates this work in specific components, revealing it in a structured way.

---

## 2. Pedagogical Approach

To maximize the library's value as a learning tool, we will adopt several specific strategies:

1.  **Educational Docstrings:** Every major class will include a docstring that not only explains what the class does, but also teaches the relevant concepts of the Parquet format.
    ```python
    class ColumnChunkReader:
        """
        Represents a single column chunk within a row group.

        Teaching Points:
        - Why Parquet stores data in chunks (compression, I/O efficiency).
        - How dictionary encoding reduces storage for repeated values.
        - Trade-offs between chunk size and memory usage.
        """
    ```

2.  **Educational Error Messages:** Error messages will be written to be as helpful as possible, explaining not just what went wrong, but the likely cause in the context of the file format.
    ```python
    raise CorruptedDataError(
        f"Page header indicates {page_size} bytes but only {actual_size} available. "
        "This suggests file truncation or an incorrect offset in the metadata."
    )
    ```

3.  **Optional Tracing:** Key methods, like `read()`, may include an optional `trace=False` parameter. When enabled, the method will print diagnostic information about its progress (e.g., file offsets, page types being read), allowing a learner to visualize the parsing process in real time.

---

## 3. Core Design Decisions

### I/O Handling

The main entry point will accept a **file-like object** (an object that supports `read`, `seek`, and `tell` methods). This provides maximum flexibility, allowing the parser to work with local files, in-memory bytes (`io.BytesIO`), or remote files via wrapper libraries.

### Error Handling

We will use a **standard exception-based error handling** strategy to ensure the code is as accessible and idiomatic as possible for a broad Python audience. Custom exceptions will be defined in `src/por_que/exceptions.py`.

---

## 4. High-Level Architecture

#### The Lazy Reader Model

*   **`ParquetFile`**: The main, user-facing object. It holds the eagerly-loaded `FileMetadata` and a list of lazy `RowGroupReader`s.
*   **`RowGroupReader`**: A lightweight object holding the metadata for a row group. Provides methods to access its columns.
*   **`ColumnChunkReader`**: A lightweight object holding the metadata for a column chunk. Its `.read()` method will be a generator that reads **one data page at a time**, decodes it, and yields the values. This ensures that even for very large chunks, memory usage remains low.
*   **`PageReader`**: An internal object used by `ColumnChunkReader` to read and decode a single data page.

#### The Component-Based Parsers (`readers/`)

*   **`file.py` (`FileParser`)**: Internal orchestrator that assembles the `ParquetFile` object.
*   **`metadata.py` (`MetadataParser`)**: Composes smaller parsers to read the `FileMetaData` Thrift struct.
*   **`page.py` (`PageParser`)**: Reads `PageHeader` structs and their raw data payloads.
*   **`encoding.py`**: Contains functions for data decoding.

---

## 5. Implementation Phases

### Phase 1: Metadata Parser Refactoring

(As previously defined: create base class, component parsers for metadata, and integrate.)

### Phase 2: Page-Level Parsing

(As previously defined: create `PageHeader` type and `PageParser`.)

### Phase 3: Data Decoding

**Objective:** To implement the logic for decoding the various Parquet data encodings and compressions.

1.  **Implement Compression:** Support `UNCOMPRESSED`, `SNAPPY`, and `GZIP`.
2.  **Implement `PLAIN` Encoding:** This is the simplest encoding and will be implemented first for all data types.
3.  **Implement Dictionary Encoding:** Implement `RLE_DICTIONARY` and the reading of dictionary pages to show how repeated values are compressed.
4.  **Implement Core Data Encodings:** Implement `RLE` and `BIT_PACKED` for integers and booleans.

### Phase 4: Top-Level Integration

(As previously defined: implement the `*Reader` objects and the user-facing `ParquetFile`.)

---

## 6. Advanced Concepts: Schema Rigidity and Data Flexibility

The Parquet format enforces a single, immutable schema per file—all row groups must conform to this schema. This design choice provides strong guarantees and predictable performance. However, Parquet achieves flexibility for complex, nested, and optional data through its sophisticated **definition and repetition level** system.

*   **Definition Levels:** These integers track which optional fields in a nested schema are actually present for a given record. A level of `0` means a value is `NULL` at some point in its hierarchy.
*   **Repetition Levels:** These integers indicate when a new item in a repeated field (a list) begins.

**Teaching Opportunity:** This is a key concept. The parser will demonstrate why Parquet chose schema immutability (which aids compression and performance) while still supporting rich, flexible data structures through the level system. Future work on the parser will involve implementing the state machine required to reconstruct records from definition and repetition levels, which is a cornerstone of the Parquet format.

---

## 7. Testing Strategy

(As previously defined: multi-layered strategy with unit tests, integration tests, and cross-validation against `pyarrow`.)

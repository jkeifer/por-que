# CLI Design: Interactive Parquet Explorer

## 1. Vision and Core Concepts

The vision is to create an interactive CLI tool that serves as a "Parquet file microscope"—allowing users to visualize the file structure and progressively dive deeper into any component, making the format's design tangible and understandable.

*   **Progressive Disclosure:** Start with a high-level view and allow users to "zoom in" on any component (`File` → `Row Group` → `Column` → `Page`).
*   **Visual Learning:** Use ASCII art, colors, and tree structures to make the binary format visually comprehensible.
*   **Educational Annotations:** Each view will include explanations of what the user is seeing and why it matters from a design and performance perspective.

---

## 2. Milestone 1: Core Metadata Inspection

**Goal:** To build the foundational, read-only inspection tools for high-level file structure and metadata. This can be completed after Phase 1 of the parser implementation.

### Commands

*   `por-que inspect <file>`: The main entry point, showing a high-level overview.
*   `por-que inspect <file> schema`: A detailed, tree-like view of the file schema.
*   `por-que inspect <file> metadata`: A key-value view of the file's key-value metadata.
*   `por-que explore <file>`: A menu-driven interactive explorer for navigating the file structure.

Where `<file>` can be:
- A local file path: `data/customers.parquet`
- An HTTP(S) URL: `https://example.com/data.parquet`

**Note:** For the initial version, only unauthenticated HTTP(S) URLs are supported for remote files. Support for other protocols (e.g., S3) is a potential future enhancement.

### Interactive Explorer Mode

The explorer provides a user-friendly, menu-driven interface for navigating the file. Crucially, it also displays the equivalent direct command for the current view, teaching the user the more powerful scriptable interface as they explore.

```
PARQUET EXPLORER: customers.parquet
════════════════════════════════════════════════════════════════════

Current: File Overview

[1] Row Group 0 (10,000 rows)
[2] Row Group 1 (10,000 rows)
[3] Row Group 2 (9,832 rows)
[4] Schema Tree
[5] File Metadata Details

Navigation: Enter number to dive in, 'b' to go back, 'q' to quit
> _
```

### Output Examples

**File Overview (`por-que inspect file.parquet`)**
```
PARQUET FILE STRUCTURE: customers.parquet
════════════════════════════════════════════════════════════════════

📁 File Layout (24.3 MB total)
├─ 🔤 Header Magic "PAR1" (4 bytes)
├─ 📊 Row Group 0 (8.1 MB) - 10,000 rows
├─ 📊 Row Group 1 (8.0 MB) - 10,000 rows
├─ 📊 Row Group 2 (7.9 MB) - 9,832 rows
├─ 📋 File Metadata (4,234 bytes)
├─ 🔢 Footer Length: 4234 (4 bytes)
└─ 🔤 Footer Magic "PAR1" (4 bytes)

📋 Metadata Summary
├─ Schema: 5 columns (user_id, name, email, created_at, status)
├─ Total Rows: 29,832
├─ Created By: parquet-cpp version 1.5.1
└─ Compression: SNAPPY (all columns)

💡 The file stores data in 3 row groups, each a self-contained unit.
   This design enables parallel processing and memory-efficient reads.
```

**Schema Tree Viewer (`por-que inspect file.parquet schema`)**
```
SCHEMA TREE
════════════════════════════════════════════════════════════════════

root
├─ user_id: INT64 (REQUIRED)
└─ address: GROUP (OPTIONAL)
   ├─ street: BYTE_ARRAY (OPTIONAL)
   └─ zip: INT32 (OPTIONAL)
```

---

## 3. Milestone 2: Data Inspection and Sampling

**Goal:** To implement the drill-down views into the data-bearing structures (row groups, columns, pages) and provide a way to sample decoded data.

### Commands

*   `por-que inspect <file> row-group <N>`: View details of a specific row group.
*   `por-que inspect <file> row-group <N> column <NAME>`: View details of a specific column chunk.
*   `por-que inspect <file> row-group <N> column <NAME> page <N>`: View details of a specific page.
*   `por-que sample <file>`: View the first N decoded rows of data from the file.

### Output Examples

**Row Group Detail View**
```
ROW GROUP 0 DETAIL
════════════════════════════════════════════════════════════════════

📊 Row Group 0: 10,000 rows (8.1 MB)
│
├─ 📁 Column: user_id (INT64)
│  ├─ 💾 File Offset: 4 bytes
│  ├─ 📏 Compressed Size: 78.5 KB
│  ├─ 📐 Uncompressed Size: 80.0 KB
│  ├─ 🔢 Encodings: [RLE_DICTIONARY, PLAIN]
│  ├─ 📄 Pages: 2 (1 dict, 1 data)
│  └─ 📊 Statistics:
│     ├─ min: 1
│     ├─ max: 10000
│     └─ null_count: 0
│
└─ [4 more columns...]
```

**Page-Level Detail**
```
PAGE DETAIL: name column, Row Group 0, Page 0 (Dictionary)
════════════════════════════════════════════════════════════════════

📄 Dictionary Page
├─ 📏 Compressed Size: 45.2 KB
├─ 📐 Uncompressed Size: 89.7 KB
├─ 🔢 Num Values: 3,847 unique strings
└─ 🗜️ Compression: SNAPPY

📊 Dictionary Contents (first 5 entries):
┌───┬──────────┐
│ # │ Value    │
├───┼──────────┤
│ 0 │ "Aaron"  │
│ 1 │ "Abigail"│
└───┴──────────┘
```

### Remote File Caching

When working with remote files, `por-que` implements intelligent caching to minimize network I/O:

**Cache Strategy:**
- **Location:** Follows XDG Base Directory specification using `platformdirs`:
  - Linux: `$XDG_CACHE_HOME/por-que` (typically `~/.cache/por-que`)
  - macOS: `~/Library/Caches/por-que`
  - Windows: `%LOCALAPPDATA%\por-que\Cache`
- **Structure:** Subdirectories based on URL hash to avoid conflicts
- **What's Cached:**
  - File metadata (footer) - cached indefinitely by default
  - Row group headers - cached when first accessed
  - Column chunks - cached on demand with LRU eviction
  - Dictionary pages - always cached when column is accessed

**Cache Management:**
```bash
# View cache status for a file
por-que cache status https://example.com/data.parquet

# Clear cache for specific file
por-que cache clear https://example.com/data.parquet

# Clear entire cache
por-que cache clear --all

# Set cache size limit (default: 1GB)
por-que cache config --max-size 2GB

# Show cache location
por-que cache info
```

**Cache Behavior:**
- Remote file metadata is cached after first access
- Subsequent `inspect` commands use cached metadata
- Data pages are cached when accessed via `sample` or `profile`
- Cache respects HTTP ETags and Last-Modified headers
- User sees cache hits/misses in verbose mode: `--verbose`

Example output showing cache usage:
```
por-que inspect https://example.com/large.parquet --verbose

[Cache] Using XDG cache at: /home/user/.cache/por-que
[Cache] Checking metadata cache... HIT (cached 2 hours ago)
[Cache] File unchanged (ETag match), using cached metadata

PARQUET FILE STRUCTURE: large.parquet
════════════════════════════════════════════════════════════════════
...
```

**Implementation Note:** Use the `platformdirs` library to handle XDG compliance and cross-platform cache directories properly.

**Implementation Backend: `diskcache` Library**

To ensure a robust, performant, and maintainable caching system, the backend will be implemented using the `diskcache` library. This decision was made for several key reasons:

*   **Provides Required Features:** `diskcache` directly supports the exact features specified in our caching strategy, including size limits and an LRU (Least Recently Used) eviction policy.
*   **Reduces Complexity:** It abstracts away the significant complexity of building a caching system from scratch (e.g., manual file I/O, size enforcement, thread safety, and eviction logic). This allows us to focus on the core logic of the parser.
*   **Stores Complex Objects:** It can store any pickle-able Python object, not just bytes. This is a major advantage, as it allows us to cache important HTTP validation headers (like `ETag` and `Last-Modified`) alongside the data chunks themselves.

---

## 4. Milestone 3: Advanced Analysis Tools

**Goal:** To build powerful features for performance analysis, comparison, and integration with other tools.

### Commands

*   `por-que profile file.parquet`: A performance profiler showing I/O timings, decompression, and decoding speed.
*   `por-que diff file1.parquet file2.parquet`: A utility to compare the schema and metadata of two files.
*   `por-que export file.parquet <TARGET> --format <FORMAT>`: A tool to export metadata or schema to other formats (JSON, SQL, etc.).
*   `por-que inspect ... --binary`: A flag to view raw bytes with annotated Thrift protocol details.

### Output Example

**Performance Profiler (`por-que profile file.parquet column user_id`)**
```
PERFORMANCE PROFILE: Reading 'user_id' column
════════════════════════════════════════════════════════════════════

Metadata Parse: 2.3 ms

Row Group 0:
├─ Seek Time: 0.01 ms
├─ Data Page Read: 3.4 ms
├─ Decompression: 2.1 ms
├─ Decoding: 4.5 ms
└─ Total: 10.0 ms (1.0M values/sec)

Total Time: 34.7 ms for 29,832 values
Bytes Read: 243 KB of 24.3 MB (1.0% of file)

💡 Reading one column accessed only 1% of the file!
   This is why columnar formats excel at analytics.
```

---

## 5. Cross-Cutting Concerns

### Machine-Readable Output

To ensure the CLI is useful for both humans and scripts, all `inspect`, `profile`, and `sample` commands **must** support a `--json` flag. This will output the full data for that view in a structured, machine-readable JSON format, allowing easy integration with tools like `jq`.

### Implementation Notes

*   **CLI Framework:** The command-line structure (commands, arguments, options) will be built using `click`. The terminal UI (tables, trees, colors, etc.) will be rendered using `rich`. This combination leverages the strengths of both libraries.
*   **Color Coding:** Use a consistent color-coding convention to distinguish between different types of information (e.g., metadata, data containers, file offsets).
*   **Educational Hooks:** Each view should be designed to answer: What is this? Why is it structured this way? What are the performance implications?

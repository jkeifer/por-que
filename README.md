[![Tests](https://github.com/jkeifer/por-que/actions/workflows/ci.yml/badge.svg)](https://github.com/jkeifer/por-que/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/por-que.svg)](https://badge.fury.io/py/por-que)

# Por Qué: Pure-Python Parquet Parser

¿Por qué? ¿Por qué no?

Si, ¿pero por qué? ¡Porque, parquet, python!

> [!WARNING]
> This is a project for education, it is NOT suitable for any production uses.

## Overview

Por Qué is a pure-Python Apache Parquet parser built from scratch for
educational purposes. It implements Parquet's binary format parsing without
external dependencies, providing insights into how Parquet files work
internally.

## Features

- **Pure Python implementation** - No native dependencies, built from scratch
- **Complete reader stack** - Parse files, row groups, column chunks, and pages
- **Lazy data loading** - Efficient memory usage with on-demand reading
- **Metadata inspection** - Parse and display Parquet file metadata
- **Schema analysis** - View detailed schema structure with logical types
- **Row group information** - Inspect row group statistics and column metadata
- **Compression analysis** - Calculate compression ratios and storage
  efficiency
- **HTTP support** - Read Parquet files from URLs using range requests
- **CLI interface** - Easy-to-use command-line tools for exploration

## Installation

With pip:

```bash
pip install 'por-que[cli]'
```

## Usage

### Command Line Interface

The CLI uses a unified file/URL argument that auto-detects the source:

```bash
# Show help
porque --help

# View file summary (local file or URL)
porque metadata /path/to/file.parquet summary
porque metadata https://example.com/file.parquet summary

# View detailed schema
porque metadata file.parquet schema

# View file statistics and compression info
porque metadata file.parquet stats

# View row group information
porque metadata file.parquet rowgroups
porque metadata file.parquet rowgroups --group 0

# View column metadata
porque metadata file.parquet columns

# View key-value metadata
porque metadata file.parquet keyvalue
porque metadata file.parquet keyvalue "spark.version"
```

### Python API

```python
from por_que import ParquetFile
from por_que.util.http_file import HttpFile

# Read from local file
with open("data.parquet", "rb") as f:
    parquet_file = ParquetFile(f)
    print(f"Total rows: {parquet_file.num_rows()}")
    print(f"Columns: {parquet_file.columns()}")

    # Access metadata
    metadata = parquet_file.metadata
    print(f"Parquet version: {metadata.version}")
    print(f"Row groups: {len(metadata.row_groups)}")

# Read from URL
with HttpFile("https://example.com/data.parquet") as f:
    parquet_file = ParquetFile(f)

    # Lazy iteration over a column
    for value in parquet_file.column("user_id"):
        print(value)  # Values are yielded one at a time

    # Access specific row group
    row_group = parquet_file.row_group(0)
    column_reader = row_group.column("email")
    for page in column_reader.read():
        # Pages contain raw data (decoding not yet implemented)
        page_header, raw_data = page
        print(f"Page type: {page_header.type}")
```

## What You'll Learn

By exploring this codebase, you can learn about:

- **Parquet file format** - Binary structure, magic bytes, footer layout
- **Thrift protocol** - Binary serialization format used by Parquet
- **Schema representation** - How nested and complex data types are encoded
- **Compression techniques** - Various compression algorithms and their
  efficiency
- **Column storage** - Columnar storage benefits and trade-offs
- **Metadata organization** - How Parquet organizes file and column statistics
- **Lazy loading patterns** - Efficient data access without loading entire
  files
- **Binary parsing** - Low-level byte manipulation and struct unpacking

## Educational Focus

This implementation prioritizes readability and understanding over performance:

- Explicit parsing logic instead of generated Thrift code
- Comprehensive comments explaining binary format details
- Step-by-step Thrift deserialization
- Clear separation of concerns between parsing and data structures
- Educational debug logging (enable with
  `logging.basicConfig(level=logging.DEBUG)`)
- Structured architecture mirroring Parquet's physical layout

## Requirements

- Python 3.13+
- No runtime dependencies for core parsing
- Click for CLI interface (optional)

## Architecture

```plaintext
src/por_que/
├── cli/                     # Command-line interface
│   ├── _cli.py             # Click CLI definitions
│   ├── formatters.py       # Output formatting functions
│   └── exceptions.py       # CLI-specific exceptions
├── parsers/                 # Low-level binary parsers
│   ├── parquet/            # Parquet format parsers
│   │   ├── metadata.py     # Metadata parser
│   │   ├── page.py         # Page header parser
│   │   ├── schema.py       # Schema parser
│   │   └── ...             # Other format parsers
│   └── thrift/             # Thrift protocol implementation
│       ├── parser.py       # Core Thrift parser
│       └── enums.py        # Thrift type definitions
├── readers/                 # High-level reader classes
│   ├── row_group.py        # Row group reader
│   ├── column_chunk.py     # Column chunk reader
│   └── page.py             # Page reader
├── util/                    # Utilities
│   └── http_file.py        # HTTP range request support
├── parquet_file.py         # Main entry point
├── protocols.py            # Type protocols
├── types.py                # Data structures and types
├── enums.py                # Parquet format enums
├── stats.py                # Statistics calculation
└── exceptions.py           # Core exceptions
```

## Current Capabilities

### Implemented Features

- **Complete metadata parsing** - All Parquet metadata structures
- **Schema parsing** - Full schema tree with logical types
- **Page header parsing** - All page types (DATA_PAGE, DATA_PAGE_V2,
  DICTIONARY_PAGE)
- **Row group access** - Lazy readers for row groups and columns
- **Statistics parsing** - Min/max values and null counts
- **HTTP support** - Range requests for remote file reading
- **Memory efficiency** - Lazy loading throughout the stack

### Work in Progress

- **Data decoding** - Converting raw page data to Python values
- **Compression support** - Snappy, GZIP, LZ4, Zstd decompression
- **Encoding support** - PLAIN, DICTIONARY, RLE, BIT_PACKED decoding
- **Nested data** - Handling definition and repetition levels

### Future Development

- Complete value extraction with all encodings
- Schema inference and validation
- Performance optimizations

### Not Planned

- Write support (creating Parquet files)

## Contributing

This is primarily an educational project. Feel free to:

- Report bugs or parsing issues
- Suggest improvements for educational value
- Add more comprehensive test cases
- Improve documentation and comments

## License

Apache License 2.0

## Why "Por Qué"?

Because asking "why" leads to understanding! This project exists to answer "why
does Parquet work the way it does?" by implementing it from first principles.

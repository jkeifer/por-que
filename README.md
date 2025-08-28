[![Tests](https://github.com/jkeifer/por-que/actions/workflows/ci.yml/badge.svg)](https://github.com/jkeifer/por-que/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/por-que.svg)](https://badge.fury.io/py/por-que)

# Por Qué: Pure-Python Parquet Parser

¿Por qué? ¿Por qué no?

Si, ¿pero por qué? ¡Porque, parquet, python!

**This is a project for education, it is NOT suitable for any production uses.**

## Overview

Por Qué is a pure-Python Apache Parquet parser built from scratch for
educational purposes. It implements Parquet's binary format parsing without
external dependencies, providing insights into how Parquet files work
internally.

## Features

- **Pure Python implementation** - No native dependencies, built from scratch
- **Metadata inspection** - Parse and display Parquet file metadata
- **Schema analysis** - View detailed schema structure with logical types
- **Row group information** - Inspect row group statistics and column metadata
- **Compression analysis** - Calculate compression ratios and storage
  efficiency
- **HTTP support** - Read Parquet files from URLs or local filesystem
- **CLI interface** - Easy-to-use command-line tools

## Installation

With pip:

```bash
pip install 'por-que[cli]'
```

## Usage

### Command Line Interface

```bash
# Show help
porque --help

# View file summary
porque metadata --file /path/to/file.parquet summary
porque metadata --url https://example.com/file.parquet summary

# View detailed schema
porque metadata --file file.parquet schema

# View file statistics and compression info
porque metadata --file file.parquet stats

# View row group information
porque metadata --file file.parquet rowgroups
porque metadata --file file.parquet rowgroups --group 0

# View column metadata
porque metadata --file file.parquet columns

# View key-value metadata
porque metadata --file file.parquet keyvalue
porque metadata --file file.parquet keyvalue "spark.version"
```

### Python API

```python
from pathlib import Path
from por_que.types import FileMetadata

# Read from local file
metadata = FileMetadata.from_file("data.parquet")

# Read from URL
metadata = FileMetadata.from_url("https://example.com/data.parquet")

# Access metadata properties
print(f"Parquet version: {metadata.version}")
print(f"Total rows: {metadata.num_rows}")
print(f"Schema: {metadata.schema}")
print(f"Row groups: {len(metadata.row_groups)}")
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

## Educational Focus

This implementation prioritizes readability and understanding over performance:

- Explicit parsing logic instead of generated Thrift code
- Comprehensive comments explaining binary format details
- Step-by-step Thrift deserialization
- Clear separation of concerns between parsing and data structures

## Requirements

- Python 3.13+
- No runtime dependencies for core parsing
- Click for CLI interface (optional)

## Architecture

```plaintext
src/por_que/
├── cli/                 # Command-line interface
│   ├── _cli.py         # Click CLI definitions
│   ├── formatters.py   # Output formatting functions
│   └── exceptions.py   # CLI-specific exceptions
├── readers/             # Core parsing logic
│   ├── metadata.py     # Main metadata parser
│   ├── thrift.py       # Thrift protocol implementation
│   ├── enums.py        # Thrift field type enums
│   └── constants.py    # Parsing constants
├── types.py            # Data structures and types
├── enums.py            # Parquet format enums
├── stats.py            # Statistics calculation
├── util/               # Utilities
│   └── http.py         # HTTP range request handling
└── exceptions.py       # Core exceptions
```

## Current Capabilities

Por Qué currently focuses on metadata parsing and inspection. Future
development may include:

- Data reading and value extraction
- Schema inference and validation
- Performance optimizations
- Additional Parquet format features

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

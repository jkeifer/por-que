[![Tests](https://github.com/jkeifer/por-que/actions/workflows/ci.yml/badge.svg)](https://github.com/jkeifer/por-que/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/por-que.svg)](https://badge.fury.io/py/por-que)

# Por Qué: Python Parquet Parser

¿Por qué? ¿Por qué no?

Si, ¿pero por qué? ¡Porque, parquet, python!

## But seriously, why "Por Qué"?

Because asking "why" leads to understanding! This project exists to answer "why
does Parquet work the way it does?" by implementing it from first principles.

> [!WARNING]
> This is a project for education, it is NOT suitable for any production uses.

## Overview

Por Qué is a Python Apache Parquet parser built from scratch for educational
purposes. It implements Parquet's binary format in highly-readable python to
more easily provide insights into how Parquet files work internally.

## Features

- **Complete reader stack** - Parse files, row groups, column chunks, and pages
- **Metadata inspection** - Parse and display Parquet file metadata
- **Schema analysis** - View detailed schema structure with logical types
- **Row group information** - Inspect row group statistics and column metadata
- **Compression analysis** - Calculate compression ratios and storage
  efficiency
- **HTTP support** - Read Parquet files from URLs using range requests
- **Async parallelism** - supports reading from async sources that support
  parallelism, like the files over HTTP

## Installation

With pip:

```bash
pip install 'por-que'
```

## Usage

### Python API

```python
from por_que import AsyncHttpFile, ParquetFile

# Read from local file
with open("data.parquet", "rb") as f:
    parquet_file = await ParquetFile.from_reader(f, "data.parquet")

    # Access file-level metadata
    print(f"Total rows: {parquet_file.metadata.metadata.row_count}")
    print(f"Columns: {parquet_file.metadata.metadata.column_count}")
    print(f"Row groups: {parquet_file.metadata.metadata.row_group_count}")
    print(f"Parquet version: {parquet_file.metadata.metadata.version}")

    # Access schema information
    schema = parquet_file.metadata.metadata.schema_root
    print(f"Schema: {schema}")

    # Access column chunks and parse data
    for column_chunk in parquet_file.column_chunks:
        print(f"Column: {column_chunk.path_in_schema}")
        print(f"  Compression: {column_chunk.codec}")
        print(f"  Values: {column_chunk.num_values}")

        # Parse all data from the column
        data = column_chunk.parse_all_data_pages(f)
        print(f"  First values: {data[:5]}")

# Read from URL
asnyc with AsyncHttpFile("https://example.com/data.parquet") as f:
    parquet_file = await ParquetFile.from_reader(f, "https://example.com/data.parquet")

    # Access pages within a column chunk
    column_chunk = parquet_file.column_chunks[0]
    for page in column_chunk.data_pages:
        print(f"Page at offset {page.start_offset}")
        print(f"  Type: {page.page_type}")
        print(f"  Values: {page.num_values}")
        print(f"  Encoding: {page.encoding}")

# Serialize to JSON or dict
json_output = parquet_file.to_json(indent=2)
dict_output = parquet_file.to_dict()

# Deserialize from JSON or dict
restored = ParquetFile.from_json(json_output)
restored = ParquetFile.from_dict(dict_output)
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

## Architecture

```plaintext
src/por_que/
├── parsers/                # Low-level binary parsers
│   ├── parquet/            # Parquet format parsers
│   │   ├── metadata.py     # File metadata parser
│   │   ├── page.py         # Page header parser
│   │   ├── page_index.py   # Page index structures
│   │   ├── schema.py       # Schema tree parser
│   │   ├── statistics.py   # Statistics parser
│   │   ├── row_group.py    # Row group metadata
│   │   ├── column.py       # Column chunk metadata
│   │   └── ...             # Other metadata parsers
│   ├── page_content/       # Page data decoding
│   │   ├── data.py         # Data page decoder
│   │   ├── dictionary.py   # Dictionary page decoder
│   │   └── compressors.py  # Compression codecs
│   ├── thrift/             # Thrift protocol implementation
│   │   ├── parser.py       # Core Thrift parser
│   │   └── enums.py        # Thrift type definitions
│   ├── logical_types.py    # Logical type converters
│   └── physical_types.py   # Physical type parsers
├── physical.py             # Main ParquetFile class
├── file_metadata.py        # Metadata data structures
├── pages.py                # Page data structures
├── protocols.py            # Type protocols
├── enums.py                # Parquet format enums
├── constants.py            # Format constants
└── exceptions.py           # Exception classes
```

## Current Capabilities

### Implemented Features

- **Complete metadata parsing** - All Parquet metadata structures
- **Schema parsing** - Full schema tree with logical types
- **Page parsing** - All page types (DATA_PAGE, DATA_PAGE_V2,
  DICTIONARY_PAGE, INDEX_PAGE)
- **Data decoding** - Convert raw page data to Python values
- **Compression support** - Snappy, GZIP, Brotli, LZ4, LZO, Zstd decompression
- **Encoding support** - PLAIN, DICTIONARY, RLE, DELTA (all variants),
  BYTE_STREAM_SPLIT
- **Nested data** - Definition and repetition level handling
- **Statistics parsing** - Min/max values, null counts, and distinct counts
- **Page indexes** - Column and offset index structures
- **HTTP support** - Range requests for remote file reading
- **Serialization** - Export to JSON/dict and restore from serialized formats

### Future Development

- Performance optimizations
- Additional test coverage for edge cases
- Refactoring and code organization improvements

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

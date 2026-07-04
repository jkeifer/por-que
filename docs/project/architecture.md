# Architecture

This page describes how por-que is actually structured today. It is curated
from the point-in-time design notes in `arch/` (which are kept as a historical
record, not published) and reconciled against the current code — several of
those plans landed differently from how they were first written.

## Guiding principle

por-que optimizes for *readability over performance*. The package layout is a
map of the Parquet format: you can navigate from a file down to a single value
and see how each layer is composed. Where a design plan and the code disagree,
the code wins — and this page tracks the code.

## Layers

From the bytes up:

### Thrift (`parsers/thrift/`)

Parquet's metadata is serialized with the Thrift *compact protocol*. por-que
implements that protocol by hand rather than using generated Thrift stubs, so
the decoding is legible. `ThriftCompactParser` works synchronously over an
in-memory buffer; the byte spans it needs are fetched at the I/O boundary
before parsing begins.

### Parquet metadata parsers (`parsers/parquet/`)

A family of small parsers, each responsible for one Thrift struct, composed on
a shared `BaseParser`: `MetadataParser`, `SchemaParser`, `RowGroupParser`,
`ColumnParser`, `RowGroupStatisticsParser`, `PageParser`, the page-index
parsers, and helpers for key-value and geospatial metadata. They read
`FileMetaData` and produce the data model in `file_metadata.py`.

### Page content decoders (`parsers/page_content/`)

Once page boundaries are known, these decode payloads: `data.py`
(data-page V1/V2), `dictionary.py` (dictionary pages), and `compressors.py`
(Snappy, GZIP, Brotli, LZ4, LZO, Zstd). Logical- and physical-type conversion
lives in `parsers/logical_types.py` and `parsers/physical_types.py`.

### The data model (`file_metadata.py`, `pages.py`)

Immutable Pydantic models (`frozen=True`) describe the parsed structures:
`FileMetadata`, `RowGroup`, `ColumnChunk`, `ColumnMetadata`, the schema tree
(`SchemaRoot` / `SchemaGroup` / `SchemaLeaf`), statistics, the offset/column
indexes, and the page models. Because they are Pydantic models they serialize
to JSON and round-trip back.

### The physical file (`physical.py`)

`ParquetFile` is the user-facing entry point. `ParquetFile.from_reader` runs
the two-pass "hybrid walk": read the footer to bootstrap `FileMetadata` and the
schema, then walk column chunks and page structure using that schema for
context. It accepts any readable/seekable object — local files, `io.BytesIO`,
or async remote readers via [`hctef`](https://github.com/jkeifer/hctef) — and
is a coroutine so remote fetches can overlap.

### Record reconstruction (`structuring/`)

Parquet stores columns, not records. The `structuring/` package rebuilds
records from column values and their definition/repetition levels:
`reconstruct.py` drives type-specific handlers for primitives, lists, maps, and
structs.

### Supporting modules

- `enums.py`, `constants.py` — Parquet format constants and enumerations.
- `exceptions.py` — the `PorQueError` hierarchy, with messages written to
  explain the likely cause in terms of the format.
- `protocols.py` — the `ReadableSeekable` / async reader protocols.
- `util/` — spans, iteration helpers, async adapters, model helpers.

## Staged access

The model is designed for cheap-to-expensive staged access — read only as far
as you need:

1. **Bounds** — the footer's size and magic bytes.
2. **Metadata** — `FileMetadata.from_reader`, optionally projected to a subset
   of columns to bound memory.
3. **Structure** — `ParquetFile.from_reader` walks page layout.
4. **Data** — decode individual pages/columns on demand.

The [Guides](../guides/staged-access.md) go into practical detail; the
[API reference](../reference/index.md) documents each type.

## Serialization and references

`ParquetFile` and `FileMetadata` serialize to JSON/dict and restore from it.
The schema tree uses references so that, on deserialization, a root walker
re-links each node to its schema element — so computed accessors keep working
on reloaded metadata. The serialized layout is a contract consumed by
[ver-por-que](https://teotl.dev/ver-por-que); the
[serialization guide](../guides/serialization.md) documents it, including the
`format_version` marker used to detect stale exports.

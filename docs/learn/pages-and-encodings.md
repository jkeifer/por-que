# Pages & encodings

A column chunk is not one indivisible blob — it is a sequence of **pages**. The
page is Parquet's unit of encoding, compression, and (with the offset index) of
seeking. This is where the format finally meets the actual bytes of your data.

## Page types

Every page begins with a Thrift header describing its type, sizes, and encoding;
Por Qué reads that header and dispatches to the right model in
[`pages`](../reference/pages.md). There are four kinds:

- **[`DictionaryPage`](../reference/pages.md)** — an optional first page in a
  chunk holding the column's distinct values *once*. When present it comes
  before the data pages, and data pages then reference values by index rather
  than repeating them.
- **[`DataPageV1`](../reference/pages.md)** — the classic data page: definition
  levels, repetition levels, and encoded values, all compressed together as one
  unit.
- **[`DataPageV2`](../reference/pages.md)** — a refinement that stores the
  definition- and repetition-level sections *uncompressed* and at known byte
  lengths (`definition_levels_byte_length`, `repetition_levels_byte_length`), so
  a reader can skip straight past the levels, and carries `num_nulls` and
  `num_rows` in the header for cheaper pruning.
- **[`IndexPage`](../reference/pages.md)** — a rarely-used page type; the modern
  page index lives in its own metadata structures instead (see
  [Statistics & pruning](statistics-and-pruning.md)).

Every page also records its physical footprint —
[`start_offset`, `header_size`, `compressed_page_size`,
`uncompressed_page_size`](../reference/pages.md), and an optional `crc`. Those
sizes are what let the sequential walk in
[`physical`](../reference/physical.md) find the next page: the next page begins
at `start_offset + header_size + compressed_page_size`.

## Encodings, from the top

An encoding is how values are packed into bytes before compression. The physical
type decides which encodings are legal; the writer picks among them. Por Qué
recognizes the full set in the [`Encoding`](../reference/por_que.md) enum. The
ones worth knowing by name:

- **PLAIN** — values written back to back with no cleverness. The baseline.
- **Dictionary** (`PLAIN_DICTIONARY` / `RLE_DICTIONARY`) — the dictionary page
  holds each distinct value once; the data page stores small integer indices
  into it. Excellent for low-cardinality columns (country codes, category
  labels), where the same handful of values repeat millions of times.
- **RLE / bit-packing** — run-length encoding collapses repeated values to a
  value plus a count, and bit-packing stores small integers in the minimum
  number of bits. This hybrid is how definition and repetition levels are stored
  (they are tiny integers, mostly zeros), and how dictionary indices are
  packed.
- **DELTA** family (`DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`,
  `DELTA_BYTE_ARRAY`) — stores differences between consecutive values instead of
  the values themselves, which is compact for sorted or slowly-changing data
  like timestamps and IDs.
- **BYTE_STREAM_SPLIT** — reorders the bytes of floating-point values so that
  like bytes sit together, giving the compressor longer runs to work with.

## Compression sits on top

After encoding, a page's body is optionally run through a general-purpose
compressor — the chunk's [`codec`](../reference/file_metadata.md), one of
`SNAPPY`, `GZIP`, `ZSTD`, `LZ4`, `BROTLI`, or none. Encoding and compression are
complementary: encoding exploits the *structure* of the data (repetition,
sortedness, low cardinality); compression squeezes the *bytes* that remain. The
page header carries both the compressed and uncompressed sizes so a reader knows
how much to fetch and how much to expect after inflation.

To actually decode a page, Por Qué reads its header, seeks to its body,
decompresses, then walks the levels and values according to the encoding — the
mechanics live in the page-content parsers, and the [reconstruction](reconstruction.md)
page picks up where the decoded `(value, definition_level, repetition_level)`
tuples come out.

Before that, though, one more thing lives in these structures: statistics. They
are what let a reader skip most of these pages entirely.

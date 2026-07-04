# File anatomy

Open any Parquet file in a hex viewer and the first thing you see is four
bytes: `PAR1`. Skip to the very last four bytes and you find them again. Those
bookends are the whole story of how Parquet is laid out — and once you see why
they are there, most of the rest of the format follows.

## Magic bytes, and why there are two of them

```
┌──────┬─────────────────────────────┬───────────────┬─────────┬──────┐
│ PAR1 │  row group 0, row group 1…  │  FileMetaData │  len(4) │ PAR1 │
└──────┴─────────────────────────────┴───────────────┴─────────┴──────┘
 0      4                              ↑ footer_start-len         EOF-8  EOF
```

The leading `PAR1` marks the file as Parquet and gives the column data a fixed
starting offset. The trailing `PAR1` is a sentinel that lets a reader validate
the file from the *end*, which is exactly where a reader has to start. Por Qué
checks both against [`PARQUET_MAGIC`](../reference/por_que.md); a file smaller
than 12 bytes cannot hold even the two magics plus a length, so it is rejected
outright.

## The footer, read back to front

The last eight bytes are the **footer**: a 4-byte little-endian integer giving
the length of the metadata block, followed by the trailing `PAR1`. To find the
metadata, a reader seeks to `EOF - 8`, reads the length, then jumps *backwards*
by that many bytes to the start of the `FileMetaData` structure. You can watch
this happen in
[`FileMetadata.from_reader`](../reference/file_metadata.md):

```python
reader.seek(-FOOTER_SIZE, SEEK_END)   # 8 bytes: length + magic
footer_bytes = await reader.read(FOOTER_SIZE)
metadata_size = struct.unpack('<I', footer_bytes[:4])[0]
metadata_start = footer_start - metadata_size   # jump back
```

The metadata itself is a Thrift structure; the next page,
[Metadata & Thrift](metadata-and-thrift.md), takes it apart. What matters here
is *where* it lives: at the end.

## Why metadata-last?

Putting the table of contents at the back feels backwards until you think about
who writes the file. A writer streams row groups out as it receives data — it
does not know the final schema statistics, row counts, or byte offsets until it
has finished writing every column. If the metadata lived at the *front*, the
writer would have to either buffer the entire file in memory or seek back and
patch the header after the fact. Neither is friendly to a streaming writer.

By emitting all the bulk data first and the metadata last, a writer can produce
a Parquet file in a **single forward pass** — never seeking backwards, filling
in offsets and statistics as it goes, and flushing the accumulated metadata at
the very end. The trailing length + magic is what makes that end-placed
metadata findable again.

## What metadata-last buys a reader

The same layout that helps writers is what makes Parquet cheap to read
remotely. A reader does not need the whole file to understand it — it needs the
footer. Over HTTP that is two small range requests:

1. Fetch the last few kilobytes to read the length and the metadata.
2. If the metadata turned out larger than the first fetch, fetch the exact byte
   range it names.

From that alone you know the schema, every row group, every column chunk, and
where each one lives in the file — without transferring a single data page.
Selective reads (this column, that row group) then become precise range
requests against offsets the footer handed you. This is the foundation the
[Fast metadata](../guides/fast-metadata.md) and
[Reading remote files](../guides/reading-remote-files.md) guides build on.

The rest of the Learn section works inward from here: the footer's Thrift
encoding, the schema tree it describes, the row groups and column chunks it
points at, and finally the pages that hold the actual values.

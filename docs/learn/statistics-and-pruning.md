# Statistics & pruning

Everything so far has been about *finding* data. Statistics are about *avoiding*
it. Parquet salts the file with summaries at three scales, and a reader that
consults them can throw away most of a file before fetching a single value.
Understanding these three stages is understanding why Parquet is fast for
selective queries.

The rule of thumb: the coarser the stage, the cheaper it is to check and the
more it can eliminate at once. So a reader works from coarse to fine.

## Stage 1 — file level: key-value metadata

The footer carries an open-ended list of
[`KeyValueMetadata`](../reference/file_metadata.md) — string key/value pairs the
writer attaches to the whole file. This is where ecosystem conventions live.
The clearest example is geospatial: Overture Maps and GeoParquet stash a `geo`
key holding a JSON blob with each geometry column's **bounding box** and CRS.

A reader with a spatial filter — "features inside this map tile" — can read that
one key straight from the footer and, if the file's bounding box does not
intersect the query, **skip the entire file**. No row groups, no chunks, no
pages. It is the cheapest possible check because it is already in the bytes you
fetched to read the metadata at all.

## Stage 2 — row group level: column statistics

Drop one level down. Every
[`ColumnChunk`](../reference/file_metadata.md) carries
[`ColumnStatistics`](../reference/file_metadata.md) for its stripe of rows:
`min`, `max`, `null_count`, and sometimes `distinct_count`. For a predicate like
`timestamp >= '2024-01-01'`, a reader compares the bound against each chunk's
min/max and skips any chunk whose range cannot contain a match — discarding tens
of thousands of rows per decision.

Statistics are stored as raw bytes in the column's physical type, which is not
much use to a human or a filter written in logical terms. Por Qué resolves that
through the owning schema leaf:
[`converted_min_value` / `converted_max_value`](../reference/file_metadata.md)
run the raw bytes through the same
physical-then-logical conversion the values use, so a `DATE` column's min comes
back as a real date, not four opaque bytes.

## Stage 3 — page level: the page index

The finest stage is the **page index**, split across two structures that live in
their own byte ranges (found via the chunk's `column_index_offset` and
`offset_index_offset`):

- **[`ColumnIndex`](../reference/file_metadata.md)** — per-*page* min/max values,
  `null_pages`, `null_counts`, and a `boundary_order` flag telling you whether
  those values are sorted. This lets a reader narrow a filter down to the exact
  pages within a chunk that could match.
- **[`OffsetIndex`](../reference/file_metadata.md)** — a list of
  [`PageLocation`](../reference/file_metadata.md)s, each giving a page's byte
  `offset`, `compressed_page_size`, and `first_row_index`.

Together they close the loop: the column index says *which* pages matter, the
offset index says exactly *where* those pages are, and the reader fetches only
those byte ranges.

## "The file tells you where everything is"

The offset index does one more thing that is easy to miss. Without it, a reader
discovers a chunk's pages by *walking* — parse a page header, use its size to
compute the next offset, read again, repeat. Each read **depends** on the
previous one, so over HTTP the round trips serialize.

With an offset index, the file has already told you where every data page
starts. Por Qué's
[`_discover_pages_via_offset_index`](../reference/physical.md) reads every page
header at its known location, and because those reads no longer depend on each
other, it issues them **concurrently** (when the reader supports independent
cursors) instead of one at a time. It then checks that the discovered pages tile
the chunk's byte range exactly; if some bytes are unaccounted for — an index
page the offset index does not describe — it falls back to the sequential walk
for correctness.

This is the [I/O-planning-versus-decoding](metadata-and-thrift.md) split from the
Thrift page made concrete: the offset index turns a dependent walk into a
planned batch of independent reads. The
[Staged access](../guides/staged-access.md) guide shows how to spend each of
these three stages deliberately.

Once you have decided *which* pages to read and decoded them, the last job is to
put the columns back together into records.

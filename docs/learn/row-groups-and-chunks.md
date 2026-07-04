# Row groups & column chunks

The schema tells you *what* columns exist. Row groups and column chunks tell you
*where their values live* — and the layout is a two-dimensional cut of the table
that is the whole reason columnar storage is fast.

## Two cuts: horizontal and vertical

A Parquet file partitions its data twice.

- **Horizontally**, into **row groups**: contiguous stripes of rows. A file with
  ten million rows might hold them as 128 row groups of ~78k rows each. Each row
  group is self-contained — it has its own copy of every column and its own
  statistics.
- **Vertically**, within each row group, into **column chunks**: one chunk per
  leaf column, holding *only* that column's values for *only* that stripe of
  rows.

```
             col: id      name     city
          ┌──────────┬──────────┬──────────┐
row grp 0 │  chunk   │  chunk   │  chunk   │
          ├──────────┼──────────┼──────────┤
row grp 1 │  chunk   │  chunk   │  chunk   │
          ├──────────┼──────────┼──────────┤
row grp 2 │  chunk   │  chunk   │  chunk   │
          └──────────┴──────────┴──────────┘
```

A [`ColumnChunk`](../reference/file_metadata.md) is the cell of this grid. On
disk, the chunks of one row group are written back to back, and each chunk's
pages are contiguous within it — so reading one column of one row group is a
single contiguous byte range, not a scatter of tiny reads. That contiguity is
what makes selective reads cheap; the [Fast metadata](../guides/fast-metadata.md)
guide leans on it directly.

## Why cut it this way?

Because most queries touch a few columns and can skip most rows. The vertical
cut means reading `city` never pulls in `name` or a hundred other columns — you
transfer only the bytes you asked for. The horizontal cut means each column
chunk carries **statistics** (min/max, null counts) for its slice of rows, so a
reader can look at a chunk's stats and skip the entire chunk if it cannot
possibly match a filter. Row group size is the tuning knob: bigger groups
compress better and amortize metadata, smaller groups prune more finely.

## What a column chunk records

The interesting content is on
[`ColumnMetadata`](../reference/file_metadata.md), reached through the chunk.
It is the map that turns "I want this column" into precise byte ranges:

- **Identity** — `path_in_schema` (the dotted path back to a
  [schema leaf](schema-tree.md)) and the physical `type`.
- **Encoding & compression** — the `encodings` used and the `codec`, so a reader
  knows how to decode pages before touching them.
- **Sizes & counts** — `num_values`, `total_uncompressed_size`,
  `total_compressed_size`.
- **Offsets** — `data_page_offset`, and where present `dictionary_page_offset`
  and `index_page_offset`. These are the byte positions the physical parser
  seeks to. (The true start of a chunk is the *minimum* of the data-page and
  dictionary-page offsets, because a dictionary page, when present, comes
  first.)
- **Statistics & indexes** — `statistics` (chunk-level min/max/null counts),
  plus offsets to the optional column index and offset index, and to a bloom
  filter. These power the pruning story in
  [Statistics & pruning](statistics-and-pruning.md).
- **Extras** — `size_statistics` and `geospatial_statistics` for the types that
  carry them.

Notice what is *not* here: the values. The metadata describes and locates the
data without containing it. That separation is the point — you can load every
[`RowGroup`](../reference/file_metadata.md) and `ColumnChunk` in a multi-gigabyte
file, understand its entire structure, and decide exactly which byte ranges to
fetch, having read only the footer.

Inside each chunk, the values are stored in **pages** — the next page opens them
up.

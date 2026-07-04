# Staged access

Reading a Parquet file is not one operation, it is a **staircase**. Each step
reads a little more of the file and tells you enough to decide whether the next
step is worth taking. Spending reads in this order — cheapest and most
discriminating first — is how you turn a multi-gigabyte remote file into a few
targeted range requests.

```
bounds  ──►  metadata  ──►  structure  ──►  data
 size        footer         page layout     values
 (HEAD)      (~KB)          (selected)       (selected)
 cheapest ───────────────────────────────► most expensive
```

## Stage 1 — bounds

Before anything, you often just need the file's **size**. Over HTTP that is a
`HEAD` request (or a `Content-Length`) and nothing more:

```python
async with AsyncHttpFile(url) as f:
    total_bytes = await f.size()
```

Por Qué itself starts here — `ParquetFile.from_reader` seeks to the end to
establish the file size and rejects anything smaller than 12 bytes as too small
to be valid Parquet. On its own, the size is enough for accounting, sanity
checks, and deciding whether a file is worth opening at all. Cost: effectively
one tiny request.

## Stage 2 — metadata

Next, the footer. [`FileMetadata.from_reader`](../reference/file_metadata.md)
reads the trailing length + magic and the metadata byte range, and returns the
complete logical picture: schema, row groups, every column chunk's offsets and
**statistics**:

```python
meta = await FileMetadata.from_reader(f)
```

This is where [pruning](../learn/statistics-and-pruning.md) decisions get made.
With the metadata alone — a few kilobytes, no data pages — you can check
file-level key-value metadata, compare row-group min/max against a filter, and
decide *which* row groups and columns you will actually read. Most "understand
this file" work never leaves this stage.

## Stage 3 — structure

Once you know what you want, read the **page layout** for just those parts.
[`ParquetFile.from_reader`](../reference/physical.md) with `columns` and
`row_groups` materializes page structures only for the selected chunks:

```python
pf = await ParquetFile.from_reader(
    f, url,
    columns=["name", "geometry"],
    row_groups=[3, 7],
)
```

This fetches each selected chunk's page headers (using the
[offset index](../learn/statistics-and-pruning.md) to read them concurrently
when it can) but still not the values themselves. It costs one read per selected
chunk's header region — proportional to what you asked for, not the file size.
The [Fast metadata](fast-metadata.md) guide measures how much projection saves
here.

## Stage 4 — data

Finally, the values. With the structure in hand you can pull the actual bytes,
page by page. For a single column chunk,
[`parse_all_data_pages`](../reference/physical.md) decodes every data page in it
(reading the dictionary page once, then the data pages — concurrently, given
independent cursors):

```python
for chunk in pf.column_chunks:
    async for value, dl, rl in chunk.parse_all_data_pages(f):
        ...
```

Or, to reassemble whole nested records across all selected columns at once, use
[`read_all_data`](../reference/physical.md), which chains each column's chunks
and runs [reconstruction](../learn/reconstruction.md):

```python
records = await pf.read_all_data(f)   # {column_path: [values...]}
```

This is the expensive stage — it transfers and decompresses the page bodies you
selected — but by the time you reach it you have already discarded everything you
do not need. You pay for data, and only the data, you deliberately chose in the
stages above.

## Spending the staircase

The discipline is simple: **descend only as far as the task requires.**

- Cataloguing files? Stop at bounds or metadata.
- Filtering on statistics? Metadata is enough to decide, structure to locate.
- Actually reading values? Prune hard in stage 2 so stages 3 and 4 stay small.

Each step is a checkpoint where the cheap information you already have can save
you from the expensive information you were about to fetch. That is the whole art
of reading Parquet efficiently, remote or local.

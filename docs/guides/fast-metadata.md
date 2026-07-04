# Fast metadata

Parsing a Parquet file does not have to mean parsing all of it. The
[metadata-last](../learn/anatomy.md) layout means you can learn a file's entire
shape from its footer, and — when you do want page structures — materialize only
the columns and row groups you care about. This guide is about spending exactly
as much as a task needs.

## Metadata only

The leanest useful read is the metadata alone.
[`FileMetadata.from_reader`](../reference/file_metadata.md) reads the footer and
the metadata byte range and stops — no column chunks, no page headers, no data:

```python
from por_que import FileMetadata, AsyncHttpFile

async with AsyncHttpFile(url) as f:
    meta = await FileMetadata.from_reader(f)

print(meta.row_count, meta.column_count, meta.row_group_count)
for name in meta.schema_root.children:
    print(name)
```

This gives you the full logical picture — schema, row groups, every column
chunk's offsets and statistics — while transferring only a few kilobytes over
HTTP. For "what is in this file?" questions, this is all you need, and it is by
far the cheapest read Por Qué offers.

## Projecting the metadata itself

For *wide* files, even the metadata deserves projection. A file with dozens of
columns and many row groups describes thousands of column chunks, and building
a model for every one of them costs memory and time you may not need to spend.
`FileMetadata.from_reader` accepts a `columns` filter of full dotted
`path_in_schema` names:

```python
BBOX_COLS = ['bbox.xmin', 'bbox.ymin', 'bbox.xmax', 'bbox.ymax']
meta = await FileMetadata.from_reader(f, columns=BBOX_COLS)
```

The schema tree, key-value metadata, and row-group scalars still parse in full;
only the unselected column chunks are skipped, so each row group's
`column_chunks` dict contains just the columns you asked for. Names that match
nothing are ignored, and computed aggregates like `compression_stats` reflect
only the selected columns.

Measured on an Overture Maps buildings file with a 616 KB footer, 128 row
groups, and 42 columns (numbers are approximate and illustrative — measure your
own workload):

| Metadata read | Retained | Relative time |
| --- | --- | --- |
| Full (all 42 columns) | ~24 MB | baseline |
| Projected to 4 bbox columns | ~2.8 MB | ~25% faster |

End to end over HTTP, the projected metadata read of that file completes in
roughly 0.64 s. When you hold metadata for *hundreds* of such files at once —
filtering a partitioned dataset, say — the memory difference is what keeps the
process inside its budget.

## Projecting page structures

When you *do* need the physical page layout — to read data, or to inspect pages
— [`ParquetFile.from_reader`](../reference/physical.md) accepts `columns` and
`row_groups` filters. The metadata is still parsed in full (it is small and you
usually want all of it); the filters control only which column chunks' **page
structures** are read from the file:

```python
pf = await ParquetFile.from_reader(
    f, url,
    columns=["name", "geometry"],   # dotted path_in_schema names
    row_groups=[0, 1, 2],           # row group ordinals
)
```

Unselected columns and row groups are simply absent from `pf.column_chunks`.
Names that match nothing are ignored. This matters even more than metadata
projection: page structures require fetching each selected chunk's header
regions from the file, so every column you skip is both memory you never build
*and* byte ranges you never request.

## Many files: a process pool

Reading *one* file is I/O-bound — you are waiting on the network, and async
concurrency (independent cursors, concurrent range requests) already covers it.
Reading *thousands* of files and decoding their data is different: the decode
work is CPU-bound Python, and one process will bottleneck on the GIL.

Por Qué's parser holds no shared global state and operates on a plain reader, so
a whole file parses independently of any other. That makes per-file parsing a
natural unit of work for a `ProcessPoolExecutor`: fan the files out, let each
worker parse its own file, collect the results (a `ParquetFile` is a pydantic
model and serializes cleanly back across the process boundary).

```python
from concurrent.futures import ProcessPoolExecutor
import asyncio

def parse_one(url: str) -> dict:
    async def _run():
        async with AsyncHttpFile(url) as f:
            meta = await FileMetadata.from_reader(f)
            return {"url": url, "rows": meta.row_count}
    return asyncio.run(_run())   # each worker drives its own event loop

with ProcessPoolExecutor() as pool:
    results = list(pool.map(parse_one, urls))
```

A few honest caveats: this helps when the per-file work is real — decoding
data, or parsing *wide* footers (a 616 KB footer costs a few tenths of a second
of pure-Python decode, and `asyncio.gather` cannot parallelize that; hundreds
of such files serialize into minutes on one core). For small, narrow footers
the process startup and pickling overhead can cost more than it saves. Each worker opens its own HTTP
connections, so a shared block cache
([Reading remote files](reading-remote-files.md)) is what keeps them from
re-fetching overlapping bytes. Tune the worker count to your CPU, not your file
count. Treat this as a pattern to reach for when profiling says decode is the
bottleneck, not a default.

The [Staged access](staged-access.md) guide puts the metadata-only and projected
reads in sequence: bounds, then metadata, then structure, then data.

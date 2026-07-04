# Reading remote files

Everything in the [File anatomy](../learn/anatomy.md) page — footer at the end,
metadata that locates every byte range — was building to this: you can read a
Parquet file over HTTP without downloading it. Por Qué does this through
[`hctef`](https://github.com/jkeifer/hctef), a small library that makes a remote
URL behave like a seekable file using **range requests**.

## AsyncHttpFile

`AsyncHttpFile` is re-exported from the top-level package. Open it as an async
context manager and hand it to `from_reader` exactly like a local file:

```python
from por_que import AsyncHttpFile, ParquetFile

url = "https://example.com/data.parquet"

async with AsyncHttpFile(url) as f:
    pf = await ParquetFile.from_reader(f, url)
    print(pf.metadata.row_count, "rows")
    print(pf.metadata.column_count, "columns")
```

No data pages were downloaded here. Reading the structure only touched the
footer and the metadata byte range — a few kilobytes, regardless of how large
the file is.

## How range requests map to the format

Under the hood, every `seek` + `read` becomes an HTTP `Range` request for just
those bytes. That is what makes the format's design pay off: because the
metadata lives at the end and names exact offsets, the read pattern is

1. a range request near the end for the footer and metadata, then
2. targeted range requests for only the column chunks and pages you actually
   want.

`AsyncHttpFile` tunes this with a few constructor options:

- `prefetch_direction` defaults to `"END"` — it reads ahead from the *end* of
  the file, which is exactly where Parquet's footer is, so the first metadata
  read usually needs no extra round trip.
- `prefetch_bytes` (default 1 MiB) controls how much is pulled in around a read,
  amortizing round trips for nearby data.
- `minimum_range_request_bytes` sets a floor on request size so tiny reads do
  not each cost a full round trip.

```python
async with AsyncHttpFile(url, prefetch_bytes=4 * 1024 * 1024) as f:
    pf = await ParquetFile.from_reader(f, url, columns=["name", "geometry"])
```

`AsyncHttpFile` also supports `clone()` (an independent cursor sharing the same
session). Por Qué uses this to fetch independent byte ranges — page headers at
known offsets, or several column chunks — **concurrently** rather than one round
trip at a time, which is the same
[offset-index-driven planning](../learn/statistics-and-pruning.md) described in
the Learn section.

## Disk block cache

Repeatedly reading the same remote file — iterating in a notebook, reprocessing
the same object — re-fetches the same byte ranges every time. A block cache on
disk avoids that by persisting fetched blocks between reads and between runs.

!!! warning "Requires hctef with block-cache support"

    The environment variables below are read by hctef's on-disk block cache.
    That cache may not be present in the released version of hctef yet; check
    your installed hctef version before relying on them. Por Qué needs no code
    changes to benefit — the cache lives entirely inside the reader.

| Variable | Purpose |
| --- | --- |
| `HCTEF_CACHE_DIR` | Directory where cached blocks are stored. Setting it enables the cache. |
| `HCTEF_CACHE_MAX_BYTES` | Upper bound on total cache size; older blocks are evicted past it. |
| `HCTEF_CACHE_BLOCK_BYTES` | Granularity of caching — reads are aligned to and stored in blocks of this size. |
| `HCTEF_CACHE_IMMUTABLE` | Treat cached URLs as immutable (content never changes), so cached blocks are reused without revalidation. |

```bash
export HCTEF_CACHE_DIR=~/.cache/hctef
export HCTEF_CACHE_MAX_BYTES=$((512 * 1024 * 1024))
export HCTEF_CACHE_BLOCK_BYTES=$((1024 * 1024))
export HCTEF_CACHE_IMMUTABLE=1
```

With the cache warm, a second pass over the same file's metadata or the same
columns is served from disk instead of the network.

Next, the [Fast metadata](fast-metadata.md) guide measures what selective remote
reads actually cost, and [Staged access](staged-access.md) lays out when to
spend each read.

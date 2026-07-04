# Notebook authoring

A running document for whoever maintains `example-notebook.ipynb` (human or
agent). It captures what the notebook should do, why, and what to check when
por-que or its data sources change. Update this document whenever notebook
practices change.

The rendered notebook itself is served in the [Workshop](../workshop/index.md)
section; see [updating fixtures](updating-fixtures.md) for the related
test-data flow.

The notebook's job: teach parquet internals by progressively filtering the
Overture Maps buildings dataset (hundreds of ~0.5-1GB partition files) down to
a handful of rows — file bboxes → row-group statistics → pages → values —
inside a resource-constrained GitHub Codespace (2 cores, 4-8GB RAM) on a
workshop clock. Every practice below exists to protect either the clock or the
RAM.

## Data source

- Overture releases live under
  `s3://overturemaps-us-west-2/release/<version>/theme=buildings/type=building/`.
  **Releases get deleted.** The `2025-09-24.0` release referenced by an earlier
  notebook version no longer exists (verified 2026-07-04; `2026-06-17.0` is
  current). Before any workshop:
  1. Verify the pinned release still lists objects (S3 ListObjectsV2 with the
     prefix, or `s3fs.ls`).
  2. Prefer mirroring the two or three files the notebook fully reads (or at
     minimum their footers) to storage you control.
- Convert `s3://` listings to plain HTTPS URLs for `AsyncHttpFile` (the
  notebook has a cell doing this).

## Metadata loading (the expensive cell)

- Use projection — the filtering stages only need the bbox columns:

  ```python
  BBOX_COLS = ['bbox.xmin', 'bbox.ymin', 'bbox.xmax', 'bbox.ymax']
  fm = await FileMetadata.from_reader(f, columns=BBOX_COLS)
  ```

  Measured on a real 616KB Overture footer (128 row groups x 42 columns):
  full parse retains ~24MB per file; projected retains ~2.8MB and is ~25%
  faster. Across ~230 files that is the difference between ~5GB retained
  (kernel OOM in a Codespace — the historical "random crash") and ~640MB.
  Note: `key_value_metadata` (the `geo` bbox) and the schema always parse
  fully regardless of projection.

- Parsing is CPU-bound on the event loop (~0.3-0.4s/file on a fast laptop,
  slower in Codespaces); `asyncio.gather` parallelizes only the network. If
  the cell is still too slow, use a process pool — the parser core is
  synchronous (as of the `jak/sync-core` work), so worker processes can do the
  parse while the main process does async fetches:

  ```python
  import asyncio, pickle
  from concurrent.futures import ProcessPoolExecutor

  def parse_footer(meta_bytes: bytes, meta_start: int, filesize: int):
      # runs in a worker process; sync wrapper over an in-memory buffer
      from por_que.parsers.parquet.metadata import MetadataParser
      # (see FileMetadata.from_reader for the footer-slicing logic)
      ...

  # main process: async-fetch each footer span, then
  # await loop.run_in_executor(pool, parse_footer, ...)
  ```

  Pydantic models pickle fine across the process boundary. On a 2-core
  Codespace expect ~2x. Only reach for this if projection alone isn't enough.

## Crash recovery (protect the expensive cell's output)

Immediately after building the `fms` dict, persist it; guard the expensive
cell with a reload. A kernel crash then costs seconds, not a re-parse:

```python
import json
from pathlib import Path
from por_que import FileMetadata

FMS_CACHE = Path('fms.json')

# after the expensive cell:
FMS_CACHE.write_text(
    json.dumps({url: json.loads(fm.model_dump_json()) for url, fm in fms.items()}),
)

# guard cell (place BEFORE the expensive cell; skip it when cache exists):
if FMS_CACHE.exists():
    fms = {
        url: FileMetadata.model_validate(dump)
        for url, dump in json.loads(FMS_CACHE.read_text()).items()
    }
```

Deserialized models re-link their schema references automatically (the root
walker added on `jak/refs-walker`), so `converted_min_value` etc. work on
reloaded metadata. If the por-que serialization format changes,
`_meta.format_version` in dumps identifies stale caches — delete `fms.json`
on mismatch rather than migrating.

## hctef disk cache (once released with the block cache)

Two lines at the top of the notebook enable a persistent, size-bounded,
crash-surviving byte cache for ALL range requests:

```python
import os
os.environ['HCTEF_CACHE_DIR'] = '/workspaces/.hctef-cache'
os.environ['HCTEF_CACHE_MAX_BYTES'] = str(4 * 2**30)  # 4 GiB
```

- Re-running data-page cells after a crash then reads from disk, not S3.
- `HCTEF_CACHE_IMMUTABLE=1` is safe for versioned Overture release paths and
  skips header revalidation.
- Workshop ace: pre-seed the cache in the devcontainer prebuild (run the
  notebook once at image build time with these vars set) so every student's
  fetches short-circuit from the image — fast and independent of venue wifi.
- No data bytes are held in Python; the OS page cache is the memory tier.
  Memory pressure from hctef is no longer a concern.

## ParquetFile / data-page stages

- `ParquetFile.from_reader` scans page structure; prefer the selective
  parameters (`columns=`, `row_groups=`, added on `jak/lazy-structure`) so
  only the chunks the notebook actually reads get materialized.
- Keep full `ParquetFile` loads scoped to the few intersecting files, inside
  `async with` blocks, as the notebook already does.
- Known issue (notebook comment): reusing one `AsyncHttpFile` across
  `parse_all_data_pages` calls raised `ServerDisconnectedError`; the
  workaround opens a fresh file per row group. If hctef fixes connection
  reuse, simplify those cells and delete the comment.
- The `azip` helper cell is intentionally fragile (equal-length assumption)
  and says so; keep its disclaimer if it stays.

## Checklist when por-que changes

1. Does the pinned por-que version still parse the pinned Overture release?
   (Run the notebook top to bottom in a fresh Codespace.)
2. Did `_meta.format_version` bump? Note it near the `fms.json` guard cell.
3. Did any API in the notebook's cells change signature? Grep the notebook
   for `from_reader`, `parse_all_data_pages`, `converted_`, `find_element`.
4. Do the printed outputs embedded in the committed notebook still match
   reality closely enough to teach from?
5. Update THIS document with anything learned.

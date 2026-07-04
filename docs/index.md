# Por Qué

*¿Por qué? ¿Por qué no?*

*Si, ¿pero por qué? ¡Porque, parquet, python!*

**Por Qué** is a pure-Python Apache Parquet parser built from scratch — not to
be fast, but to be *read*. It implements Parquet's binary format in highly
readable Python so you can see exactly how the format works, byte by byte.

!!! warning

    This is a project for education. It is **not** suitable for any production
    use.

## Why "Por Qué"?

Because asking "why" leads to understanding. The name is the pitch: this
project exists to answer *"why does Parquet work the way it does?"* by
implementing it from first principles. Every design choice in the format —
the footer at the end, the metadata-last layout, definition and repetition
levels, dictionary and RLE encodings — becomes obvious once you have written
the code that walks it. (And yes: *por qué*, *parquet*, *python*.)

## Teaching first

Most Parquet libraries optimize for throughput and hide the format behind
generated Thrift code and vectorized kernels. Por Qué does the opposite:

- **Explicit parsing logic** instead of generated Thrift stubs.
- **Docstrings written as lessons**, with "Teaching Points" that explain the
  *why* behind each structure — the [reference](reference/index.md) section
  surfaces them directly.
- **A structure that mirrors the file.** The code is a tangible map of the
  format: a `ParquetFile` holds `FileMetadata`, which holds row groups, which
  hold column chunks, which hold pages.
- **Readable errors** that explain the likely cause in terms of the format.

If you want to *understand* Parquet, start with the [Learn](learn/anatomy.md)
section. If you want to *use* Por Qué to inspect real files, see the
[Guides](guides/reading-remote-files.md) and the
[API reference](reference/index.md).

## A quick tour

```python
from por_que import ParquetFile

with open("data.parquet", "rb") as f:
    pf = ParquetFile.from_reader(f, "data.parquet")

    print(f"Rows:       {pf.metadata.metadata.row_count}")
    print(f"Columns:    {pf.metadata.metadata.column_count}")
    print(f"Row groups: {pf.metadata.metadata.row_group_count}")

    for chunk in pf.column_chunks:
        print(chunk.path_in_schema, chunk.codec, chunk.num_values)
```

Por Qué reads from local files, in-memory buffers, and — via
[`hctef`](https://github.com/jkeifer/hctef) — remote files over HTTP using
range requests. It can also serialize the parsed structure to JSON.

## See it in a browser: ver-por-que

Por Qué can export the structure it parses to JSON:

```python
json_output = pf.to_json(indent=2)
```

Those exports feed [**ver-por-que**](https://teotl.dev/ver-por-que), an
experimental, 100% client-side web UI that visualizes a Parquet file's
structure. The JSON layout it consumes is a contract; the
[serialization guide](guides/serialization.md) documents it.

## Where to go next

- **[Learn](learn/anatomy.md)** — the teaching content: how Parquet is laid
  out, why the parsers look the way they do, and how records are reconstructed.
- **[Workshop](workshop/index.md)** — a hands-on notebook that filters a large
  Overture Maps dataset down to a handful of rows.
- **[Reference](reference/index.md)** — API documentation generated from the
  source.
- **[Project](project/architecture.md)** — architecture notes, contributor
  guides, and the roadmap.

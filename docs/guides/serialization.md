# The serialization contract

Por Qué can export the entire parsed structure of a file to JSON, and read it
back. That JSON is a **contract**: the [ver-por-que](https://teotl.dev/ver-por-que)
web app — and any other external consumer — depends on its shape. This page
documents that shape and, most importantly, how the JSON stays self-contained:
every reference in the object graph can be rebuilt from the JSON alone.

## Producing and consuming JSON

A [`ParquetFile`](../reference/physical.md) is a pydantic model, so serialization
is just model serialization:

```python
pf = await ParquetFile.from_reader(f, url)

text  = pf.to_json(indent=2)      # str  — uses by_alias=True
data  = pf.to_dict()              # dict — model_dump()

restored = ParquetFile.from_json(text)
restored = ParquetFile.from_dict(data)
```

`from_json` / `from_dict` reconstruct the full object graph, including the
internal references described below — the round trip is lossless. Bytes fields
(raw statistics, min/max values) are base64-encoded in JSON.

## `_meta` and the format version

Every export carries a `_meta` object at the top level:

```json
{
  "_meta": { "format_version": 1, "por_que_version": "…" },
  "source": "…",
  "filesize": 123456,
  "metadata": { "...": "..." },
  "column_chunks": [ "..." ]
}
```

`_meta.format_version` is the number a consumer should branch on. **It is
currently `1`.** Format 1 means:

- Reference keys are carried as **`schema_path`** strings (see below).
- There is **no nested `PhysicalMetadata` layer** — the physical file model
  (`ParquetFile`) holds its `metadata` (a [`FileMetadata`](../reference/file_metadata.md))
  directly, not wrapped in an intermediate object.

Treat the version as the compatibility gate: if it is a value your consumer does
not recognize, do not assume the layout below. (Note that `_meta` is the JSON
key produced by `to_json`, which serializes by alias; the Python field is
`meta_info`.)

## Reference normalization

The parsed structure is a graph, not a tree. Statistics, page indexes, and data
pages all need to know *which column* they describe — their
[schema leaf](../learn/schema-tree.md) — and column chunks need their logical
metadata. If those links were serialized as embedded objects, the same schema
leaf would be duplicated dozens of times and the JSON could disagree with
itself.

Por Qué avoids that with **reference normalization**. There is exactly one
authoritative copy of the schema — `metadata.schema_root` — and everything that
needs a schema leaf refers to it *by name* rather than by embedding it.

### `schema_path` string keys

Models that reference a column mix in
[`SchemaLinked`](../reference/file_metadata.md), which serializes the reference
as a single string field, **`schema_path`** — the leaf's full dotted path
(`addresses.element.city`). This applies to column statistics, the column index,
and every data page; column metadata exposes the same key through its existing
`path_in_schema`. The resolved leaf object itself is held in a *private*
attribute and is **not serialized**.

### Object references are excluded, then re-linked on load

The same principle covers the other back-reference in the graph: each physical
column chunk's link to its logical `ColumnChunk` metadata is marked
`exclude=True` and left out of the JSON entirely.

On load, pydantic root validators put every reference back:

- Resolving each `schema_path` against `metadata.schema_root` with
  [`find_element`](../reference/file_metadata.md) and calling `link()` (which
  validates that the leaf's `full_path` matches the stored `schema_path`).
- Re-attaching each column chunk's logical metadata by matching its `row_group`
  ordinal and `path_in_schema`.

This all happens automatically inside `from_json` / `from_dict`; you never call
`link()` yourself.

## What this guarantees consumers

The contract for anyone reading the JSON — ver-por-que or otherwise — is:

- **The JSON is self-contained.** Every reference is expressed as data
  (`schema_path` strings, `row_group` + `path_in_schema` pairs) that resolves
  against `metadata.schema_root`, itself fully present in the same document. No
  external lookups are required to rebuild the object graph.
- **There is one source of truth for the schema.** `metadata.schema_root` is the
  only place a schema leaf is defined; all references point at it by path, so the
  document cannot contradict itself.
- **`_meta.format_version` gates compatibility.** Read it first; everything else
  in this page describes version `1`.

A consumer that walks `schema_root`, then resolves each `schema_path` and each
`(row_group, path_in_schema)` pair against it, can reconstruct exactly the graph
Por Qué holds in memory — which is precisely what ver-por-que does to render a
file's structure in the browser.

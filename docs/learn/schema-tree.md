# The schema tree

Parquet stores tables, but real data is rarely a flat table — it has structs,
lists, maps, nullable fields, lists of structs of lists. Parquet represents all
of that with a single idea: the schema is a **tree**, and only its *leaves* hold
actual columns of values. Everything else is structure.

## Elements: root, group, leaf

The footer encodes the schema as a flat list of elements, which Por Qué
reassembles into a tree of three shapes (all subclasses of
[`SchemaElement`](../reference/file_metadata.md)):

- **[`SchemaRoot`](../reference/file_metadata.md)** — the single top node. It has
  no type and no repetition; it exists only to hold the top-level fields.
- **[`SchemaGroup`](../reference/file_metadata.md)** — an interior node: a
  struct, or the machinery of a list or map. It has children but no physical
  values of its own.
- **[`SchemaLeaf`](../reference/file_metadata.md)** — a terminal column. Every
  leaf, and *only* a leaf, corresponds to a stored column chunk of values.

```
root
├── id            leaf   (INT64, REQUIRED)
├── name          leaf   (BYTE_ARRAY/UTF8, OPTIONAL)
└── addresses     group  (LIST, REPEATED)
    └── element   group  (struct)
        ├── city  leaf   (BYTE_ARRAY/UTF8, OPTIONAL)
        └── zip   leaf   (BYTE_ARRAY/UTF8, OPTIONAL)
```

A column's identity is its **path** from the root — `addresses.element.city` —
and that dotted string is how everything downstream refers back to its schema:
[`find_element`](../reference/file_metadata.md) walks the path to resolve a leaf,
and the [serialization contract](../guides/serialization.md) uses the same path
as a reference key.

## Repetition, and the two levels it creates

Each element carries a **repetition**: `REQUIRED` (exactly one), `OPTIONAL`
(zero or one — i.e. nullable), or `REPEATED` (zero or more — a list). This one
attribute is how a *flat* column of values can encode arbitrarily *nested*,
*nullable* data.

The trick is two small integers stored alongside each value:

- **Definition level** — how many of the optional/repeated ancestors on this
  column's path are actually *present*. If a value's definition level is lower
  than the maximum for its path, the value is null (or an empty list) somewhere
  up the chain, and no data byte is stored.
- **Repetition level** — at what depth a new value *continues* a repeated field
  versus *starts* a new one. Repetition level 0 always begins a new top-level
  record.

Por Qué computes the maximum definition and repetition level for every element
during tree construction and stores them on the element
([`definition_level`, `repetition_level`](../reference/file_metadata.md)). Those
maxima are exactly what [Record reconstruction](reconstruction.md) needs to turn
a flat stream of `(value, definition_level, repetition_level)` tuples back into
nested records — this levels machinery is the crux of that page.

## Three kinds of "type"

A single leaf can describe its values three ways, and it helps to keep them
straight:

- **Physical type** — how the bytes are actually stored on disk: `BOOLEAN`,
  `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`,
  `FIXED_LEN_BYTE_ARRAY`. This is all the page decoder needs.
- **Converted type** — the *legacy* annotation that layers meaning on top of a
  physical type: a `BYTE_ARRAY` that is really `UTF8` text, an `INT32` that is
  really a `DATE`, and so on.
- **Logical type** — the *modern* replacement for converted types, richer and
  parameterized (a `DECIMAL` with its scale and precision, a `TIMESTAMP` with its
  unit and time zone). New writers emit logical types; older files only have
  converted types.

Por Qué unifies the two: [`get_logical_type`](../reference/file_metadata.md)
returns the logical type if present, otherwise translates the converted type
into its logical equivalent, so downstream code only ever reasons about one
scheme. A leaf then knows how to take raw bytes to a Python value in two steps —
[`bytes_to_physical_type`](../reference/file_metadata.md) then
[`physical_to_logical_type`](../reference/file_metadata.md) — the same path the
statistics use to present human-readable min/max values.

With the schema understood, the next question is *where the values live*: row
groups and the column chunks inside them.

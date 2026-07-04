# Record reconstruction

Parquet took your nested records apart, column by column, and flattened each
leaf into a stream of values. Reading them back means running that process in
reverse: reassembling separate columns into whole records, restoring nulls and
list boundaries that were never stored as data. This is the payoff for the
[definition and repetition levels](schema-tree.md) — and it is the most subtle
part of the format.

## The problem

A single column arrives as a flat stream of tuples: `(value, definition_level,
repetition_level)`. Nothing in that stream directly says "this is where a list
ends" or "this field was null." That information is *encoded in the levels*, and
reconstruction is the act of decoding it back into structure.

Consider `addresses.element.city` from the [schema tree](schema-tree.md). Given
a record with two addresses and one with none, the flat stream might look like:

```
(value="NYC", dl=2, rl=0)   # new record, first address present
(value="LA",  dl=2, rl=1)   # same record, list continues (rl=1)
(value=None,  dl=1, rl=0)   # new record, empty list (dl below max)
```

- **Repetition level** disambiguates *new record* from *continue the list*: `rl=0`
  starts a new top-level record; a higher `rl` continues a repeated field at
  that depth.
- **Definition level** below the path's maximum means something on the path was
  absent — a null value, or an empty list — so no data value was stored.

## How Por Qué reassembles it

The entry point is
[`reconstruct`](../reference/physical.md) (in `structuring/`), driven by
`ParquetFile.read_all_data`. Because a column can be split across several row
groups, the physical layer first **chains** each column's per-chunk streams into
one continuous stream per column path, then hands `reconstruct` a dictionary of
`{path: async iterator of tuples}` together with the schema root.

From there the design mirrors the schema itself: an **assembler** per element,
built by a small factory. The shape of the schema element picks the assembler —
a struct assembler for groups, a list assembler for repeated fields, a map
assembler for map groups, and a primitive assembler for leaves. Each assembler
knows how to consume exactly as many tuples as belong to it and hand a finished
sub-value up to its parent. The root is always a struct assembler, and it is
asked for one record at a time until the streams run dry.

The streams are wrapped as **peekable** async iterators. That is the key
mechanism: an assembler often has to look at the *next* tuple's repetition level
to decide whether the current list continues or a new record begins — but
without consuming it if it belongs to the parent. Peeking lets each assembler
make that boundary decision locally, using only the levels, without any
assembler needing to know the global position.

```
schema tree            assemblers            output
  root      ─────────► struct    ─────────►  {id, name, addresses: [...]}
  ├ id                 primitive
  ├ name               primitive
  └ addresses          list  ──► struct  ──► [{city, zip}, ...]
```

The result is transposed back into a column-oriented dictionary of lists — the
shape callers and tests expect — but conceptually you have rebuilt whole nested
records from independent columns, using nothing but the levels the writer
recorded.

## Why it is done this way

The compositional, one-assembler-per-element design means nesting depth is not a
special case: a list of structs of lists is just assemblers wired to match the
schema tree, each handling one level. And because the whole thing is streaming —
peekable async iterators, one record produced at a time — reconstruction never
materializes an entire column in memory to produce output. It reads exactly as
far ahead as the levels require.

If the levels are where nesting is *stored*, reconstruction is where nesting is
*restored*. With it, the round trip is complete: bytes to metadata to schema to
pages to values to records.

Where to go from here is up to your goals — the [Going deeper](going-deeper.md)
page points past the workshop, and the [Guides](../guides/staged-access.md) turn
all of this into practical reading strategies.

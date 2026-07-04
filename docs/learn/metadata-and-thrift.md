# Metadata & Thrift

The footer we found in [File anatomy](anatomy.md) is not JSON, or protobuf, or
anything you can read with your eyes. It is an Apache **Thrift** structure
serialized with the *compact protocol* — a binary encoding tuned to make small
integers and sparse structs cost as few bytes as possible. Understanding a
handful of its tricks is enough to decode the whole thing, and Por Qué's
[`ThriftCompactParser`](../reference/parsers.md) implements each one in a few
lines you can read.

## Varints: small numbers are cheap

Most numbers in Parquet metadata are small — field IDs, page counts, repetition
levels. A fixed 4- or 8-byte integer would waste space on all of them, so Thrift
uses a **variable-length integer**. Each byte carries 7 bits of value plus a
continuation bit; if the top bit is set, another byte follows.

```
0x05        → 0000_0101 → continue bit clear → value 5   (1 byte)
0xAC 0x02   → more…     → value 300                       (2 bytes)
```

The number 5 costs one byte; 300 costs two. See
[`read_varint`](../reference/parsers.md).

## Zigzag: making negatives small too

Varints are only efficient for small *positive* numbers — a naive `-1` would set
every high bit and blow out to ten bytes. **Zigzag** encoding fixes this by
mapping signed integers onto unsigned ones so that numbers near zero, positive
or negative, all stay small:

```
 0 → 0    -1 → 1    1 → 2    -2 → 3    2 → 4 …
```

The decode is a single expression, `(n >> 1) ^ -(n & 1)`, in
[`read_zigzag`](../reference/parsers.md).

## Field headers: deltas instead of IDs

A Thrift struct is a sequence of fields, each tagged with its numeric ID and
type. Rather than store the full ID every time, the compact protocol stores the
**delta** from the previous field's ID, packed into the high nibble of a single
header byte (the low nibble holds the type). Because fields are written in
ascending ID order, those deltas are almost always 1, so a whole field header
fits in one byte. A `0x00` byte is the STOP marker that ends the struct.
[`ThriftStructParser`](../reference/parsers.md) tracks `last_field_id` to undo
the delta as it reads.

This is why unknown fields are safe to skip: the header tells you the type, and
the type tells you how many bytes to jump. Parsers built this way tolerate
newer, richer files gracefully — a forward-compatibility property the format
depends on.

## Why the parser looks the way it does

The interesting design decision in Por Qué is not the bit-twiddling — it is the
boundary between *fetching bytes* and *decoding them*. The thrift parser is
**fully synchronous**: it walks an in-memory buffer (a `memoryview` plus a
position that reports absolute file offsets) and never performs I/O at all.
There is not a single `await` in the decoding core.

The async code lives one level up, at the entry points
(`FileMetadata.from_reader` and friends): they decide *which byte span* to
fetch — the footer tells you the metadata's exact range; a column chunk records
its index offsets and lengths — await one read for that whole span, and hand
the bytes to the parser. When a structure's size isn't knowable up front (a
page header, for instance), the entry point fetches a speculative span and
grows it if the parser runs off the end.

That split separates two concerns that are easy to tangle:

- **I/O planning** — *where* and *how much* to read. Over a local file this is
  nearly free; over HTTP each read is a round trip, so the goal is to fetch
  known byte ranges whole and, when possible, fetch independent ranges
  concurrently. This is the part that deserves `async`.
- **Decoding** — turning those bytes into varints, zigzags, and structs. This
  is pure computation; making it async would add ceremony to millions of tiny
  operations without ever having anything to wait for.

Because decoding never touches I/O, the same parser core runs unchanged whether
the span came from a local file, an HTTP range request, or a test's in-memory
bytes — and the decoding code reads like what it is: a plain loop over bytes.

You will see this planning-versus-decoding split pay off directly in
[Statistics & pruning](statistics-and-pruning.md), where an offset index lets
the parser fetch every page header at a known location at once instead of
walking them one dependent read at a time.

Next: the schema tree the metadata describes, and the levels that make nested
data possible.

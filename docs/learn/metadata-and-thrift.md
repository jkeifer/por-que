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
boundary between *fetching bytes* and *decoding them*. The parser reads through
an async `read()`, but everything else is synchronous:
[`seek` and `tell`](../reference/protocols.md) require no I/O, so they are plain
methods. The **only** place the parser awaits is the byte fetch itself.

That split matters because it separates two concerns that are easy to tangle:

- **I/O planning** — *where* and *how much* to read. Over a local file this is
  free; over HTTP each read is a round trip, so the goal is to read known byte
  ranges and, when possible, read independent ranges concurrently.
- **Decoding** — turning those bytes into varints, zigzags, and structs. This is
  pure computation and never touches the network.

Because decoding never blocks on I/O, the same parser core runs unchanged over a
local file, an in-memory buffer, or a remote HTTP reader — the reader is just an
object that answers `read`. Local files are adapted to the async interface by a
thin wrapper ([`ensure_async_reader`](../reference/protocols.md)) whose `read`
returns immediately, so you pay nothing for the abstraction when the bytes are
already on disk.

You will see this planning-versus-decoding split pay off directly in
[Statistics & pruning](statistics-and-pruning.md), where an offset index lets
the parser fetch every page header at a known location at once instead of
walking them one dependent read at a time.

Next: the schema tree the metadata describes, and the levels that make nested
data possible.

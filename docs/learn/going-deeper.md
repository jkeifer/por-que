# Going deeper

The [workshop](../workshop/index.md) and the rest of the Learn section cover the
spine of the format: anatomy, Thrift, the schema tree, row groups, pages,
statistics, and reconstruction. That is enough to read real files and understand
why they are shaped the way they are. Parquet has more, though, and this page
points at the parts beyond the guided path — some that Por Qué does not yet
implement, and the authoritative sources for everything.

## Bloom filters

The [pruning story](statistics-and-pruning.md) skips data using ranges: if a
value falls outside a chunk's min/max, it cannot be there. But range pruning is
useless for the common case of an *equality* filter on high-cardinality data —
`user_id = 4837291` sits inside almost every chunk's min/max, so ranges cannot
rule anything out. A **bloom filter** is a compact probabilistic structure that
answers "is this exact value definitely absent?" — never a false negative, only
occasional false positives — letting a reader skip chunks that provably do not
contain a wanted value even when their ranges overlap it. Parquet stores one per
column chunk when the writer opts in.

Por Qué already reads the *pointers* to them — a column chunk exposes
[`bloom_filter_offset` and `bloom_filter_length`](../reference/file_metadata.md)
straight from the metadata — but **parsing the filter blocks themselves is
planned, not yet implemented**. When it lands it will slot in as a fourth,
value-level pruning stage alongside the three on the statistics page.

## Encryption

Parquet supports **modular encryption**: individual columns, or the metadata
itself, can be encrypted so that a reader without the key can still process the
columns it is permitted to see. This changes the read path — footers and pages
carry encryption metadata and the bytes must be decrypted before the Thrift and
page decoders run. Por Qué does not implement encryption; it is out of scope for
a teaching parser, but worth knowing exists when you meet a file it cannot open.

## External references

When you want the ground truth rather than a narrative, go to the source:

- **[Parquet format specification](https://github.com/apache/parquet-format)** —
  the canonical `parquet.thrift` definition and the format documentation. Every
  structure Por Qué parses is defined there; reading `parquet.thrift` alongside
  Por Qué's models is a good way to check your understanding.
- **[parquet-testing](https://github.com/apache/parquet-testing)** — the
  community corpus of real and deliberately-weird Parquet files. Por Qué's test
  suite pins a commit of this repository and parses its files as fixtures; see
  [Updating fixtures](../project/updating-fixtures.md) for how that pin is
  managed.
- **[Dremel paper](https://research.google/pubs/pub36632/)** — the original
  Google paper that introduced the definition/repetition level model Parquet
  uses for nested data. If [record reconstruction](reconstruction.md) left you
  wanting the theory, this is it.
- **[hctef](https://github.com/jkeifer/hctef)** — the HTTP range-request file
  reader Por Qué uses for remote files, covered in
  [Reading remote files](../guides/reading-remote-files.md).

And, of course, [the source itself](../reference/index.md) — this is a teaching
library, so the code is written to be read.

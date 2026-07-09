# Public API for ver-por-que's internal couplings — design

Date: 2026-07-09
Target: por-que v0.5.0
Source: `por-que-internal-api.md` (worker wishlist, pinned wheel 0.4.1)

## Goal

Give the ver-por-que worker a stable public surface for the four things it
currently reaches internals for: the bloom-probe derivation, dictionary-page
decoding, physical values from a data-page decode, and codec-unavailable
detection. por-que work only; the worker migration happens after release.

## 1. Bloom probe — `BloomFilter.explain(value)`

New public method on `BloomFilter` (`src/por_que/statistics.py`) returning:

```python
class BloomBitCheck(NamedTuple):
    word_index: int   # 0..7 within the block
    bit_index: int    # 0..31 within the word
    is_set: bool

class BloomProbe(NamedTuple):
    hash: int           # xxh64 of the PLAIN-encoded value
    block_index: int
    num_blocks: int
    block_bytes: bytes  # the selected 32-byte block
    bits: tuple[BloomBitCheck, ...]  # the 8 salt-derived checks
    might_contain: bool
```

`explain(value)` performs the full derivation (PLAIN-encode → xxh64 → block
select → 8 salted bit checks) and returns all intermediates. `might_contain()`
is reimplemented as `self.explain(value).might_contain`, so there is exactly
one derivation code path. `_block_check` folds into `explain`; `_plain_encode`
stays private (subsumed).

`bitset` and `num_blocks` are declared stable public API in their
docstrings/class docs. This legitimizes the worker's density, block-window,
and prefetch reads without adding accessors.

## 2. Dictionary — `chunk.parse_dictionary(reader, apply_logical_types=True)`

Public async method on the physical column chunk (`src/por_que/physical.py`),
mirroring `parse_data_page`'s ergonomics:

- Accepts `ReadableSeekable | AsyncReadableSeekable`; adapts via
  `ensure_async_reader` internally.
- Returns the distinct dictionary values: logically converted via
  `schema_element.physical_to_logical_type` by default, raw physical when
  `apply_logical_types=False`.
- Returns `[]` when there is no dictionary page (current behavior).
- `_parse_dictionary` becomes its internal core (callers inside
  `parse_data_page` / `parse_all_data_pages` keep getting physical values for
  decoding).

## 3. Page values — `PageValue` NamedTuple carrying physical

> Revised post-review: the original single `value` field aliased physical and
> logical, which loses provenance. Provenance now travels with the value
> itself, as fields, because the consumer of a stream is not always the code
> that chose the conversion flag.

`ValueTuple` (bare `tuple[Any | None, int, int]` in
`src/por_que/parsers/page_content/data.py`) is replaced by:

```python
class PageValue(NamedTuple):
    logical: Any | None           # populated iff conversion ran this entry
    definition_level: int
    repetition_level: int
    physical: Any | None = None   # raw pre-conversion value

    @property
    def value(self) -> Any | None:
        return self.logical if self.logical is not None else self.physical
```

- `physical` is always populated on real parses — it is the decoder's raw
  output, one extra reference per tuple. `None` only for null entries.
- `logical` is populated iff logical-type conversion actually ran for that
  entry: `None` for nulls, and also `None` when conversion is off or the
  column is excluded. It does **not** alias `physical` — the two fields
  independently record what the decoder produced and what conversion (if
  any) produced from it.
- `value` is a convenience property, not a stored field: `logical` when set,
  else `physical`. This is what most consumers want and keeps existing
  `.value` access working unchanged.
- All internal consumers migrate to named access (notably
  `structuring/primitive.py` which does positional `value, dl, _ = ...`
  unpacking), so tuple arity never matters again.
- `apply_logical_types` and `excluded_logical_columns` are keyword-only on
  `chunk.parse_data_page`, `chunk.parse_all_data_pages`,
  `ParquetFile.read_all_data`, `chunk.parse_dictionary`, and the `pages.py`
  `parse_content` entry points, so a 0.4.x caller passing
  `excluded_logical_columns` positionally gets a loud `TypeError` instead of
  silently binding it to `apply_logical_types`.
- `excluded_logical_columns` **stays**: the original "removed, not deprecated"
  call assumed the worker's repurposing was its only consumer, but the test
  suite uses it for its designed purpose (per-column exclusions for
  `nested_structs.rust` / `int96_from_spark` fixture comparisons, which a
  global flag cannot express). The worker simply stops needing it once
  `physical` is always present.

## 4. Codec errors — `CodecUnavailableError`

```python
class CodecUnavailableError(ParquetDataError):
    def __init__(self, message: str, codec: str) -> None: ...
    codec: str
```

Added to `src/por_que/exceptions.py`; raised by
`parsers/page_content/compressors.py` for the missing-optional-package cases
(brotli, lzo, zstandard). Messages unchanged. Consumers catch the class and
read `.codec` instead of sniffing `'requires' in str(e)`.

## Testing

- `explain()` agrees with `might_contain()` on known-hit and known-miss values
  against the existing bloom fixtures; intermediates sanity-checked
  (`block_index < num_blocks`, `len(block_bytes) == 32`, 8 bit checks,
  `might_contain == all(b.is_set for b in bits)`).
- `parse_dictionary` returns logical values by default, physical with
  `apply_logical_types=False`, `[]` for chunks without a dictionary page, and
  accepts a sync reader.
- `PageValue`: named access; `logical` is populated (and `value` derives from
  it) on a logically-typed column with conversion on, `logical` is `None`
  (and `value` falls back to `physical`) with `apply_logical_types=False`;
  the existing reconstruction/structure suite stays green (regression net
  for the tuple migration).
- `CodecUnavailableError` is raised for a missing codec package and carries
  `.codec`; still catchable as `ParquetDataError`.

## Out of scope

- ver-por-que worker migration (separate effort after the 0.5.0 release).
- hctef wishlist items (`hctef-api-wishlist.md`).
- `block_bytes()` / `block_popcounts()` accessors — blessed `bitset` covers
  those reads.

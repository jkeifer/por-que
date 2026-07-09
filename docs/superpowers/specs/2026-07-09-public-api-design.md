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

`ValueTuple` (bare `tuple[Any | None, int, int]` in
`src/por_que/parsers/page_content/data.py`) is replaced by:

```python
class PageValue(NamedTuple):
    value: Any | None            # logical (or == physical when conversion off)
    definition_level: int
    repetition_level: int
    physical: Any | None         # raw pre-conversion value
```

- `physical` is always populated — it is the decoder's input, one extra
  reference per tuple. No `include_physical` flag.
- All internal consumers migrate to named access (notably
  `structuring/primitive.py` which does positional `value, dl, _ = ...`
  unpacking), so tuple arity never matters again.
- `apply_logical_types: bool = True` is plumbed through
  `chunk.parse_data_page`, `chunk.parse_all_data_pages`, and the `pages.py`
  entry points — the name already exists at the page-content layer.
- `excluded_logical_columns` is **removed** (not deprecated): pre-1.0, and its
  only known external consumer was the worker's repurposing, which the
  always-present `physical` field retires.

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
- `PageValue`: named access; `physical` differs from `value` on a
  logically-typed column and equals it with `apply_logical_types=False`; the
  existing reconstruction/structure suite stays green (regression net for the
  tuple migration).
- `CodecUnavailableError` is raised for a missing codec package and carries
  `.codec`; still catchable as `ParquetDataError`.

## Out of scope

- ver-por-que worker migration (separate effort after the 0.5.0 release).
- hctef wishlist items (`hctef-api-wishlist.md`).
- `block_bytes()` / `block_popcounts()` accessors — blessed `bitset` covers
  those reads.
- Deprecation shims for `excluded_logical_columns`.

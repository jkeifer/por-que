# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added

### Changed

### Fixed

### Deprecated

### Removed

### Security

## [v0.5.0] - 2026-07-09

### Added

- `BloomFilter.explain(value)` returning a `BloomProbe` with the full
  split-block lookup derivation: hash, selected block index and bytes, the
  eight salt-derived bit checks, and the `might_contain` verdict.
- `BloomFilter.bitset` and `BloomFilter.num_blocks` are now documented stable
  public API.
- Public `chunk.parse_dictionary(reader, apply_logical_types=True)` for
  decoding a column chunk's dictionary page; accepts sync or async readers
  and returns `[]` when the chunk has no dictionary page.
- Data-page decoding yields `PageValue` named tuples with separate `logical`
  and `physical` fields: `physical` always carries the decoder's raw value
  (`None` for nulls), `logical` is populated iff logical-type conversion ran
  for that entry, and the `.value` property returns `logical` when set, else
  `physical`. Provenance travels with the value, since a stream's consumer
  doesn't always know which conversion flag produced it.
- `apply_logical_types: bool = True` on `chunk.parse_data_page`,
  `chunk.parse_all_data_pages`, and `ParquetFile.read_all_data` to disable
  logical-type conversion for all columns.
- `CodecUnavailableError` (subclass of `ParquetDataError`) with a `.codec`
  attribute, raised when a compression codec's optional package (brotli,
  python-lzo, zstandard) is not installed — catch the class instead of
  parsing the message.

### Changed

- **Breaking:** data-page iterators (`parse_data_page`,
  `parse_all_data_pages`, `read_all_data(reconstruct=False)`) yield 4-field
  `PageValue` named tuples instead of 3-tuples. Positional
  `value, dl, rl = ...` unpacking breaks; use the named fields (`.value`,
  `.definition_level`, `.repetition_level`, `.physical`).
- **Breaking:** `ValueTuple` (the `por_que.parsers.page_content` type alias)
  is replaced by `PageValue`.
- **Breaking:** `apply_logical_types` and `excluded_logical_columns` (and the
  matching optional decode parameters on `parse_content`/`parse_data_page`)
  are now keyword-only across the public decode API. A 0.4.x caller passing
  `excluded_logical_columns` positionally now raises `TypeError` instead of
  silently binding it to `apply_logical_types`.
- `BloomFilter.might_contain()` now delegates to `explain()`; behavior is
  unchanged.

## [v0.4.1] - 2026-07-08

First tracked version!

[unreleased]: https://github.com/jkeifer/por-que/compare/v0.5.0...HEAD
[v0.5.0]: https://github.com/jkeifer/por-que/releases/tag/v0.5.0
[v0.4.1]: https://github.com/jkeifer/por-que/releases/tag/v0.4.1

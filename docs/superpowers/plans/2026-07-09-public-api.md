# por-que Public API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Public APIs for the four things the ver-por-que worker currently reaches por-que internals for: bloom-probe derivation, dictionary decoding, physical values from data-page decodes, and codec-unavailable detection.

**Architecture:** Four independent additions to existing modules — a typed exception, a `BloomFilter.explain()` method returning probe intermediates, a `PageValue` NamedTuple that always carries the raw physical value, and a public `parse_dictionary()` on the column chunk. No new files in `src/`.

**Tech Stack:** Python 3.12+ (uses `type` aliases, `match`), pydantic v2, pytest + pytest-asyncio. Tests fetch fixtures from apache/parquet-testing over HTTP via the `parquet_url` conftest fixture.

**Spec:** `docs/superpowers/specs/2026-07-09-public-api-design.md`

## Global Constraints

- Run tests with `uv run pytest <path> -v` from the repo root. The full suite is `uv run pytest tests/ -x -q` (network required; ~293 tests).
- Commits run lefthook (ruff check, ruff format, mypy). A commit that fails hooks is a failed step — fix and re-commit.
- `excluded_logical_columns` is NOT removed anywhere; it coexists with the new `apply_logical_types` parameter.
- Do not touch `example-notebook.ipynb`, `ver-por-que/`, or anything outside `src/por_que/` and `tests/`.
- Follow existing code style: single quotes, trailing commas, Google-style docstrings.

---

### Task 1: `CodecUnavailableError`

**Files:**
- Modify: `src/por_que/exceptions.py` (add class after `ParquetDataError`, ~line 31)
- Modify: `src/por_que/parsers/page_content/compressors.py:56-79` and `:175-182`
- Create: `tests/test_codec_unavailable.py`

**Interfaces:**
- Produces: `por_que.exceptions.CodecUnavailableError(message: str, codec: str)`, subclass of `ParquetDataError`, with `.codec` attribute holding the codec name (`'BROTLI'`, `'LZO'`, `'ZSTD'`).

Note: tests go in a NEW file, not `tests/test_compressors.py` — that module has a module-level `pytest.importorskip('snappy')` which would skip these tests when python-snappy is absent.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_codec_unavailable.py`:

```python
"""The missing-optional-codec condition is a typed error, not prose.

Consumers (the ver-por-que worker) distinguish "codec package not
installed" from real data errors. Before ``CodecUnavailableError`` they
sniffed the message text; these tests pin the typed contract instead.
"""

import sys

import pytest

from por_que.exceptions import CodecUnavailableError, ParquetDataError
from por_que.parsers.page_content import compressors

CODEC_GETTERS = [
    ('brotli', compressors.get_brotli, 'BROTLI'),
    ('lzo', compressors.get_lzo, 'LZO'),
    ('zstandard', compressors.get_zstd, 'ZSTD'),
]


@pytest.mark.parametrize(
    ('module_name', 'getter', 'codec_name'),
    CODEC_GETTERS,
    ids=[c[2] for c in CODEC_GETTERS],
)
def test_missing_codec_package_raises_typed_error(
    module_name: str,
    getter,
    codec_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # sys.modules[name] = None makes ``import name`` raise ImportError,
    # simulating the package being absent even when it is installed.
    monkeypatch.setitem(sys.modules, module_name, None)

    with pytest.raises(CodecUnavailableError) as excinfo:
        getter()

    assert excinfo.value.codec == codec_name
    # Existing consumers catch ParquetDataError; the new class must still be one.
    assert isinstance(excinfo.value, ParquetDataError)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_codec_unavailable.py -v`
Expected: FAIL with `ImportError: cannot import name 'CodecUnavailableError'`

- [ ] **Step 3: Add the exception class**

In `src/por_que/exceptions.py`, directly after the `ParquetDataError` class:

```python
class CodecUnavailableError(ParquetDataError):
    """A compression codec's optional dependency is not installed.

    Raised instead of a generic :class:`ParquetDataError` so consumers can
    distinguish "install the package (or accept the limitation, e.g. under
    pyodide)" from genuinely corrupt data, without parsing the message.

    Attributes:
        codec: Name of the affected codec (e.g. ``'BROTLI'``, ``'LZO'``,
            ``'ZSTD'``).
    """

    def __init__(self, message: str, codec: str) -> None:
        super().__init__(message)
        self.codec = codec
```

- [ ] **Step 4: Raise it in the codec getters**

In `src/por_que/parsers/page_content/compressors.py`, update the import and the three getters (`get_snappy` is untouched — it has a pure-python fallback and never raises):

```python
from por_que.exceptions import CodecUnavailableError, ParquetDataError
```

```python
def get_brotli():
    try:
        import brotli
    except ImportError:
        raise CodecUnavailableError(
            'Brotli compression requires brotli package',
            codec='BROTLI',
        ) from None
    return brotli
```

```python
def get_lzo():
    try:
        import lzo
    except ImportError:
        raise CodecUnavailableError(
            'LZO compression requires python-lzo package',
            codec='LZO',
        ) from None
    return lzo
```

```python
def get_zstd():
    try:
        import zstandard
    except ImportError:
        raise CodecUnavailableError(
            'Zstandard compression requires zstandard package',
            codec='ZSTD',
        ) from None
    return zstandard
```

Message text stays byte-for-byte identical (the worker's current sniffing keeps working until it migrates).

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_codec_unavailable.py -v`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add src/por_que/exceptions.py src/por_que/parsers/page_content/compressors.py tests/test_codec_unavailable.py
git commit -m "feat: typed CodecUnavailableError for missing codec packages"
```

---

### Task 2: `BloomFilter.explain()`

**Files:**
- Modify: `src/por_que/statistics.py` (BloomFilter class, lines ~254-415; new NamedTuples above the class)
- Test: `tests/test_bloom_filter.py` (append tests)

**Interfaces:**
- Produces:
  - `por_que.statistics.BloomBitCheck(word_index: int, bit_index: int, is_set: bool)` — NamedTuple
  - `por_que.statistics.BloomProbe(hash: int, block_index: int, num_blocks: int, block_bytes: bytes, bits: tuple[BloomBitCheck, ...], might_contain: bool)` — NamedTuple
  - `BloomFilter.explain(value: Any) -> BloomProbe`
  - `BloomFilter.might_contain(value)` behavior unchanged (now delegates to `explain`)
  - `BloomFilter.bitset` / `num_blocks` docstrings declare them stable public API
- Consumes: nothing from other tasks.

Existing module facts the implementer needs: `statistics.py` already defines `SBBF_SALT` (8 salts), `_U32_MASK`, `_SBBF_BLOCK_BYTES` (32), `_SBBF_WORDS_PER_BLOCK` (8), and `BloomFilter._plain_encode(value) -> bytes`. `xxh64` is imported function-locally from `.util.xxhash` (keep that pattern). The private `_block_check` method is deleted — `explain` becomes the single derivation path.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bloom_filter.py` (module already has `BLOOM_FILES`, `ABSENT_PROBES`, `_column_values`, and tests that load filters via `chunk.load_bloom_filter`; follow their pattern — see `test_bloom_filter_has_no_false_negatives` at line 50 for how a chunk+reader is obtained):

```python
@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_explain_matches_might_contain(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """explain() is the same derivation as might_contain, with intermediates."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = pf.column_chunks[0]
        bloom = await chunk.load_bloom_filter(hf)
        present = await _column_values(chunk, hf)

    for value in [*present, *ABSENT_PROBES]:
        probe = bloom.explain(value)
        assert probe.might_contain == bloom.might_contain(value)


@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_explain_intermediates_are_consistent(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """The intermediates describe one 256-bit block and its 8 checked bits."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = pf.column_chunks[0]
        bloom = await chunk.load_bloom_filter(hf)
        present = await _column_values(chunk, hf)

    for value in [present[0], ABSENT_PROBES[0]]:
        probe = bloom.explain(value)

        assert probe.num_blocks == bloom.num_blocks
        assert 0 <= probe.block_index < probe.num_blocks
        assert len(probe.block_bytes) == 32
        # The block is a verbatim window into the (public) bitset.
        start = probe.block_index * 32
        assert probe.block_bytes == bloom.bitset[start : start + 32]

        assert len(probe.bits) == 8
        for i, check in enumerate(probe.bits):
            assert check.word_index == i
            assert 0 <= check.bit_index < 32
            # Each check must agree with the block bytes it claims to describe.
            word = int.from_bytes(
                probe.block_bytes[i * 4 : (i + 1) * 4],
                'little',
            )
            assert check.is_set == bool((word >> check.bit_index) & 1)

        assert probe.might_contain == all(c.is_set for c in probe.bits)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_bloom_filter.py -v -k explain`
Expected: FAIL with `AttributeError: 'BloomFilter' object has no attribute 'explain'` (4 failures)

- [ ] **Step 3: Implement `explain` and delegate `might_contain`**

In `src/por_que/statistics.py`:

Add `NamedTuple` to the `typing` import (line ~5):

```python
from typing import TYPE_CHECKING, Any, NamedTuple, Self
```

Immediately above `class BloomFilter` add:

```python
class BloomBitCheck(NamedTuple):
    """One of the eight bit probes a split-block bloom lookup performs."""

    word_index: int
    """Which 32-bit word of the block this probe reads (0-7)."""

    bit_index: int
    """Which bit of that word the salt selected (0-31)."""

    is_set: bool
    """Whether the bit is set. All eight must be set for a maybe."""


class BloomProbe(NamedTuple):
    """The full derivation behind one bloom-filter lookup.

    :meth:`BloomFilter.explain` returns this instead of a bare bool so
    consumers (teaching UIs, debuggers) can show *why* a value is a maybe
    or a definite no: the hash, the block it selected, and the eight
    salt-derived bit probes within it.
    """

    hash: int
    """xxHash64 of the PLAIN-encoded value."""

    block_index: int
    """Index of the 256-bit block the top 32 hash bits selected."""

    num_blocks: int
    """Total blocks in the filter (same as :attr:`BloomFilter.num_blocks`)."""

    block_bytes: bytes
    """The selected block, verbatim: ``bitset[block_index*32 : +32]``."""

    bits: tuple[BloomBitCheck, ...]
    """The eight bit probes, in word order."""

    might_contain: bool
    """The lookup verdict: true iff every probed bit is set."""
```

Replace `might_contain` and `_block_check` (keep `_plain_encode` as is) with:

```python
    def might_contain(self, value: Any) -> bool:
        """Whether ``value`` might be present in the column chunk.

        ``False`` is exact (the value is definitely absent). ``True`` means the
        value is probably present but may be a false positive. See the class
        docstring for why this asymmetry is the useful part.
        """
        return self.explain(value).might_contain

    def explain(self, value: Any) -> BloomProbe:
        """Perform a lookup for ``value``, returning every intermediate.

        The derivation: PLAIN-encode the physical value, xxHash64 it, let the
        top 32 hash bits select one 256-bit block (scaled into range so the
        distribution stays uniform without a modulo), then let the low 32
        bits pick -- via each of the eight salts -- one bit per 32-bit word
        of that block. The value might be present only if all eight bits are
        set.

        Raises:
            ParquetFormatError: If the column's physical type cannot be
                bloom-queried.
        """
        from .util.xxhash import xxh64

        hash_ = xxh64(self._plain_encode(value))
        block_index = ((hash_ >> 32) * self.num_blocks) >> 32
        low = hash_ & _U32_MASK
        base = block_index * _SBBF_BLOCK_BYTES
        block_bytes = self.bitset[base : base + _SBBF_BLOCK_BYTES]

        bits = []
        for i in range(_SBBF_WORDS_PER_BLOCK):
            word = int.from_bytes(block_bytes[i * 4 : (i + 1) * 4], 'little')
            bit_index = ((low * SBBF_SALT[i]) & _U32_MASK) >> 27
            bits.append(
                BloomBitCheck(
                    word_index=i,
                    bit_index=bit_index,
                    is_set=bool((word >> bit_index) & 1),
                ),
            )

        return BloomProbe(
            hash=hash_,
            block_index=block_index,
            num_blocks=self.num_blocks,
            block_bytes=block_bytes,
            bits=tuple(bits),
            might_contain=all(check.is_set for check in bits),
        )
```

The old `_block_check` method is deleted entirely.

- [ ] **Step 4: Bless `bitset` and `num_blocks` as public**

In the same class, update the `bitset` field declaration and `num_blocks` docstring:

```python
    bitset: bytes = Field(repr=False)
    """The raw SBBF bitset: ``num_blocks`` consecutive 256-bit blocks.

    Stable public API: consumers may read this directly (e.g. to render
    block windows or compute per-block popcounts). Each block is 32 bytes,
    eight little-endian 32-bit words.
    """
```

(Pydantic picks up the docstring-after-field form used here; if `ruff` objects, put the same text in the class docstring instead — the requirement is that the stability guarantee is documented.)

```python
    @property
    def num_blocks(self) -> int:
        """Number of 256-bit blocks in the bitset. Stable public API."""
        return self.num_bytes // _SBBF_BLOCK_BYTES
```

- [ ] **Step 5: Run the bloom tests**

Run: `uv run pytest tests/test_bloom_filter.py -v`
Expected: all PASS (existing no-false-negatives/rejects-absent tests prove the delegation preserved behavior)

- [ ] **Step 6: Commit**

```bash
git add src/por_que/statistics.py tests/test_bloom_filter.py
git commit -m "feat: BloomFilter.explain() exposes the probe derivation"
```

---

### Task 3: `PageValue` NamedTuple carrying physical values

**Files:**
- Modify: `src/por_que/parsers/page_content/data.py` (ValueTuple → PageValue; `_create_tuple_stream`)
- Modify: `src/por_que/parsers/page_content/__init__.py` (exports)
- Modify: `src/por_que/pages.py` (`DataPageV1.parse_content` :127-155, `DataPageV2.parse_content` :175-203, import :20-26)
- Modify: `src/por_que/physical.py` (import :34, `parse_data_page` :367-404, `parse_all_data_pages` :406-448, `read_all_data` :718-750)
- Modify: `src/por_que/structuring/primitive.py:39`, `src/por_que/structuring/struct.py:93,112`, `src/por_que/structuring/list.py:65,82,96,110,137`, `src/por_que/structuring/map.py` (all peeked-tuple unpacks/index accesses), `src/por_que/structuring/reconstruct.py:23` (docstring)
- Modify: `tests/test_reconstruction.py:16-21` (`tuple_stream` helper), `tests/test_bloom_filter.py:42` (`_column_values` unpack)
- Test: `tests/test_page_values.py` (new)

**Interfaces:**
- Produces:
  - `por_que.parsers.page_content.PageValue` NamedTuple: `value: Any | None` (logical), `definition_level: int`, `repetition_level: int`, `physical: Any | None = None` (raw pre-conversion value; the default exists so tests can build 3-field instances)
  - `chunk.parse_data_page(page_index, reader, dictionary_values=None, apply_logical_types=True, excluded_logical_columns=None) -> Iterator[PageValue]`
  - `chunk.parse_all_data_pages(reader, apply_logical_types=True, excluded_logical_columns=None) -> AsyncIterator[PageValue]`
  - `ParquetFile.read_all_data(reader, apply_logical_types=True, excluded_logical_columns=None, reconstruct=True)`
  - The name `ValueTuple` is gone from the codebase.
- Consumes: nothing from other tasks. **Task 4 consumes the `apply_logical_types` convention from this task.**

Semantics: `physical` is ALWAYS populated on real parses. For null entries both `value` and `physical` are `None`. When logical conversion is off (globally via `apply_logical_types=False` or per-column via `excluded_logical_columns`), `value` is the same object as `physical`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_page_values.py`:

```python
"""PageValue carries the raw physical value alongside the logical one.

The invariants here hold for any column: ``physical`` is what the decoder
produced before logical conversion, ``value`` is after (or the same object
when conversion is off), and nulls are ``None`` on both.
"""

import pytest

from por_que import AsyncHttpFile, ParquetFile
from por_que.parsers.page_content import PageValue


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_page_values_carry_physical(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(cc for cc in pf.column_chunks if cc.data_pages)
        schema_element = chunk.metadata.schema_element

        converted = list(await chunk.parse_data_page(0, hf))
        raw = list(
            await chunk.parse_data_page(0, hf, apply_logical_types=False),
        )

    assert converted, 'fixture page decoded no values'
    for pv, rv in zip(converted, raw, strict=True):
        assert isinstance(pv, PageValue)
        assert pv.definition_level == rv.definition_level
        assert pv.repetition_level == rv.repetition_level

        # With conversion off, the logical slot holds the physical value.
        assert rv.value is rv.physical

        # One decode serves both: physical is the pre-conversion value...
        assert pv.physical == rv.physical
        if pv.physical is None:
            assert pv.value is None
        else:
            # ...and value is exactly its logical conversion.
            assert pv.value == schema_element.physical_to_logical_type(
                pv.physical,
            )


def test_page_value_named_fields() -> None:
    pv = PageValue('a', 1, 0, b'a')
    assert (pv.value, pv.definition_level, pv.repetition_level, pv.physical) == (
        'a',
        1,
        0,
        b'a',
    )
    # physical defaults to None so fixtures/tests can build bare triples.
    assert PageValue('a', 1, 0).physical is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_page_values.py -v`
Expected: FAIL with `ImportError: cannot import name 'PageValue'`

- [ ] **Step 3: Define `PageValue` and produce it in `data.py`**

In `src/por_que/parsers/page_content/data.py`:

Add `NamedTuple` to the typing import (line 11):

```python
from typing import TYPE_CHECKING, Any, NamedTuple
```

Replace `type ValueTuple = tuple[Any | None, int, int]` (line 27) with:

```python
class PageValue(NamedTuple):
    """One decoded entry of a data page.

    A NamedTuple so field access survives shape changes: consumers use
    ``.value`` / ``.definition_level`` / ``.repetition_level`` /
    ``.physical``, never positional unpacking.
    """

    value: Any | None
    """The logical value (or the physical one when conversion is off)."""

    definition_level: int
    repetition_level: int

    physical: Any | None = None
    """The raw physical value the decoder produced, before logical
    conversion. ``None`` only for null entries (and hand-built test
    triples, where it defaults)."""
```

Update every `Iterator[ValueTuple]` annotation in this file to `Iterator[PageValue]` (`parse`, `_read_and_reassemble_values`, `_create_tuple_stream`).

In `_create_tuple_stream` (line 125), update the yields so physical is always carried:

```python
        # Helper producing the full entry from one decoded physical value
        def make_page_value(
            physical: Any | None,
            def_level: int,
            rep_level: int,
        ) -> PageValue:
            value = physical
            if apply_logical_types and physical is not None:
                value = self.schema_element.physical_to_logical_type(physical)
            return PageValue(
                value=value,
                definition_level=def_level,
                repetition_level=rep_level,
                physical=physical,
            )
```

This replaces the current `convert_value` helper. The three yield sites become:

- no-definition-levels loop: `yield make_page_value(value, 0, 0)`
- non-null branch: `yield make_page_value(value, def_level, rep_level)`
- null branch: `yield make_page_value(None, def_level, rep_level)`

Also update the method docstring ("Yield PageValue entries as a stream…") — remove the stale "(value, definition_level, repetition_level) tuples" wording.

- [ ] **Step 4: Update the export**

`src/por_que/parsers/page_content/__init__.py`:

```python
from .data import DataPageV1Parser, DataPageV2Parser, PageValue
from .dictionary import DictionaryPageParser, DictType

__all__ = [
    'DataPageV1Parser',
    'DataPageV2Parser',
    'DictType',
    'DictionaryPageParser',
    'PageValue',
]
```

- [ ] **Step 5: Plumb `apply_logical_types` through `pages.py`**

In `src/por_que/pages.py`: change the import (line 25) from `ValueTuple` to `PageValue`. In BOTH `DataPageV1.parse_content` and `DataPageV2.parse_content`:

- return type `Iterator[ValueTuple]` → `Iterator[PageValue]`
- add parameter `apply_logical_types: bool = True` before `excluded_logical_columns`
- pass it through: `.parse(apply_logical_types=apply_logical_types, excluded_logical_columns=excluded_logical_columns)`

(The underlying `BaseDataPageParser.parse` already accepts both.)

- [ ] **Step 6: Plumb `apply_logical_types` through `physical.py`**

In `src/por_que/physical.py`: change the import (line 34) from `ValueTuple` to `PageValue`, update the `AsyncChain[ValueTuple]` annotation in `read_all_data` to `AsyncChain[PageValue]`, and the `Iterator[ValueTuple]`/`AsyncIterator[ValueTuple]` return annotations on `parse_data_page`/`parse_all_data_pages`.

Add `apply_logical_types: bool = True` to and pass it down through the chain, keeping `excluded_logical_columns` alongside:

- `parse_data_page(self, page_index, reader, dictionary_values=None, apply_logical_types=True, excluded_logical_columns=None)` → pass both to `data_page.parse_content(...)`
- `parse_all_data_pages(self, reader, apply_logical_types=True, excluded_logical_columns=None)` → pass both to each `parse_data_page(...)` coroutine
- `read_all_data(self, reader, apply_logical_types=True, excluded_logical_columns=None, reconstruct=True)` → pass both to `cc.parse_all_data_pages(...)`

- [ ] **Step 7: Migrate structuring assemblers to named access**

Every consumer of a page-value entry switches to field access (the whole point of the NamedTuple — positional 3-unpacks would crash on the 4th field):

`src/por_que/structuring/primitive.py:39`:

```python
        entry = await anext(self.leader_stream)

        if entry.definition_level >= self.schema_element.definition_level:
            return (entry.value, None)
```

`src/por_que/structuring/struct.py`:
- line 93: `_, dl, rl = peeked_tuple` → `dl, rl = peeked_tuple.definition_level, peeked_tuple.repetition_level`
- line 112: `peeked[1]` → `peeked.definition_level`

`src/por_que/structuring/list.py`:
- line 65: `_, dl, rl = peeked_tuple` → `dl, rl = peeked_tuple.definition_level, peeked_tuple.repetition_level`
- lines 82, 96, 137: `peeked[1] == dl and peeked[2] == rl` → `peeked.definition_level == dl and peeked.repetition_level == rl`
- line 110: `_, _, rl = peeked_tuple` → `rl = peeked_tuple.repetition_level`

`src/por_que/structuring/map.py` — the complete list of sites:
- line 80: `_, dl, rl = peeked_tuple` → `dl, rl = peeked_tuple.definition_level, peeked_tuple.repetition_level`
- lines 99 and 122: `value_peek[1] == dl and value_peek[2] == rl` → `value_peek.definition_level == dl and value_peek.repetition_level == rl`
- line 136: `_, _, key_rl = peeked_key_tuple` → `key_rl = peeked_key_tuple.repetition_level`
- lines 155 and 158: `val_peek[2]` → `val_peek.repetition_level`
- line 212: `peeked[1] == dl and peeked[2] == rl` → `peeked.definition_level == dl and peeked.repetition_level == rl`

After the edits, `grep -n '\[1\]\|\[2\]\|_, dl, rl\|_, _, ' src/por_que/structuring/` must show no remaining tuple-field indexing/unpacking of stream entries.

`src/por_que/structuring/reconstruct.py:23`: docstring "(value, dl, rl) tuples" → "``PageValue`` entries".

- [ ] **Step 8: Update the two test consumers**

`tests/test_reconstruction.py` — fixture JSON stores bare 3-element arrays; wrap them (lines 16-21):

```python
from por_que.parsers.page_content import PageValue


async def tuple_stream(
    data: Sequence[tuple[Any, int, int]],
) -> AsyncIterator[PageValue]:
    """Convert fixture rows to an async stream of PageValue entries."""
    for tup in data:
        yield PageValue(*tup)
```

(Adjust the `AsyncIterator` import usage as needed; `Sequence`/`Any` are already imported.)

`tests/test_bloom_filter.py:40-45` — the helper's unpack:

```python
async def _column_values(chunk, reader) -> list:
    values = []
    async for entry in chunk.parse_all_data_pages(reader):
        if entry.value is not None:
            values.append(entry.value)
    return values
```

- [ ] **Step 9: Verify nothing references the old name, run the suite**

Run: `grep -rn "ValueTuple" src/ tests/`
Expected: no matches

Run: `uv run pytest tests/test_page_values.py tests/test_reconstruction.py tests/test_bloom_filter.py -v`
Expected: all PASS

Run: `uv run pytest tests/ -x -q`
Expected: all PASS (data-fixture comparisons in `test_parquet_file.py` prove reconstructed output is byte-identical; reconstruction consumes `.value`, so fixtures must NOT change — if a data fixture shows a diff, the migration has a bug; do not regenerate fixtures)

- [ ] **Step 10: Commit**

```bash
git add src/por_que tests/test_page_values.py tests/test_reconstruction.py tests/test_bloom_filter.py
git commit -m "feat: PageValue named tuple carries physical values; plumb apply_logical_types"
```

---

### Task 4: public `chunk.parse_dictionary()`

**Files:**
- Modify: `src/por_que/physical.py` (`PhysicalColumnChunk`, replace `_parse_dictionary` at :347-365 area)
- Test: `tests/test_parse_dictionary.py` (new)

**Interfaces:**
- Consumes: the `apply_logical_types: bool = True` parameter convention from Task 3 (name and default must match).
- Produces: `PhysicalColumnChunk.parse_dictionary(reader: ReadableSeekable | AsyncReadableSeekable, apply_logical_types: bool = True) -> DictType` — `[]` when the chunk has no dictionary page.

Existing facts: `PhysicalColumnChunk` (physical.py:86) has `dictionary_page`, `metadata.schema_element`, `codec`, and the private `_parse_dictionary(reader)` returning raw physical values via `self.dictionary_page.parse_content(...)`. `ensure_async_reader` is already imported in the module. Internal callers (`parse_data_page`, `parse_all_data_pages`) need PHYSICAL values (the data-page decoder applies logical conversion itself), so they must keep getting unconverted values.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_parse_dictionary.py`:

```python
"""Public dictionary-page decoding on the column chunk.

``parse_dictionary`` mirrors ``parse_data_page`` ergonomics: takes a sync
or async reader, applies logical types by default, and returns ``[]``
when the chunk has no dictionary page.
"""

import io
import urllib.request

import pytest

from por_que import AsyncHttpFile, ParquetFile


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_parse_dictionary_logical_and_physical(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(
            cc for cc in pf.column_chunks if cc.dictionary_page is not None
        )
        schema_element = chunk.metadata.schema_element

        physical = await chunk.parse_dictionary(hf, apply_logical_types=False)
        logical = await chunk.parse_dictionary(hf)

    assert physical, 'fixture chunk decoded an empty dictionary'
    assert logical == [
        schema_element.physical_to_logical_type(v) for v in physical
    ]


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_parse_dictionary_accepts_sync_reader(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    with urllib.request.urlopen(parquet_url) as resp:  # noqa: S310
        data = resp.read()

    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(
            cc for cc in pf.column_chunks if cc.dictionary_page is not None
        )
        via_async = await chunk.parse_dictionary(hf)

    via_sync = await chunk.parse_dictionary(io.BytesIO(data))
    assert via_sync == via_async


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
@pytest.mark.asyncio
async def test_parse_dictionary_without_dictionary_page(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = pf.column_chunks[0]
        assert chunk.dictionary_page is None
        assert await chunk.parse_dictionary(hf) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_parse_dictionary.py -v`
Expected: FAIL with `AttributeError: ... has no attribute 'parse_dictionary'` (3 failures)

- [ ] **Step 3: Implement**

In `src/por_que/physical.py`, directly above `_parse_dictionary`, add:

```python
    async def parse_dictionary(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
        apply_logical_types: bool = True,
    ) -> DictType:
        """Decode this chunk's dictionary page to its distinct values.

        Mirrors :meth:`parse_data_page` ergonomics: accepts a sync or async
        reader and applies logical types by default. Pass
        ``apply_logical_types=False`` for the raw physical values (the bytes
        the bloom filter hashes, for example).

        Returns:
            The dictionary's values, or ``[]`` if the chunk has no
            dictionary page.
        """
        reader = ensure_async_reader(reader)
        values = await self._parse_dictionary(reader)
        if not apply_logical_types:
            return values
        schema_element = self.metadata.schema_element
        return [
            schema_element.physical_to_logical_type(value) for value in values
        ]
```

`_parse_dictionary` stays as the internal core (data-page decoding needs the physical values). Verify `ReadableSeekable` is in the `protocols` import list in this module (it is, via the existing `parse_data_page` signature).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_parse_dictionary.py -v`
Expected: 3 PASS

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest tests/ -x -q`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/por_que/physical.py tests/test_parse_dictionary.py
git commit -m "feat: public chunk.parse_dictionary with logical-type control"
```

---

## Task order

1 (codec error), 2 (bloom explain), 3 (PageValue), 4 (parse_dictionary). Tasks 1 and 2 are fully independent; Task 4 depends on Task 3 only for the `apply_logical_types` naming convention and the updated `tests/test_bloom_filter.py` helper it doesn't touch. Run them sequentially anyway — they share files with hook-formatted commits.

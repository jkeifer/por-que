# Updating fixtures

!!! info "Lands with the `jak/fixtures-pin` branch"

    The `scripts/update-fixtures.py` command described here is added on the
    `jak/fixtures-pin` branch. If it is not present in your checkout yet, this
    page documents the intended flow it provides.

por-que's tests download Parquet test files from the
[apache/parquet-testing](https://github.com/apache/parquet-testing) repository
at test time (see `tests/conftest.py`). Downloads are **pinned to a single
commit SHA** via the `PARQUET_TESTING_REF` constant, rather than tracking a
branch — upstream changing a file out from under us silently breaks fixtures
(this happened with `fixed_length_byte_array.parquet`).

The generated `*_expected.json` fixtures under `tests/fixtures/` are the parsed
output por-que produces for each pinned test file. When you bump the pin, those
fixtures must be regenerated.

## One command

`scripts/update-fixtures.py` is the one way to bump the pin and regenerate
everything in a single shot:

```bash
# pin to upstream's current HEAD
uv run scripts/update-fixtures.py

# or pin to a specific ref
uv run scripts/update-fixtures.py <git-ref>
```

It performs the whole flow end to end:

1. **Resolve the ref** — the one you passed, or the current HEAD of
   apache/parquet-testing.
2. **Rewrite `PARQUET_TESTING_REF`** in `tests/conftest.py` in place.
3. **Clear any on-disk cache** of downloaded test files.
4. **Delete every `tests/fixtures/**/*_expected.json`** fixture, since they
   were generated against the old ref.
5. **Run pytest once** — tests whose fixture is now missing regenerate it from
   the newly pinned files and skip (this is expected).
6. **Run pytest a second time** to verify the suite is green against the
   freshly generated fixtures.
7. **Print the new ref** and a `git status --short tests/fixtures` summary so
   you can review the diff.

If either pytest run fails for a real reason (not the expected
"generated fixture" skips), the script stops and prints the pytest exit code so
you can investigate before anything is committed.

## After running

Review the regenerated fixture diff, confirm it reflects intended parser
behavior, and commit `tests/conftest.py` together with the updated fixtures.

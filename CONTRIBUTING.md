# Contributing

por-que is an educational, pure-Python Parquet parser. Contributions that
improve correctness, teaching value, or documentation are welcome. This guide
covers the developer workflow.

## Project Setup

This project uses [uv](https://docs.astral.sh/uv) for dependency and
environment management. Install it globally (`brew install uv` on macOS, or see
the uv docs).

`uv` manages its own virtual environment. Sync the project with its dev
dependencies:

```bash
uv sync
```

Every command below is run through `uv run`, which resolves against that
environment — you do not need to activate a venv manually.

## Git Hooks

Hooks are managed with [lefthook](https://lefthook.dev). Install them once:

```bash
uv run lefthook install
```

On `git commit`, lefthook runs ruff (lint + format), mypy, the schema staleness
check, and a set of file hygiene checks against your staged files. Run the full
pre-commit set manually against everything with:

```bash
uv run lefthook run pre-commit --all-files
```

## Linting and Type Checking

Ruff handles linting and formatting; mypy handles type checking. Run them
directly when you want to check without committing:

```bash
uv run ruff check src
uv run ruff format src
uv run mypy src
```

## Testing

Tests use `pytest`:

```bash
uv run pytest
```

The suite downloads real Parquet files from
[apache/parquet-testing](https://github.com/apache/parquet-testing) at test
time (see `tests/conftest.py`) and measures coverage of `por_que`.

### Updating test fixtures

Downloads are **pinned to a single commit SHA** via `PARQUET_TESTING_REF` in
`tests/conftest.py`, rather than tracking a branch — upstream changing a file
out from under us silently breaks fixtures. The generated
`tests/fixtures/**/*_expected.json` files are the parsed output por-que produces
for each pinned file, so bumping the pin means regenerating them.

`scripts/update-fixtures.py` does the whole flow in one shot — resolve the ref,
rewrite `PARQUET_TESTING_REF`, clear the download cache, delete the stale
`*_expected.json` fixtures, and run pytest twice (once to regenerate, once to
verify green):

```bash
# pin to upstream's current HEAD
uv run scripts/update-fixtures.py

# or pin to a specific ref
uv run scripts/update-fixtures.py <git-ref>
```

Review the resulting diff (`git status --short tests/fixtures`), confirm it
reflects intended parser behavior, and commit `tests/conftest.py` together with
the updated fixtures. See `--help` for details.

## Dump Schema

`ParquetFile`'s serialization-mode JSON Schema *is* the dump contract consumed
by the webapp; the committed copy lives at
`ver-por-que/schema/por-que.schema.json`. Regenerate it after changing any
serialized model:

```bash
uv run python scripts/emit-schema.py
```

A pre-commit hook (and CI) runs `--check`, which exits non-zero when the
committed schema is stale, so regenerate and commit the update alongside your
model change:

```bash
uv run python scripts/emit-schema.py --check
```

## Documentation

The docs site is built with [Zensical](https://zensical.org), configured in
`zensical.toml`. The dependencies live in the `docs` group.

```bash
# build the static site (must complete cleanly)
uv run --group docs zensical build

# live-reload authoring server
uv run --group docs zensical serve
```

## Webapp (ver-por-que)

The `ver-por-que/` subproject is the client-side web viewer that consumes
por-que's JSON dumps. It has its own Node-based toolchain and dev workflow —
see [`ver-por-que/CONTRIBUTING.md`](ver-por-que/CONTRIBUTING.md).

#!/usr/bin/env python3
"""Emit the canonical JSON Schema for the por-que dump format.

``ParquetFile`` is the root model that ``parquet.to_json()`` (the CLI ``dump``
command) serializes, so its serialization-mode schema *is* the dump contract.
The webapp consumes the committed copy at
``ver-por-que/schema/por-que.schema.json``.

Run with no arguments to (re)write that file. Run with ``--check`` to exit
non-zero when the committed file is stale (used in CI and pre-commit).
"""

from __future__ import annotations

import argparse
import json
import sys

from pathlib import Path

from por_que.physical import ParquetFile

SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / 'ver-por-que'
    / 'schema'
    / 'por-que.schema.json'
)


def render() -> str:
    """Render the deterministic schema text (serialization mode, by alias)."""
    # by_alias matches to_json (which dumps with by_alias=True); serialization
    # mode matches what a dump actually contains. sort_keys keeps the output
    # byte-stable across runs so --check is meaningful.
    schema = ParquetFile.model_json_schema(by_alias=True, mode='serialization')
    return json.dumps(schema, indent=2, sort_keys=True) + '\n'


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if the committed schema is stale',
    )
    args = parser.parse_args()

    content = render()

    if args.check:
        current = SCHEMA_PATH.read_text() if SCHEMA_PATH.exists() else None
        if current != content:
            print(  # noqa: T201
                f'{SCHEMA_PATH} is out of date; regenerate with '
                '`uv run python scripts/emit-schema.py`.',
                file=sys.stderr,
            )
            sys.exit(1)
        return

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(content)
    print(f'wrote {SCHEMA_PATH}')  # noqa: T201


if __name__ == '__main__':
    main()

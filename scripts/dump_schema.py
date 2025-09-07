#!/usr/bin/env python3
"""
Script to dump the ParquetFile JSON schema to stdout.

This generates the complete JSON schema for the ParquetFile model,
which can be used for validation, documentation, or integration
with other tools.
"""

import json
import sys

from por_que.physical import ParquetFile


def main() -> None:
    """Generate and dump ParquetFile JSON schema to stdout."""
    try:
        json.dump(
            ParquetFile.model_json_schema(),
            sys.stdout,
            indent=2,
            sort_keys=True,
        )
        sys.stdout.write('\n')
    except Exception as e:  # noqa: BLE001
        print(f'Error generating schema: {e}', file=sys.stderr)  # noqa: T201
        sys.exit(1)


if __name__ == '__main__':
    main()

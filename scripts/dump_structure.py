#!/usr/bin/env python3
"""
Quick script to dump parquet file structure to JSON.

Usage: python dump_structure.py <parquet_file_path>
"""

import sys

from pathlib import Path

from por_que import ParquetFile


def main():
    if len(sys.argv) != 2:
        print("Usage: python dump_structure.py <parquet_file_path>", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    file_path = Path(sys.argv[1])

    try:
        with file_path.open('rb') as f:
            parquet_file = ParquetFile.from_reader(
                f,
                source=file_path,
            )
            print(parquet_file.to_json())  # noqa: T201
    except Exception as e:  # noqa: BLE001
        print(f"Error: {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)


if __name__ == '__main__':
    main()

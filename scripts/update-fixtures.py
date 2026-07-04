#!/usr/bin/env python3
"""
Update the pinned apache/parquet-testing commit and regenerate fixtures.

Our tests download Parquet test files from the apache/parquet-testing repo
at test time (see tests/conftest.py). Downloads are pinned to a single
commit SHA (the `PARQUET_TESTING_REF` constant in tests/conftest.py)
rather than tracking a branch, because upstream changing a file out from
under us silently breaks our fixtures (this happened with
`fixed_length_byte_array.parquet`).

This script is THE way to bump that pin -- run it with:

    uv run scripts/update-fixtures.py

and, in one shot, it will:

  1. Resolve the new ref: either the one passed as an argument, or the
     current HEAD of https://github.com/apache/parquet-testing.
  2. Rewrite `PARQUET_TESTING_REF` in tests/conftest.py in place.
  3. Clear any on-disk cache of downloaded parquet test files. (por-que's
     HTTP reader only caches bytes in memory for the life of a single
     request, so today this just sweeps up stray temp files that could be
     left behind if a previous test run was killed mid-download.)
  4. Delete every tests/fixtures/**/*_expected.json fixture, since they
     were generated against the old ref and may no longer match.
  5. Run pytest once. Tests whose fixture is now missing regenerate it
     from the newly pinned files and skip (this is expected).
  6. Run pytest a second time to verify the suite is green against the
     freshly generated fixtures.
  7. Print the new ref and a `git status --short tests/fixtures` summary
     so you can review the diff and commit it.

If either pytest run fails outright (a real failure, not the expected
"generated fixture" skips), the script stops and prints the pytest exit
code so you can investigate before anything is committed.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFTEST = REPO_ROOT / 'tests' / 'conftest.py'
FIXTURES_DIR = REPO_ROOT / 'tests' / 'fixtures'
PARQUET_TESTING_URL = 'https://github.com/apache/parquet-testing.git'

REF_ASSIGNMENT = re.compile(r"^PARQUET_TESTING_REF = '[^']*'$", re.MULTILINE)


def resolve_ref(ref: str | None) -> str:
    """Return the ref to pin to: the given one, or upstream's current HEAD."""
    if ref:
        return ref

    result = subprocess.run(  # noqa: S603
        ['git', 'ls-remote', PARQUET_TESTING_URL, 'HEAD'],  # noqa: S607
        capture_output=True,
        check=True,
        text=True,
    )
    sha, _, _ = result.stdout.partition('\t')
    sha = sha.strip()
    if not sha:
        msg = f'could not resolve HEAD of {PARQUET_TESTING_URL}'
        raise RuntimeError(msg)
    return sha


def rewrite_conftest(new_ref: str) -> None:
    """Rewrite the PARQUET_TESTING_REF constant in tests/conftest.py."""
    content = CONFTEST.read_text()
    new_content, count = REF_ASSIGNMENT.subn(
        f"PARQUET_TESTING_REF = '{new_ref}'",
        content,
    )
    if count != 1:
        msg = (
            f'expected exactly one PARQUET_TESTING_REF assignment in '
            f'{CONFTEST}, found {count}'
        )
        raise RuntimeError(msg)
    CONFTEST.write_text(new_content)


def clear_download_cache() -> None:
    """
    Clear any on-disk cache of downloaded parquet test files.

    por-que's HTTP file reader (hctef) only caches bytes in memory for the
    lifetime of a single `AsyncHttpFile`, so there is normally nothing on
    disk to clear. The one place a stray file can appear is the
    (skip-by-default) PyArrow comparison test, which downloads to a temp
    file and removes it when done -- unless the process was killed
    mid-test. This sweeps up any such leftovers.
    """
    for stray in Path(tempfile.gettempdir()).glob('tmp*.parquet'):
        stray.unlink(missing_ok=True)


def clear_fixtures() -> list[Path]:
    """Delete every generated expected-JSON fixture and return the paths."""
    removed = sorted(FIXTURES_DIR.rglob('*_expected.json'))
    for fixture in removed:
        fixture.unlink()
    return removed


def run_pytest(label: str) -> None:
    """Run the test suite, aborting the script if it fails outright."""
    print(f'--- running pytest ({label}) ---')  # noqa: T201
    result = subprocess.run(['uv', 'run', 'pytest'], check=False)  # noqa: S607
    if result.returncode != 0:
        print(  # noqa: T201
            f'pytest failed during the "{label}" run '
            f'(exit code {result.returncode}); aborting.',
            file=sys.stderr,
        )
        sys.exit(result.returncode)


def git_status_summary() -> str:
    """Return the first 10 lines of `git status --short` for the fixtures."""
    result = subprocess.run(  # noqa: S603
        ['git', 'status', '--short', str(FIXTURES_DIR)],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    lines = result.stdout.splitlines()
    return '\n'.join(lines[:10])


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'ref',
        nargs='?',
        default=None,
        help=(
            'parquet-testing commit SHA (or other git ref) to pin to; '
            'defaults to the current HEAD of '
            'https://github.com/apache/parquet-testing'
        ),
    )
    args = parser.parse_args()

    new_ref = resolve_ref(args.ref)
    print(f'pinning to apache/parquet-testing@{new_ref}')  # noqa: T201

    rewrite_conftest(new_ref)
    clear_download_cache()
    removed = clear_fixtures()
    print(f'removed {len(removed)} stale fixture(s)')  # noqa: T201

    run_pytest('regenerate fixtures')
    run_pytest('verify')

    print()  # noqa: T201
    print(f'done: pinned tests/conftest.py to {new_ref}')  # noqa: T201
    print()  # noqa: T201
    print('git status --short tests/fixtures:')  # noqa: T201
    print(git_status_summary() or '(no changes)')  # noqa: T201
    print()  # noqa: T201
    print(  # noqa: T201
        'Review the diff above, then commit tests/conftest.py and tests/fixtures/.',
    )


if __name__ == '__main__':
    main()

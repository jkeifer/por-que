#!/usr/bin/env python3
"""Render the workshop notebook to Markdown for the docs site.

Zensical has no mkdocs-jupyter equivalent, so the workshop notebook is
converted to Markdown as a build-prep step (run manually and in CI, before
`zensical build`). The output lives at ``docs/workshop/notebook.md`` and is
gitignored -- it is generated, never edited by hand.

Usage:

    uv run python scripts/render-notebook.py

If ``example-notebook.ipynb`` is not present in the checkout (some branches do
not carry it), a short placeholder is written instead so the site still
builds.
"""

from __future__ import annotations

import subprocess
import sys

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = REPO_ROOT / 'example-notebook.ipynb'
OUTPUT = REPO_ROOT / 'docs' / 'workshop' / 'notebook.md'

PLACEHOLDER = """# Notebook

!!! warning "Not rendered in this build"

    `example-notebook.ipynb` was not present when the site was built, so the
    rendered notebook is unavailable here. Run
    `uv run python scripts/render-notebook.py` in a checkout that contains the
    notebook to generate this page.
"""


def render() -> int:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    if not NOTEBOOK.exists():
        OUTPUT.write_text(PLACEHOLDER)
        print(f'notebook not found; wrote placeholder to {OUTPUT}')  # noqa: T201
        return 0

    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            '-m',
            'nbconvert',
            '--to',
            'markdown',
            '--output-dir',
            str(OUTPUT.parent),
            '--output',
            OUTPUT.stem,
            str(NOTEBOOK),
        ],
        check=False,
    )
    if result.returncode != 0:
        print('nbconvert failed', file=sys.stderr)  # noqa: T201
        return result.returncode

    print(f'rendered {NOTEBOOK.name} -> {OUTPUT}')  # noqa: T201
    return 0


if __name__ == '__main__':
    raise SystemExit(render())

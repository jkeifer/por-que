#!/usr/bin/env python3
"""Build a python-only por-que wheel and stage it for the webapp.

The webapp parses raw .parquet files in the browser via pyodide, which needs a
por-que wheel as a static asset. This builds that wheel and copies it, plus a
tiny manifest, into ``ver-por-que/static/vendor/`` (gitignored).

``POR_QUE_NO_WEBAPP=1`` is mandatory: without it ``uv build`` recurses into the
hatch webapp build hook, which runs ``npm run build`` -- and this script is what
that build ultimately serves, so we'd have a build calling a build.

ponytail: no PyPI download path -- the released wheel predates the current dump
format. Once a release ships the current format, fetching a pinned wheel from
PyPI instead of building locally becomes an option and this script can go away.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENDOR = ROOT / 'ver-por-que' / 'static' / 'vendor'


def main() -> None:
    env = {**os.environ, 'POR_QUE_NO_WEBAPP': '1'}
    with __import__('tempfile').TemporaryDirectory() as tmp:
        subprocess.run(  # noqa: S603
            ['uv', 'build', '--wheel', '--out-dir', tmp],  # noqa: S607
            cwd=ROOT,
            check=True,
            env=env,
        )
        wheels = list(Path(tmp).glob('*.whl'))
        if len(wheels) != 1:
            sys.exit(f'expected exactly one wheel, found {len(wheels)}')
        wheel = wheels[0]

        VENDOR.mkdir(parents=True, exist_ok=True)
        for old in VENDOR.glob('*.whl'):
            old.unlink()
        # micropip parses the filename for name/version -- keep it intact.
        shutil.copy2(wheel, VENDOR / wheel.name)
        (VENDOR / 'manifest.json').write_text(json.dumps({'wheel': wheel.name}) + '\n')

    print(f'staged {wheel.name} -> {VENDOR.relative_to(ROOT)}')  # noqa: T201


if __name__ == '__main__':
    main()

"""Bundle the ver-por-que webapp into build artifacts.

Wheels get the built app as package data (``por_que/cli/_webapp``); sdists
carry the prebuilt ``ver-por-que/dist`` so installing from an sdist needs no
node. npm only runs when no prebuilt dist is present. Editable installs skip
bundling entirely (``por-que serve`` falls back to ``ver-por-que/dist``).
Set ``POR_QUE_NO_WEBAPP=1`` to skip bundling (e.g. python-only CI).
"""

from __future__ import annotations

import os
import shutil
import subprocess

from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class WebappBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if version == 'editable' or os.environ.get('POR_QUE_NO_WEBAPP'):
            return

        dist = Path(self.root, 'ver-por-que', 'dist')
        if not (dist / 'index.html').exists():
            self._npm_build(dist.parent)

        target = (
            'por_que/cli/_webapp' if self.target_name == 'wheel' else 'ver-por-que/dist'
        )
        build_data['force_include'][str(dist)] = target

    def _npm_build(self, webapp: Path) -> None:
        npm = shutil.which('npm')
        if npm is None:
            raise RuntimeError(
                'npm is required to bundle the ver-por-que webapp; install '
                'node or set POR_QUE_NO_WEBAPP=1 to build without it',
            )
        subprocess.run([npm, 'ci'], cwd=webapp, check=True)  # noqa: S603
        subprocess.run([npm, 'run', 'build'], cwd=webapp, check=True)  # noqa: S603

"""Serve the bundled ver-por-que webapp locally against a parquet dump.

Asset resolution order: an explicit override, the packaged wheel data
(``por_que/cli/_webapp``), then a repo-checkout fallback (``ver-por-que/dist``)
for editable installs that have been built with ``npm run build``.
"""

from __future__ import annotations

import http.server
import importlib.resources

from pathlib import Path
from typing import Any

import por_que


class WebappNotFoundError(Exception):
    """No usable webapp asset directory could be resolved."""


def resolve_webapp_dir(override: Path | None = None) -> Path:
    if override is not None:
        if (override / 'index.html').is_file():
            return override
        raise WebappNotFoundError(f'{override} has no index.html')

    packaged = Path(str(importlib.resources.files('por_que.cli') / '_webapp'))
    if (packaged / 'index.html').is_file():
        return packaged

    for parent in Path(por_que.__file__).resolve().parents:
        dist = parent / 'ver-por-que' / 'dist'
        if (dist / 'index.html').is_file():
            return dist

    raise WebappNotFoundError(
        'no webapp assets found -- run `npm run build` in ver-por-que/, '
        'or install por-que from a wheel',
    )


def make_handler(
    directory: Path,
    payload: bytes,
    verbose: bool,
) -> type[http.server.SimpleHTTPRequestHandler]:
    """Build a request handler serving ``directory``, plus ``payload`` at
    ``/data.json``.
    """

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(directory), **kwargs)

        def do_GET(self) -> None:
            if self.path == '/data.json':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(payload)
                return
            super().do_GET()

        def log_message(self, format: str, *args: Any) -> None:
            if verbose:
                super().log_message(format, *args)

    return Handler

"""Console-script entry point.

The ``por-que`` script is declared unconditionally in ``pyproject.toml``, but
the CLI dependencies (``typer``, ``rich``) live in the ``cli`` optional extra.
Importing the real app therefore happens lazily inside ``main`` so that, when
the extra is not installed, the user gets one friendly line instead of an
``ImportError`` traceback.
"""

from __future__ import annotations

import sys


def main() -> None:
    try:
        from por_que.cli.app import app
    except ImportError:
        sys.stderr.write(
            'The por-que CLI requires extra dependencies. '
            "Install them with: pip install 'por-que[cli]'\n",
        )
        raise SystemExit(1) from None

    app()

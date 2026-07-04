"""Optional command-line interface for por-que.

This package is an *optional extra*: install it with ``pip install
'por-que[cli]'``. It is deliberately kept separate from the core library, which
has zero CLI dependencies, and it consumes only por-que's public API
(``ParquetFile``, ``FileMetadata``, ``AsyncHttpFile``) -- so it doubles as a
worked example of how to build a tool on top of the library.

This v1 is a pragmatic, non-interactive subset of the vision in
``arch/CLI_DESIGN.md``: read-only inspection of schema, metadata, row groups,
and page structure, plus a raw JSON ``dump``. The interactive "microscope"
explorer described in the design docs is intentionally deferred.

Stretch goal: a ``ver-por-que`` local-launch mode that bundles and serves the
web viewer directly from the CLI is out of scope for this release.
"""

# API reference

These pages are generated from the source with
[mkdocstrings](https://mkdocstrings.github.io/). Because por-que is a teaching
library, the docstrings *are* the lessons and the source is shown inline —
read them alongside the [Learn](../learn/anatomy.md) narrative.

!!! note

    mkdocstrings support in Zensical is preliminary and the upstream API-
    reference rendering is being reworked during 2026; expect these pages to
    improve as that lands.

One page per public module:

- [`por_que`](por_que.md) — the top-level package and its exports.
- [`physical`](physical.md) — `ParquetFile`, the user-facing entry point.
- [`file_metadata`](file_metadata.md) — the parsed metadata data model.
- [`pages`](pages.md) — page models (data, dictionary, index).
- [`exceptions`](exceptions.md) — the error hierarchy.
- [`protocols`](protocols.md) — reader protocols.
- [`parsers`](parsers.md) — a curated subset of the low-level parsers.

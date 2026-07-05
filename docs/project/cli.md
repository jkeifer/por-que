# CLI

!!! info "Status: designed, not yet built"

    por-que has a designed command-line interface but it is **not implemented
    yet**. This page records the intended shape so the design docs are
    discoverable; nothing here ships today.

## Intended packaging

When built, the CLI will ship as an optional extra so the core library stays
dependency-light:

```bash
pip install 'por-que[cli]'
```

## Vision

A "Parquet file microscope": start with a high-level overview of a file and
progressively zoom in — file → row group → column → page — with educational
annotations explaining what you are looking at and why it matters. Commands
would accept a local path or an unauthenticated HTTP(S) URL, and support
JSON export for downstream tools.

## Design documents

The full design lives in the historical design notes under `arch/` (kept as a
record, not published):

- `arch/CLI_DESIGN.md` — vision, milestones, and command surface.
- `arch/CLI_COMMAND_REFERENCE.md` — the intended command/flag reference.
- `arch/CLI_IMPLEMENTATION_PLAN.md` — the phased build plan.

Interactive exploration and visualization were split out of the CLI into a
separate web viewer; that work became
[ver-por-que](https://teotl.dev/ver-por-que), which consumes por-que's JSON
exports (see the [serialization contract](../guides/serialization.md)).

## Local launch of ver-por-que

`por-que serve PATH_OR_URL` bundles and launches
[ver-por-que](https://teotl.dev/ver-por-que) locally against a freshly
generated export — one command to inspect a file in the browser without a
network round-trip:

```bash
por-que serve data.parquet
```

This starts a local server, opens the webapp pointed at the file's dump, and
serves until interrupted with `Ctrl-C`. Useful flags:

- `--port` / `--host` — bind to a specific address instead of an ephemeral
  local port.
- `--no-browser` — print the URL instead of opening it automatically.
- `--metadata-only` — dump only file metadata, mirroring `dump`'s flag.

A path ending in `.json` is served as-is (skipping parquet parsing entirely),
so you can also point `serve` at a dump produced earlier by `dump`.

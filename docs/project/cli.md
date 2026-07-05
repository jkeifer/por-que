# CLI

por-que ships a command-line interface for inspecting parquet files: a
"parquet file microscope" that starts at a high-level overview and zooms
progressively into the physical details — file → row group → column → page.
Every command accepts a local path or an unauthenticated HTTP(S) URL, and
remote reads fetch only the bytes each command actually needs.

## Installation

The CLI is an optional extra so the core library stays dependency-light:

```bash
pip install 'por-que[cli]'
```

The `por-que` console script is always installed, but without the extra it
prints a one-line hint instead of a traceback.

## Global options

- `--verbose` / `-v` — show diagnostic output (e.g. which source is read).
- `--quiet` / `-q` — suppress non-essential output, including the educational
  hints some commands print.

## Commands

### `schema`

Show the schema tree with physical types, repetition, and logical types:

```bash
por-que schema data.parquet
```

### `meta`

Show a file-level summary and key-value metadata:

```bash
por-que meta data.parquet
por-que meta data.parquet --key pandas   # print one raw key-value entry
```

`--key` / `-k` prints the raw value of a single key-value metadata entry to
stdout, handy for piping embedded JSON (like pandas metadata) into `jq`.

### `row-groups`

Show a per-row-group table of row counts and sizes:

```bash
por-que row-groups data.parquet
por-que row-groups data.parquet --column trip_distance
```

`--column` / `-c` adds converted min/max/null statistics for one column path.

### `pages`

Show page-level structure for a single column via selective loading — only
that column's pages are read, which is the columnar-format punchline:

```bash
por-que pages data.parquet --column trip_distance
por-que pages data.parquet --column trip_distance --row-group 0
```

### `dump`

Dump the full JSON serialization to stdout:

```bash
por-que dump data.parquet > dump.json
por-que dump data.parquet --metadata-only   # skip page structure
```

The output follows the canonical dump schema (see the
[serialization contract](../guides/serialization.md)); enums are serialized
by name (`"codec": "SNAPPY"`), so dumps are self-describing.

To visualize a dump in the browser, feed it to
[ver-por-que](https://teotl.dev/ver-por-que), a standalone web viewer
that consumes por-que's JSON dumps (source at
[github.com/jkeifer/ver-por-que](https://github.com/jkeifer/ver-por-que)).

# ver-por-que: Parquet Explorer Web App

See why parquet with por-que, okay?

Pairs with [the python package por-que](https://pypi.org/project/por-que)
([github repo here](https://github.com/jkeifer/por-que)).

A browser-based Parquet file explorer that visualizes the physical structure of
Parquet files. This application allows you to examine how Parquet files are
organized on disk, including the layout of row groups, column chunks, and data
pages.

> [!WARNING]
> This is utterly and completely vibe-coded. It is crap. I have not had a
> chance to review the code and fix the issues, which I know to be plentiful.
> YYMV until that happens

## Features

- **Drag & Drop Interface**: Simply drag a por-que JSON structure dump onto the
  browser window
- **Physical File Structure Visualization**: Visual representation of how bytes
  are organized in the Parquet file
- **Interactive Exploration**: Click on segments to see detailed information
  about row groups, columns, and pages
- **Byte-level Analysis**: Understand compression ratios, encoding types, and
  storage characteristics

## How It Works

You can feed the app two things, and it handles both:

- **A raw `.parquet` file** — dropped, picked, or pointed at via `?url=`. The
  app boots por-que itself in the browser (Python compiled to WebAssembly via
  [pyodide](https://pyodide.org/), running in a Web Worker) and produces the
  same structure dump `por-que dump` would, with no server involved.
- **A por-que dump JSON** — produced by `por-que dump file.parquet`, if you'd
  rather run the parse yourself.

Either way you then explore the visual representation of the file's physical
structure. Detection is by magic bytes (`PAR1`), so extension doesn't matter.

### In-browser parsing notes

- The first parquet parse downloads the ~12MB Python runtime from a CDN (cached
  afterward); the JSON path pays none of that cost.
- Page **content** decompression is not needed for the structure dump, so the
  missing wasm build of Snappy doesn't matter — a SNAPPY-compressed file dumps
  fine. (This only affects future value-reconstruction features, not structure.)

## Technology Stack

- **TypeScript (ESM)**: Strict, framework-free modules bundled natively by
  Parcel
- **Parcel**: Fast, zero-configuration bundler for development and production
  builds
- **Vitest**: Unit tests for the domain/business logic
- **Modern CSS**: Responsive design for an optimal viewing experience

## Getting Started

### Running Locally

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for development setup instructions.

### Using the Live App

Visit
[https://jkeifer.github.io/ver-por-que](https://jkeifer.github.io/ver-por-que)
to use the deployed version.

### Workflow

1. **Generate metadata with por-que**:

   ```bash
   pip install por-que
   por-que dump your-file.parquet > metadata.json
   ```

1. **Load the JSON file** into this application via:

   - Drag and drop onto the browser window
   - File picker (click the drop zone)
   - URL input (for remote JSON files)

1. **Explore the structure**:

   - View the physical layout of the file as a visual byte map
   - Click on different segments to see details about row groups, columns, and pages
   - Understand how your data is compressed and encoded

   The app can also auto-load a dump on startup via a `?url=` query parameter
   (e.g. `/?url=data.json`), which is how `por-que serve` opens a file.

## Schema-generated types

The dump JSON shape is defined by the canonical JSON Schema at
[`schema/por-que.schema.json`](./schema/por-que.schema.json). TypeScript types
and a standalone runtime validator are generated from it into `src/generated/`
(gitignored) by `npm run generate`. The `dev`, `build`, `test`, `typecheck`,
and `lint` scripts all run `generate` first, so a fresh clone just works — but
if you edit the schema, re-run `npm run generate`.

## License

Apache License 2.0, same as the por-que repository it lives in (see the root `LICENSE`).

## Resources

- [Por Que Library](https://github.com/jkeifer/por-que)
- [Apache Parquet Format](https://parquet.apache.org/)

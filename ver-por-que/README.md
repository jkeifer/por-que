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

1. Use the [por-que Python library](https://github.com/jkeifer/por-que) to
   analyze a Parquet file and export its structure to JSON
1. Load the JSON file into this web application (via drag-and-drop, file
   picker, or URL)
1. Explore the visual representation of your Parquet file's physical structure

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

ISC license.

## Resources

- [Por Que Library](https://github.com/jkeifer/por-que)
- [Apache Parquet Format](https://parquet.apache.org/)

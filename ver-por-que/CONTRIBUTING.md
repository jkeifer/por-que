# Contributing to ver-por-que

Thank you for your interest in contributing to ver-por-que! This guide will
help you get started with development and contributing to the project.

## 🚀 Quick Start

### Prerequisites

- Node.js (v24 recommended; matches CI)
- npm
- Git

### Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/jkeifer/ver-por-que.git
   cd ver-por-que
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Start the development server**

   ```bash
   npm run dev
   ```

   The application will, by default, be available at
   [`http://localhost:1234`](http://localhost:1234)

4. **Test the application**

   You'll need a JSON file from the [por-que Python
   library](https://github.com/jkeifer/por-que) to test:

   ```bash
   pip install por-que
   por-que dump your-file.parquet > test-metadata.json
   ```

   Then drag the `test-metadata.json` file into the browser window.

## 🏗️ Development Workflow

### Available Commands

| Command                | Description                                        |
| ---------------------- | ------------------------------------------------- |
| `npm run generate`     | Generate types + validator from the JSON Schema   |
| `npm run wheel`        | Build the por-que wheel for the in-browser parser |
| `npm run dev`          | Start development server with hot reload          |
| `npm run build`        | Build for production                              |
| `npm run typecheck`    | Type-check with `tsc --noEmit`                    |
| `npm test`             | Run unit tests with Vitest                        |
| `npm run lint`         | Check code style and quality                      |
| `npm run lint:fix`     | Auto-fix linting issues                           |
| `npm run format`       | Format code with Prettier                         |
| `npm run format:check` | Check code formatting                             |

`dev`, `build`, `test`, `typecheck`, and `lint` each run `generate` first (via
npm pre-hooks), so a fresh clone works without a manual step. `generate` reads
[`../src/por_que/dump-schema.json`](../src/por_que/dump-schema.json) — the
canonical contract for the dump JSON (a union of the `file` and `metadata`
roots) — and emits `src/generated/` (gitignored):

- `por-que.d.ts` — TypeScript types ([json-schema-to-typescript])
- `validate.js` / `validate.d.ts` — standalone AJV validators (one per root)
  used at the load boundary in `main.ts`

Edit the schema, not the generated files. Re-run `npm run generate` after any
schema change.

[json-schema-to-typescript]: https://github.com/bcherny/json-schema-to-typescript

### In-browser parquet parsing

Dropping a raw `.parquet` file runs por-que in the browser via
[pyodide](https://pyodide.org/) in a Web Worker (`src/js/worker/`). The worker
needs a por-que wheel as a static asset:

```bash
npm run wheel   # builds the wheel into static/vendor/ (gitignored), once
```

- The wheel is built from the sibling python package (`uv build --wheel` with
  `POR_QUE_NO_WEBAPP=1`, which stops the python build from recursing back into
  this webapp) by [`scripts/build-webapp-wheel.py`](../scripts/build-webapp-wheel.py).
- Without the wheel, the JSON path still works and the pyodide integration test
  skips. You only need `npm run wheel` to exercise browser parquet parsing (dev
  or the integration test).
- pyodide itself loads from a CDN, pinned to one version constant in
  `src/js/worker/worker.ts` (keep it equal to the `pyodide` devDep).
- The parse never decompresses page content, so the lack of a wasm Snappy wheel
  is irrelevant to the structure dump. Static assets under `static/` are copied
  into the build (and served in dev) by `parcel-reporter-static-files-copy`.

### Pre-commit Hooks

The project uses Husky and lint-staged to run quality checks before commits:

- **ESLint**: Code quality and style checks
- **Prettier**: Code formatting

If pre-commit hooks fail, fix the issues before committing:

```bash
npm run lint:fix
npm run format
```

## 🏛️ Architecture Overview

### Project Structure

```plaintext
../src/por_que/dump-schema.json  # Canonical contract for the dump JSON (in the py package)
src/
├── index.html             # Main HTML entry point (loads main.ts as a module)
├── css/                   # Styles
├── main.ts                # Entry point; JSON.parse → AJV validate → typed dump
├── detect.ts              # Parquet-vs-JSON detection by magic bytes
├── format.ts              # Shared byte/number formatting helpers
├── types.ts               # Friendly aliases over the schema-generated types
├── generated/             # GENERATED (gitignored): por-que.d.ts + validate.js
├── js/worker/             # In-browser parquet parsing (pyodide in a Web Worker)
│   ├── client.ts          # Main-thread handle; lazily spins up the worker
│   ├── worker.ts          # Worker shell (loads pyodide from the CDN)
│   ├── pyodide-parquet.ts # Worker-agnostic boot+parse (testable under node)
│   └── protocol.ts        # Request/response message shapes
├── domain/
│   └── parquet-type-resolver.ts # Logical-type pretty-printing / display logic
├── business/
│   ├── segment-tree.ts              # projectDump(): dump → SegmentNode tree
│   └── segment-layout-calculator.ts # Calculates byte positions
├── components/
│   ├── info-panel-manager.ts       # Declarative kind → panel sections registry
│   └── svg-byte-visualizer.ts      # Byte visualization renderer
└── config/
    └── visualization-config.ts     # Layout constants + kind → color map
```

Everything is TypeScript ESM using real `import`/`export`; there is no
global-script sharing. Unit tests live in `test/` and run under Vitest.

### Key Components

#### ParquetExplorer (`main.ts`)

- Main application controller; loads files (local, URL, or `?url=`)
- Validates the dump against the schema at the boundary, then trusts the shape
- Coordinates between components

#### projectDump (`business/segment-tree.ts`)

- One recursive pass turns a validated dump into a `SegmentNode` tree
- Every span is a REAL byte offset off the wire — nothing is estimated
- A node's `kind` (a string-literal union) says what it is; its `children` are
  the next drill-down level

#### SvgByteVisualizer (`components/svg-byte-visualizer.ts`)

- Renders the tree: each level is a node's children, colored by `kind`
- Handles user interactions (clicks to drill down, hovers)

#### InfoPanelManager (`components/info-panel-manager.ts`)

- A `Record<Kind, (node, dump) => Section[]>` registry plus one renderer

#### SegmentLayoutCalculator (`business/segment-layout-calculator.ts`)

- Calculates byte positions and sizes for the visual layout

## 🎯 Contributing Guidelines

### Code Style

The project uses ESLint and Prettier with the following conventions:

#### Code Conventions

- **TypeScript (strict)**: No framework dependencies; `npm run typecheck` must pass
- **ESM**: Real `import`/`export`, no global-script sharing
- **Descriptive variable names** over comments
- **Separation of concerns**: Domain, business, and UI layers

### Making Changes

1. **Create a feature branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**

   - Follow existing code patterns
   - Keep changes focused and atomic
   - Test your changes manually with sample Parquet files

3. **Test your changes**

   ```bash
   npm run lint
   npm run build
   ```

   Test the built application in `dist/` directory.

4. **Commit your changes**

   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

   Pre-commit hooks will run automatically.

5. **Push and create a pull request**

   ```bash
   git push origin feature/your-feature-name
   ```

### Pull Request Process

1. **Ensure code quality**: All linting and formatting checks must pass
2. **Test manually**: Verify your changes work with real Parquet files
3. **Update docs**: Update README or this file if adding features
4. **Describe changes**: Provide clear description in PR

## 🚀 Deployment

### GitHub Pages Deployment

The site automatically deploys to GitHub Pages on push to main:

1. **Push to main** (or merge a PR)
2. **GitHub Actions** builds and deploys automatically
3. [**Visit** the live site](https://jkeifer.github.io/ver-por-que)

The deployment workflow is in `.github/workflows/deploy.yml`.

### Build Process

```bash
npm run build              # Production build (runs generate + get-git-info.js first)
```

Output goes to `dist/` directory with assets at `./` public URL.

## 🐛 Debugging & Troubleshooting

### Common Issues

#### Development Server Won't Start

```bash
rm -rf .parcel-cache
npm run dev
```

#### Linting Errors

```bash
npm run lint:fix          # Auto-fix most issues
npm run format            # Format code
```

#### Build Issues

```bash
rm -rf .parcel-cache dist  # Clear caches
npm run build              # Rebuild
```

#### JSON File Won't Load

- Ensure the JSON file is from a recent version of por-que
- Check browser console for errors
- Verify the JSON structure matches expected format

## 🧪 Testing

Unit tests run under [Vitest](https://vitest.dev/) (`npm test`) and live in
`test/`. They cover the pure logic: formatting helpers, the segment layout
calculator, and the tree projection (`projectDump`) — the projection tests read
real dump fixtures from `../tests/fixtures/metadata/`, assert the validator
accepts them and rejects mutations, and check the tree has real offsets, sorted
children, and correct `kind` coverage. New logic in those layers should come
with a focused test.

`test/pyodide-parquet.integration.test.ts` is the end-to-end check for
in-browser parsing: it boots real pyodide, installs the locally-built wheel,
parses a real parquet file, and asserts the dump passes the validator. It skips
(with a message) when the wheel is absent, so run `npm run wheel` first — CI
does. It downloads a parquet fixture from `apache/parquet-testing` pinned to the
same ref as the python fixtures, and has a long timeout.

Welcome additions:

- Visual regression tests for the byte visualizer

## 🤝 Community

- **Issues**: Report bugs or request features on [GitHub
  Issues](https://github.com/jkeifer/ver-por-que/issues)
- **Discussions**: Use GitHub Discussions for questions and ideas

## 📋 Development Tips

### Testing with Different Parquet Files

Generate test files with different characteristics:

```bash
# Simple file
por-que dump simple.parquet > simple.json

# File with multiple row groups
por-que dump large-file.parquet > multi-rowgroup.json

# File with nested schema
por-que dump nested.parquet > nested.json
```

### Debugging Visualization Issues

- Use browser DevTools to inspect SVG elements
- Check the console for layout calculation logs
- Verify segment data structure in `ParquetExplorer.parquetData`

### Performance Considerations

- Large Parquet files (many row groups/columns) can create complex
  visualizations
- Consider viewport optimization for files with thousands of segments
- SVG performance may degrade with very detailed visualizations

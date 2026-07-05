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

| Command                | Description                               |
| ---------------------- | ----------------------------------------- |
| `npm run dev`          | Start development server with hot reload  |
| `npm run build`        | Build for production                      |
| `npm run typecheck`    | Type-check with `tsc --noEmit`            |
| `npm test`             | Run unit tests with Vitest                |
| `npm run lint`         | Check code style and quality              |
| `npm run lint:fix`     | Auto-fix linting issues                   |
| `npm run format`       | Format code with Prettier                 |
| `npm run format:check` | Check code formatting                     |

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
src/
├── index.html             # Main HTML entry point (loads main.ts as a module)
├── css/                   # Styles
├── main.ts                # Application entry point and main controller
├── format.ts              # Shared byte/number formatting helpers
├── types.ts               # Interfaces for the por-que JSON dump shapes
├── domain/                # Core domain models and logic
│   ├── parquet-constants.ts     # Parquet format constants
│   ├── parquet-type-resolver.ts # Type resolution logic
│   └── parquet-segment.ts       # Segment domain model
├── business/              # Business logic layer
│   ├── segment-hierarchy-builder.ts  # Builds segment hierarchy
│   └── segment-layout-calculator.ts  # Calculates byte positions
├── components/            # UI components
│   ├── info-panel-manager.ts       # Manages info panels
│   └── svg-byte-visualizer.ts      # Byte visualization renderer
└── config/
    └── visualization-config.ts     # Visualization settings
```

Everything is TypeScript ESM using real `import`/`export`; there is no
global-script sharing. Unit tests live in `test/` and run under Vitest.

### Key Components

#### ParquetExplorer (`app.js`)

- Main application controller
- Handles file loading (local files and URLs)
- Manages application state
- Coordinates between components

#### SVGByteVisualizer (`svg-byte-visualizer.js`)

- Renders the visual representation of the Parquet file structure
- Displays row groups, column chunks, and data pages as colored segments
- Handles user interactions (clicks, hovers)

#### SegmentHierarchyBuilder (`business/segment-hierarchy-builder.js`)

- Transforms por-que JSON into a hierarchical segment structure
- Builds the tree of file → row groups → columns → pages

#### SegmentLayoutCalculator (`business/segment-layout-calculator.js`)

- Calculates byte positions and sizes for visual layout
- Determines colors and labels for segments

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
npm run build              # Production build (runs get-git-info.js first)
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
`test/`. They cover the pure domain/business logic: formatting helpers, the
segment layout calculator, the `ParquetSegment` model, and the segment
hierarchy builder. New logic in those layers should come with a focused test.

Welcome additions:

- Integration tests for file loading and parsing
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

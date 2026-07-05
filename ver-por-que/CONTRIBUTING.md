# Contributing to ver-por-que

Thank you for your interest in contributing to ver-por-que! This guide will
help you get started with development and contributing to the project.

## 🚀 Quick Start

### Prerequisites

- Node.js (v16 or higher)
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

| Command             | Description                                      |
| ------------------- | ------------------------------------------------ |
| `npm run dev`       | Start development server with hot reload        |
| `npm run build`     | Build for production                             |
| `npm run build:dev` | Build for development (unminified)               |
| `npm test`          | Run all tests                                    |
| `npm run lint`      | Check code style and quality                     |
| `npm run lint:fix`  | Auto-fix linting issues                          |
| `npm run format`    | Format code with Prettier                        |
| `npm run format:check` | Check code formatting                         |

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
├── index.html             # Main HTML entry point
├── css/                   # Styles
└── js/
    ├── app.js             # Application entry point and main controller
    ├── file-adapter.js    # Handles file/URL loading
    ├── domain/            # Core domain models and logic
    │   ├── parquet-constants.js    # Parquet format constants
    │   ├── parquet-type-resolver.js # Type resolution logic
    │   └── parquet-segment.js      # Segment domain model
    ├── business/          # Business logic layer
    │   ├── segment-hierarchy-builder.js  # Builds segment hierarchy
    │   └── segment-layout-calculator.js  # Calculates byte positions
    ├── components/        # UI components
    │   ├── file-structure-analyzer.js  # Analyzes file structure
    │   ├── info-panel-manager.js       # Manages info panels
    │   ├── svg-byte-visualizer.js      # Byte visualization renderer
    │   ├── schema-tree.js              # Schema tree viewer
    │   └── column-browser.js           # Column browser component
    └── config/
        └── visualization-config.js     # Visualization settings
```

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

- **Vanilla JavaScript**: No framework dependencies
- **ES6 modules**: Modern JavaScript features
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
npm run build              # Production build with git info
npm run build:dev          # Development build (faster)
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
npm run build:dev          # Build without minification for debugging
```

#### JSON File Won't Load

- Ensure the JSON file is from a recent version of por-que
- Check browser console for errors
- Verify the JSON structure matches expected format

## 🧪 Testing

Currently, the project uses manual testing. We welcome contributions to add
automated tests!

Potential testing additions:

- Unit tests for business logic (segment hierarchy, layout calculations)
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

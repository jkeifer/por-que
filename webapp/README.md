# Parquet Explorer Web App

A browser-based Parquet file explorer powered by Pyodide and the `por_que` library. This application allows you to examine Parquet file structure and metadata directly in your web browser without requiring a server.

## Features

- **Drag & Drop Interface**: Simply drag a Parquet file onto the browser window
- **Remote File Support**: Load Parquet files from URLs (CORS permitting)
- **Interactive Schema Explorer**: Browse the hierarchical schema with expand/collapse functionality
- **File Overview**: View key statistics including file size, compression ratios, and type distributions
- **Row Group Analysis**: Examine row group distribution and characteristics
- **Column Details**: Deep dive into individual column metadata and statistics
- **Zero Installation**: Runs entirely in the browser using WebAssembly

## Technology Stack

- **Pyodide**: Runs Python directly in the browser via WebAssembly
- **Por Que Library**: Pure Python Parquet parser for educational purposes
- **D3.js**: Interactive data visualizations
- **Vanilla JavaScript**: No framework dependencies for maximum compatibility
- **Modern CSS**: Responsive design with dark/light mode support

## Getting Started

### Development Server

Since the app uses Pyodide, you need to serve it over HTTP (not file://) due to CORS restrictions:

```bash
# Navigate to the webapp directory
cd webapp

# Start a simple HTTP server
python -m http.server 8000

# Open http://localhost:8000 in your browser
```

### Usage

1. **Load a File**:
   - Drag and drop a `.parquet` file onto the drop zone
   - Or click to browse and select a file
   - Or enter a URL to a remote Parquet file

2. **Explore the Structure**:
   - **Overview**: Quick file statistics and visualizations
   - **Schema**: Interactive tree view of the schema structure
   - **Row Groups**: Analysis of row group distribution
   - **Columns**: Detailed information about individual columns
   - **Pages**: Examine page-level details
   - **Metadata**: Raw metadata browser with search functionality

3. **Export Data**:
   - Use the "Export JSON" button to download the parsed structure

## Browser Support

- Chrome 90+ (recommended)
- Firefox 88+
- Safari 14+
- Edge 90+

**Requirements:**
- WebAssembly support
- ES6+ JavaScript support
- File API and Drag & Drop API support

## File Size Limitations

Since the app runs entirely in the browser, there are practical limits:
- **Recommended**: Files under 100MB
- **Maximum**: Limited by available browser memory (typically 2-4GB)
- Large files may take several seconds to parse

## Architecture

### Core Components

- `index.html`: Main application structure
- `js/app.js`: Application orchestration and Pyodide integration
- `js/file-adapter.js`: Browser File API adapter for Python compatibility
- `js/components/`: Individual UI components (schema tree, overview, etc.)
- `js/utils/`: Utility functions for formatting and data processing
- `css/styles.css`: Comprehensive styling with responsive design

### Pyodide Integration

The app creates a bridge between JavaScript File objects and Python's file-like interface:

```javascript
// JavaScript file adapter
class BrowserFileAdapter {
    async read(size) { /* Use file.slice() */ }
    seek(offset, whence) { /* Update position */ }
    tell() { return this.position; }
}

// Python wrapper
class ReadableSeekableAdapter:
    def __init__(self, js_adapter):
        self.js_adapter = js_adapter

    def read(self, size=-1):
        # Bridge to JavaScript
        return self.js_adapter.read(size)
```

## Development

### Adding New Features

1. **New Visualizations**: Add components to `js/components/`
2. **Data Processing**: Extend utilities in `js/utils/`
3. **UI Improvements**: Modify `css/styles.css`
4. **Python Integration**: Enhance the Pyodide bridge in `js/app.js`

### Testing

Test with various Parquet files:
- Different schemas (nested, flat, mixed types)
- Various encodings and compression algorithms
- Files from different generators (Spark, pandas, etc.)
- Different sizes (small test files to larger datasets)

## Limitations

### Current Limitations
- Uses a simplified version of por_que for the demo
- No actual data value display (metadata and structure only)
- Limited error handling for malformed files
- Basic visualizations (could be enhanced with more D3.js features)

### Future Enhancements
- Full por_que library integration
- Advanced visualizations (file layout diagrams, compression analysis)
- Column value sampling and histograms
- File comparison features
- Export to various formats
- Shareable visualization links

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test your changes with various Parquet files
4. Ensure responsive design works on mobile
5. Submit a pull request

## License

This project is part of the por_que library and follows its licensing terms.

## Resources

- [Por Que Library](https://github.com/jkeifer/por-que)
- [Pyodide Documentation](https://pyodide.org/)
- [Apache Parquet Format](https://parquet.apache.org/)
- [D3.js Documentation](https://d3js.org/)

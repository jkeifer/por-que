# CLI Implementation Plan

## Overview
This document provides a step-by-step implementation plan for the por-que CLI tool, organized into phases that align with the milestones defined in the CLI_DESIGN.md document.

**Note:** Interactive exploration features (TUI explorer, binary view mode) have been removed in favor of a separate web-based viewer. The CLI focuses on data extraction and export, while the webapp handles visualization and exploration. See WEB_VIEWER_IMPLEMENTATION.md for the webapp plan.

## Key Risks and Dependencies
This plan is subject to the following risks and dependencies that must be managed for successful delivery.

- **Dependency on Core Parser:** Phases 4.4 (Data Sampling) and 5 (Advanced Analysis) are entirely dependent on the availability of a core page data parsing engine. The implementation of these features cannot begin until that parser is delivered and integrated.
- **Scope Management:** The plan is ambitious. The "Polish and Educational Features" in Phase 6, while important, can lead to scope creep. These features should be added incrementally throughout the development process rather than being treated as a final, monolithic block of work.

## Phase 1: Project Setup and Core Infrastructure

### 1.1 Refactor Existing CLI Structure
- [ ] Rename existing `metadata` command group to align with new design
- [ ] Explicitly define and document the new command hierarchy (e.g., `por-que inspect <subcommand>`)
- [ ] Move existing commands under the new `inspect` group
- [ ] Update `_cli.py` to support the new command hierarchy
- [ ] Ensure backward compatibility is not needed (as specified)

### 1.2 Environment Variable and Configuration Support
- [ ] Add click option decorator wrapper that supports environment variables
- [ ] Implement cache-related environment variables:
  - [ ] POR_QUE_CACHE_ENABLED (default: true)
  - [ ] POR_QUE_CACHE_DIR (custom cache location)
  - [ ] POR_QUE_CACHE_MAX_SIZE (cache size limit)
- [ ] Implement output control environment variables:
  - [ ] POR_QUE_VERBOSE (verbose output)
  - [ ] POR_QUE_QUIET (quiet mode)
  - [ ] POR_QUE_OUTPUT_FORMAT (default output format)
  - [ ] POR_QUE_BINARY_MODE (default binary view)
- [ ] Implement command-specific environment variables:
  - [ ] POR_QUE_DIFF_SCHEMA_ONLY (default diff mode)
- [ ] Add short option forms for all long options
- [ ] Ensure precedence: CLI flag > environment variable > default

### 1.3 Output Formatting Infrastructure
- [ ] Extend existing formatters.py with rich-based formatting
- [ ] Add base formatter classes for consistent output
- [ ] Implement format registry for human and json formats only
- [ ] Add `--format` flag support to all output-producing commands
- [ ] Define consistent color coding scheme using rich
- [ ] Ensure existing ParquetFileType parameter type works with new commands

## Phase 2: Core Metadata Inspection (Milestone 1)

### 2.1 Basic File Inspection
- [ ] Implement `por-que inspect <file>` command
- [ ] Parse and display file header/footer magic bytes
- [ ] Show file size and basic layout
- [ ] Display row group count and sizes
- [ ] Show metadata size information

### 2.2 Schema Viewer
- [ ] Implement `por-que inspect <file> schema` command
- [ ] Parse schema from file metadata
- [ ] Create tree visualization for nested schemas
- [ ] Display column types and repetition levels
- [ ] Show logical types alongside physical types

### 2.3 Metadata Viewer
- [ ] Implement `por-que inspect <file> metadata` command
- [ ] Extract and display key-value metadata
- [ ] Show file creation information (created_by, version)
- [ ] Display compression codecs per column
- [ ] Show encoding information


## Phase 3: Remote File Support and Caching

### 3.1 Cache Infrastructure
- [ ] Integrate platformdirs for XDG-compliant cache directories
- [ ] Set up diskcache backend
- [ ] Implement cache key generation from URLs
- [ ] Create cache configuration management

### 3.2 Enhanced HTTP Support
- [ ] Extend existing HttpFile class with ETag and Last-Modified header support
- [ ] Add retry logic for network failures to HttpFile
- [ ] Implement progress indicators for downloads
- [ ] Create persistent cache wrapper around HttpFile using diskcache

### 3.3 Cache Management Commands
- [ ] Implement `por-que cache` command group
- [ ] Implement `por-que cache status <url>` command
- [ ] Implement `por-que cache clear [<url>|--all]` command
- [ ] Implement `por-que cache config --max-size <size>` command
- [ ] Implement `por-que cache info` command to show cache location and settings
- [ ] Add verbose mode logging for cache hits/misses
- [ ] Respect --no-cache flag and POR_QUE_CACHE_ENABLED env var

### 3.4 Intelligent Caching Strategy
- [ ] Cache file metadata (footer) indefinitely by default
- [ ] Implement on-demand caching for row group headers
- [ ] Add LRU eviction for column chunks
- [ ] Always cache dictionary pages when accessed
- [ ] Respect HTTP cache headers
- [ ] Disable all caching when --no-cache or POR_QUE_CACHE_ENABLED=false

## Phase 4: Data Inspection and Sampling (Milestone 2)

### 4.1 Row Group Inspection
- [ ] Implement `por-que inspect <file> row-group <N>` command
- [ ] Display row count and size information
- [ ] Show column chunk details (offsets, sizes, encodings)
- [ ] Display statistics for each column
- [ ] Add compression ratio calculations

### 4.2 Column Chunk Inspection
- [ ] Implement `por-que inspect <file> row-group <N> column <NAME>` command
- [ ] Show detailed column metadata
- [ ] Display page information summary
- [ ] Show encoding and compression details
- [ ] Display column statistics

### 4.3 Page-Level Inspection
- [ ] Implement `por-que inspect <file> row-group <N> column <NAME> page <N>` command
- [ ] Display page header information
- [ ] Show page type (data, dictionary, index)
- [ ] Display compression and encoding details
- [ ] Show value counts and sizes

### 4.4 Data Sampling
- [ ] Implement `por-que sample <file>` command with subcommand structure:
  - [ ] `por-que sample <file>` - sample from whole file
  - [ ] `por-que sample <file> row-group <N>` - sample from specific row group
  - [ ] `por-que sample <file> column <names>` - sample specific columns
  - [ ] `por-que sample <file> row-group <N> column <names>` - sample specific columns from specific row group
- [ ] Add `--rows N` option for sample size
- [ ] Support comma-separated column names
- [ ] Handle column names with spaces/special characters (require quotes)
- [ ] Create table formatter for sample data
- [ ] Handle various data types properly in display
- [ ] Add validation for invalid row group indices
- [ ] Add validation for non-existent column names

## Phase 5: Advanced Analysis Tools (Milestone 3 - Requires Page Data Parsing)

### 5.1 Performance Profiler
- [ ] Implement `por-que profile <file>` command with subcommand structure:
  - [ ] `por-que profile <file>` - profile entire file
  - [ ] `por-que profile <file> row-group <N>` - profile specific row group
  - [ ] `por-que profile <file> column <names>` - profile specific columns
  - [ ] `por-que profile <file> row-group <N> column <names>` - profile specific columns from specific row group
- [ ] Add timing instrumentation to parser operations
- [ ] Track I/O operations and byte counts
- [ ] Measure decompression time per column
- [ ] Measure decoding time per column
- [ ] Create performance summary visualization
- [ ] Add validation for invalid row group/column references

### 5.2 File Comparison Tool
- [ ] Implement `por-que diff <file1> <file2>` command
- [ ] Compare schemas (additions, deletions, type changes)
- [ ] Compare metadata (compression, encodings, statistics)
- [ ] Compare row group structures
- [ ] Create side-by-side diff visualization
- [ ] Add `--schema-only` option

### 5.3 Export Functionality
- [ ] Implement `por-que export <file> --format <format>` command
- [ ] Default output to stdout
- [ ] Add `--output/-o <file>` option for file output
- [ ] Add JSON export format for metadata with byte offsets
- [ ] Ensure JSON includes all structure information for webapp
- [ ] Future formats (separate enhancement):
  - [ ] Add SQL DDL export for schema
  - [ ] Add Avro schema export
  - [ ] Add CSV export for sample data
- [ ] Validate format parameter against supported formats per command


## Phase 6: Polish and Educational Features

### 6.1 Educational Annotations
- [ ] Add contextual explanations to each view
- [ ] Create "Did you know?" tips about Parquet design
- [ ] Add performance implications explanations
- [ ] Include best practices suggestions

### 6.2 Error Handling and User Experience
- [ ] Implement comprehensive error messages
- [ ] Add helpful suggestions for common errors:
  - [ ] Invalid row group index: show valid range
  - [ ] Non-existent column: list available columns
  - [ ] Invalid page index: show valid range
  - [ ] Network errors: suggest cache or retry options
- [ ] Create progress indicators for long operations
- [ ] Add interrupt handling (Ctrl+C)
- [ ] Implement proper cleanup on exit
- [ ] Validate mutually exclusive options early

### 6.3 Documentation and Help
- [ ] Create detailed help text for each command
- [ ] Add examples to help output
- [ ] Create man page
- [ ] Write user guide with common workflows
- [ ] Add inline help in interactive mode

## Testing and Quality Strategy
Testing is a continuous process, not a final phase. Each feature will be accompanied by a suite of tests to ensure correctness and prevent regressions.
- **Concurrent Testing:** Unit and integration tests will be developed alongside each feature in Phases 1-5. A feature is not considered complete until it is fully tested.
- **Unit Tests:** Write unit tests for each command and utility (e.g., cache logic, formatters).
- **Integration Tests:** Create integration tests using a variety of sample Parquet files to validate end-to-end behavior.
- **CI/CD Pipeline:** Implement a CI/CD pipeline to automate testing on every commit.
- **Benchmarks:** Add performance benchmarks to track and prevent regressions in I/O and processing speed.
- **Test Fixtures:** Create a library of test fixtures for various Parquet features (encodings, compression types, nested schemas).

## Implementation Order Recommendations

1. **Start with Phase 1-3**: These provide the foundation and can be implemented without the page data parsing functionality.

2. **Implement Phase 4.1-4.3**: These can use existing metadata parsing without requiring actual data decoding.

3. **Defer Phase 4.4 and Phase 5**: These are blocked until the core page data parser is available.

4. **Complete Phase 6**: Polish and educational features should be added incrementally throughout development.

## Future Enhancements

### Additional Output Formats
After the initial implementation with human/json formats:
- [ ] CSV format for `sample` command - output data in CSV format
- [ ] Table format using rich tables for structured output
- [ ] SQL DDL format for `export` command - generate CREATE TABLE statements
- [ ] Avro schema format for `export` command - convert Parquet schema to Avro
- [ ] CSV format for `export` command - export sample data as CSV
- [ ] Format-specific options (e.g., CSV delimiter, quoting rules)

## Dependencies

### Required Libraries
- `click`: Command-line interface creation (already in use)
- `rich`: Terminal UI rendering
- `platformdirs`: Cross-platform cache directory handling
- `diskcache`: Caching backend for persistent storage

### Optional Libraries
- `pytest`: Testing framework
- `click-testing`: CLI testing utilities
- `hypothesis`: Property-based testing
- `tox`: Test automation

## Integration with Existing Code

### Existing Components to Leverage
- `ParquetFileType`: Already handles file/URL conversion
- `HttpFile`: Already provides HTTP range request support
- `formatters.py`: Base for output formatting (needs rich integration)
- `MetadataContext`: Pattern for passing data between commands

### New Components Needed
- Cache wrapper for HttpFile
- Rich-based formatters
- Environment variable support decorators

## Success Criteria

- [ ] All commands work with both local and remote files
- [ ] JSON output is available for all inspection commands
- [ ] Cache management is transparent and efficient
- [ ] Export command produces comprehensive JSON for webapp visualization
- [ ] Performance profiling provides actionable insights
- [ ] Educational content helps users understand Parquet internals
- [ ] Error messages are helpful and guide users to solutions
- [ ] Documentation is comprehensive and includes examples
- [ ] The codebase has a comprehensive, automated test suite
- [ ] Format support matches the table in CLI_COMMAND_REFERENCE.md

## Developer Implementation Notes

### Option Parsing Order
- Parse global options first
- Then parse command-specific options
- Apply environment variables before parsing CLI

### Validation
- `--quiet` and `--verbose` are mutually exclusive
- `--format` values depend on the command (see format support table)
- Cache options only apply to remote files
- Subcommand order must follow hierarchy
- Row group indices must be valid (0 to N-1)
- Column names must exist in schema
- Page indices must be valid for the column
- Column names with spaces/special characters require quotes

### Help Text
- Show applicable environment variables in help
- Show current values from environment
- Indicate which options support env vars

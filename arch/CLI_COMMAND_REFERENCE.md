# Complete CLI Command Reference

## Global Options
```bash
por-que [--verbose|-v] [--quiet|-q] <command>
```

- `--verbose` / `-v`: Show detailed information including cache hits/misses
- `--quiet` / `-q`: Suppress non-essential output

## File-specific Options
For commands that take a file argument:
- `--no-cache` / `-n`: Disable caching for remote files

## Output Format Option
For all commands that produce output:
- `--format` / `-f`: Output format - default: human
  - Initially supported: `human`, `json`
  - Future formats: `csv`, `table`, etc.

## Core Commands

### Basic Commands
```bash
por-que version                                    # Show version information
por-que --help                                     # Show help
```

### File Inspection Commands
```bash
# High-level file structure overview
por-que inspect <file>

# Detailed schema tree view
por-que inspect <file> schema

# Key-value metadata pairs
por-que inspect <file> metadata

# Column-level metadata and encoding information
por-que inspect <file> columns

# File statistics and compression info
por-que inspect <file> stats

# Details of specific row group
por-que inspect <file> rowgroup <N>

# Details of specific column chunk
por-que inspect <file> rowgroup <N> column <NAME>

# Details of specific page
por-que inspect <file> rowgroup <N> column <NAME> page <N>

```

### Data Sampling
```bash
# Sample from the whole file
por-que sample <file> [--rows <N>]

# Sample from a specific row group
por-que sample <file> rowgroup <N> [--rows <N>]

# Sample specific columns from all row groups
por-que sample <file> column <names> [--rows <N>]

# Sample specific columns from a specific row group
por-que sample <file> rowgroup <N> column <names> [--rows <N>]

# Options:
# --rows <N> / -r <N>: Number of rows to sample (default: 10)
```

### Cache Management Commands
```bash
por-que cache info                                 # Show cache location and settings
por-que cache status <url>                         # Show cache status for specific file
por-que cache clear <url>                          # Clear cache for specific file
por-que cache clear --all                          # Clear entire cache
por-que cache config --max-size <size>             # Set cache size limit (e.g., 2GB)
```

### Advanced Analysis Commands (Phase 5 - requires page parsing)

# Performance profiling
```bash
# Profile the entire file
por-que profile <file>

# Profile a specific row group
por-que profile <file> rowgroup <N>

# Profile specific columns across all row groups
por-que profile <file> column <names>

# Profile specific columns from a specific row group
por-que profile <file> rowgroup <N> column <names>
```

# File comparison
```bash
por-que diff <file1> <file2>

# Options:
# --schema-only / -s: Compare only schemas
```

# Export Functionality
```bash
# Export to stdout (the default behavior)
por-que export <file> --format json

# Export to a named file
por-que export <file> --format json --output metadata.json

# Options:
# --output / -o: Path to an output file.
```


## File Input Support

All `<file>` arguments support both:
- Local file paths: `data/customers.parquet`, `/path/to/file.parquet`
- HTTP(S) URLs: `https://example.com/data.parquet`

## Environment Variables

### Cache-Related
```bash
POR_QUE_CACHE_ENABLED=false    # Disable caching globally (default: true)
POR_QUE_CACHE_DIR=/custom/path # Set custom cache directory
POR_QUE_CACHE_MAX_SIZE=5GB     # Set cache size limit
```

### Output Control
```bash
POR_QUE_VERBOSE=true           # Verbose output (same as -v)
POR_QUE_QUIET=true             # Suppress non-essential output (same as -q)
POR_QUE_OUTPUT_FORMAT=json     # Default output format (e.g., json)
```

### Command-Specific
```bash
POR_QUE_DIFF_SCHEMA_ONLY=true  # Default to schema-only for diff command
```

## Example Usage

```bash
# Inspect a local file
por-que inspect data/users.parquet

# Inspect a remote file with caching disabled
por-que inspect https://example.com/large.parquet --no-cache

# View schema in JSON format
por-que inspect data/users.parquet schema --format json

# View detailed column information
por-que inspect data/users.parquet columns


# Sample 20 rows from row group 2
por-que sample data/users.parquet rowgroup 2 --rows 20

# Short form
por-que sample data/users.parquet rowgroup 2 -r 20

# Profile a specific column from a specific row group
por-que profile data/users.parquet rowgroup 0 column user_id

# Profile multiple columns across all row groups
por-que profile data/users.parquet column "user_id,email,created_at"

# Compare two files
por-que diff old_data.parquet new_data.parquet

# Export metadata as JSON to a file
por-que export data/users.parquet --format json --output metadata.json

# Clear cache for a specific remote file
por-que cache clear https://example.com/data.parquet
```

## Command Hierarchy Summary

```
por-que
├── version
├── inspect <file>
│   ├── schema
│   ├── metadata
│   ├── columns
│   ├── stats
│   └── rowgroup <N>
│       └── column <NAME>
│           └── page <N>
├── sample <file>
│   ├── rowgroup <N>
│   │   └── column <names>
│   └── column <names>
├── cache
│   ├── info
│   ├── status <url>
│   ├── clear [<url> | --all]
│   └── config
├── profile <file>
│   ├── rowgroup <N>
│   │   └── column <names>
│   └── column <names>
├── diff <file1> <file2>
└── export <file>
```

## Notes

1. All inspection commands support `--json` for machine-readable output
2. Remote files are automatically cached unless `--no-cache` is specified
3. Commands are designed for progressive disclosure - start with `inspect` and drill down as needed

## Format Support by Command

| Command | human | json | csv | sql | avro |
|---------|-------|------|-----|-----|------|
| inspect | ✓ | ✓ | - | - | - |
| sample  | ✓ | ✓ | Future | - | - |
| profile | ✓ | ✓ | - | - | - |
| diff    | ✓ | ✓ | - | - | - |
| export  | - | ✓ | Future | Future | Future |

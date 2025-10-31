# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`csv2parquet` is a Rust command-line tool for converting CSV files to Parquet format. It supports batch processing, multiple compression algorithms, and extensive configuration options for both CSV parsing and Parquet writing.

## Dependencies

- `polars` (v0.51.0) - DataFrame library with CSV reading and Parquet writing capabilities
  - Features: `lazy`, `parquet`, `csv`
- `clap` (v4.5.51) - Command-line argument parsing with derive macros
- `anyhow` (v1.0.100) - Error handling in application code
- `thiserror` (v2.0.17) - Custom error types in library code

## Code Architecture

The project follows a clean separation between library and binary:

### `src/lib.rs` - Core Library
Contains all business logic that can be reused:
- **Error types**: `ConversionError` enum using `thiserror` for typed errors
- **Compression enum**: Abstracts Polars' compression types with support for:
  - Uncompressed, Snappy, Gzip, Lz4, Zstd (default), Brotli
  - Optional compression levels
- **ConversionOptions**: Configuration struct with sensible defaults
  - CSV parsing: delimiter, quote char, header detection
  - Schema inference: configurable row scan limit
  - Performance: thread count, low memory mode, parallel writing
  - Parquet: compression, row group size, statistics
- **ConversionStats**: Return type with rows processed, output size, duration
- **Core functions**:
  - `convert_csv_to_parquet()` - Main conversion function
  - `read_csv()` - CSV reading with Polars
  - `write_parquet()` - Parquet writing with Polars

### `src/main.rs` - CLI Interface
Thin wrapper around library that handles:
- **Clap CLI**: Derives command-line interface with all options
- **CompressionType**: CLI-friendly enum that maps to library's Compression
- **Helper functions**:
  - `parse_delimiter()` - Converts string to byte delimiter
  - `parse_quote_char()` - Handles quote character including "none"
  - `determine_output_path()` - Auto-derives .parquet filename
  - `build_conversion_options()` - Maps CLI args to ConversionOptions
- **Batch processing**: Processes multiple files with success/failure tracking

### Design Principles
- Library code is pure, testable, and reusable
- Main.rs is a thin CLI wrapper (uses library via `use csv2parquet::...`)
- Errors: `thiserror` in library, `anyhow` in application
- Functional style: minimal mutable state, pure functions where possible

## Development Commands

### Building
```bash
cargo build           # Build in debug mode
cargo build --release # Build optimized release binary (for production use)
```

### Running
```bash
# Basic usage - convert single file
cargo run -- input.csv

# Multiple files with options
cargo run -- file1.csv file2.csv -c snappy -o output_dir/

# With compression settings
cargo run -- data.csv --compression zstd --compression-level 5

# Custom CSV parsing
cargo run -- data.tsv --delimiter '\t' --quote-char "'"

# Performance tuning
cargo run -- large.csv -j 8 --row-group-size 1000000 --low-memory
```

### Testing
```bash
cargo test                    # Run all tests
cargo test <test_name>        # Run a specific test
cargo test -- --nocapture     # Run tests with stdout output
cargo test -- --test-threads=1 # Run tests serially
```

### Linting and Quality
```bash
cargo clippy                  # Run linter (always use this to identify linting errors)
cargo clippy -- -D warnings   # Run clippy treating warnings as errors
cargo fmt                     # Format code
cargo fmt -- --check          # Check formatting without modifying files
```

### Other Useful Commands
```bash
cargo check         # Quick compile check without producing binary
cargo clean         # Remove target directory and build artifacts
cargo doc --open    # Generate and open documentation
```

## Usage Examples

```bash
# Install the binary
cargo install --path .

# Convert a single CSV file (auto-generates output.parquet)
csv2parquet input.csv

# Convert multiple files
csv2parquet file1.csv file2.csv file3.csv

# Specify output directory
csv2parquet *.csv -o /path/to/output/

# Use different compression
csv2parquet data.csv -c snappy           # Fast, compatible
csv2parquet data.csv -c zstd             # Best balance (default)
csv2parquet data.csv -c gzip --compression-level 9  # Maximum compression

# Custom CSV format (tab-delimited, no quotes)
csv2parquet data.tsv --delimiter '\t' --quote-char none

# Performance optimization for large files
csv2parquet huge.csv -j 16 --row-group-size 1000000 --low-memory

# Full schema inference (scan all rows)
csv2parquet data.csv --infer-schema-rows 0

# View all options
csv2parquet --help
```

## Key Implementation Details

### Polars Integration
- Uses modern CsvReadOptions API (v0.44+) with method chaining
- ParquetWriter configured with optimal defaults:
  - Row group size: 500,000 rows (good for most use cases)
  - Statistics: enabled by default (enables query pruning)
  - Parallel writing: enabled for better performance
- Thread pool uses all available cores unless explicitly limited

### CLI Argument Handling
- Output path auto-derivation: `input.csv` → `input.parquet`
- Special values:
  - `--threads 0` = auto (use all cores)
  - `--infer-schema-rows 0` = scan entire file
  - `--quote-char none` = disable quote handling
- Delimiter shortcuts: `\t` for tab, `,`, `;`, `|`

### Error Handling Strategy
- Library returns `Result<T, ConversionError>` with typed errors
- CLI uses `anyhow::Result` for context-rich error messages
- Batch mode continues on failure, reports summary at end
- Exit code 1 if any files failed

### Performance Considerations
- Files processed sequentially (not in parallel) to avoid memory pressure
- Default Zstd compression balances speed and size
- Row group size of 500k is optimal for most analytics workloads
- Parallel Parquet writing enabled by default
- Low memory mode available for constrained environments

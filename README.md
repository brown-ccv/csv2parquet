# csv2parquet

A fast, feature-rich command-line tool for converting CSV files to Parquet format, built with Rust and Polars.

## Features

- **Batch Processing**: Convert multiple CSV files in a single command
- **Multiple Compression Algorithms**: Support for Snappy, Gzip, Lz4, Zstd, Brotli, and uncompressed
- **Flexible CSV Parsing**: Configurable delimiters, quote characters, and header detection
- **Performance Optimized**: Multi-threaded processing, parallel Parquet writing, and low-memory mode
- **Schema Inference**: Automatic type detection with configurable row scanning
- **Parquet Optimization**: Configurable row group sizes and column statistics

## Installation

### From Source

```bash
git clone https://github.com/yourusername/csv2parquet.git
cd csv2parquet
cargo install --path .
```

### Build Requirements

- Rust 1.70 or later
- Cargo

## Quick Start

```bash
# Convert a single CSV file (auto-generates output.parquet)
csv2parquet input.csv

# Convert multiple files
csv2parquet file1.csv file2.csv file3.csv

# Specify output directory
csv2parquet data.csv -o /path/to/output/

# Use different compression
csv2parquet data.csv -c snappy
```

## Usage

### Basic Usage

```bash
csv2parquet [OPTIONS] <INPUT_FILES>...
```

### Common Examples

**Convert with specific compression:**
```bash
csv2parquet data.csv -c zstd --compression-level 5
```

**Handle tab-delimited files:**
```bash
csv2parquet data.tsv --delimiter '\t'
```

**Disable quote character handling:**
```bash
csv2parquet data.csv --quote-char none
```

**Process large files with memory constraints:**
```bash
csv2parquet huge.csv --low-memory --row-group-size 1000000
```

**Full schema inference (scan all rows):**
```bash
csv2parquet data.csv --infer-schema-rows 0
```

**Multi-threaded processing:**
```bash
csv2parquet data.csv -j 16
```

## Command-Line Options

### Input/Output
- `<INPUT_FILES>...` - One or more CSV files to convert
- `-o, --output-dir <DIR>` - Output directory (default: same as input)

### CSV Parsing
- `-d, --delimiter <CHAR>` - Field delimiter (default: `,`)
  - Shortcuts: `\t` for tab, `;`, `|`
- `-q, --quote-char <CHAR>` - Quote character (default: `"`)
  - Use `none` to disable quote handling
- `--no-header` - CSV has no header row

### Schema Inference
- `--infer-schema-rows <N>` - Number of rows to scan for schema inference (default: 100)
  - Use `0` to scan entire file

### Compression
- `-c, --compression <TYPE>` - Compression algorithm (default: `zstd`)
  - Options: `uncompressed`, `snappy`, `gzip`, `lz4`, `zstd`, `brotli`
- `--compression-level <N>` - Compression level (algorithm-specific)

### Performance
- `-j, --threads <N>` - Number of threads (default: 0 = auto)
- `--row-group-size <N>` - Parquet row group size (default: 500000)
- `--low-memory` - Enable low memory mode
- `--no-parallel` - Disable parallel Parquet writing
- `--no-statistics` - Disable Parquet column statistics

## Compression Options

| Algorithm | Speed | Ratio | Compatibility | Use Case |
|-----------|-------|-------|---------------|----------|
| `uncompressed` | Fastest | None | Universal | Testing, pre-compressed data |
| `snappy` | Very Fast | Good | Excellent | Real-time processing |
| `lz4` | Very Fast | Good | Good | Low-latency applications |
| `zstd` | Fast | Excellent | Good | **Default - best balance** |
| `gzip` | Moderate | Very Good | Universal | Maximum compatibility |
| `brotli` | Slow | Excellent | Moderate | Archival storage |

### Compression Level Examples

```bash
# Fast compression (level 1)
csv2parquet data.csv -c zstd --compression-level 1

# Balanced (default level)
csv2parquet data.csv -c zstd

# Maximum compression (level 9-22 depending on algorithm)
csv2parquet data.csv -c zstd --compression-level 22
csv2parquet data.csv -c gzip --compression-level 9
```

## Performance Tuning

### Large Files
```bash
csv2parquet huge.csv \
  -j 16 \
  --row-group-size 1000000 \
  --low-memory \
  -c snappy
```

### Maximum Compression
```bash
csv2parquet archive.csv \
  -c zstd \
  --compression-level 22 \
  --row-group-size 100000
```

### Fast Processing
```bash
csv2parquet data.csv \
  -j 0 \
  -c lz4 \
  --no-statistics
```

## Building from Source

### Development Build
```bash
cargo build
```

### Release Build (Optimized)
```bash
cargo build --release
```

The optimized binary will be available at `target/release/csv2parquet`.

### Run Tests
```bash
cargo test
```

### Run Linter
```bash
cargo clippy
```

## Dependencies

- [Polars](https://pola.rs/) (v0.51.0) - DataFrame library with CSV and Parquet support
- [Clap](https://github.com/clap-rs/clap) (v4.5.51) - Command-line argument parsing
- [anyhow](https://github.com/dtolnay/anyhow) (v1.0.100) - Error handling
- [thiserror](https://github.com/dtolnay/thiserror) (v2.0.17) - Custom error types

## License

See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

use polars::prelude::*;
use std::fs::File;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Failed to read CSV file: {0}")]
    CsvRead(String),

    #[error("Failed to write Parquet file: {0}")]
    ParquetWrite(String),

    #[error("Invalid delimiter character")]
    InvalidDelimiter,

    #[error("Invalid compression level: {0}")]
    InvalidCompressionLevel(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
}

pub type Result<T> = std::result::Result<T, ConversionError>;

/// Compression algorithm options for Parquet output
#[derive(Debug, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip(Option<u8>),
    Lz4,
    Zstd(Option<i32>),
    Brotli(Option<u32>),
}

impl Compression {
    /// Convert to Polars ParquetCompression type
    fn to_parquet_compression(self) -> Result<ParquetCompression> {
        match self {
            Compression::Uncompressed => Ok(ParquetCompression::Uncompressed),
            Compression::Snappy => Ok(ParquetCompression::Snappy),
            Compression::Gzip(level) => {
                if let Some(l) = level {
                    Ok(ParquetCompression::Gzip(Some(
                        GzipLevel::try_new(l)
                            .map_err(|e| ConversionError::InvalidCompressionLevel(e.to_string()))?,
                    )))
                } else {
                    Ok(ParquetCompression::Gzip(None))
                }
            }
            Compression::Lz4 => Ok(ParquetCompression::Lz4Raw),
            Compression::Zstd(level) => {
                if let Some(l) = level {
                    Ok(ParquetCompression::Zstd(Some(
                        ZstdLevel::try_new(l)
                            .map_err(|e| ConversionError::InvalidCompressionLevel(e.to_string()))?,
                    )))
                } else {
                    Ok(ParquetCompression::Zstd(None))
                }
            }
            Compression::Brotli(level) => {
                if let Some(l) = level {
                    Ok(ParquetCompression::Brotli(Some(
                        BrotliLevel::try_new(l)
                            .map_err(|e| ConversionError::InvalidCompressionLevel(e.to_string()))?,
                    )))
                } else {
                    Ok(ParquetCompression::Brotli(None))
                }
            }
        }
    }
}

/// Configuration options for CSV to Parquet conversion
#[derive(Debug, Clone)]
pub struct ConversionOptions {
    /// Whether the CSV file has a header row
    pub has_header: bool,
    /// Field delimiter character
    pub delimiter: u8,
    /// Quote character (None to disable quoting)
    pub quote_char: Option<u8>,
    /// Number of rows to scan for schema inference (None = scan all)
    pub infer_schema_rows: Option<usize>,
    /// Compression algorithm for Parquet output
    pub compression: Compression,
    /// Row group size (None = single row group)
    pub row_group_size: Option<usize>,
    /// Number of threads to use (None = use all available)
    pub n_threads: Option<usize>,
    /// Enable low memory mode
    pub low_memory: bool,
    /// Write statistics to Parquet file
    pub statistics: bool,
    /// Enable parallel Parquet writing
    pub parallel: bool,
}

impl Default for ConversionOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            quote_char: Some(b'"'),
            infer_schema_rows: Some(1000),
            compression: Compression::Zstd(None),
            row_group_size: Some(500_000),
            n_threads: None,
            low_memory: false,
            statistics: true,
            parallel: true,
        }
    }
}

/// Statistics returned after conversion
#[derive(Debug, Clone)]
pub struct ConversionStats {
    /// Number of rows processed
    pub rows_processed: usize,
    /// Size of output file in bytes
    pub output_size: u64,
    /// Time taken for conversion
    pub duration: Duration,
}

/// Convert a CSV file to Parquet format
pub fn convert_csv_to_parquet(
    input_path: &Path,
    output_path: &Path,
    options: &ConversionOptions,
) -> Result<ConversionStats> {
    let start = std::time::Instant::now();

    // Read CSV file
    let mut df = read_csv(input_path, options)?;
    let rows_processed = df.height();

    // Write Parquet file
    let output_size = write_parquet(&mut df, output_path, options)?;

    let duration = start.elapsed();

    Ok(ConversionStats {
        rows_processed,
        output_size,
        duration,
    })
}

/// Read CSV file with specified options
fn read_csv(path: &Path, options: &ConversionOptions) -> Result<DataFrame> {
    let mut csv_options = CsvReadOptions::default()
        .with_has_header(options.has_header)
        .with_infer_schema_length(options.infer_schema_rows)
        .map_parse_options(|parse_opts| {
            let mut opts = parse_opts.with_separator(options.delimiter);
            if let Some(quote) = options.quote_char {
                opts = opts.with_quote_char(Some(quote));
            } else {
                opts = opts.with_quote_char(None);
            }
            opts
        });

    if let Some(threads) = options.n_threads {
        csv_options = csv_options.with_n_threads(Some(threads));
    }

    csv_options = csv_options.with_low_memory(options.low_memory);

    let df = csv_options
        .try_into_reader_with_file_path(Some(path.to_path_buf()))
        .map_err(|e| ConversionError::CsvRead(e.to_string()))?
        .finish()
        .map_err(|e| ConversionError::CsvRead(e.to_string()))?;

    Ok(df)
}

/// Write DataFrame to Parquet file
fn write_parquet(df: &mut DataFrame, path: &Path, options: &ConversionOptions) -> Result<u64> {
    let file = File::create(path)?;
    let compression = options.compression.to_parquet_compression()?;

    let statistics = if options.statistics {
        StatisticsOptions::full()
    } else {
        StatisticsOptions::empty()
    };

    let mut writer = ParquetWriter::new(file)
        .with_compression(compression)
        .with_statistics(statistics)
        .set_parallel(options.parallel);

    if let Some(row_group_size) = options.row_group_size {
        writer = writer.with_row_group_size(Some(row_group_size));
    }

    let bytes_written = writer
        .finish(df)
        .map_err(|e| ConversionError::ParquetWrite(e.to_string()))?;

    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = ConversionOptions::default();
        assert_eq!(options.has_header, true);
        assert_eq!(options.delimiter, b',');
        assert_eq!(options.infer_schema_rows, Some(1000));
    }

    #[test]
    fn test_compression_conversion() {
        assert!(Compression::Uncompressed.to_parquet_compression().is_ok());
        assert!(Compression::Snappy.to_parquet_compression().is_ok());
        assert!(Compression::Zstd(Some(3)).to_parquet_compression().is_ok());
    }
}

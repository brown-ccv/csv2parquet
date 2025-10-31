use anyhow::{Context, Result};
use clap::Parser;
use csv2parquet::{convert_csv_to_parquet, Compression, ConversionOptions};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "csv2parquet")]
#[command(version, about = "Convert CSV files to Parquet format", long_about = None)]
struct Cli {
    /// Input CSV file(s) to convert
    #[arg(required = true, value_name = "FILES")]
    input_files: Vec<PathBuf>,

    /// Output directory (defaults to input file directory)
    #[arg(short, long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    /// Compression algorithm
    #[arg(short, long, value_enum, default_value = "zstd")]
    compression: CompressionType,

    /// Compression level (algorithm-specific)
    #[arg(long)]
    compression_level: Option<u32>,

    /// CSV has header row
    #[arg(long, default_value = "true")]
    has_header: bool,

    /// Field delimiter character
    #[arg(long, default_value = ",")]
    delimiter: String,

    /// Quote character (use 'none' to disable)
    #[arg(long, default_value = "\"")]
    quote_char: String,

    /// Rows to scan for schema inference (0 = all)
    #[arg(long, default_value = "1000")]
    infer_schema_rows: usize,

    /// Row group size for Parquet
    #[arg(long, default_value = "500000")]
    row_group_size: usize,

    /// Number of threads (0 = auto)
    #[arg(short = 'j', long, default_value = "0")]
    threads: usize,

    /// Enable low memory mode
    #[arg(long)]
    low_memory: bool,

    /// Disable statistics in Parquet output
    #[arg(long)]
    no_statistics: bool,

    /// Disable parallel writing
    #[arg(long)]
    no_parallel: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum CompressionType {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Brotli,
}

impl CompressionType {
    fn to_compression(&self, level: Option<u32>) -> Compression {
        match self {
            CompressionType::Uncompressed => Compression::Uncompressed,
            CompressionType::Snappy => Compression::Snappy,
            CompressionType::Gzip => Compression::Gzip(level.map(|l| l as u8)),
            CompressionType::Lz4 => Compression::Lz4,
            CompressionType::Zstd => Compression::Zstd(level.map(|l| l as i32)),
            CompressionType::Brotli => Compression::Brotli(level),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Build conversion options from CLI arguments
    let options = build_conversion_options(&cli)?;

    // Track statistics
    let mut total_files = 0;
    let mut successful = 0;
    let mut failed = 0;

    // Process each input file
    for input_file in &cli.input_files {
        total_files += 1;

        // Determine output path
        let output_path = determine_output_path(input_file, cli.output_dir.as_ref())?;

        // Perform conversion
        match convert_csv_to_parquet(input_file, &output_path, &options) {
            Ok(stats) => {
                successful += 1;
                println!(
                    "✓ {} -> {} ({} rows, {} bytes, {:.2}s)",
                    input_file.display(),
                    output_path.display(),
                    stats.rows_processed,
                    stats.output_size,
                    stats.duration.as_secs_f64()
                );
            }
            Err(e) => {
                failed += 1;
                eprintln!("✗ {} - {}", input_file.display(), e);
            }
        }
    }

    // Print summary if processing multiple files
    if total_files > 1 {
        println!(
            "\nSummary: {} successful, {} failed out of {} total",
            successful, failed, total_files
        );
    }

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

fn build_conversion_options(cli: &Cli) -> Result<ConversionOptions> {
    // Parse delimiter
    let delimiter = parse_delimiter(&cli.delimiter).context("Invalid delimiter")?;

    // Parse quote character
    let quote_char = parse_quote_char(&cli.quote_char).context("Invalid quote character")?;

    // Convert compression type
    let compression = cli.compression.to_compression(cli.compression_level);

    // Handle infer_schema_rows (0 means None)
    let infer_schema_rows = if cli.infer_schema_rows == 0 {
        None
    } else {
        Some(cli.infer_schema_rows)
    };

    // Handle threads (0 means None for auto)
    let n_threads = if cli.threads == 0 {
        None
    } else {
        Some(cli.threads)
    };

    Ok(ConversionOptions {
        has_header: cli.has_header,
        delimiter,
        quote_char,
        infer_schema_rows,
        compression,
        row_group_size: Some(cli.row_group_size),
        n_threads,
        low_memory: cli.low_memory,
        statistics: !cli.no_statistics,
        parallel: !cli.no_parallel,
    })
}

fn parse_delimiter(s: &str) -> Result<u8> {
    match s {
        "," => Ok(b','),
        ";" => Ok(b';'),
        "|" => Ok(b'|'),
        "\\t" | "\t" => Ok(b'\t'),
        _ if s.len() == 1 => Ok(s.as_bytes()[0]),
        _ => anyhow::bail!("Delimiter must be a single character"),
    }
}

fn parse_quote_char(s: &str) -> Result<Option<u8>> {
    match s.to_lowercase().as_str() {
        "none" | "" => Ok(None),
        "\"" => Ok(Some(b'"')),
        "'" => Ok(Some(b'\'')),
        _ if s.len() == 1 => Ok(Some(s.as_bytes()[0])),
        _ => anyhow::bail!("Quote character must be a single character or 'none'"),
    }
}

fn determine_output_path(input: &Path, output_dir: Option<&PathBuf>) -> Result<PathBuf> {
    // Get the input filename without extension
    let input_stem = input
        .file_stem()
        .context("Invalid input filename")?
        .to_str()
        .context("Filename is not valid UTF-8")?;

    // Create output filename with .parquet extension
    let output_filename = format!("{}.parquet", input_stem);

    // Determine the output directory
    let output_path = if let Some(dir) = output_dir {
        dir.join(output_filename)
    } else {
        // Use input file's directory
        if let Some(parent) = input.parent() {
            parent.join(output_filename)
        } else {
            PathBuf::from(output_filename)
        }
    };

    Ok(output_path)
}

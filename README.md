# Parity

Parity is a high-performance dataset comparison tool designed to efficiently detect and report differences between large datasets. By leveraging the power of Apache Arrow for in-memory columnar data processing and DuckDB for SQL-based operations, Parity can handle massive datasets.

## Features

- **High-Speed Dataset Diffing**: Compare large datasets efficiently using vectorized, batch-wise operations
- **Multiple Data Sources**: Support for Arrow IPC, Parquet, CSV files, and ADBC-compatible databases
- **Comprehensive Diff Reports**: Identify added, deleted, and modified records with column-level detail
- **SQL-Powered Analysis**: Leverage DuckDB for high-performance SQL operations on datasets
- **Streaming Processing**: Handle multi-terabyte datasets without loading them entirely in memory
- **Parallel Execution**: Utilize Go's concurrency model for processing partitions simultaneously
- **Flexible Output**: Export results in various formats including Arrow IPC, Parquet, JSON, Markdown, and HTML

## Installation

To install Parity, use Go 1.24 or later:

```bash
go install github.com/TFMV/parity/cmd/parity@latest
```

Or clone the repository and build from source:

```bash
git clone https://github.com/TFMV/parity.git
cd parity
go build ./cmd/parity
```

## Quick Start

### Basic Comparison

Compare two Parquet files:

```bash
parity diff data/source.parquet data/target.parquet
```

Compare with specific key columns:

```bash
parity diff --key id,timestamp data/source.parquet data/target.parquet
```

Export differences to a Parquet file:

```bash
parity diff --output diffs.parquet source.parquet target.parquet
```

### Advanced Usage

Compare with a tolerance for numeric values:

```bash
parity diff --tolerance 0.0001 --key id financial_data_v1.parquet financial_data_v2.parquet
```

Ignore specific columns in comparison:

```bash
parity diff --ignore updated_at,metadata source.parquet target.parquet
```

Change output format:

```bash
parity diff --format json --output diffs.json source.parquet target.parquet
```

## Architecture

Parity is designed with a modular architecture that separates different concerns:

1. **Core**: Core types and interfaces for dataset operations
2. **Readers**: Implementations for reading from various data sources
3. **Writers**: Implementations for writing data to various formats
4. **Diff**: Dataset comparison algorithms and implementations
5. **Util**: Utility functions and helpers
6. **CLI**: Command-line interface

### Dataset Readers

- `ParquetReader`: Reads data from Parquet files
- `ArrowReader`: Reads data from Arrow IPC files
- `CSVReader`: Reads and converts CSV data to Arrow format
- `DuckDBReader`: Reads data from DuckDB databases

### Dataset Writers

- `ParquetWriter`: Writes data to Parquet files
- `ArrowWriter`: Writes data to Arrow IPC files
- `JSONWriter`: Writes data to JSON files
- `MarkdownWriter`: Generates Markdown reports
- `HTMLWriter`: Generates HTML reports

### Diff Engines

- `DuckDBDiffer`: Uses DuckDB SQL engine for efficient dataset comparison

## Technical Details

### DuckDB Diffing Process

The DuckDB differ works by:

1. Converting input datasets to Parquet files
2. Loading these files into DuckDB tables
3. Using SQL to efficiently identify differences
4. Extracting the differences into Arrow records
5. Generating comprehensive summary statistics

The process is highly optimized for both memory usage and performance, with features like:

- Batch processing to manage memory footprint
- Transaction management for large datasets
- Temporary file management
- Type-aware comparisons with customizable tolerance
- Multi-threaded processing

### DuckDB Optimizations

Parity configures DuckDB with:

- Automatic thread count determination
- 4GB memory limit for handling large string columns
- Efficient extraction and insertion of Arrow data

## Development

### Prerequisites

- Go 1.24 or later
- DuckDB
- Apache Arrow libraries

### Building

```bash
go build ./cmd/parity
```

### Testing

```bash
go test ./...
```

### Adding New Readers/Writers

To add a new data source reader, implement the `core.DatasetReader` interface:

```go
type DatasetReader interface {
    Read(ctx context.Context) (arrow.Record, error)
    Schema() *arrow.Schema
    Close() error
}
```

To add a new output format writer, implement the `core.DatasetWriter` interface:

```go
type DatasetWriter interface {
    Write(ctx context.Context, record arrow.Record) error
    Close() error
}
```

## License

Parity is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Apache Arrow](https://arrow.apache.org/) - For the Arrow columnar memory format
- [DuckDB](https://duckdb.org/) - For the embedded SQL OLAP database engine
- [Go-DuckDB](https://github.com/marcboeker/go-duckdb) - For Go bindings to DuckDB

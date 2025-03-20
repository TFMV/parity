# Parity

Parity is a high-performance dataset comparison tool designed to efficiently detect and report differences between large datasets. By leveraging the power of Apache Arrow for in-memory columnar data processing, Parity can handle massive datasets with speed and efficiency.

## Features

- **High-Speed Dataset Diffing**: Compare large datasets efficiently using vectorized, batch-wise operations
- **Multiple Data Sources**: Support for Arrow IPC, Parquet, CSV files, and ADBC-compatible databases
- **Comprehensive Diff Reports**: Identify added, deleted, and modified records with column-level detail
- **Schema Validation**: Validate and compare Apache Arrow schemas with customizable rules
- **Arrow-Powered Analysis**: Leverage Arrow's in-memory columnar format for high-performance operations
- **Flexible Memory Management**: Process data in streaming batches or load complete datasets based on your needs
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

Enable parallel processing with a specific number of workers:

```bash
parity diff --parallel --workers 8 source.parquet target.parquet
```

### Memory Management Strategies

Control memory usage based on your dataset size:

For large datasets (streaming mode):

```bash
parity diff --batch-size 10000 --stream large_source.parquet large_target.parquet
```

For smaller datasets (full load mode):

```bash
parity diff --full-load source.parquet target.parquet
```

## Diff Strategies

Parity provides different diff strategies to suit your needs:

### ArrowDiffer (Default)

The default Arrow-based differ uses the Apache Arrow columnar format for high-performance comparisons:

```bash
parity diff --differ arrow source.parquet target.parquet
```

Key features:

- Vectorized operations for maximum performance
- Efficient memory usage with the option to load full datasets or process record batches
- Type-aware comparisons with configurable tolerance for numeric values
- Support for parallel processing

### DuckDBDiffer

A DuckDB-based differ can be added by the community or I can add if needed.

## Schema Validation

Parity includes a schema validation system for Apache Arrow schemas. The schema functionality allows you to:

- Validate a single schema against a set of rules
- Compare two schemas for compatibility
- Enforce rules for field types, nullability, and encoding
- Support different validation levels for schema evolution

### Basic Schema Validation

Validate a single schema:

```bash
parity schema dataset.parquet
```

Compare two schemas for compatibility:

```bash
parity schema source.parquet target.parquet
```

### Validation Rules

Require specific fields to be present:

```bash
parity schema --required id,name,timestamp dataset.parquet
```

Ensure certain fields are not nullable:

```bash
parity schema --non-nullable id,primary_key dataset.parquet
```

Enforce dictionary encoding for string fields:

```bash
parity schema --require-dict-encoding dataset.parquet
```

Validate temporal fields:

```bash
parity schema --require-utc --timestamp-fields created_at,updated_at dataset.parquet
```

### Validation Levels

Parity supports three validation levels:

- **Strict**: Requires exact schema matches (types, names, nullability)
- **Compatible** (default): Allows schema evolution (adding fields, relaxing nullability)
- **Relaxed**: Only checks that common fields have compatible types

Specify the validation level:

```bash
parity schema --level strict source.parquet target.parquet
```

### Output Options

Output validation results as text (default) or JSON:

```bash
parity schema --format json dataset.parquet
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

All readers support both streaming batch processing and full dataset loading through:

- `Read()`: Returns the next batch of records
- `ReadAll()`: Loads the entire dataset into memory at once

### Dataset Writers

- `ParquetWriter`: Writes data to Parquet files
- `ArrowWriter`: Writes data to Arrow IPC files
- `JSONWriter`: Writes data to JSON files

### Diff Engines

- `ArrowDiffer`: Uses Arrow's in-memory columnar format for efficient dataset comparison

## Technical Details

### Arrow Diffing Process

The Arrow differ works by:

1. Loading input datasets (either fully or in batches) as Arrow records
2. Building key arrays for efficient record matching
3. Comparing columns with type-aware logic and customizable tolerance
4. Identifying added, deleted, and modified records
5. Producing detailed output with indicators for which fields were modified

The process is highly optimized for both memory usage and performance, with features like:

- Streaming record processing to manage memory footprint
- Efficient key-based record matching
- Type-aware comparisons with customizable tolerance for floating-point values
- Parallel comparison of records with configurable worker pools

### Memory Management

Parity offers two main memory management approaches:

**Streaming Mode (Default)**:

- Processes data in batches, with configurable batch size
- Minimizes memory footprint for large datasets
- Ideal for production environments with memory constraints

**Full Load Mode**:

- Loads entire datasets into memory
- Provides maximum performance for smaller datasets
- Best for interactive use and small to medium datasets

Control this behavior with the `--batch-size` and `--full-load` options.

### Arrow Optimizations

Parity leverages Arrow's strengths:

- Zero-copy operations where possible
- Columnar data representation for efficient comparison
- Vectorized operations for high throughput
- Memory-efficient data structures

## Development

### Prerequisites

- Go 1.24 or later
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
    // Read returns a record batch and an error if any.
    // Returns io.EOF when there are no more batches.
    Read(ctx context.Context) (arrow.Record, error)
    
    // ReadAll reads all records from the dataset into a single record.
    // This is useful for small datasets, but may use a lot of memory for large datasets.
    // Returns io.EOF if there are no records.
    ReadAll(ctx context.Context) (arrow.Record, error)
    
    // Schema returns the schema of the dataset.
    Schema() *arrow.Schema
    
    // Close closes the reader and releases resources.
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

### Adding New Diff Engines

To add a new diff engine, implement the `core.Differ` interface:

```go
type Differ interface {
    // Diff computes the difference between two datasets.
    Diff(ctx context.Context, source, target DatasetReader, options DiffOptions) (*DiffResult, error)
}
```

## Examples

### Comparing Large Production Datasets

```go
package main

import (
    "context"
    "log"

    "github.com/TFMV/parity/pkg/core"
    "github.com/TFMV/parity/pkg/diff"
    "github.com/TFMV/parity/pkg/readers"
)

func main() {
    // Create source and target readers
    sourceConfig := core.ReaderConfig{
        Type:      "parquet",
        Path:      "large_source.parquet",
        BatchSize: 10000, // Process in batches of 10,000 rows
    }
    
    targetConfig := core.ReaderConfig{
        Type:      "parquet",
        Path:      "large_target.parquet",
        BatchSize: 10000,
    }
    
    sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
    if err != nil {
        log.Fatalf("Failed to create source reader: %v", err)
    }
    defer sourceReader.Close()
    
    targetReader, err := readers.DefaultFactory.Create(targetConfig)
    if err != nil {
        log.Fatalf("Failed to create target reader: %v", err)
    }
    defer targetReader.Close()
    
    // Create differ with options
    differ, err := diff.NewArrowDiffer()
    if err != nil {
        log.Fatalf("Failed to create differ: %v", err)
    }
    defer differ.Close()
    
    // Configure diff options
    options := core.DiffOptions{
        KeyColumns:    []string{"id", "timestamp"},
        IgnoreColumns: []string{"updated_at"},
        BatchSize:     10000,
        Tolerance:     0.0001,
        Parallel:      true,
        NumWorkers:    8,
    }
    
    // Perform diff
    ctx := context.Background()
    result, err := differ.Diff(ctx, sourceReader, targetReader, options)
    if err != nil {
        log.Fatalf("Failed to diff datasets: %v", err)
    }
    
    // Print summary
    log.Printf("Total records: Source=%d, Target=%d", result.Summary.TotalSource, result.Summary.TotalTarget)
    log.Printf("Differences: Added=%d, Deleted=%d, Modified=%d", 
        result.Summary.Added, result.Summary.Deleted, result.Summary.Modified)
}
```

## License

Parity is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Apache Arrow](https://arrow.apache.org/) - For the Arrow columnar memory format and efficient data processing capabilities

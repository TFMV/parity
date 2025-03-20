// Package core provides the core types and interfaces for the Parity dataset comparison tool.
package core

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// DatasetReader defines an interface for reading data from various sources.
type DatasetReader interface {
	// Read returns a record batch and an error if any.
	// Returns io.EOF when there are no more batches.
	Read(ctx context.Context) (arrow.Record, error)

	// Schema returns the schema of the dataset.
	Schema() *arrow.Schema

	// Close closes the reader and releases resources.
	Close() error
}

// DatasetWriter defines an interface for writing data to various destinations.
type DatasetWriter interface {
	// Write writes a record to the destination.
	Write(ctx context.Context, record arrow.Record) error

	// Close closes the writer and flushes any pending data.
	Close() error
}

// DiffResult represents the difference between two datasets.
type DiffResult struct {
	// Added contains records that exist in the target but not in the source.
	Added arrow.Record

	// Deleted contains records that exist in the source but not in the target.
	Deleted arrow.Record

	// Modified contains records that exist in both but have different values.
	// This includes columns to indicate which fields were modified.
	Modified arrow.Record

	// Summary provides a summary of the differences.
	Summary DiffSummary
}

// DiffSummary provides a summary of the differences between two datasets.
type DiffSummary struct {
	// TotalSource is the total number of records in the source dataset.
	TotalSource int64

	// TotalTarget is the total number of records in the target dataset.
	TotalTarget int64

	// Added is the number of records added.
	Added int64

	// Deleted is the number of records deleted.
	Deleted int64

	// Modified is the number of records modified.
	Modified int64

	// Columns is a map of column names to the number of modifications in that column.
	Columns map[string]int64
}

// DiffOptions provides options for the diff operation.
type DiffOptions struct {
	// KeyColumns specifies the columns to use as keys for matching records.
	KeyColumns []string

	// IgnoreColumns specifies columns to ignore when comparing records.
	IgnoreColumns []string

	// BatchSize is the size of batches to process at once.
	BatchSize int64

	// Tolerance is the tolerance to use for floating point comparisons.
	Tolerance float64

	// Parallel indicates whether to use parallel processing.
	Parallel bool

	// NumWorkers is the number of workers to use for parallel processing.
	// If 0, defaults to the number of CPUs.
	NumWorkers int
}

// ReaderConfig provides configuration for creating a reader.
type ReaderConfig struct {
	// Type is the type of the reader.
	Type string

	// Path is the path to the file or directory.
	Path string

	// ConnectionString is the connection string for a database.
	ConnectionString string

	// Table is the table name for a database.
	Table string

	// Query is the query to execute for a database.
	Query string

	// BatchSize is the size of batches to read.
	BatchSize int64
}

// WriterConfig provides configuration for creating a writer.
type WriterConfig struct {
	// Type is the type of the writer.
	Type string

	// Path is the path to the file or directory.
	Path string

	// ConnectionString is the connection string for a database.
	ConnectionString string

	// Table is the table name for a database.
	Table string

	// BatchSize is the size of batches to write.
	BatchSize int64
}

// Differ defines an interface for computing differences between datasets.
type Differ interface {
	// Diff computes the difference between two datasets.
	Diff(ctx context.Context, source, target DatasetReader, options DiffOptions) (*DiffResult, error)
}

// Reporter defines an interface for generating reports from diff results.
type Reporter interface {
	// Report generates a report from a diff result.
	Report(ctx context.Context, result *DiffResult) (io.Reader, error)
}

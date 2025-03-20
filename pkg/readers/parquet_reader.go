package readers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetReader implements a reader for Parquet files.
type ParquetReader struct {
	schema      *arrow.Schema
	fileReader  *file.Reader
	arrowReader *pqarrow.FileReader
	batchSize   int64
	currentRow  int64
	totalRows   int64
	file        *os.File
	alloc       memory.Allocator
}

// NewParquetReader creates a new Parquet reader.
func NewParquetReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for Parquet reader")
	}

	// Set default batch size if not specified
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	// Open the file
	f, err := os.Open(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}

	// Create allocator
	alloc := memory.NewGoAllocator()

	// Create parquet file reader - file is a ReaderAtSeeker
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to create Parquet file reader: %w", err)
	}

	// Create Arrow reader from the Parquet file
	arrowProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: batchSize,
	}
	arrowReader, err := pqarrow.NewFileReader(parquetReader, arrowProps, alloc)
	if err != nil {
		parquetReader.Close()
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	// Get the schema
	schema, err := arrowReader.Schema()
	if err != nil {
		parquetReader.Close()
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	return &ParquetReader{
		schema:      schema,
		fileReader:  parquetReader,
		arrowReader: arrowReader,
		batchSize:   batchSize,
		currentRow:  0,
		totalRows:   parquetReader.NumRows(),
		file:        f,
		alloc:       alloc,
	}, nil
}

// Read returns the next batch of records.
func (r *ParquetReader) Read(ctx context.Context) (arrow.Record, error) {
	if r.currentRow >= r.totalRows {
		return nil, io.EOF
	}

	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Calculate the number of rows to read
	rowsToRead := r.batchSize
	if r.currentRow+rowsToRead > r.totalRows {
		rowsToRead = r.totalRows - r.currentRow
	}

	// For simplicity, we read all columns in the current row group
	rowGroupIdx := int(r.currentRow / r.batchSize)

	// Create a reader for the current row group
	rowGroupReader := r.arrowReader.RowGroup(rowGroupIdx)

	// Read all columns from the row group
	table, err := rowGroupReader.ReadTable(ctx, nil) // nil means read all columns
	if err != nil {
		return nil, fmt.Errorf("failed to read row group: %w", err)
	}

	// Convert table to record batch
	reader := array.NewTableReader(table, rowsToRead)
	defer reader.Release()

	// Read the record
	if !reader.Next() {
		if reader.Err() != nil {
			return nil, reader.Err()
		}
		return nil, io.EOF
	}

	record := reader.Record()
	record.Retain() // Retain the record so it's not released when the reader is released

	r.currentRow += rowsToRead
	return record, nil
}

// Schema returns the schema of the dataset.
func (r *ParquetReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *ParquetReader) Close() error {
	var err error

	// FileReader closes the file for us
	if r.fileReader != nil {
		if closeErr := r.fileReader.Close(); closeErr != nil {
			err = closeErr
		}
	}

	return err
}

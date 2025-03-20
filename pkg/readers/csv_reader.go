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
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CSVReader implements a reader for CSV files, converting to Arrow.
type CSVReader struct {
	schema        *arrow.Schema
	file          *os.File
	reader        *csv.Reader
	alloc         memory.Allocator
	records       []arrow.Record
	currentRecord int
}

// NewCSVReader creates a new CSV reader.
func NewCSVReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for CSV reader")
	}

	// Open the file
	file, err := os.Open(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	// Set default chunk size if not specified
	chunkSize := config.BatchSize
	if chunkSize <= 0 {
		chunkSize = 10000 // Default chunk size
	}

	// Create allocator
	alloc := memory.NewGoAllocator()

	// Create CSV reader with inference options
	reader := csv.NewInferringReader(
		file,
		csv.WithChunk(int(chunkSize)),
		csv.WithHeader(true),
		csv.WithNullReader(true, ""), // Empty string is treated as null
		csv.WithAllocator(alloc),
	)

	return &CSVReader{
		file:          file,
		reader:        reader,
		alloc:         alloc,
		records:       make([]arrow.Record, 0),
		currentRecord: 0,
	}, nil
}

// Read returns the next batch of records.
func (r *CSVReader) Read(ctx context.Context) (arrow.Record, error) {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// If we've already read some records and there are more to return
	if r.currentRecord < len(r.records) {
		record := r.records[r.currentRecord]
		r.currentRecord++
		return record, nil
	}

	// Clean up any previously loaded records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = r.records[:0]
	r.currentRecord = 0

	// Read next batch from the CSV reader
	if !r.reader.Next() {
		if r.reader.Err() != nil {
			return nil, fmt.Errorf("failed to read CSV: %w", r.reader.Err())
		}
		return nil, io.EOF
	}

	// Get the schema (only on first read)
	if r.schema == nil {
		r.schema = r.reader.Schema()
	}

	// Get the record and retain it
	record := r.reader.Record()
	record.Retain()
	r.records = append(r.records, record)
	r.currentRecord = 1 // Move to index 1 since we're returning the first record

	return record, nil
}

// ReadAll loads all CSV data into memory at once
// This is useful for smaller files, but be careful with large files
func (r *CSVReader) ReadAll(ctx context.Context) (arrow.Record, error) {
	// Check if already read everything
	if r.currentRecord > 0 && !r.reader.Next() {
		if r.reader.Err() != nil {
			return nil, fmt.Errorf("failed to read CSV: %w", r.reader.Err())
		}
		return nil, io.EOF
	}

	// Clean up any previously loaded records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = r.records[:0]
	r.currentRecord = 0

	// Read all records
	for r.reader.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Get the schema (only on first read)
		if r.schema == nil {
			r.schema = r.reader.Schema()
		}

		rec := r.reader.Record()
		rec.Retain()
		r.records = append(r.records, rec)
	}

	if r.reader.Err() != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", r.reader.Err())
	}

	if len(r.records) == 0 {
		return nil, io.EOF
	}

	// Combine all records into a single record
	if len(r.records) == 1 {
		r.currentRecord = 1
		return r.records[0], nil
	}

	// Combine multiple records into one
	schema := r.schema
	table := array.NewTableFromRecords(schema, r.records)
	defer table.Release()

	// Create a record batch reader from the table
	tableReader := array.NewTableReader(table, table.NumRows())
	defer tableReader.Release()

	// Read the combined record
	tableReader.Next()
	combinedRecord := tableReader.Record()
	r.currentRecord = len(r.records) // Mark as all read

	// Create a new record that we own
	result := array.NewRecord(schema, combinedRecord.Columns(), combinedRecord.NumRows())
	return result, nil
}

// Schema returns the schema of the dataset.
func (r *CSVReader) Schema() *arrow.Schema {
	// If we haven't read any data yet, need to read the first batch to get schema
	if r.schema == nil && r.reader != nil {
		if r.reader.Next() {
			r.schema = r.reader.Schema()
			// Get the record and retain it for later use
			record := r.reader.Record()
			record.Retain()
			r.records = append(r.records, record)
		} else if r.reader.Err() != nil {
			// This is not ideal, but we need to handle the error case
			return arrow.NewSchema([]arrow.Field{}, nil)
		}
	}
	return r.schema
}

// Close closes the reader and releases resources.
func (r *CSVReader) Close() error {
	// Release all records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = nil

	// Release the reader
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}

	// Close the file
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}

	return nil
}

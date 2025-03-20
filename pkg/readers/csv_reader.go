package readers

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CSVReader implements a reader for CSV files, converting to Arrow.
type CSVReader struct {
	schema    *arrow.Schema
	reader    *csv.Reader
	file      *os.File
	header    []string
	batchSize int64
	alloc     *memory.GoAllocator
	eof       bool
}

// NewCSVReader creates a new CSV reader.
func NewCSVReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for CSV reader")
	}

	// Set default batch size if not specified
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	file, err := os.Open(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	reader := csv.NewReader(file)
	reader.ReuseRecord = true // For better performance

	// Read header
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Infer schema from header
	fields := make([]arrow.Field, len(header))
	for i, name := range header {
		// Default to string type
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}

	schema := arrow.NewSchema(fields, nil)

	return &CSVReader{
		schema:    schema,
		reader:    reader,
		file:      file,
		header:    header,
		batchSize: batchSize,
		alloc:     memory.NewGoAllocator(),
		eof:       false,
	}, nil
}

// Read returns the next batch of records.
func (r *CSVReader) Read(ctx context.Context) (arrow.Record, error) {
	if r.eof {
		return nil, io.EOF
	}

	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Create builders for each column
	builders := make([]array.Builder, len(r.header))
	for i, field := range r.schema.Fields() {
		builders[i] = array.NewBuilder(r.alloc, field.Type)
		defer builders[i].Release()
	}

	// Read rows in batches
	var rowCount int64
	for rowCount < r.batchSize {
		// Check if context is canceled
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := r.reader.Read()
		if err == io.EOF {
			r.eof = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}

		// Append values to builders
		for i, val := range row {
			if i >= len(builders) {
				continue
			}

			// Add value to appropriate builder
			if builder, ok := builders[i].(*array.StringBuilder); ok {
				builder.Append(val)
			}
		}

		rowCount++
	}

	// If no rows were read, return EOF
	if rowCount == 0 {
		r.eof = true
		return nil, io.EOF
	}

	// Build column arrays
	cols := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		cols[i] = builder.NewArray()
		defer cols[i].Release()
	}

	// Create record batch
	record := array.NewRecord(r.schema, cols, int64(rowCount))
	return record, nil
}

// Schema returns the schema of the dataset.
func (r *CSVReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *CSVReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// inferType attempts to infer the type of a value.
func inferType(val string) arrow.DataType {
	// Try to convert to int
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return arrow.PrimitiveTypes.Int64
	}

	// Try to convert to float
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return arrow.PrimitiveTypes.Float64
	}

	// Try to convert to bool
	if val == "true" || val == "false" {
		return arrow.FixedWidthTypes.Boolean
	}

	// Default to string
	return arrow.BinaryTypes.String
}

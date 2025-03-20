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
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowReader implements a reader for Arrow IPC files.
type ArrowReader struct {
	schema     *arrow.Schema
	reader     *ipc.FileReader
	file       *os.File
	currentIdx int64
	batchSize  int64
	alloc      memory.Allocator
	records    []arrow.Record
	currentRec int
}

// NewArrowReader creates a new Arrow IPC reader.
func NewArrowReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for Arrow reader")
	}

	// Set default batch size if not specified
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	file, err := os.Open(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open Arrow file: %w", err)
	}

	reader, err := ipc.NewFileReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create Arrow file reader: %w", err)
	}

	return &ArrowReader{
		schema:     reader.Schema(),
		reader:     reader,
		file:       file,
		currentIdx: 0,
		batchSize:  batchSize,
		alloc:      memory.NewGoAllocator(),
		records:    make([]arrow.Record, 0),
		currentRec: 0,
	}, nil
}

// Read returns the next batch of records.
func (r *ArrowReader) Read(ctx context.Context) (arrow.Record, error) {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// If we've already read some records and there are more to return
	if r.currentRec < len(r.records) {
		record := r.records[r.currentRec]
		// Explicitly retain the record before returning it
		record.Retain()
		r.currentRec++
		return record, nil
	}

	// Clean up any previously loaded records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = r.records[:0]
	r.currentRec = 0

	// Check if we've reached the end of the file
	if int(r.currentIdx) >= r.reader.NumRecords() {
		return nil, io.EOF
	}

	// Calculate number of records to read in this batch
	recordsLeft := r.reader.NumRecords() - int(r.currentIdx)
	recordsToRead := int(r.batchSize)
	if recordsToRead > recordsLeft {
		recordsToRead = recordsLeft
	}

	// Read records in batches
	for i := 0; i < recordsToRead; i++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			// Clean up any records we've loaded in this batch
			for _, rec := range r.records {
				rec.Release()
			}
			r.records = r.records[:0]
			return nil, ctx.Err()
		default:
		}

		record, err := r.reader.Record(int(r.currentIdx))
		if err != nil {
			// Clean up any records we've loaded in this batch
			for _, rec := range r.records {
				rec.Release()
			}
			r.records = r.records[:0]
			return nil, fmt.Errorf("failed to read record at index %d: %w", r.currentIdx, err)
		}

		// Create a copy that we own
		clonedRecord := r.cloneRecord(record)
		r.records = append(r.records, clonedRecord)
		r.currentIdx++
	}

	// If we didn't read any records, return EOF
	if len(r.records) == 0 {
		return nil, io.EOF
	}

	// Return the first record and retain it
	record := r.records[0]
	record.Retain()
	r.currentRec = 1
	return record, nil
}

// cloneRecord creates a deep copy of a record to ensure ownership
func (r *ArrowReader) cloneRecord(record arrow.Record) arrow.Record {
	// Create new arrays for each column
	cols := make([]arrow.Array, record.NumCols())
	for i, col := range record.Columns() {
		// Create a new array from the data in the original
		cols[i] = array.MakeFromData(col.Data())
	}

	// Create a new record with the cloned data
	return array.NewRecord(record.Schema(), cols, record.NumRows())
}

// ReadAll reads all records from the Arrow file into a single record.
// Warning: This can use a lot of memory for large files.
func (r *ArrowReader) ReadAll(ctx context.Context) (arrow.Record, error) {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Clean up any previously loaded records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = r.records[:0]
	r.currentRec = 0

	// Reset position to start of file
	r.currentIdx = 0

	// Fast path: if there are no records, return an empty record
	if r.reader.NumRecords() == 0 {
		return r.createEmptyRecord(), nil
	}

	// Fast path: if there's only one record, just return it
	if r.reader.NumRecords() == 1 {
		record, err := r.reader.Record(0)
		if err != nil {
			return nil, fmt.Errorf("failed to read single record: %w", err)
		}

		result := r.cloneRecord(record)
		r.records = []arrow.Record{result}
		r.currentRec = 1
		result.Retain() // Retain before returning
		return result, nil
	}

	// Read all records
	var allRecords []arrow.Record
	for int(r.currentIdx) < r.reader.NumRecords() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			// Clean up on context cancellation
			for _, rec := range allRecords {
				rec.Release()
			}
			return nil, ctx.Err()
		default:
		}

		record, err := r.reader.Record(int(r.currentIdx))
		if err != nil {
			// Clean up on error
			for _, rec := range allRecords {
				rec.Release()
			}
			return nil, fmt.Errorf("failed to read record at index %d: %w", r.currentIdx, err)
		}

		// Clone the record to ensure we own it
		clonedRecord := r.cloneRecord(record)
		allRecords = append(allRecords, clonedRecord)
		r.currentIdx++
	}

	// Combine all records into one using the tableToRecord approach
	result, err := r.tableToRecord(allRecords)
	if err != nil {
		// Clean up records on error
		for _, rec := range allRecords {
			rec.Release()
		}
		return nil, err
	}

	// Clean up individual records since we now have a combined one
	for _, rec := range allRecords {
		rec.Release()
	}

	// Store the result so we can release it properly
	r.records = []arrow.Record{result}
	r.currentRec = 1

	// Retain it before returning
	result.Retain()
	return result, nil
}

// tableToRecord efficiently converts a set of records to a single record
func (r *ArrowReader) tableToRecord(records []arrow.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to combine")
	}

	// Combine records into a table
	table := array.NewTableFromRecords(r.schema, records)
	defer table.Release()

	// Determine the total number of rows
	totalRows := int64(0)
	for _, rec := range records {
		totalRows += rec.NumRows()
	}

	// Use a TableReader to get a consolidated record
	tableReader := array.NewTableReader(table, totalRows)
	defer tableReader.Release()

	if !tableReader.Next() {
		if tableReader.Err() != nil {
			return nil, tableReader.Err()
		}
		return nil, fmt.Errorf("failed to read combined table")
	}

	// Get the consolidated record and clone it
	record := tableReader.Record()
	return r.cloneRecord(record), nil
}

// createEmptyRecord creates an empty record with the given schema
func (r *ArrowReader) createEmptyRecord() arrow.Record {
	// Create empty arrays for each field in the schema
	cols := make([]arrow.Array, r.schema.NumFields())
	for i, field := range r.schema.Fields() {
		// Create an empty array of the appropriate type
		arrBuilder := array.NewBuilder(r.alloc, field.Type)
		cols[i] = arrBuilder.NewArray()
		arrBuilder.Release()
	}

	// Create the empty record
	record := array.NewRecord(r.schema, cols, 0)

	// Release the arrays after creating the record
	for _, col := range cols {
		col.Release()
	}

	return record
}

// Schema returns the schema of the dataset.
func (r *ArrowReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *ArrowReader) Close() error {
	// Release all records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = nil

	var err error

	if r.reader != nil {
		if closeErr := r.reader.Close(); closeErr != nil {
			err = closeErr
		}
		r.reader = nil
	}

	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		r.file = nil
	}

	return err
}

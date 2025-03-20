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
			return nil, ctx.Err()
		default:
		}

		record, err := r.reader.Record(int(r.currentIdx))
		if err != nil {
			return nil, fmt.Errorf("failed to read record at index %d: %w", r.currentIdx, err)
		}

		// Clone the record to ensure we own it
		clonedRecord := array.NewRecord(record.Schema(), record.Columns(), record.NumRows())
		r.records = append(r.records, clonedRecord)
		r.currentIdx++
	}

	// If we didn't read any records, return EOF
	if len(r.records) == 0 {
		return nil, io.EOF
	}

	// Return the first record
	record := r.records[0]
	r.currentRec = 1
	return record, nil
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
		clonedRecord := array.NewRecord(record.Schema(), record.Columns(), record.NumRows())
		allRecords = append(allRecords, clonedRecord)
		r.currentIdx++
	}

	// If we didn't read any records, return EOF
	if len(allRecords) == 0 {
		return nil, io.EOF
	}

	// If we only got one record, just return it
	if len(allRecords) == 1 {
		r.records = allRecords
		r.currentRec = 1
		return allRecords[0], nil
	}

	// Otherwise, combine all records into one
	combinedTable := array.NewTableFromRecords(r.schema, allRecords)
	defer combinedTable.Release()

	// Create a record batch reader from the table
	tableReader := array.NewTableReader(combinedTable, combinedTable.NumRows())
	defer tableReader.Release()

	// Read the combined record
	if !tableReader.Next() {
		// Clean up records
		for _, rec := range allRecords {
			rec.Release()
		}
		if tableReader.Err() != nil {
			return nil, tableReader.Err()
		}
		return nil, fmt.Errorf("unexpected error: failed to read combined record")
	}

	combinedRecord := tableReader.Record()

	// Create a new record that we own
	result := array.NewRecord(r.schema, combinedRecord.Columns(), combinedRecord.NumRows())

	// Clean up individual records since we now have a combined one
	for _, rec := range allRecords {
		rec.Release()
	}

	// Store the result so we can release it properly
	r.records = []arrow.Record{result}
	r.currentRec = 1

	return result, nil
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

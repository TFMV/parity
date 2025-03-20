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
	records     []arrow.Record
	currentRec  int
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
		f.Close()
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	// Get the schema
	schema, err := arrowReader.Schema()
	if err != nil {
		// No need to close arrowReader as it doesn't have a Close method
		parquetReader.Close()
		f.Close()
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
		records:     make([]arrow.Record, 0),
		currentRec:  0,
	}, nil
}

// Read returns the next batch of records.
func (r *ParquetReader) Read(ctx context.Context) (arrow.Record, error) {
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
	if r.currentRow >= r.totalRows {
		return nil, io.EOF
	}

	// Calculate the number of rows to read
	rowsToRead := r.batchSize
	if r.currentRow+rowsToRead > r.totalRows {
		rowsToRead = r.totalRows - r.currentRow
	}

	// Calculate current row group
	rowGroupSize := r.fileReader.RowGroup(0).NumRows()
	startRowGroup := int(r.currentRow / rowGroupSize)
	endRowGroup := int((r.currentRow + rowsToRead - 1) / rowGroupSize)

	// We need to read potentially multiple row groups
	for rgIdx := startRowGroup; rgIdx <= endRowGroup && rgIdx < r.fileReader.NumRowGroups(); rgIdx++ {
		// Calculate row offset within row group
		rowGroupStartIdx := int64(rgIdx) * rowGroupSize
		startOffset := r.currentRow - rowGroupStartIdx

		// Calculate how many rows to read from this row group
		rowsInGroup := r.fileReader.RowGroup(rgIdx).NumRows()
		rowsToReadFromGroup := rowsInGroup - startOffset
		if r.currentRow+rowsToReadFromGroup > r.totalRows {
			rowsToReadFromGroup = r.totalRows - r.currentRow
		}
		if r.currentRow+rowsToReadFromGroup > r.currentRow+rowsToRead {
			rowsToReadFromGroup = rowsToRead
		}

		// Read records from the row group
		rgReader := r.arrowReader.RowGroup(rgIdx)

		// Read the section of the row group we need
		table, err := rgReader.ReadTable(ctx, nil) // nil means read all columns
		if err != nil {
			return nil, fmt.Errorf("failed to read row group %d: %w", rgIdx, err)
		}
		defer table.Release()

		// Create table reader - will create records with specific number of rows
		tableReader := array.NewTableReader(table, rowsToReadFromGroup)
		defer tableReader.Release()

		// Read records
		for tableReader.Next() {
			rec := tableReader.Record()
			rec.Retain() // We're keeping this record, so retain it
			r.records = append(r.records, rec)
		}

		if tableReader.Err() != nil {
			return nil, fmt.Errorf("error reading from table: %w", tableReader.Err())
		}

		// Update currentRow
		r.currentRow += rowsToReadFromGroup
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

// ReadAll reads the entire Parquet file into a single Arrow record.
// Warning: This can use a lot of memory for large files.
func (r *ParquetReader) ReadAll(ctx context.Context) (arrow.Record, error) {
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
	r.currentRow = 0

	// Read the entire file as a table
	table, err := r.arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet file: %w", err)
	}
	defer table.Release()

	// Create a record batch reader from the table
	tableReader := array.NewTableReader(table, table.NumRows())
	defer tableReader.Release()

	// Read all batches
	var allRecords []arrow.Record
	for tableReader.Next() {
		rec := tableReader.Record()
		rec.Retain() // Retain the record so it's not released when the reader is released
		allRecords = append(allRecords, rec)
	}

	if tableReader.Err() != nil {
		// Clean up records on error
		for _, rec := range allRecords {
			rec.Release()
		}
		return nil, fmt.Errorf("error reading batches: %w", tableReader.Err())
	}

	// If we only got one record, just return it
	if len(allRecords) == 1 {
		r.records = allRecords
		r.currentRec = 1
		r.currentRow = r.totalRows // Mark as fully read
		return allRecords[0], nil
	}

	// Otherwise, combine all records into one
	combinedTable := array.NewTableFromRecords(r.schema, allRecords)
	defer combinedTable.Release()

	// Create a record batch reader from the table
	combinedReader := array.NewTableReader(combinedTable, combinedTable.NumRows())
	defer combinedReader.Release()

	// Read the combined record
	if !combinedReader.Next() {
		// Clean up records
		for _, rec := range allRecords {
			rec.Release()
		}
		if combinedReader.Err() != nil {
			return nil, combinedReader.Err()
		}
		return nil, fmt.Errorf("unexpected error: failed to read combined record")
	}

	combinedRecord := combinedReader.Record()

	// Create a new record that we own
	result := array.NewRecord(r.schema, combinedRecord.Columns(), combinedRecord.NumRows())

	// Clean up individual records since we now have a combined one
	for _, rec := range allRecords {
		rec.Release()
	}

	// Update state
	r.currentRow = r.totalRows // Mark as fully read

	return result, nil
}

// Schema returns the schema of the dataset.
func (r *ParquetReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *ParquetReader) Close() error {
	// Release all records
	for _, rec := range r.records {
		rec.Release()
	}
	r.records = nil

	// Close readers - note that we don't need to close arrowReader
	// as it doesn't have a Close method
	var err error

	if r.fileReader != nil {
		if err2 := r.fileReader.Close(); err2 != nil && err == nil {
			err = err2
		}
		r.fileReader = nil
	}

	// Close file
	if r.file != nil {
		if err2 := r.file.Close(); err2 != nil && err == nil {
			err = err2
		}
		r.file = nil
	}

	return err
}

package writers

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetWriter implements a writer for Parquet files.
type ParquetWriter struct {
	writer     *pqarrow.FileWriter
	file       *os.File
	schema     *arrow.Schema
	properties pqarrow.ArrowWriterProperties
}

// NewParquetWriter creates a new Parquet writer.
func NewParquetWriter(config core.WriterConfig) (core.DatasetWriter, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for Parquet writer")
	}

	// Create file
	file, err := os.Create(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet file: %w", err)
	}

	// Create default properties
	properties := pqarrow.NewArrowWriterProperties()

	// We will create the writer when we receive the first record
	// because we need the schema
	return &ParquetWriter{
		file:       file,
		properties: properties,
	}, nil
}

// Write writes a record to the file.
func (w *ParquetWriter) Write(ctx context.Context, record arrow.Record) error {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// If this is the first record, initialize the writer
	if w.writer == nil {
		// Get schema from record
		schema := record.Schema()

		// Create Parquet writer with SNAPPY compression
		writeProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
			parquet.WithDictionaryDefault(false),
		)

		// Create file writer
		writer, err := pqarrow.NewFileWriter(
			schema,
			w.file,
			writeProps,
			w.properties,
		)
		if err != nil {
			return fmt.Errorf("failed to create Parquet writer: %w", err)
		}

		w.writer = writer
		w.schema = schema
	}

	// Write the record
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

// Close closes the writer and flushes any pending data.
func (w *ParquetWriter) Close() error {
	var err error

	// Close the writer
	if w.writer != nil {
		if closeErr := w.writer.Close(); closeErr != nil {
			err = closeErr
		}
	}

	// Close the file
	if w.file != nil {
		if closeErr := w.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

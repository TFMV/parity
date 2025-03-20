package writers

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// ArrowWriter implements a writer for Arrow IPC files.
type ArrowWriter struct {
	writer *ipc.FileWriter
	file   *os.File
	schema *arrow.Schema
}

// NewArrowWriter creates a new Arrow IPC writer.
func NewArrowWriter(config core.WriterConfig) (core.DatasetWriter, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for Arrow writer")
	}

	// Create file
	file, err := os.Create(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow file: %w", err)
	}

	// We will create the writer when we receive the first record
	// because we need the schema
	return &ArrowWriter{
		file: file,
	}, nil
}

// Write writes a record to the file.
func (w *ArrowWriter) Write(ctx context.Context, record arrow.Record) error {
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

		// Create writer
		writer, err := ipc.NewFileWriter(w.file, ipc.WithSchema(schema))
		if err != nil {
			return fmt.Errorf("failed to create Arrow writer: %w", err)
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
func (w *ArrowWriter) Close() error {
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

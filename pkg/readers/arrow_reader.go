package readers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// ArrowReader implements a reader for Arrow IPC files.
type ArrowReader struct {
	schema     *arrow.Schema
	reader     *ipc.FileReader
	file       *os.File
	currentIdx int64
}

// NewArrowReader creates a new Arrow IPC reader.
func NewArrowReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for Arrow reader")
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

	if int(r.currentIdx) >= r.reader.NumRecords() {
		return nil, io.EOF
	}

	record, err := r.reader.Record(int(r.currentIdx))
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	r.currentIdx++
	return record, nil
}

// Schema returns the schema of the dataset.
func (r *ArrowReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *ArrowReader) Close() error {
	var err error

	if r.reader != nil {
		if closeErr := r.reader.Close(); closeErr != nil {
			err = closeErr
		}
	}

	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

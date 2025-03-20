package writers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// JSONWriter implements a writer for JSON files.
type JSONWriter struct {
	file     *os.File
	encoder  *json.Encoder
	isArray  bool
	firstRow bool
}

// NewJSONWriter creates a new JSON writer.
func NewJSONWriter(config core.WriterConfig) (core.DatasetWriter, error) {
	if config.Path == "" {
		return nil, errors.New("path is required for JSON writer")
	}

	// Create file
	file, err := os.Create(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON file: %w", err)
	}

	// Write opening bracket for array
	if _, err := file.WriteString("[\n"); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write opening bracket: %w", err)
	}

	// Create encoder
	encoder := json.NewEncoder(file)
	encoder.SetIndent("  ", "  ")

	return &JSONWriter{
		file:     file,
		encoder:  encoder,
		isArray:  true,
		firstRow: true,
	}, nil
}

// Write writes a record to the file.
func (w *JSONWriter) Write(ctx context.Context, record arrow.Record) error {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Convert records to JSON
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// Iterate through rows
	for i := 0; i < numRows; i++ {
		// Create a map for the row
		row := make(map[string]interface{})

		// Add field values
		for j := 0; j < numCols; j++ {
			col := record.Column(j)
			field := record.Schema().Field(j)

			// Get value based on type
			var value interface{}
			switch col := col.(type) {
			case *array.Int8:
				value = col.Value(i)
			case *array.Int16:
				value = col.Value(i)
			case *array.Int32:
				value = col.Value(i)
			case *array.Int64:
				value = col.Value(i)
			case *array.Uint8:
				value = col.Value(i)
			case *array.Uint16:
				value = col.Value(i)
			case *array.Uint32:
				value = col.Value(i)
			case *array.Uint64:
				value = col.Value(i)
			case *array.Float32:
				value = col.Value(i)
			case *array.Float64:
				value = col.Value(i)
			case *array.Boolean:
				value = col.Value(i)
			case *array.String:
				value = col.Value(i)
			default:
				value = nil
			}

			row[field.Name] = value
		}

		// If not the first row, write a comma
		if !w.firstRow {
			if _, err := w.file.WriteString(",\n"); err != nil {
				return fmt.Errorf("failed to write comma: %w", err)
			}
		} else {
			w.firstRow = false
		}

		// Write the row
		if err := w.encoder.Encode(row); err != nil {
			return fmt.Errorf("failed to encode row: %w", err)
		}
	}

	return nil
}

// Close closes the writer and flushes any pending data.
func (w *JSONWriter) Close() error {
	var err error

	// Write closing bracket for array
	if w.isArray {
		if _, closeErr := w.file.WriteString("\n]"); closeErr != nil {
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

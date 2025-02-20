package utils

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a test Arrow record
func createTestRecord() arrow.Record {
	pool := memory.NewGoAllocator()

	// Define schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Create sample data
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	return b.NewRecord()
}

// TestNewSingleRecordReader ensures the reader initializes correctly.
func TestNewSingleRecordReader(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)

	assert.NotNil(t, reader)
	assert.False(t, reader.done)
	assert.Equal(t, record.Schema(), reader.Schema())
}

// TestNext ensures Next() behaves correctly.
func TestNext(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)

	assert.True(t, reader.Next(), "First call to Next() should return true")
	assert.False(t, reader.Next(), "Subsequent calls to Next() should return false")
}

// TestRecord ensures Record() returns the expected record.
func TestRecord(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)

	assert.Equal(t, record, reader.Record(), "Record() should return the stored record")
}

// TestErr ensures Err() always returns nil.
func TestErr(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)

	assert.Nil(t, reader.Err(), "Err() should always return nil")
}

// TestRelease ensures Release() properly releases the record
func TestRelease(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)
	reader.Release()
}

// TestRetain ensures Retain() works correctly
func TestRetain(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)
	reader.Retain()
	reader.Release() // Release the retained reference
}

// TestClose ensures Close() does nothing but still executes safely.
func TestClose(t *testing.T) {
	record := createTestRecord()
	defer record.Release()

	reader := NewSingleRecordReader(record)

	err := reader.Close()

	assert.NoError(t, err, "Close() should not return an error")
}

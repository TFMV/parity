package core

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

type TestRow struct {
	ID    int32  `arrow:"id"`
	Name  string `arrow:"name"`
	Valid bool   `arrow:"valid"`
}

func TestReader(t *testing.T) {
	// Create Arrow schema and mock data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "valid", Type: arrow.FixedWidthTypes.Boolean},
		},
		nil,
	)

	// Allocate memory
	pool := memory.NewGoAllocator()

	// Create mock column data
	idBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int32{1, 2, 3}, nil)

	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	validBuilder := array.NewBooleanBuilder(pool)
	defer validBuilder.Release()
	validBuilder.AppendValues([]bool{true, false, true}, nil)

	// Create Arrow columns
	idCol := idBuilder.NewArray()
	defer idCol.Release()
	nameCol := nameBuilder.NewArray()
	defer nameCol.Release()
	validCol := validBuilder.NewArray()
	defer validCol.Release()

	// Create a record
	record := array.NewRecord(
		schema,
		[]arrow.Array{idCol, nameCol, validCol},
		3,
	)
	defer record.Release()

	// Initialize Reader with mock data
	reader := NewReader[TestRow](record)

	// Test NumRows
	assert.Equal(t, int64(3), reader.NumRows(), "NumRows should return the total number of rows")

	// Test Value retrieval
	row, err := reader.Value(0)
	assert.NoError(t, err, "Value(0) should not return an error")
	assert.Equal(t, TestRow{ID: 1, Name: "Alice", Valid: true}, row, "Value(0) should match the expected data")

	row, err = reader.Value(1)
	assert.NoError(t, err, "Value(1) should not return an error")
	assert.Equal(t, TestRow{ID: 2, Name: "Bob", Valid: false}, row, "Value(1) should match the expected data")

	row, err = reader.Value(2)
	assert.NoError(t, err, "Value(2) should not return an error")
	assert.Equal(t, TestRow{ID: 3, Name: "Charlie", Valid: true}, row, "Value(2) should match the expected data")

	// Test out-of-bounds Value
	_, err = reader.Value(3)
	assert.Error(t, err, "Value(3) should return an error as the index is out of bounds")
}

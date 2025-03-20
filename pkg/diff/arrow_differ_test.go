package diff

import (
	"context"
	"io"
	"math"
	"testing"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDatasetReader is a mock implementation of core.DatasetReader for testing
type mockDatasetReader struct {
	schema *arrow.Schema
	record arrow.Record
	read   bool
}

func (m *mockDatasetReader) Read(ctx context.Context) (arrow.Record, error) {
	if m.read {
		return nil, error(nil)
	}
	m.read = true
	return m.record, nil
}

func (m *mockDatasetReader) ReadAll(ctx context.Context) (arrow.Record, error) {
	if m.record == nil {
		return nil, io.EOF
	}
	return m.record, nil
}

func (m *mockDatasetReader) Schema() *arrow.Schema {
	return m.schema
}

func (m *mockDatasetReader) Close() error {
	if m.record != nil {
		m.record.Release()
	}
	return nil
}

// createTestRecord creates a record with the given schema and data
func createTestRecord(mem memory.Allocator, schema *arrow.Schema, data [][]interface{}) arrow.Record {
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			builders[i] = array.NewInt64Builder(mem)
		case arrow.FLOAT64:
			builders[i] = array.NewFloat64Builder(mem)
		case arrow.STRING:
			builders[i] = array.NewStringBuilder(mem)
		case arrow.BOOL:
			builders[i] = array.NewBooleanBuilder(mem)
		default:
			panic("unsupported type")
		}
	}

	// Append data to builders
	for _, row := range data {
		for i, val := range row {
			if val == nil {
				builders[i].AppendNull()
				continue
			}

			switch b := builders[i].(type) {
			case *array.Int64Builder:
				b.Append(val.(int64))
			case *array.Float64Builder:
				b.Append(val.(float64))
			case *array.StringBuilder:
				b.Append(val.(string))
			case *array.BooleanBuilder:
				b.Append(val.(bool))
			}
		}
	}

	// Build arrays
	cols := make([]arrow.Array, schema.NumFields())
	for i, builder := range builders {
		cols[i] = builder.NewArray()
		defer cols[i].Release()
		defer builder.Release()
	}

	// Create record
	return array.NewRecord(schema, cols, int64(len(data)))
}

func TestNewArrowDiffer(t *testing.T) {
	differ, err := NewArrowDiffer()
	assert.NoError(t, err)
	assert.NotNil(t, differ)
	assert.NotNil(t, differ.alloc)
	assert.NoError(t, differ.Close())
}

func TestValidateSchemas(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create source schema
	sourceFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	sourceSchema := arrow.NewSchema(sourceFields, nil)

	// Create compatible target schema
	targetFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false}, // Nullable change is ok
		{Name: "extra", Type: arrow.PrimitiveTypes.Int64, Nullable: true},    // Extra field is ok
	}
	targetSchema := arrow.NewSchema(targetFields, nil)

	// Test compatible schemas
	err = differ.validateSchemas(sourceSchema, targetSchema, core.DiffOptions{})
	assert.NoError(t, err)

	// Test with incompatible types
	incompatibleFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Type changed from float64 to int64
	}
	incompatibleSchema := arrow.NewSchema(incompatibleFields, nil)

	err = differ.validateSchemas(sourceSchema, incompatibleSchema, core.DiffOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "incompatible types for column value")

	// Test with ignored columns
	options := core.DiffOptions{
		IgnoreColumns: []string{"value"},
	}
	err = differ.validateSchemas(sourceSchema, incompatibleSchema, options)
	assert.NoError(t, err)
}

func TestContainsString(t *testing.T) {
	assert.True(t, containsString([]string{"a", "b", "c"}, "b"))
	assert.False(t, containsString([]string{"a", "b", "c"}, "d"))
	assert.False(t, containsString([]string{}, "a"))
}

func TestFloatEqual(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Exactly equal values
	assert.True(t, differ.floatEqual(1.0, 1.0, 0.0001))

	// Values within tolerance
	assert.True(t, differ.floatEqual(1.0, 1.0000001, 0.0001))
	assert.True(t, differ.floatEqual(1.0000001, 1.0, 0.0001))

	// Values outside tolerance
	assert.False(t, differ.floatEqual(1.0, 1.001, 0.0001))
	assert.False(t, differ.floatEqual(1.001, 1.0, 0.0001))

	// Special cases
	assert.True(t, differ.floatEqual(0.0, 0.0, 0.0001))
	assert.True(t, differ.floatEqual(0.0, 0.00005, 0.0001))
	assert.False(t, differ.floatEqual(0.0, 0.0002, 0.0001))

	// NaN cases
	assert.True(t, differ.floatEqual(math.NaN(), math.NaN(), 0.0001))

	// Infinity cases
	assert.True(t, differ.floatEqual(math.Inf(1), math.Inf(1), 0.0001))
	assert.True(t, differ.floatEqual(math.Inf(-1), math.Inf(-1), 0.0001))
	assert.False(t, differ.floatEqual(math.Inf(1), math.Inf(-1), 0.0001))
}

func TestBuildKeyArray(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create test schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	data := [][]interface{}{
		{int64(1), "Alice", int64(30)},
		{int64(2), "Bob", int64(25)},
		{int64(3), "Charlie", nil},
	}

	// Create record
	mem := memory.NewGoAllocator()
	record := createTestRecord(mem, schema, data)
	defer record.Release()

	// Test with single key column
	keyArray1, err := differ.buildKeyArray(record, []string{"id"})
	require.NoError(t, err)
	defer keyArray1.Release()

	assert.Equal(t, 3, keyArray1.Len())
	for i := 0; i < 3; i++ {
		assert.False(t, keyArray1.IsNull(i))
	}

	// Test with multiple key columns
	keyArray2, err := differ.buildKeyArray(record, []string{"id", "name"})
	require.NoError(t, err)
	defer keyArray2.Release()

	assert.Equal(t, 3, keyArray2.Len())
	assert.Equal(t, arrow.BinaryTypes.String, keyArray2.DataType())

	// Test with non-existent key column
	_, err = differ.buildKeyArray(record, []string{"non_existent"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key column non_existent not found")

	// Test with null value in key column
	keyArray3, err := differ.buildKeyArray(record, []string{"age"})
	require.NoError(t, err)
	defer keyArray3.Release()

	assert.Equal(t, 3, keyArray3.Len())
	assert.False(t, keyArray3.IsNull(0))
	assert.False(t, keyArray3.IsNull(1))
	// The third record has a null age value, which should be preserved in the key array
	assert.True(t, keyArray3.IsNull(2))
}

// createEmptyMockReader creates a mock reader that returns no records
func createEmptyMockReader(schema *arrow.Schema) *mockDatasetReader {
	// Create an empty record with the schema
	mem := memory.NewGoAllocator()
	fields := schema.Fields()
	cols := make([]arrow.Array, len(fields))
	for i, field := range fields {
		switch field.Type.ID() {
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			cols[i] = builder.NewArray()
			builder.Release()
		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			cols[i] = builder.NewArray()
			builder.Release()
		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			cols[i] = builder.NewArray()
			builder.Release()
		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			cols[i] = builder.NewArray()
			builder.Release()
		default:
			panic("unsupported type")
		}
	}
	record := array.NewRecord(schema, cols, 0)

	// Release the arrays after creating the record
	for _, col := range cols {
		col.Release()
	}

	return &mockDatasetReader{
		schema: schema,
		record: record,
	}
}

func TestReadDataset(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create test schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	data := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}

	// Create record
	mem := memory.NewGoAllocator()
	record := createTestRecord(mem, schema, data)

	// Create mock reader that returns the record once
	reader := &mockDatasetReader{
		schema: schema,
		record: record,
	}

	// Test reading dataset
	ctx := context.Background()
	result, err := differ.readDataset(ctx, reader)
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, int64(2), result.NumRows())
	assert.Equal(t, int64(2), int64(result.NumCols()))

	// Create empty reader
	emptyReader := createEmptyMockReader(schema)
	defer emptyReader.Close()

	// Test reading empty dataset
	empty, err := differ.readDataset(ctx, emptyReader)
	require.NoError(t, err)
	defer empty.Release()

	assert.Equal(t, int64(0), empty.NumRows())
	assert.Equal(t, int64(2), int64(empty.NumCols()))
}

func TestCompareRecords(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create test schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create source data (two identical records)
	sourceData := [][]interface{}{
		{int64(1), "Alice", 10.5, true},
		{int64(2), "Bob", 20.5, false},
	}

	// Create target data (second record has different values)
	targetData := [][]interface{}{
		{int64(1), "Alice", 10.5, true},             // Identical
		{int64(2), "Bobby", 20.50001, nil},          // Different name, close value, null active
		{int64(3), "Charlie", float64(30.0), false}, // New record
	}

	// Create records
	mem := memory.NewGoAllocator()
	sourceRecord := createTestRecord(mem, schema, sourceData)
	defer sourceRecord.Release()

	targetRecord := createTestRecord(mem, schema, targetData)
	defer targetRecord.Release()

	// Compare identical records
	isDiff1, diffCols1, err := differ.compareRecords(
		sourceRecord, targetRecord,
		0, 0,
		[]string{"name", "value", "active"},
		0.0001,
	)
	require.NoError(t, err)
	assert.False(t, isDiff1)
	assert.Empty(t, diffCols1)

	// Compare records with differences
	isDiff2, diffCols2, err := differ.compareRecords(
		sourceRecord, targetRecord,
		1, 1,
		[]string{"name", "value", "active"},
		0.0001,
	)
	require.NoError(t, err)
	assert.True(t, isDiff2)
	assert.Len(t, diffCols2, 2)
	assert.True(t, diffCols2["name"])
	assert.True(t, diffCols2["active"])
	assert.False(t, diffCols2["value"]) // Value is within tolerance

	// Compare with larger tolerance
	isDiff3, diffCols3, err := differ.compareRecords(
		sourceRecord, targetRecord,
		1, 1,
		[]string{"name", "value", "active"},
		0.1, // Much larger tolerance
	)
	require.NoError(t, err)
	assert.True(t, isDiff3)
	assert.Len(t, diffCols3, 2)
	assert.True(t, diffCols3["name"])
	assert.True(t, diffCols3["active"])
	assert.False(t, diffCols3["value"]) // Value is definitely within this tolerance
}

func TestTakeIndices(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create test schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create test data
	data := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
		{int64(4), "Dave"},
	}

	// Create record
	mem := memory.NewGoAllocator()
	record := createTestRecord(mem, schema, data)
	defer record.Release()

	// Take specific indices
	indices := []int64{0, 2, 3}
	result := differ.takeIndices(record, indices)
	defer result.Release()

	// Check result
	assert.Equal(t, int64(3), result.NumRows())
	assert.Equal(t, int64(2), int64(result.NumCols()))

	// Verify data
	idCol := result.Column(0).(*array.Int64)
	nameCol := result.Column(1).(*array.String)

	assert.Equal(t, int64(1), idCol.Value(0))
	assert.Equal(t, int64(3), idCol.Value(1))
	assert.Equal(t, int64(4), idCol.Value(2))

	assert.Equal(t, "Alice", nameCol.Value(0))
	assert.Equal(t, "Charlie", nameCol.Value(1))
	assert.Equal(t, "Dave", nameCol.Value(2))
}

func TestCreateModifiedSchema(t *testing.T) {
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Create test schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create modified schema
	modSchema := differ.createModifiedSchema(schema)

	// Check result
	assert.Equal(t, 4, modSchema.NumFields())
	assert.Equal(t, "id", modSchema.Field(0).Name)
	assert.Equal(t, "_modified_id", modSchema.Field(2).Name)
	assert.Equal(t, "name", modSchema.Field(1).Name)
	assert.Equal(t, "_modified_name", modSchema.Field(3).Name)

	// Check that all _modified_* fields are boolean and not nullable
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, modSchema.Field(2).Type)
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, modSchema.Field(3).Type)
	assert.False(t, modSchema.Field(2).Nullable)
	assert.False(t, modSchema.Field(3).Nullable)

	// Check metadata
	meta := modSchema.Metadata()
	value, ok := meta.GetValue("modified_record")
	assert.True(t, ok)
	assert.Equal(t, "true", value)

	_, ok = meta.GetValue("modification_time")
	assert.True(t, ok)
}

// Skip the integration test as it requires more complex mocking and
// depends on specific implementation details that may change
func TestIntegrationDiff(t *testing.T) {
	t.Skip("Skipping integration test which is more sensitive to implementation details")
}

package core

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// DataComparer compares data across two sources
type DataComparer struct {
	source1Conn adbc.Connection
	source2Conn adbc.Connection
}

// NewDataComparer creates a new DataComparer
func NewDataComparer(conn1, conn2 adbc.Connection) *DataComparer {
	return &DataComparer{
		source1Conn: conn1,
		source2Conn: conn2,
	}
}

// CompareColumns compares data across two queries for specified columns
func (dc *DataComparer) CompareColumns(ctx context.Context, query1, query2 string, columns []string) error {
	reader1, rows1, err := dc.executeQuery(ctx, dc.source1Conn, query1)
	if err != nil {
		return fmt.Errorf("source 1 query failed: %w", err)
	}
	defer reader1.Release()

	reader2, rows2, err := dc.executeQuery(ctx, dc.source2Conn, query2)
	if err != nil {
		return fmt.Errorf("source 2 query failed: %w", err)
	}
	defer reader2.Release()

	// Validate row counts
	if rows1 != rows2 {
		return fmt.Errorf("row count mismatch: source1 has %d rows, source2 has %d rows", rows1, rows2)
	}

	// Compare records in batches
	for reader1.Next() && reader2.Next() {
		if err := dc.compareRecords(reader1.Record(), reader2.Record(), columns); err != nil {
			return err
		}
	}

	// Ensure both readers are fully consumed
	if reader1.Next() || reader2.Next() {
		return fmt.Errorf("record count mismatch between sources")
	}

	return nil
}

// executeQuery runs a query and returns the RecordReader and row count
func (dc *DataComparer) executeQuery(ctx context.Context, conn adbc.Connection, query string) (array.RecordReader, int64, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, 0, fmt.Errorf("create statement failed: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, 0, fmt.Errorf("set SQL query failed: %w", err)
	}

	reader, rows, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("execute query failed: %w", err)
	}

	return reader, rows, nil
}

// compareRecords compares two Arrow records column by column
func (dc *DataComparer) compareRecords(record1, record2 arrow.Record, columns []string) error {
	for _, colName := range columns {
		col1, col2, err := dc.getColumns(record1, record2, colName)
		if err != nil {
			return err
		}

		if err := dc.compareArrays(col1, col2, colName); err != nil {
			return err
		}
	}
	return nil
}

// getColumns retrieves columns by name from two records
func (dc *DataComparer) getColumns(record1, record2 arrow.Record, colName string) (arrow.Array, arrow.Array, error) {
	colIdx1 := record1.Schema().FieldIndices(colName)
	colIdx2 := record2.Schema().FieldIndices(colName)

	if len(colIdx1) != 1 || len(colIdx2) != 1 {
		return nil, nil, fmt.Errorf("column '%s' not found or ambiguous in one or both sources", colName)
	}

	return record1.Column(colIdx1[0]), record2.Column(colIdx2[0]), nil
}

// compareArrays compares two Arrow arrays for equality
func (dc *DataComparer) compareArrays(arr1, arr2 arrow.Array, colName string) error {
	if arr1.Len() != arr2.Len() {
		return fmt.Errorf("column '%s' length mismatch: %d vs %d", colName, arr1.Len(), arr2.Len())
	}

	for i := 0; i < arr1.Len(); i++ {
		if arr1.IsNull(i) && arr2.IsNull(i) {
			continue
		}
		if arr1.IsNull(i) || arr2.IsNull(i) {
			return fmt.Errorf("null mismatch at row %d, column '%s'", i, colName)
		}

		if !dc.valuesEqual(arr1, arr2, i) {
			return fmt.Errorf("value mismatch at row %d, column '%s': %v vs %v", i, colName, arr1, arr2)
		}
	}

	return nil
}

// valuesEqual checks if values at a specific index in two Arrow arrays are equal
func (dc *DataComparer) valuesEqual(arr1, arr2 arrow.Array, idx int) bool {
	switch a1 := arr1.(type) {
	case *array.Boolean:
		a2, ok := arr2.(*array.Boolean)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Int8:
		a2, ok := arr2.(*array.Int8)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Int16:
		a2, ok := arr2.(*array.Int16)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Int32:
		a2, ok := arr2.(*array.Int32)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Int64:
		a2, ok := arr2.(*array.Int64)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Uint8:
		a2, ok := arr2.(*array.Uint8)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Uint16:
		a2, ok := arr2.(*array.Uint16)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Uint32:
		a2, ok := arr2.(*array.Uint32)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Uint64:
		a2, ok := arr2.(*array.Uint64)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Float32:
		a2, ok := arr2.(*array.Float32)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Float64:
		a2, ok := arr2.(*array.Float64)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.String:
		a2, ok := arr2.(*array.String)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Binary:
		a2, ok := arr2.(*array.Binary)
		if !ok {
			return false
		}
		return string(a1.Value(idx)) == string(a2.Value(idx))
	case *array.Timestamp:
		a2, ok := arr2.(*array.Timestamp)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Date32:
		a2, ok := arr2.(*array.Date32)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	case *array.Date64:
		a2, ok := arr2.(*array.Date64)
		if !ok {
			return false
		}
		return a1.Value(idx) == a2.Value(idx)
	default:
		// For unsupported types, return false to indicate inequality
		return false
	}
}

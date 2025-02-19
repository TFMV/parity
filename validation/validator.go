package validation

import (
	"context"
	"fmt"

	"github.com/TFMV/parity/integrations"
	"github.com/apache/arrow-go/v18/arrow"
)

type Validator struct {
	integration integrations.Integration
}

func NewValidator(integration integrations.Integration) *Validator {
	return &Validator{
		integration: integration,
	}
}

func (v *Validator) Compare(ctx context.Context, query1, query2 string) error {
	db1 := v.integration.Source1()
	db2 := v.integration.Source2()

	conn1, err := db1.OpenConnection()
	if err != nil {
		return err
	}
	defer conn1.Close()

	conn2, err := db2.OpenConnection()
	if err != nil {
		return err
	}
	defer conn2.Close()

	reader1, err := conn1.Query(ctx, query1)
	if err != nil {
		return fmt.Errorf("source 1 query failed: %w", err)
	}
	defer reader1.Release()

	reader2, err := conn2.Query(ctx, query2)
	if err != nil {
		return fmt.Errorf("source 2 query failed: %w", err)
	}
	defer reader2.Release()

	// Compare schemas first
	schema1 := reader1.Schema()
	schema2 := reader2.Schema()
	if !schema1.Equal(schema2) {
		return fmt.Errorf("schemas do not match: %v vs %v", schema1, schema2)
	}

	var rowCount int64
	// Compare records batch by batch
	for {
		hasMore1 := reader1.Next()
		hasMore2 := reader2.Next()

		if !hasMore1 && !hasMore2 {
			break // Both readers are done
		}
		if hasMore1 != hasMore2 {
			return fmt.Errorf("record count mismatch at row %d", rowCount)
		}

		record1 := reader1.Record()
		record2 := reader2.Record()

		// Compare number of rows in this batch
		if record1.NumRows() != record2.NumRows() {
			return fmt.Errorf("batch size mismatch at row %d: %d vs %d",
				rowCount, record1.NumRows(), record2.NumRows())
		}

		// Compare each column in the batch
		for i := 0; i < int(record1.NumCols()); i++ {
			col1 := record1.Column(i)
			col2 := record2.Column(i)

			// Compare each row in the column
			for j := 0; j < int(record1.NumRows()); j++ {
				if col1.IsNull(j) != col2.IsNull(j) {
					return fmt.Errorf("null value mismatch at row %d, column %s",
						rowCount+int64(j), schema1.Field(i).Name)
				}
				if !col1.IsNull(j) {
					// Compare actual values
					if !v.valuesEqual(col1, col2, j) {
						return fmt.Errorf("value mismatch at row %d, column %s",
							rowCount+int64(j), schema1.Field(i).Name)
					}
				}
			}
		}

		rowCount += int64(record1.NumRows())
	}

	return nil
}

// valuesEqual compares values from two Arrow arrays at a specific index
func (v *Validator) valuesEqual(arr1, arr2 arrow.Array, idx int) bool {
	// Implementation of value comparison based on type
	// This would need to handle all Arrow data types you expect to compare
	// See previous implementation of valuesEqual
	return true // Placeholder
}

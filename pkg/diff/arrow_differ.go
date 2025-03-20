// Package diff provides implementations for computing differences between datasets.
package diff

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
)

// modificationRecord represents a record that has been modified between source and target
type modificationRecord struct {
	sourceIdx, targetIdx int64
	columns              map[string]bool
}

// ArrowDiffer implements dataset diffing using the Apache Arrow library.
// It leverages Arrow's compute functions and in-memory processing for efficient comparison.
type ArrowDiffer struct {
	alloc memory.Allocator
}

// NewArrowDiffer creates a new Arrow-based differ.
func NewArrowDiffer() (*ArrowDiffer, error) {
	return &ArrowDiffer{
		alloc: memory.NewGoAllocator(),
	}, nil
}

// Close closes the differ and releases resources.
func (d *ArrowDiffer) Close() error {
	// No explicit resources to close
	return nil
}

// Diff computes the difference between two datasets.
func (d *ArrowDiffer) Diff(ctx context.Context, source, target core.DatasetReader, options core.DiffOptions) (*core.DiffResult, error) {
	// Get schemas for source and target
	sourceSchema := source.Schema()
	targetSchema := target.Schema()

	// Ensure schemas are compatible for comparison
	if err := d.validateSchemas(sourceSchema, targetSchema, options); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// Determine key columns for record matching
	keyColumns := options.KeyColumns
	if len(keyColumns) == 0 {
		// Try to use primary key fields if available
		meta := sourceSchema.Metadata()
		if pkValue, ok := meta.GetValue("primary_key"); ok {
			keyColumns = append(keyColumns, pkValue)
		} else {
			// Use all columns as key if not specified
			for _, field := range sourceSchema.Fields() {
				if !containsString(options.IgnoreColumns, field.Name) {
					keyColumns = append(keyColumns, field.Name)
				}
			}
		}
	}

	// Read the complete datasets into memory
	// Note: For large datasets, we could implement chunking or streaming here
	sourceData, err := d.readDataset(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("failed to read source dataset: %w", err)
	}
	defer sourceData.Release()

	targetData, err := d.readDataset(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("failed to read target dataset: %w", err)
	}
	defer targetData.Release()

	// Compute differences
	return d.computeDiff(ctx, sourceData, targetData, keyColumns, options)
}

// validateSchemas checks if the schemas are compatible for comparison.
func (d *ArrowDiffer) validateSchemas(sourceSchema, targetSchema *arrow.Schema, options core.DiffOptions) error {
	// Check if schemas match, ignoring ignored columns
	sourceFields := make(map[string]arrow.Field)
	for _, field := range sourceSchema.Fields() {
		if !containsString(options.IgnoreColumns, field.Name) {
			sourceFields[field.Name] = field
		}
	}

	for _, field := range targetSchema.Fields() {
		if containsString(options.IgnoreColumns, field.Name) {
			continue
		}

		sourceField, ok := sourceFields[field.Name]
		if !ok {
			// Target has a column that source doesn't - this is okay for comparison
			continue
		}

		// Check data type compatibility
		if !arrow.TypeEqual(sourceField.Type, field.Type) {
			return fmt.Errorf("incompatible types for column %s: source=%s, target=%s",
				field.Name, sourceField.Type, field.Type)
		}
	}

	return nil
}

// readDataset reads all records from a dataset into a single table.
func (d *ArrowDiffer) readDataset(ctx context.Context, reader core.DatasetReader) (arrow.Record, error) {
	// Check if the reader implements the ReadAll method
	if readAllReader, ok := reader.(interface {
		ReadAll(context.Context) (arrow.Record, error)
	}); ok {
		// Use the ReadAll method if available
		record, err := readAllReader.ReadAll(ctx)
		if err != nil {
			return nil, err
		}
		return record, nil
	}

	// Fallback to the original implementation for readers that don't have ReadAll
	schema := reader.Schema()

	var records []arrow.Record
	for {
		select {
		case <-ctx.Done():
			// Clean up any records we've read so far
			for _, r := range records {
				r.Release()
			}
			return nil, ctx.Err()
		default:
		}

		record, err := reader.Read(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			// Clean up any records we've read so far
			for _, r := range records {
				r.Release()
			}
			return nil, err
		}

		if record == nil || record.NumRows() == 0 {
			break
		}

		// Retain the record before adding it to our list
		// This is safer than cloning as it properly handles reference counting
		record.Retain()
		records = append(records, record)
	}

	// Combine all records into a single record
	if len(records) == 0 {
		// Return an empty record with the schema
		return createEmptyRecord(d.alloc, schema), nil
	}

	// If there's only one record, return a clone to ensure ownership
	if len(records) == 1 {
		result := cloneRecord(d.alloc, records[0])
		// Release the original after cloning
		records[0].Release()
		return result, nil
	}

	// Use tableFromRecords to combine multiple records efficiently
	result, err := tableToRecord(d.alloc, schema, records)

	// Release all records after combining them
	for _, r := range records {
		r.Release()
	}

	return result, err
}

// createEmptyRecord creates an empty record with the given schema
func createEmptyRecord(alloc memory.Allocator, schema *arrow.Schema) arrow.Record {
	// Create empty arrays for each field in the schema
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		// Create an empty array of the appropriate type
		arrBuilder := array.NewBuilder(alloc, field.Type)
		cols[i] = arrBuilder.NewArray()
		arrBuilder.Release()
	}

	// Create the empty record
	record := array.NewRecord(schema, cols, 0)

	// Release the arrays after creating the record
	for _, col := range cols {
		col.Release()
	}

	return record
}

// cloneRecord creates a deep copy of a record to ensure ownership
func cloneRecord(alloc memory.Allocator, record arrow.Record) arrow.Record {
	schema := record.Schema()

	// Create new arrays for each column
	cols := make([]arrow.Array, record.NumCols())
	for i, col := range record.Columns() {
		// Create a new array from the data in the original
		cols[i] = array.MakeFromData(col.Data())
	}

	// Create a new record with the cloned data
	return array.NewRecord(schema, cols, record.NumRows())
}

// tableToRecord efficiently converts a set of records to a single record
func tableToRecord(alloc memory.Allocator, schema *arrow.Schema, records []arrow.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to combine")
	}

	// Combine records into a table
	table := array.NewTableFromRecords(schema, records)
	defer table.Release()

	// Determine the total number of rows
	totalRows := int64(0)
	for _, rec := range records {
		totalRows += rec.NumRows()
	}

	// Use a TableReader to get a consolidated record
	tableReader := array.NewTableReader(table, totalRows)
	defer tableReader.Release()

	if !tableReader.Next() {
		return nil, fmt.Errorf("failed to read combined table")
	}

	// Get the consolidated record
	record := tableReader.Record()

	// Create a new record that we own and return it
	return cloneRecord(alloc, record), nil
}

// computeDiff computes differences between two records (tables) using Arrow's compute functions.
func (d *ArrowDiffer) computeDiff(
	ctx context.Context,
	source, target arrow.Record,
	keyColumns []string,
	options core.DiffOptions,
) (*core.DiffResult, error) {
	// Build key arrays for source and target
	sourceKey, err := d.buildKeyArray(source, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to build source keys: %w", err)
	}
	defer sourceKey.Release()

	targetKey, err := d.buildKeyArray(target, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to build target keys: %w", err)
	}
	defer targetKey.Release()

	// Create maps to track record indices by key
	sourceKeyMap := make(map[string]int64)
	targetKeyMap := make(map[string]int64)

	// Populate source key map
	for i := int64(0); i < int64(sourceKey.Len()); i++ {
		key := d.arrayValueToString(sourceKey, int(i))
		sourceKeyMap[key] = i
	}

	// Populate target key map
	for i := int64(0); i < int64(targetKey.Len()); i++ {
		key := d.arrayValueToString(targetKey, int(i))
		targetKeyMap[key] = i
	}

	// Track indices for added/deleted/modified records
	var addedIndices, deletedIndices []int64
	var modifiedSourceIndices, modifiedTargetIndices []int64
	modifiedColumns := make(map[string]int64)

	// Find added records (in target but not in source)
	for i := int64(0); i < int64(targetKey.Len()); i++ {
		key := d.arrayValueToString(targetKey, int(i))
		if _, exists := sourceKeyMap[key]; !exists {
			addedIndices = append(addedIndices, i)
		}
	}

	// Find deleted records (in source but not in target)
	for i := int64(0); i < int64(sourceKey.Len()); i++ {
		key := d.arrayValueToString(sourceKey, int(i))
		if _, exists := targetKeyMap[key]; !exists {
			deletedIndices = append(deletedIndices, i)
		}
	}

	// Find and compute modified records
	// For each key in both source and target, compare the records
	var mutex sync.Mutex
	var g errgroup.Group
	semaphore := make(chan struct{}, options.NumWorkers)
	if options.NumWorkers == 0 {
		// Default to 4 workers if not specified
		semaphore = make(chan struct{}, 4)
	}

	// Get the columns to compare (excluding key and ignored columns)
	compareColumns := d.getCompareColumns(source, keyColumns, options.IgnoreColumns)

	// Prepare structures to hold modifications
	var modifications []modificationRecord

	// Compare records with matching keys
	for i := int64(0); i < int64(sourceKey.Len()); i++ {
		key := d.arrayValueToString(sourceKey, int(i))
		if targetIdx, exists := targetKeyMap[key]; exists {
			// Both records exist, need to compare field by field
			sourceIdx := i

			if options.Parallel {
				semaphore <- struct{}{} // Acquire semaphore
				g.Go(func() error {
					defer func() { <-semaphore }() // Release semaphore when done

					isDifferent, diffColumns, err := d.compareRecords(
						source, target,
						sourceIdx, targetIdx,
						compareColumns,
						options.Tolerance,
					)
					if err != nil {
						return err
					}

					if isDifferent {
						mutex.Lock()
						defer mutex.Unlock()

						modifications = append(modifications, modificationRecord{
							sourceIdx: sourceIdx,
							targetIdx: targetIdx,
							columns:   diffColumns,
						})

						// Update column modification counts
						for col := range diffColumns {
							modifiedColumns[col]++
						}
					}
					return nil
				})
			} else {
				// Serial comparison
				isDifferent, diffColumns, err := d.compareRecords(
					source, target,
					sourceIdx, targetIdx,
					compareColumns,
					options.Tolerance,
				)
				if err != nil {
					return nil, err
				}

				if isDifferent {
					modifications = append(modifications, modificationRecord{
						sourceIdx: sourceIdx,
						targetIdx: targetIdx,
						columns:   diffColumns,
					})

					// Update column modification counts
					for col := range diffColumns {
						modifiedColumns[col]++
					}
				}
			}
		}
	}

	// Wait for all comparison goroutines to complete
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error during parallel record comparison: %w", err)
	}

	// Extract the modified indices from the modifications slice
	for _, mod := range modifications {
		modifiedSourceIndices = append(modifiedSourceIndices, mod.sourceIdx)
		modifiedTargetIndices = append(modifiedTargetIndices, mod.targetIdx)
	}

	// Create result records by taking slices of the original records
	var addedRecord, deletedRecord, modifiedRecord arrow.Record

	if len(addedIndices) > 0 {
		addedRecord = d.takeIndices(target, addedIndices)
	} else {
		// Create empty record with target schema
		addedRecord = array.NewRecord(target.Schema(), []arrow.Array{}, 0)
	}

	if len(deletedIndices) > 0 {
		deletedRecord = d.takeIndices(source, deletedIndices)
	} else {
		// Create empty record with source schema
		deletedRecord = array.NewRecord(source.Schema(), []arrow.Array{}, 0)
	}

	if len(modifiedSourceIndices) > 0 {
		// For modified records, we use the source record with additional columns
		// to indicate which fields were modified
		modifiedRecord = d.createModifiedRecord(
			source, target,
			modifiedSourceIndices, modifiedTargetIndices,
			modifications,
		)
	} else {
		// Create an empty record with the appropriate schema for modified records
		modSchema := d.createModifiedSchema(source.Schema())
		modifiedRecord = array.NewRecord(modSchema, []arrow.Array{}, 0)
	}

	// Create diff summary
	summary := core.DiffSummary{
		TotalSource: int64(source.NumRows()),
		TotalTarget: int64(target.NumRows()),
		Added:       int64(len(addedIndices)),
		Deleted:     int64(len(deletedIndices)),
		Modified:    int64(len(modifiedSourceIndices)),
		Columns:     modifiedColumns,
	}

	return &core.DiffResult{
		Added:    addedRecord,
		Deleted:  deletedRecord,
		Modified: modifiedRecord,
		Summary:  summary,
	}, nil
}

// buildKeyArray creates a dict array from key columns for efficient comparison
func (d *ArrowDiffer) buildKeyArray(record arrow.Record, keyColumns []string) (arrow.Array, error) {
	// Special case: if there's just one key column, use it directly
	if len(keyColumns) == 1 {
		idx := d.findColumnIndex(record, keyColumns[0])
		if idx < 0 {
			return nil, fmt.Errorf("key column %s not found", keyColumns[0])
		}

		// Clone the column to avoid ownership issues
		col := record.Column(idx)
		// Create a new array from the existing one
		arr := array.MakeFromData(col.Data())
		return arr, nil
	}

	// For multiple key columns, concatenate them into a dictionary array
	var keyValues []string
	for i := int64(0); i < record.NumRows(); i++ {
		var sb strings.Builder
		for j, colName := range keyColumns {
			colIdx := d.findColumnIndex(record, colName)
			if colIdx < 0 {
				return nil, fmt.Errorf("key column %s not found", colName)
			}

			col := record.Column(colIdx)
			if col.IsNull(int(i)) {
				sb.WriteString("NULL")
			} else {
				sb.WriteString(d.arrayValueToString(col, int(i)))
			}

			if j < len(keyColumns)-1 {
				sb.WriteString(":")
			}
		}
		keyValues = append(keyValues, sb.String())
	}

	// Create a string array from the concatenated keys
	builder := array.NewStringBuilder(d.alloc)
	defer builder.Release()

	for _, val := range keyValues {
		builder.Append(val)
	}

	return builder.NewArray(), nil
}

// compareRecords compares two records with the same key
func (d *ArrowDiffer) compareRecords(
	source, target arrow.Record,
	sourceIdx, targetIdx int64,
	compareColumns []string,
	tolerance float64,
) (bool, map[string]bool, error) {
	isDifferent := false
	diffColumns := make(map[string]bool)

	for _, colName := range compareColumns {
		sourceColIdx := d.findColumnIndex(source, colName)
		if sourceColIdx < 0 {
			// Column not in source, skip it
			continue
		}

		targetColIdx := d.findColumnIndex(target, colName)
		if targetColIdx < 0 {
			// Column not in target, mark as different
			isDifferent = true
			diffColumns[colName] = true
			continue
		}

		sourceCol := source.Column(sourceColIdx)
		targetCol := target.Column(targetColIdx)

		// Handle null values
		if sourceCol.IsNull(int(sourceIdx)) && targetCol.IsNull(int(targetIdx)) {
			// Both null, considered equal
			continue
		}

		if sourceCol.IsNull(int(sourceIdx)) || targetCol.IsNull(int(targetIdx)) {
			// One is null, the other isn't
			isDifferent = true
			diffColumns[colName] = true
			continue
		}

		// Compare values based on type
		isEqual, err := d.compareValues(
			sourceCol, targetCol,
			int(sourceIdx), int(targetIdx),
			tolerance,
		)
		if err != nil {
			return false, nil, err
		}

		if !isEqual {
			isDifferent = true
			diffColumns[colName] = true
		}
	}

	return isDifferent, diffColumns, nil
}

// compareValues compares two values in arrays, with tolerance for floating point
func (d *ArrowDiffer) compareValues(
	sourceCol, targetCol arrow.Array,
	sourceIdx, targetIdx int,
	tolerance float64,
) (bool, error) {
	if !arrow.TypeEqual(sourceCol.DataType(), targetCol.DataType()) {
		// Different types, compare as strings
		sourceStr := d.arrayValueToString(sourceCol, sourceIdx)
		targetStr := d.arrayValueToString(targetCol, targetIdx)
		return sourceStr == targetStr, nil
	}

	// Handle numeric types with tolerance
	switch sourceCol.DataType().ID() {
	case arrow.FLOAT32:
		sourceVal := sourceCol.(*array.Float32).Value(sourceIdx)
		targetVal := targetCol.(*array.Float32).Value(targetIdx)
		return d.floatEqual(float64(sourceVal), float64(targetVal), tolerance), nil

	case arrow.FLOAT64:
		sourceVal := sourceCol.(*array.Float64).Value(sourceIdx)
		targetVal := targetCol.(*array.Float64).Value(targetIdx)
		return d.floatEqual(sourceVal, targetVal, tolerance), nil

	case arrow.DECIMAL128:
		// For decimals, compare as strings to handle precision correctly
		sourceStr := d.arrayValueToString(sourceCol, sourceIdx)
		targetStr := d.arrayValueToString(targetCol, targetIdx)
		return sourceStr == targetStr, nil

	case arrow.DECIMAL256:
		// For decimals, compare as strings to handle precision correctly
		sourceStr := d.arrayValueToString(sourceCol, sourceIdx)
		targetStr := d.arrayValueToString(targetCol, targetIdx)
		return sourceStr == targetStr, nil

	default:
		// For other types, compare as strings
		sourceStr := d.arrayValueToString(sourceCol, sourceIdx)
		targetStr := d.arrayValueToString(targetCol, targetIdx)
		return sourceStr == targetStr, nil
	}
}

// floatEqual compares two float values with tolerance
func (d *ArrowDiffer) floatEqual(a, b, tolerance float64) bool {
	if a == b {
		return true
	}

	// Handle special cases like NaN, +/-Inf
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}

	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}

	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}

	// Compare with tolerance
	diff := math.Abs(a - b)
	if a == 0 || b == 0 {
		return diff < tolerance
	}

	// Use relative tolerance based on the larger value
	return diff/math.Max(math.Abs(a), math.Abs(b)) < tolerance
}

// takeIndices creates a new record by selecting specific indices from an existing record
func (d *ArrowDiffer) takeIndices(record arrow.Record, indices []int64) arrow.Record {
	// Convert Go slice to Arrow array for indices
	indicesBuilder := array.NewInt64Builder(d.alloc)
	defer indicesBuilder.Release()

	for _, idx := range indices {
		indicesBuilder.Append(idx)
	}

	indicesArr := indicesBuilder.NewArray()
	defer indicesArr.Release()

	// Take the indices from each column by directly slicing
	cols := make([]arrow.Array, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)

		// Create a builder for this column type
		builder := array.NewBuilder(d.alloc, col.DataType())
		defer builder.Release()

		// Append only the selected indices
		for _, idx := range indices {
			if int(idx) < col.Len() {
				if col.IsNull(int(idx)) {
					builder.AppendNull()
				} else {
					// This is a simplification - a full implementation would need
					// type-specific appending for each Arrow type
					val := col.GetOneForMarshal(int(idx))
					builder.AppendValueFromString(fmt.Sprintf("%v", val))
				}
			}
		}

		cols[i] = builder.NewArray()
	}

	// Create new record with the selected indices
	result := array.NewRecord(record.Schema(), cols, int64(len(indices)))

	// Release the column arrays after they're added to the record
	for _, col := range cols {
		col.Release()
	}

	return result
}

// createModifiedRecord creates a record for rows that were modified
func (d *ArrowDiffer) createModifiedRecord(
	source, target arrow.Record,
	sourceIndices, targetIndices []int64,
	modifications []modificationRecord,
) arrow.Record {
	// Create a schema for the modified record
	modSchema := d.createModifiedSchema(source.Schema())

	// Take source rows that were modified
	sourceRecord := d.takeIndices(source, sourceIndices)
	defer sourceRecord.Release()

	// Create arrays for each column in the result
	cols := make([]arrow.Array, modSchema.NumFields())

	// Copy source columns
	for i, field := range source.Schema().Fields() {
		colIdx := d.findColumnIndex(sourceRecord, field.Name)
		if colIdx >= 0 {
			col := sourceRecord.Column(colIdx)
			// Clone the column
			cols[i] = array.MakeFromData(col.Data())
		}
	}

	// Add indicator columns for each field in the source schema
	offset := source.Schema().NumFields()
	fieldNames := make([]string, source.Schema().NumFields())
	for i, field := range source.Schema().Fields() {
		fieldNames[i] = field.Name

		// Create a boolean array indicating if the field was modified
		modBuilder := array.NewBooleanBuilder(d.alloc)
		defer modBuilder.Release()

		for _, mod := range modifications {
			// Check if this column was modified for this record
			wasModified := mod.columns[field.Name]
			modBuilder.Append(wasModified)
		}

		cols[offset+i] = modBuilder.NewArray()
	}

	// Create the modified record
	result := array.NewRecord(modSchema, cols, int64(len(sourceIndices)))

	// Release the column arrays after they're added to the record
	for _, col := range cols {
		col.Release()
	}

	return result
}

// createModifiedSchema creates a schema for the modified record
func (d *ArrowDiffer) createModifiedSchema(sourceSchema *arrow.Schema) *arrow.Schema {
	// Start with the source schema fields
	fields := make([]arrow.Field, sourceSchema.NumFields()*2)

	// Copy source schema fields
	for i, field := range sourceSchema.Fields() {
		fields[i] = field
	}

	// Add indicator fields
	offset := sourceSchema.NumFields()
	for i, field := range sourceSchema.Fields() {
		fields[offset+i] = arrow.Field{
			Name:     "_modified_" + field.Name,
			Type:     arrow.FixedWidthTypes.Boolean,
			Nullable: false,
		}
	}

	// Create metadata for the new schema
	// Get existing metadata
	meta := sourceSchema.Metadata()

	// Create new metadata with existing key-values plus our additions
	keys := make([]string, 0, meta.Len()+2)
	values := make([]string, 0, meta.Len()+2)

	// Copy existing metadata
	for i := 0; i < meta.Len(); i++ {
		keys = append(keys, meta.Keys()[i])
		values = append(values, meta.Values()[i])
	}

	// Add new metadata
	keys = append(keys, "modified_record")
	values = append(values, "true")

	keys = append(keys, "modification_time")
	values = append(values, time.Now().Format(time.RFC3339))

	// Create the new metadata
	newMeta := arrow.NewMetadata(keys, values)

	return arrow.NewSchema(fields, &newMeta)
}

// getCompareColumns returns columns to compare, excluding key and ignored columns
func (d *ArrowDiffer) getCompareColumns(record arrow.Record, keyColumns, ignoreColumns []string) []string {
	var result []string

	for i := 0; i < record.Schema().NumFields(); i++ {
		fieldName := record.Schema().Field(i).Name

		if containsString(keyColumns, fieldName) || containsString(ignoreColumns, fieldName) {
			continue
		}

		result = append(result, fieldName)
	}

	return result
}

// findColumnIndex returns the index of a column by name, or -1 if not found
func (d *ArrowDiffer) findColumnIndex(record arrow.Record, colName string) int {
	for i := 0; i < record.Schema().NumFields(); i++ {
		if record.Schema().Field(i).Name == colName {
			return i
		}
	}
	return -1
}

// arrayValueToString converts a value at a specific index to a string
func (d *ArrowDiffer) arrayValueToString(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "NULL"
	}

	// Use Arrow's value formatting functions
	val := arr.GetOneForMarshal(idx)
	if val == nil {
		return "NULL"
	}

	return fmt.Sprintf("%v", val)
}

// containsString checks if a slice contains a string
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

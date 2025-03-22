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
	// Check if we have key columns - they're essential for large datasets
	if len(keyColumns) == 0 {
		// No key columns specified, print performance warning
		fmt.Println("WARNING: No key columns specified for diffing. This may be extremely slow for large datasets.")
		fmt.Println("Hint: Specify key columns for better performance.")

		// For large datasets, generate a warning if they're over a certain size
		if source.NumRows() > 100000 || target.NumRows() > 100000 {
			fmt.Printf("Large dataset detected: %d source rows, %d target rows\n", source.NumRows(), target.NumRows())
			fmt.Println("Processing without key columns may be very slow. Consider specifying key columns.")
		}
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Build key arrays for source and target
	fmt.Println("Building key arrays...")
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

	fmt.Println("Creating key maps...")
	// Create maps to track record indices by key
	// Pre-allocate maps with capacity based on number of rows
	sourceKeyMap := make(map[string]int64, source.NumRows())
	targetKeyMap := make(map[string]int64, target.NumRows())

	// Populate source key map
	for i := int64(0); i < source.NumRows(); i++ {
		// Check for cancellation periodically
		if i > 0 && i%1000000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			fmt.Printf("Processed %d/%d source keys...\n", i, source.NumRows())
		}

		key := d.arrayValueToString(sourceKey, int(i))
		sourceKeyMap[key] = i
	}

	// Populate target key map
	for i := int64(0); i < target.NumRows(); i++ {
		// Check for cancellation periodically
		if i > 0 && i%1000000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			fmt.Printf("Processed %d/%d target keys...\n", i, target.NumRows())
		}

		key := d.arrayValueToString(targetKey, int(i))
		targetKeyMap[key] = i
	}

	fmt.Println("Finding added and deleted records...")
	// Track indices for added/deleted/modified records
	// Pre-allocate slices with a capacity hint
	estimatedChanges := int(float64(source.NumRows()+target.NumRows()) * 0.1) // Assume ~10% changes
	if estimatedChanges < 1000 {
		estimatedChanges = 1000
	}

	addedIndices := make([]int64, 0, estimatedChanges)
	deletedIndices := make([]int64, 0, estimatedChanges)
	modifiedColumns := make(map[string]int64, source.Schema().NumFields())

	// Find added records (in target but not in source)
	for i := int64(0); i < target.NumRows(); i++ {
		// Check for cancellation periodically
		if i > 0 && i%1000000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		key := d.arrayValueToString(targetKey, int(i))
		if _, exists := sourceKeyMap[key]; !exists {
			addedIndices = append(addedIndices, i)
		}
	}

	// Find deleted records (in source but not in target)
	for i := int64(0); i < source.NumRows(); i++ {
		// Check for cancellation periodically
		if i > 0 && i%1000000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		key := d.arrayValueToString(sourceKey, int(i))
		if _, exists := targetKeyMap[key]; !exists {
			deletedIndices = append(deletedIndices, i)
		}
	}

	fmt.Println("Comparing matching records...")
	// Find and compute modified records
	// For each key in both source and target, compare the records
	var mutex sync.Mutex
	var g errgroup.Group

	// Initialize semaphore for worker count
	numWorkers := options.NumWorkers
	if numWorkers <= 0 {
		numWorkers = 4 // Default to 4 workers
	}

	// Create semaphore with the specified number of workers
	semaphore := make(chan struct{}, numWorkers)

	// Get the columns to compare (excluding key and ignored columns)
	compareColumns := d.getCompareColumns(source, keyColumns, options.IgnoreColumns)

	// Prepare structures to hold modifications
	var modifications []modificationRecord

	// Create source keys slice for parallel processing to avoid map access contention
	sourceKeys := make([]string, 0, len(sourceKeyMap))
	for key := range sourceKeyMap {
		sourceKeys = append(sourceKeys, key)
	}

	// Compare records with matching keys
	if options.Parallel && numWorkers > 1 {
		fmt.Printf("Using parallel comparison with %d workers\n", numWorkers)

		// Calculate chunk size for balanced distribution of work
		chunkSize := (len(sourceKeys) + numWorkers - 1) / numWorkers

		// Process in chunks to reduce goroutine overhead
		for i := 0; i < len(sourceKeys); i += chunkSize {
			// Capture loop variables
			start := i
			end := start + chunkSize
			if end > len(sourceKeys) {
				end = len(sourceKeys)
			}

			// Submit this chunk to the worker pool
			semaphore <- struct{}{} // Acquire semaphore
			g.Go(func() error {
				defer func() { <-semaphore }() // Release semaphore when done

				localMods := make([]modificationRecord, 0, end-start)
				localColMods := make(map[string]int64)

				// Process this chunk of keys
				for j := start; j < end; j++ {
					key := sourceKeys[j]
					sourceIdx := sourceKeyMap[key]

					if targetIdx, exists := targetKeyMap[key]; exists {
						// Records exist in both source and target, compare them
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
							// Found differences, record them
							localMods = append(localMods, modificationRecord{
								sourceIdx: sourceIdx,
								targetIdx: targetIdx,
								columns:   diffColumns,
							})

							// Update local column modification counts
							for col := range diffColumns {
								localColMods[col]++
							}
						}
					}
				}

				// Merge results back to main arrays under lock
				if len(localMods) > 0 {
					mutex.Lock()
					defer mutex.Unlock()

					modifications = append(modifications, localMods...)

					// Merge column counts
					for col, count := range localColMods {
						modifiedColumns[col] += count
					}
				}

				return nil
			})
		}

		// Wait for all comparison goroutines to complete
		if err := g.Wait(); err != nil {
			return nil, fmt.Errorf("error during parallel record comparison: %w", err)
		}
	} else {
		// Serial comparison
		fmt.Println("Using serial comparison")
		for key, sourceIdx := range sourceKeyMap {
			// Check for cancellation periodically
			if sourceIdx > 0 && sourceIdx%100000 == 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
				}
				fmt.Printf("Compared %d/%d matching records...\n", sourceIdx, len(sourceKeyMap))
			}

			if targetIdx, exists := targetKeyMap[key]; exists {
				// Both records exist, need to compare field by field
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

	fmt.Println("Creating result records...")
	// Extract the modified indices from the modifications slice
	modifiedSourceIndices := make([]int64, len(modifications))
	modifiedTargetIndices := make([]int64, len(modifications))

	for i, mod := range modifications {
		modifiedSourceIndices[i] = mod.sourceIdx
		modifiedTargetIndices[i] = mod.targetIdx
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
		TotalSource: source.NumRows(),
		TotalTarget: target.NumRows(),
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

// buildKeyArray creates a string array from record keys for efficient comparisons.
func (d *ArrowDiffer) buildKeyArray(record arrow.Record, keyColumns []string) (arrow.Array, error) {
	// Early return if no key columns
	if len(keyColumns) == 0 {
		return nil, fmt.Errorf("no key columns specified")
	}

	// For a single key column that's already a string type, just extract it
	if len(keyColumns) == 1 {
		colIdx := d.findColumnIndex(record, keyColumns[0])
		if colIdx == -1 {
			return nil, fmt.Errorf("key column not found: %s", keyColumns[0])
		}

		col := record.Column(colIdx)

		// If it's already a string column, return a reference
		if col.DataType().ID() == arrow.STRING {
			col.Retain() // Must retain since we're returning a reference
			return col, nil
		}
	}

	// For multiple key columns or non-string columns, we need to concatenate them
	builder := array.NewStringBuilder(d.alloc)
	defer builder.Release()

	// Process record batch in chunks of ~1M rows at a time to reduce memory pressure
	const chunkSize = 1000000
	numRows := int(record.NumRows())

	// Pre-allocate memory for the expected number of strings
	builder.Reserve(min(numRows, chunkSize))

	// Process in chunks
	for startRow := 0; startRow < numRows; startRow += chunkSize {
		endRow := min(startRow+chunkSize, numRows)

		// Build keys for this chunk
		for i := startRow; i < endRow; i++ {
			// For each row, concatenate the key column values
			var keyBuilder strings.Builder

			for j, keyCol := range keyColumns {
				colIdx := d.findColumnIndex(record, keyCol)
				if colIdx == -1 {
					return nil, fmt.Errorf("key column not found: %s", keyCol)
				}

				// Add a separator between key parts
				if j > 0 {
					keyBuilder.WriteByte('|')
				}

				col := record.Column(colIdx)
				if col.IsNull(i) {
					keyBuilder.WriteString("NULL")
				} else {
					// Convert value to string representation
					keyBuilder.WriteString(d.arrayValueToString(col, i))
				}
			}

			builder.Append(keyBuilder.String())
		}
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
	// If no indices to take, return empty record with the schema
	if len(indices) == 0 {
		return array.NewRecord(record.Schema(), []arrow.Array{}, 0)
	}

	// For large result sets, print progress
	if len(indices) > 100000 {
		fmt.Printf("Extracting %d records from dataset of %d rows\n", len(indices), record.NumRows())
	}

	// Convert Go slice to Arrow array for indices
	indicesBuilder := array.NewInt64Builder(d.alloc)
	defer indicesBuilder.Release()

	// Pre-allocate capacity for better performance
	indicesBuilder.Reserve(len(indices))

	for _, idx := range indices {
		indicesBuilder.Append(idx)
	}

	indicesArr := indicesBuilder.NewArray()
	defer indicesArr.Release()

	// Process in batches for large result sets to reduce memory pressure
	const batchSize = 100000
	numIndices := len(indices)
	numBatches := (numIndices + batchSize - 1) / batchSize

	if numBatches > 1 {
		// For multiple batches, we'll process one batch at a time
		resultBatches := make([]arrow.Record, 0, numBatches)

		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			startIdx := batchIdx * batchSize
			endIdx := (batchIdx + 1) * batchSize
			if endIdx > numIndices {
				endIdx = numIndices
			}

			// Extract this batch of indices
			batchIndices := indices[startIdx:endIdx]

			// Take the indices from each column
			cols := make([]arrow.Array, record.NumCols())

			for i := 0; i < int(record.NumCols()); i++ {
				col := record.Column(i)

				// Create a builder for this column type
				builder := array.NewBuilder(d.alloc, col.DataType())
				defer builder.Release()

				// Pre-allocate for better performance
				if cap := builder.Cap(); cap < len(batchIndices) {
					builder.Reserve(len(batchIndices))
				}

				// Append only the selected indices
				for _, idx := range batchIndices {
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

			// Create batch record
			batchRecord := array.NewRecord(record.Schema(), cols, int64(len(batchIndices)))

			// Release the column arrays after they're added to the record
			for _, col := range cols {
				col.Release()
			}

			resultBatches = append(resultBatches, batchRecord)
		}

		// Combine all batches into a single result
		if len(resultBatches) == 1 {
			// Just one batch, return it
			return resultBatches[0]
		}

		// Create a table from all batches
		resultTable := array.NewTableFromRecords(record.Schema(), resultBatches)
		defer resultTable.Release()

		// Release the individual batch records
		for _, batch := range resultBatches {
			batch.Release()
		}

		// Convert table to record batch
		tableReader := array.NewTableReader(resultTable, resultTable.NumRows())
		defer tableReader.Release()

		if !tableReader.Next() {
			// Shouldn't happen, but let's be safe
			return array.NewRecord(record.Schema(), []arrow.Array{}, 0)
		}

		// Create a consolidated record
		result := tableReader.Record()
		// Clone the record to ensure we own it
		clonedResult := array.NewRecord(record.Schema(), result.Columns(), result.NumRows())
		return clonedResult
	} else {
		// Single batch processing
		// Take the indices from each column
		cols := make([]arrow.Array, record.NumCols())
		for i := 0; i < int(record.NumCols()); i++ {
			col := record.Column(i)

			// Create a builder for this column type
			builder := array.NewBuilder(d.alloc, col.DataType())
			defer builder.Release()

			// Pre-allocate for better performance
			if cap := builder.Cap(); cap < len(indices) {
				builder.Reserve(len(indices))
			}

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

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

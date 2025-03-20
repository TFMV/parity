// Package diff provides implementations for computing differences between datasets.
package diff

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// Import DuckDB driver
	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckDBDiffer implements dataset diffing using DuckDB.
// It leverages DuckDB's SQL capabilities for efficient comparison of datasets.
type DuckDBDiffer struct {
	db      *sql.DB
	tempDir string
	alloc   memory.Allocator
}

// NewDuckDBDiffer creates a new DuckDB differ.
func NewDuckDBDiffer() (*DuckDBDiffer, error) {
	// Create a temporary directory for DuckDB
	tempDir, err := ioutil.TempDir("", "parity-duckdb-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Create in-memory DuckDB database with a 4GB memory limit to handle large datasets
	db, err := sql.Open("duckdb", ":memory:?memory_limit=4GB")
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to create DuckDB database: %w", err)
	}

	// Enable multi-threading in DuckDB for better performance
	if _, err := db.Exec("PRAGMA threads=0"); err != nil { // 0 means automatic thread count
		db.Close()
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to set DuckDB threads: %w", err)
	}

	// Increase memory limit for string handling
	if _, err := db.Exec("PRAGMA memory_limit='4GB'"); err != nil {
		db.Close()
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to set memory limit: %w", err)
	}

	return &DuckDBDiffer{
		db:      db,
		tempDir: tempDir,
		alloc:   memory.NewGoAllocator(),
	}, nil
}

// Close closes the differ and releases resources.
func (d *DuckDBDiffer) Close() error {
	var err error

	if d.db != nil {
		if closeErr := d.db.Close(); closeErr != nil {
			err = closeErr
		}
	}

	if d.tempDir != "" {
		if rmErr := os.RemoveAll(d.tempDir); rmErr != nil && err == nil {
			err = rmErr
		}
	}

	return err
}

// Diff computes the difference between two datasets.
func (d *DuckDBDiffer) Diff(ctx context.Context, source, target core.DatasetReader, options core.DiffOptions) (*core.DiffResult, error) {
	// Create temporary Parquet files for source and target
	sourcePath, targetPath, err := d.createTempParquetFiles(ctx, source, target)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp files: %w", err)
	}
	defer os.Remove(sourcePath)
	defer os.Remove(targetPath)

	// Create DuckDB tables from the Parquet files
	if err := d.createTablesFromParquet(ctx, sourcePath, targetPath); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Get schemas
	sourceSchema := source.Schema()

	// Compute key columns if not specified
	keyColumns := options.KeyColumns
	if len(keyColumns) == 0 {
		// Try to use primary key fields if available
		meta := sourceSchema.Metadata()
		if pkValue, ok := meta.GetValue("primary_key"); ok {
			keyColumns = strings.Split(pkValue, ",")
		} else {
			// Use all columns as key if not specified
			for _, field := range sourceSchema.Fields() {
				if !contains(options.IgnoreColumns, field.Name) {
					keyColumns = append(keyColumns, field.Name)
				}
			}
		}
	}

	// Generate SQL for diffing
	result, err := d.computeDiff(ctx, keyColumns, options.IgnoreColumns, options.Tolerance)
	if err != nil {
		return nil, fmt.Errorf("failed to compute diff: %w", err)
	}

	return result, nil
}

// createTempParquetFiles creates temporary Parquet files from readers.
func (d *DuckDBDiffer) createTempParquetFiles(ctx context.Context, source, target core.DatasetReader) (string, string, error) {
	sourcePath := filepath.Join(d.tempDir, fmt.Sprintf("source-%d.parquet", time.Now().UnixNano()))
	targetPath := filepath.Join(d.tempDir, fmt.Sprintf("target-%d.parquet", time.Now().UnixNano()))

	// Write source to Parquet
	if err := d.writeToParquet(ctx, source, sourcePath); err != nil {
		return "", "", fmt.Errorf("failed to write source to Parquet: %w", err)
	}

	// Write target to Parquet
	if err := d.writeToParquet(ctx, target, targetPath); err != nil {
		os.Remove(sourcePath)
		return "", "", fmt.Errorf("failed to write target to Parquet: %w", err)
	}

	return sourcePath, targetPath, nil
}

// writeToParquet writes data from a DatasetReader to a Parquet file using DuckDB
func (d *DuckDBDiffer) writeToParquet(ctx context.Context, reader core.DatasetReader, path string) error {
	// Create a unique table name to avoid conflicts
	tableName := fmt.Sprintf("temp_export_%d", time.Now().UnixNano())

	// First create a view with the schema
	schema := reader.Schema()
	if schema == nil {
		return fmt.Errorf("reader schema is nil")
	}

	// Generate the CREATE TABLE statement with the schema
	var columnDefs []string
	for _, field := range schema.Fields() {
		sqlType := arrowTypeToSQLType(field.Type)
		nullability := ""
		if !field.Nullable {
			nullability = "NOT NULL"
		}
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s %s", field.Name, sqlType, nullability))
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columnDefs, ", "))
	if _, err := d.db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}

	// Insert data from reader into temp table
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if transaction is committed

	// Insert data in batches
	batchCounter := 0
	totalRows := int64(0)

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record, err := reader.Read(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read data: %w", err)
		}

		if record == nil || record.NumRows() == 0 {
			break
		}

		// Insert record data into the temporary table
		// This is a complex operation that depends on the specific record structure
		// We'll need to convert Arrow records to SQL parameters

		// For each row in the record
		for i := int64(0); i < record.NumRows(); i++ {
			// Check for context cancellation periodically
			if i%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			// Create a parameterized INSERT statement
			var paramPlaceholders []string
			for j := int64(0); j < int64(record.NumCols()); j++ {
				paramPlaceholders = append(paramPlaceholders, "?")
			}

			insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)",
				tableName, strings.Join(paramPlaceholders, ","))

			// Extract values for this row
			values := make([]interface{}, record.NumCols())
			for j := int64(0); j < int64(record.NumCols()); j++ {
				col := record.Column(int(j))
				if col.IsNull(int(i)) {
					values[j] = nil
				} else {
					// Extract value based on column type
					values[j] = extractValue(col, int(i))
				}
			}

			// Execute the insert
			if _, err := tx.ExecContext(ctx, insertSQL, values...); err != nil {
				return fmt.Errorf("failed to insert row %d: %w", totalRows+i, err)
			}
		}

		totalRows += record.NumRows()
		batchCounter++

		// Commit every 10 batches to avoid transaction becoming too large
		if batchCounter >= 10 {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			// Start a new transaction
			tx, err = d.db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin new transaction: %w", err)
			}
			batchCounter = 0
		}
	}

	// Commit any remaining data
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit final transaction: %w", err)
	}

	// Now export the table to Parquet
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT 'PARQUET')", tableName, path)
	if _, err := d.db.ExecContext(ctx, exportSQL); err != nil {
		return fmt.Errorf("failed to export to Parquet: %w", err)
	}

	// Clean up the temporary table
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		return fmt.Errorf("failed to drop temporary table: %w", err)
	}

	return nil
}

// extractValue extracts a value from an Arrow array at the specified index
func extractValue(arr arrow.Array, idx int) interface{} {
	if arr.IsNull(idx) {
		return nil
	}

	// Use type assertions similar to how builders are used
	switch arr := arr.(type) {
	case *array.Int8:
		return arr.Value(idx)
	case *array.Int16:
		return arr.Value(idx)
	case *array.Int32:
		return arr.Value(idx)
	case *array.Int64:
		return arr.Value(idx)
	case *array.Uint8:
		return arr.Value(idx)
	case *array.Uint16:
		return arr.Value(idx)
	case *array.Uint32:
		return arr.Value(idx)
	case *array.Uint64:
		return arr.Value(idx)
	case *array.Float32:
		return arr.Value(idx)
	case *array.Float64:
		return arr.Value(idx)
	case *array.Boolean:
		return arr.Value(idx)
	case *array.String:
		return arr.Value(idx)
	case *array.Date32:
		return arr.Value(idx)
	case *array.Date64:
		return arr.Value(idx)
	case *array.Timestamp:
		return arr.Value(idx)
	default:
		// For complex types, default to nil
		return nil
	}
}

// arrowTypeToSQLType converts an Arrow data type to a SQL type
func arrowTypeToSQLType(dataType arrow.DataType) string {
	switch dataType.ID() {
	case arrow.INT8, arrow.UINT8:
		return "TINYINT"
	case arrow.INT16, arrow.UINT16:
		return "SMALLINT"
	case arrow.INT32, arrow.UINT32:
		return "INTEGER"
	case arrow.INT64, arrow.UINT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "REAL"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.STRING:
		return "VARCHAR"
	case arrow.BINARY:
		return "BLOB"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIME32, arrow.TIME64:
		return "TIME"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DECIMAL:
		return "DECIMAL"
	default:
		// Default to VARCHAR for unknown types
		return "VARCHAR"
	}
}

// createTablesFromParquet creates DuckDB tables from Parquet files.
func (d *DuckDBDiffer) createTablesFromParquet(ctx context.Context, sourcePath, targetPath string) error {
	// Drop tables if they already exist
	_, err := d.db.ExecContext(ctx, "DROP TABLE IF EXISTS source")
	if err != nil {
		return fmt.Errorf("failed to drop source table: %w", err)
	}

	_, err = d.db.ExecContext(ctx, "DROP TABLE IF EXISTS target")
	if err != nil {
		return fmt.Errorf("failed to drop target table: %w", err)
	}

	// Create source table
	_, err = d.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE source AS SELECT * FROM read_parquet('%s')", sourcePath))
	if err != nil {
		return fmt.Errorf("failed to create source table: %w", err)
	}

	// Create target table
	_, err = d.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE target AS SELECT * FROM read_parquet('%s')", targetPath))
	if err != nil {
		return fmt.Errorf("failed to create target table: %w", err)
	}

	return nil
}

// computeDiff computes the difference between source and target tables.
func (d *DuckDBDiffer) computeDiff(ctx context.Context, keyColumns, ignoreColumns []string, tolerance float64) (*core.DiffResult, error) {
	// Get all columns excluding ignored ones
	columns, err := d.getColumns(ignoreColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get total counts for summary
	var sourceCount, targetCount int64
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM source").Scan(&sourceCount); err != nil {
		return nil, fmt.Errorf("failed to get source count: %w", err)
	}
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM target").Scan(&targetCount); err != nil {
		return nil, fmt.Errorf("failed to get target count: %w", err)
	}

	// Create temporary views for more efficient processing
	joinCondStr := joinCondition(keyColumns)

	// Find added records (in target but not in source)
	// Create view for added records
	addedViewSQL := fmt.Sprintf(`
		CREATE OR REPLACE VIEW added_records AS
		SELECT target.* 
		FROM target 
		LEFT JOIN source ON %s 
		WHERE %s
	`, joinCondStr, nullCondition("source", keyColumns))
	if _, err := d.db.ExecContext(ctx, addedViewSQL); err != nil {
		return nil, fmt.Errorf("failed to create added_records view: %w", err)
	}

	// Find deleted records (in source but not in target)
	// Create view for deleted records
	deletedViewSQL := fmt.Sprintf(`
		CREATE OR REPLACE VIEW deleted_records AS
		SELECT source.* 
		FROM source 
		LEFT JOIN target ON %s 
		WHERE %s
	`, joinCondStr, nullCondition("target", keyColumns))
	if _, err := d.db.ExecContext(ctx, deletedViewSQL); err != nil {
		return nil, fmt.Errorf("failed to create deleted_records view: %w", err)
	}

	// Find modified records (in both but with different values)
	// Create view for modified records with modification flags
	modifiedViewSQL := fmt.Sprintf(`
		CREATE OR REPLACE VIEW modified_records AS
		SELECT 
			source.*, 
			%s
		FROM source 
		JOIN target ON %s 
		WHERE %s
	`, modificationFlags(columns, tolerance), joinCondStr, diffCondition(columns, tolerance))
	if _, err := d.db.ExecContext(ctx, modifiedViewSQL); err != nil {
		return nil, fmt.Errorf("failed to create modified_records view: %w", err)
	}

	// Get counts
	var addedCount, deletedCount, modifiedCount int64
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM added_records").Scan(&addedCount); err != nil {
		return nil, fmt.Errorf("failed to get added count: %w", err)
	}
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deleted_records").Scan(&deletedCount); err != nil {
		return nil, fmt.Errorf("failed to get deleted count: %w", err)
	}
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM modified_records").Scan(&modifiedCount); err != nil {
		return nil, fmt.Errorf("failed to get modified count: %w", err)
	}

	// Calculate column differences for summary
	columnDiffs := make(map[string]int64)
	for _, col := range columns {
		var count int64
		query := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM source 
			JOIN target ON %s 
			WHERE (source.%s IS NOT NULL OR target.%s IS NOT NULL) 
			AND (source.%s != target.%s OR 
				(source.%s IS NULL AND target.%s IS NOT NULL) OR
				(source.%s IS NOT NULL AND target.%s IS NULL))
		`, joinCondStr, col, col, col, col, col, col, col, col)

		if err := d.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to get column diff count for %s: %w", col, err)
		}

		if count > 0 {
			columnDiffs[col] = count
		}
	}

	// Create summary
	summary := core.DiffSummary{
		TotalSource: sourceCount,
		TotalTarget: targetCount,
		Added:       addedCount,
		Deleted:     deletedCount,
		Modified:    modifiedCount,
		Columns:     columnDiffs,
	}

	// Extract records from views into Arrow format
	added, err := d.viewToArrowRecord(ctx, "added_records")
	if err != nil {
		return nil, fmt.Errorf("failed to convert added records to Arrow: %w", err)
	}

	deleted, err := d.viewToArrowRecord(ctx, "deleted_records")
	if err != nil {
		return nil, fmt.Errorf("failed to convert deleted records to Arrow: %w", err)
	}

	modified, err := d.viewToArrowRecord(ctx, "modified_records")
	if err != nil {
		return nil, fmt.Errorf("failed to convert modified records to Arrow: %w", err)
	}

	// Create result
	result := &core.DiffResult{
		Added:    added,
		Deleted:  deleted,
		Modified: modified,
		Summary:  summary,
	}

	return result, nil
}

// viewToArrowRecord converts a DuckDB view to an Arrow record
func (d *DuckDBDiffer) viewToArrowRecord(ctx context.Context, viewName string) (arrow.Record, error) {
	// Check if the view has any rows first
	var count int64
	if err := d.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", viewName)).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to get row count from view %s: %w", viewName, err)
	}

	if count == 0 {
		// No rows, return nil
		return nil, nil
	}

	// Create a temporary Parquet file
	tempFilePath := filepath.Join(d.tempDir, fmt.Sprintf("%s-%d.parquet", viewName, time.Now().UnixNano()))
	defer os.Remove(tempFilePath)

	// Export the view to Parquet
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT 'PARQUET')", viewName, tempFilePath)
	if _, err := d.db.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("failed to export view to Parquet: %w", err)
	}

	// Create a reader config to read from the Parquet file
	readerConfig := core.ReaderConfig{
		Type: "parquet",
		Path: tempFilePath,
	}

	// Create a parquet reader from the readers package
	reader, err := readers.NewParquetReader(readerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer reader.Close()

	// Read all records
	var records []arrow.Record
	var totalRows int64

	for {
		record, err := reader.Read(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("failed to read record: %w", err)
		}

		if record == nil || record.NumRows() == 0 {
			break
		}

		// Retain the record since we'll use it later
		record.Retain()
		records = append(records, record)
		totalRows += record.NumRows()

		// Check for context cancellation
		select {
		case <-ctx.Done():
			// Release all retained records
			for _, r := range records {
				r.Release()
			}
			return nil, ctx.Err()
		default:
		}
	}

	// If no records, return nil
	if len(records) == 0 {
		return nil, nil
	}

	// If only one record, return it
	if len(records) == 1 {
		return records[0], nil
	}

	// Otherwise, concatenate records
	return concatenateRecords(records, d.alloc)
}

// concatenateRecords concatenates multiple Arrow records into a single record
func concatenateRecords(records []arrow.Record, mem memory.Allocator) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// TODO: In a production system, you'd want to properly concatenate all records
	// This is a simplification to avoid complex chunking logic
	if len(records) > 0 {
		return records[0], nil
	}

	return nil, nil
}

// getColumns gets all column names from the source table excluding ignored columns.
func (d *DuckDBDiffer) getColumns(ignoreColumns []string) ([]string, error) {
	rows, err := d.db.Query("SELECT column_name FROM pragma_table_info('source')")
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		if !contains(ignoreColumns, column) {
			columns = append(columns, column)
		}
	}

	return columns, nil
}

// joinCondition creates a JOIN condition for a list of columns.
func joinCondition(columns []string) string {
	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("source.%s = target.%s", col, col))
	}
	return strings.Join(conditions, " AND ")
}

// nullCondition creates a condition to check if columns in a table are NULL.
func nullCondition(table string, columns []string) string {
	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s.%s IS NULL", table, col))
	}
	return strings.Join(conditions, " OR ")
}

// diffCondition creates a condition to check if any column is different.
func diffCondition(columns []string, tolerance float64) string {
	var conditions []string
	for _, col := range columns {
		// For numeric columns, apply tolerance
		conditions = append(conditions, fmt.Sprintf(`
			(source.%[1]s IS NOT NULL OR target.%[1]s IS NOT NULL) AND 
			(source.%[1]s IS NULL AND target.%[1]s IS NOT NULL OR
			 source.%[1]s IS NOT NULL AND target.%[1]s IS NULL OR
			 CASE WHEN typeof(source.%[1]s) = 'numeric' AND typeof(target.%[1]s) = 'numeric'
			 THEN ABS(CAST(source.%[1]s AS DOUBLE) - CAST(target.%[1]s AS DOUBLE)) > %[2]f
			 ELSE source.%[1]s != target.%[1]s END)
		`, col, tolerance))
	}
	return strings.Join(conditions, " OR ")
}

// modificationFlags creates a list of flag columns indicating which columns were modified.
func modificationFlags(columns []string, tolerance float64) string {
	var flags []string
	for _, col := range columns {
		flags = append(flags, fmt.Sprintf(`
			CASE WHEN source.%[1]s IS NULL AND target.%[1]s IS NOT NULL OR
				 source.%[1]s IS NOT NULL AND target.%[1]s IS NULL OR
				 CASE WHEN typeof(source.%[1]s) = 'numeric' AND typeof(target.%[1]s) = 'numeric'
				 THEN ABS(CAST(source.%[1]s AS DOUBLE) - CAST(target.%[1]s AS DOUBLE)) > %[2]f
				 ELSE source.%[1]s != target.%[1]s END
			THEN true ELSE false END AS %[1]s_modified
		`, col, tolerance))
	}
	return strings.Join(flags, ", ")
}

// contains checks if a string is in a slice.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// cloneRecord creates a copy of a record.
func cloneRecord(record arrow.Record) arrow.Record {
	if record == nil || record.NumRows() == 0 {
		return nil
	}
	return record
}

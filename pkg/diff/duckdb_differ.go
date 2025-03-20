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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// Import DuckDB driver
	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckDBDiffer implements dataset diffing using DuckDB.
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

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to create DuckDB database: %w", err)
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

// writeToParquet writes a reader to a Parquet file.
func (d *DuckDBDiffer) writeToParquet(ctx context.Context, reader core.DatasetReader, path string) error {
	// Execute a COPY statement to write to Parquet
	query := fmt.Sprintf("CREATE TABLE temp_export AS SELECT * FROM read_arrow_stream();")
	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Export to Parquet file
	copySQL := fmt.Sprintf("COPY temp_export TO '%s' (FORMAT 'parquet');", path)
	_, err = d.db.ExecContext(ctx, copySQL)
	if err != nil {
		return fmt.Errorf("failed to export to Parquet: %w", err)
	}

	// Drop temporary table
	_, err = d.db.ExecContext(ctx, "DROP TABLE temp_export")
	if err != nil {
		return fmt.Errorf("failed to drop temp table: %w", err)
	}

	return nil
}

// createTablesFromParquet creates DuckDB tables from Parquet files.
func (d *DuckDBDiffer) createTablesFromParquet(ctx context.Context, sourcePath, targetPath string) error {
	// Create source table
	_, err := d.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE source AS SELECT * FROM read_parquet('%s')", sourcePath))
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
	_, err := d.getColumns(ignoreColumns)
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

	// TODO: Implement the actual diff logic using SQL queries
	// This is a placeholder for the actual implementation

	// Create summary
	summary := core.DiffSummary{
		TotalSource: sourceCount,
		TotalTarget: targetCount,
		Added:       0, // TODO: Calculate actual values
		Deleted:     0, // TODO: Calculate actual values
		Modified:    0, // TODO: Calculate actual values
		Columns:     make(map[string]int64),
	}

	// Create result
	result := &core.DiffResult{
		Added:    nil, // TODO: Populate with actual data
		Deleted:  nil, // TODO: Populate with actual data
		Modified: nil, // TODO: Populate with actual data
		Summary:  summary,
	}

	return result, nil
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

// nullCondition creates a condition to check if all columns in a table are NULL.
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
		// Use NULL-safe equals operator (<=>)
		conditions = append(conditions, fmt.Sprintf("NOT (source.%s <=> target.%s)", col, col))
	}
	return strings.Join(conditions, " OR ")
}

// modificationFlags creates a list of flag columns indicating which columns were modified.
func modificationFlags(columns []string, tolerance float64) string {
	var flags []string
	for _, col := range columns {
		flags = append(flags, fmt.Sprintf("NOT (source.%s <=> target.%s) AS %s_modified", col, col, col))
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

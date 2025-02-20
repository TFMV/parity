// validator_test.go
package validation

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/TFMV/parity/integrations"
	"github.com/TFMV/parity/integrations/duckdb"
	"github.com/TFMV/parity/metrics"
	"github.com/TFMV/parity/report"
)

// setupDuckDB creates an in-memory DuckDB instance with test data
func setupDuckDB(t *testing.T) *duckdb.DuckDB {
	t.Helper()

	db, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	conn, err := db.OpenConnection()
	if err != nil {
		db.Close()
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create test table and insert data
	queries := []string{
		"CREATE TABLE test (id INTEGER, value DOUBLE)",
		"INSERT INTO test VALUES (1, 10.5), (2, 20.5)",
	}

	for _, q := range queries {
		if _, err := conn.Exec(context.Background(), q); err != nil {
			db.Close()
			t.Fatalf("failed to execute query %q: %v", q, err)
		}
	}

	return db
}

// setupDuckDBDifferentSchema creates a DuckDB with different schema
func setupDuckDBDifferentSchema(t *testing.T) *duckdb.DuckDB {
	t.Helper()
	db, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	conn, err := db.OpenConnection()
	if err != nil {
		db.Close()
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Exec(context.Background(), "CREATE TABLE test (id INTEGER, different_column VARCHAR)"); err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}

	return db
}

// setupDuckDBDifferentData creates a DuckDB with different data
func setupDuckDBDifferentData(t *testing.T) *duckdb.DuckDB {
	t.Helper()
	db, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	conn, err := db.OpenConnection()
	if err != nil {
		db.Close()
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	queries := []string{
		"CREATE TABLE test (id INTEGER, value DOUBLE)",
		"INSERT INTO test VALUES (1, 11.5)", // Different value
	}

	for _, q := range queries {
		if _, err := conn.Exec(context.Background(), q); err != nil {
			db.Close()
			t.Fatalf("failed to execute query %q: %v", q, err)
		}
	}

	return db
}

func TestValidateSchema_Success(t *testing.T) {
	ctx := context.Background()
	db1 := setupDuckDB(t)
	db2 := setupDuckDB(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integration := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integration, metrics.ValidationThresholds{}, &report.JSONReportGenerator{})

	result, err := validator.ValidateSchema(ctx, "test")
	if err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
	if !result.Status {
		t.Error("expected schema validation to pass")
	}
}

func TestValidateSchema_Failure(t *testing.T) {
	ctx := context.Background()
	db1 := setupDuckDB(t)
	db2 := setupDuckDBDifferentSchema(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integration := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integration, metrics.ValidationThresholds{}, &report.JSONReportGenerator{})

	result, err := validator.ValidateSchema(ctx, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.Status {
		t.Error("expected schema validation to fail")
	}
}

func TestValidateRowCounts_Success(t *testing.T) {
	ctx := context.Background()
	db1 := setupDuckDB(t)
	db2 := setupDuckDB(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integration := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integration, metrics.ValidationThresholds{
		RowCountTolerance: 0,
	}, &report.JSONReportGenerator{})

	result, err := validator.ValidateRowCounts(ctx, "test")
	if err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
	if !result.Status {
		t.Error("expected row count validation to pass")
	}
	if result.Difference != 0 {
		t.Errorf("expected no difference in row counts, got: %d", result.Difference)
	}
}

func TestValidateRowCounts_Failure(t *testing.T) {
	ctx := context.Background()
	db1 := setupDuckDB(t)
	db2 := setupDuckDBDifferentData(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integration := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integration, metrics.ValidationThresholds{
		RowCountTolerance: 0,
	}, &report.JSONReportGenerator{})

	result, err := validator.ValidateRowCounts(ctx, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.Status {
		t.Error("expected row count validation to fail")
	}
	if result.Difference != 1 {
		t.Errorf("expected difference of 1 row, got: %d", result.Difference)
	}
}

func TestValidateAll(t *testing.T) {
	// Add timeout to context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db1 := setupDuckDB(t)
	db2 := setupDuckDB(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integration := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integration, metrics.ValidationThresholds{
		NumericDifferenceTolerance: 0.001,
		RowCountTolerance:          0,
		SamplingConfidenceLevel:    0.95,
	}, &report.JSONReportGenerator{})

	columns := []string{"id", "value"}
	report, err := validator.ValidateAll(ctx, "test", columns, metrics.Full, 100)
	if err != nil {
		t.Errorf("expected no error but got: %v", err)
	}

	if !report.RowCountResult.Status {
		t.Error("expected row count validation to pass")
	}
	if !report.SchemaResult.Status {
		t.Error("expected schema validation to pass")
	}
	if !report.ValueResult.Status {
		t.Error("expected value validation to pass")
	}
}

// Cleanup any temporary files if necessary.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// validator_test.go
package validation

import (
	"context"
	"os"
	"testing"

	"github.com/TFMV/parity/integrations"
	"github.com/TFMV/parity/integrations/duckdb"
)

// setupDuckDB creates an in‑memory DuckDB instance, creates table "test", and inserts two rows.
func setupDuckDB(t *testing.T) *duckdb.DuckDB {
	t.Helper()

	// Create an in‑memory DuckDB instance (empty path means in‑memory)
	db, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}

	conn, err := db.OpenConnection()
	if err != nil {
		db.Close()
		t.Fatalf("failed to open connection: %v", err)
	}

	// Create table and insert data.
	queries := []string{
		"CREATE TABLE test (id INTEGER)",
		"INSERT INTO test VALUES (1), (2)",
	}
	for _, q := range queries {
		if _, err := conn.Exec(context.Background(), q); err != nil {
			conn.Close()
			db.Close()
			t.Fatalf("failed to execute query %q: %v", q, err)
		}
	}
	conn.Close()
	return db
}

// setupDuckDBDifferent creates an in‑memory DuckDB instance with a different table schema.
func setupDuckDBDifferent(t *testing.T) *duckdb.DuckDB {
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

	// Create a table with a different column name.
	queries := []string{
		"CREATE TABLE test (value INTEGER)",
		"INSERT INTO test VALUES (1), (2)",
	}
	for _, q := range queries {
		if _, err := conn.Exec(context.Background(), q); err != nil {
			conn.Close()
			db.Close()
			t.Fatalf("failed to execute query %q: %v", q, err)
		}
	}
	conn.Close()
	return db
}

func TestValidatorCompare_Success(t *testing.T) {
	ctx := context.Background()

	// Create two identical DuckDB instances.
	db1 := setupDuckDB(t)
	db2 := setupDuckDB(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	// Create a DatabasePair integration.
	integrationPair := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integrationPair)

	// Compare the identical data.
	if err := validator.Compare(ctx, "SELECT * FROM test", "SELECT * FROM test"); err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
}

func TestValidatorCompare_Failure_SchemaMismatch(t *testing.T) {
	ctx := context.Background()

	// db1 has the "test" table with column "id",
	// db2 has the "test" table with column "value".
	db1 := setupDuckDB(t)
	db2 := setupDuckDBDifferent(t)
	defer func() {
		db1.Close()
		db2.Close()
	}()

	integrationPair := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integrationPair)

	err := validator.Compare(ctx, "SELECT * FROM test", "SELECT * FROM test")
	if err == nil {
		t.Error("expected error due to schema mismatch, got nil")
	} else {
		t.Logf("received expected error: %v", err)
	}
}

func TestValidatorCompare_Failure_RecordCountMismatch(t *testing.T) {
	ctx := context.Background()

	// Create db1 with two rows.
	db1 := setupDuckDB(t)
	// Create db2 and insert only one row.
	db2, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	conn, err := db2.OpenConnection()
	if err != nil {
		db2.Close()
		t.Fatalf("failed to open connection: %v", err)
	}
	queries := []string{
		"CREATE TABLE test (id INTEGER)",
		"INSERT INTO test VALUES (1)",
	}
	for _, q := range queries {
		if _, err := conn.Exec(context.Background(), q); err != nil {
			conn.Close()
			db2.Close()
			t.Fatalf("failed to execute query %q: %v", q, err)
		}
	}
	conn.Close()

	defer func() {
		db1.Close()
		db2.Close()
	}()

	integrationPair := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integrationPair)

	err = validator.Compare(ctx, "SELECT * FROM test", "SELECT * FROM test")
	if err == nil {
		t.Error("expected error due to record count mismatch, got nil")
	} else {
		t.Logf("received expected error: %v", err)
	}
}

func BenchmarkValidatorCompare(b *testing.B) {
	ctx := context.Background()

	// Create test databases
	db1, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		b.Fatalf("failed to create DuckDB: %v", err)
	}
	db2, err := duckdb.NewDuckDB(duckdb.WithPath(""))
	if err != nil {
		b.Fatalf("failed to create DuckDB: %v", err)
	}
	defer func() {
		db1.Close()
		db2.Close()
	}()

	// Setup test data
	for _, db := range []*duckdb.DuckDB{db1, db2} {
		conn, err := db.OpenConnection()
		if err != nil {
			b.Fatalf("failed to open connection: %v", err)
		}
		for _, q := range []string{
			"CREATE TABLE test (id INTEGER)",
			"INSERT INTO test VALUES (1), (2)",
		} {
			if _, err := conn.Exec(ctx, q); err != nil {
				b.Fatalf("failed to execute query %q: %v", q, err)
			}
		}
		conn.Close()
	}

	integrationPair := integrations.NewDatabasePair(db1, db2)
	validator := NewValidator(integrationPair)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := validator.Compare(ctx, "SELECT * FROM test", "SELECT * FROM test"); err != nil {
			b.Fatalf("expected no error but got: %v", err)
		}
	}
}

// Cleanup any temporary files if necessary.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

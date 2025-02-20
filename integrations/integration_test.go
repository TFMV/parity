// File: integration_test.go
package integrations_test

import (
	"context"
	"testing"

	"github.com/TFMV/parity/integrations"
)

// ===========================
// 1. Test for Option Functions
// ===========================

func TestOptions(t *testing.T) {
	ctx := context.Background()
	testPath := "mock_db_path"
	testDriverPath := "driver_path"

	// Use the WithXXX functions to configure Options
	opts := &integrations.Options{}
	integrations.WithContext(ctx)(opts)
	integrations.WithPath(testPath)(opts)
	integrations.WithDriverPath(testDriverPath)(opts)

	// Validate the fields
	if opts.Context != ctx {
		t.Errorf("expected context %v, got %v", ctx, opts.Context)
	}
	if opts.Path != testPath {
		t.Errorf("expected path %q, got %q", testPath, opts.Path)
	}
	if opts.DriverPath != testDriverPath {
		t.Errorf("expected driverPath %q, got %q", testDriverPath, opts.DriverPath)
	}
}

// =======================
// 2. Mocks for the Database
// =======================

// mockDB implements the Database interface for testing
type mockDB struct {
	connCount int
	closed    bool
}

// OpenConnection is a stub that returns a nil connection and no error
func (m *mockDB) OpenConnection() (integrations.Connection, error) {
	m.connCount++
	return nil, nil
}

// Close sets closed to true
func (m *mockDB) Close() {
	m.closed = true
}

// ConnCount returns current connCount
func (m *mockDB) ConnCount() int {
	return m.connCount
}

// =======================
// 3. Tests for DatabasePair
// =======================

func TestDatabasePair_SourceDatabases(t *testing.T) {
	db1 := &mockDB{}
	db2 := &mockDB{}

	pair := integrations.NewDatabasePair(db1, db2)
	if pair.Source1() != db1 {
		t.Errorf("Source1() should return db1")
	}
	if pair.Source2() != db2 {
		t.Errorf("Source2() should return db2")
	}
}

func TestDatabasePair_ConnCount(t *testing.T) {
	db1 := &mockDB{connCount: 2}
	db2 := &mockDB{connCount: 3}

	pair := integrations.NewDatabasePair(db1, db2)
	total := pair.ConnCount()
	expected := 5

	if total != expected {
		t.Errorf("expected total ConnCount of %d, got %d", expected, total)
	}
}

func TestDatabasePair_Close(t *testing.T) {
	db1 := &mockDB{}
	db2 := &mockDB{}

	pair := integrations.NewDatabasePair(db1, db2)
	pair.Close()

	if !db1.closed {
		t.Errorf("expected db1 to be closed after pair.Close()")
	}
	if !db2.closed {
		t.Errorf("expected db2 to be closed after pair.Close()")
	}
}

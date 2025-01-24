package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/TFMV/parity/internal/adapters"
)

func TestDuckDBConnectivity(t *testing.T) {
	// Load environment variables for the test
	dbPath := os.Getenv("DUCKDB_PATH")
	if dbPath == "" {
		t.Fatal("DUCKDB_PATH environment variable is not set")
	}

	// Create a context with a timeout for the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt to connect to the DuckDB database
	duckDBSource, err := adapters.NewDuckDBSource(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer duckDBSource.Conn.Conn.Close()

	t.Logf("Successfully connected to DuckDB at %s", dbPath)
}

package adapters

import (
	"context"
	"fmt"
)

// DuckDBSource represents a connection to a DuckDB database.
type DuckDBSource struct {
	Conn *ADBCConnection
}

// NewDuckDBSource creates a new DuckDB source using ADBC.
func NewDuckDBSource(ctx context.Context, dbURL string) (*DuckDBSource, error) {
	dbConfig := map[string]string{
		"driver":     "/usr/local/lib/libduckdb.dylib",
		"entrypoint": "duckdb_adbc_init",
		"path":       dbURL,
	}

	// Use the common ADBC connection logic
	conn, err := NewADBCConnection(ctx, dbConfig["driver"], dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB source: %w", err)
	}

	return &DuckDBSource{Conn: conn}, nil
}

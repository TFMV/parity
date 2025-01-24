package adapters

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// PostgresSource represents a connection to a PostgreSQL database.
type PostgresSource struct {
	Conn *ADBCConnection
}

// NewPostgresSource creates a new PostgreSQL source using ADBC.
func NewPostgresSource(ctx context.Context, dbURL string) (*PostgresSource, error) {
	driverPath := "/usr/local/lib/libadbc_driver_postgresql.dylib"
	options := map[string]string{
		adbc.OptionKeyURI: dbURL,
	}

	// Use the common ADBC connection logic
	conn, err := NewADBCConnection(ctx, driverPath, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL source: %w", err)
	}

	return &PostgresSource{Conn: conn}, nil
}

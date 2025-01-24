package adapters

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

// ADBCConnection represents a generic database connection.
type ADBCConnection struct {
	Conn adbc.Connection
}

// NewADBCConnection establishes a new ADBC connection with the provided driver and options.
func NewADBCConnection(ctx context.Context, driverPath string, options map[string]string) (*ADBCConnection, error) {
	drv := drivermgr.Driver{}
	options["driver"] = driverPath

	db, err := drv.NewDatabase(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	return &ADBCConnection{Conn: conn}, nil
}

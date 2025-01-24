package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/TFMV/parity/internal/adapters"
)

func TestPostgresConnectivity(t *testing.T) {
	// Load environment variables for the test
	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		t.Fatal("POSTGRES_URL environment variable is not set")
	}

	// Create a context with a timeout for the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt to connect to the PostgreSQL database
	postgresSource, err := adapters.NewPostgresSource(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer postgresSource.Conn.Conn.Close()

	t.Logf("Successfully connected to PostgreSQL at %s", dbURL)
}

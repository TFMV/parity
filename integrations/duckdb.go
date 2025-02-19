// This module implements the DuckDB driver for the Arrow ADBC interface.
// It is a wrapper around the DuckDB C API, providing a Go interface.

package integrations

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Options define the configuration for opening a DuckDB database.
type Options struct {
	// Path to the DuckDB file ("" => in-memory)
	Path string

	// DriverPath is the location of libduckdb.so, if empty => auto-detect
	DriverPath string

	// Context for new database/connection usage
	Context context.Context
}

// Option is a functional config approach
type Option func(*Options)

// WithPath sets a file path for the DuckDB DB.
func WithPath(p string) Option {
	return func(o *Options) {
		o.Path = p
	}
}

// WithDriverPath sets the path to the DuckDB driver library.
// If not provided, the driver will be auto-detected based on the current OS.
func WithDriverPath(p string) Option {
	return func(o *Options) {
		o.DriverPath = p
	}
}

// WithContext sets a custom Context for DB usage.
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// DuckDB is the primary struct managing a DuckDB database via ADBC.
// Use NewDuckDB(...) to construct.
type DuckDB struct {
	mu     sync.Mutex
	db     adbc.Database
	driver adbc.Driver
	opts   Options

	conns []*duckConn // track open connections
}

// duckConn is a simple wrapper holding an open connection.
type duckConn struct {
	parent *DuckDB
	adbc.Connection
}

// NewDuckDB opens or creates a DuckDB instance (file-based or in-memory).
// The driver library is auto-detected if not provided. Example usage:
//
//	duck, err := NewDuckDB(bigquack.WithPath("/tmp/duck.db"))
//	if err != nil { ... }
func NewDuckDB(options ...Option) (*DuckDB, error) {
	// gather defaults
	var opts Options
	for _, opt := range options {
		opt(&opts)
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}

	// auto-detect driver if empty
	dPath := opts.DriverPath
	if dPath == "" {
		switch runtime.GOOS {
		case "darwin":
			dPath = "/usr/local/lib/libduckdb.dylib"
		case "linux":
			dPath = "/usr/local/lib/libduckdb.so"
		case "windows":
			if home, err := os.UserHomeDir(); err == nil {
				dPath = home + "/Downloads/duckdb-windows-amd64/duckdb.dll"
			}
		}
	}

	dbOpts := map[string]string{
		"driver":     dPath,
		"entrypoint": "duckdb_adbc_init",
	}
	if opts.Path != "" {
		dbOpts["path"] = opts.Path
	}

	driver := drivermgr.Driver{}
	db, err := driver.NewDatabase(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("error creating new DuckDB database: %w", err)
	}

	duck := &DuckDB{
		db:     db,
		driver: driver,
		opts:   opts,
	}
	return duck, nil
}

// OpenConnection opens a new connection to DuckDB. The returned connection
// should be closed by calling its Close method, or you can rely on DuckDB.Close()
// to automatically close all open connections.
func (d *DuckDB) OpenConnection() (*duckConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	conn, err := d.db.Open(d.opts.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	dc := &duckConn{parent: d, Connection: conn}
	d.conns = append(d.conns, dc)
	return dc, nil
}

// Close closes the DuckDB database and all open connections. It is recommended
// to call this when finished to ensure all WAL data is flushed if file-based.
func (d *DuckDB) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// close all open conns
	for _, c := range d.conns {
		c.Connection.Close()
	}
	d.conns = nil

	// close db
	d.db.Close()
	d.db = nil
}

// ConnCount returns the current number of open connections.
func (d *DuckDB) ConnCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// Path returns the database file path, or empty if in-memory.
// Note: For PostgreSQL equivalent, see URI() which returns the connection string.
func (d *DuckDB) Path() string {
	return d.opts.Path
}

// Exec runs a statement that doesn't produce a result set, returning
// the number of rows affected if known, else -1.
func (c *duckConn) Exec(ctx context.Context, sql string) (int64, error) {
	stmt, err := c.NewStatement()
	if err != nil {
		return -1, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return -1, fmt.Errorf("failed to set SQL query: %w", err)
	}
	affected, err := stmt.ExecuteUpdate(ctx)
	return affected, err
}

// Query runs a SQL query returning (RecordReader, adbc.Statement, rowCount).
// rowCount will be -1 if not known. Caller is responsible for closing the
// returned statement and the RecordReader.
func (c *duckConn) Query(ctx context.Context, sql string) (array.RecordReader, adbc.Statement, int64, error) {
	stmt, err := c.NewStatement()
	if err != nil {
		return nil, nil, -1, fmt.Errorf("failed to create statement: %w", err)
	}
	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, nil, -1, fmt.Errorf("failed to set SQL query: %w", err)
	}

	rr, rowsAffected, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, nil, -1, err
	}
	return rr, stmt, rowsAffected, nil
}

// GetTableSchema fetches the Arrow schema of a table in the given catalog/schema
// (pass nil for defaults).
func (c *duckConn) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return c.Connection.GetTableSchema(ctx, catalog, dbSchema, tableName)
}

// Close closes the connection, removing it from the parent DuckDB's tracking.
func (c *duckConn) Close() {
	c.parent.mu.Lock()
	defer c.parent.mu.Unlock()

	for i, conn := range c.parent.conns {
		if conn == c {
			c.parent.conns[i] = c.parent.conns[len(c.parent.conns)-1]
			c.parent.conns = c.parent.conns[:len(c.parent.conns)-1]
			break
		}
	}
	c.Connection.Close()
	c.parent = nil
}

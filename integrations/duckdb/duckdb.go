// duckdb.go
// This module implements the DuckDB driver for the Arrow ADBC interface.
// It is a wrapper around the DuckDB C API, providing a Go interface.
package duckdb

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

	integrations "github.com/TFMV/parity/integrations"
)

// Ensure DuckDB implements Database.
var _ integrations.Database = (*DuckDB)(nil)

// Ensure duckConn implements Connection.
var _ integrations.Connection = (*duckConn)(nil)

// Options define the configuration for opening a DuckDB database.
type Options struct {
	// Path to the DuckDB file ("" => in-memory)
	Path string

	// DriverPath is the location of libduckdb.so, if empty => auto-detect
	DriverPath string

	// Context for new database/connection usage
	Context context.Context
}

// Option is a functional config approach.
type Option func(*Options)

// WithPath sets a file path for the DuckDB DB.
func WithPath(p string) Option {
	return func(o *Options) {
		o.Path = p
	}
}

// WithDriverPath sets the path to the DuckDB driver library.
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
type DuckDB struct {
	mu     sync.Mutex
	db     adbc.Database
	driver adbc.Driver
	opts   Options

	conns []*duckConn // track open connections
}

// duckConn is a wrapper holding an open connection.
type duckConn struct {
	parent *DuckDB
	adbc.Connection
}

// NewDuckDB opens or creates a DuckDB instance (file-based or in-memory).
func NewDuckDB(options ...Option) (*DuckDB, error) {
	var opts Options
	for _, opt := range options {
		opt(&opts)
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}

	// Auto-detect driver if empty.
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

	// Create a new struct for cleanup
	cleanupDuck := new(DuckDB)
	*cleanupDuck = *duck
	runtime.AddCleanup(duck, func(db *DuckDB) { db.Close() }, cleanupDuck)

	return duck, nil
}

// OpenConnection creates a new connection to DuckDB.
func (d *DuckDB) OpenConnection() (integrations.Connection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	conn, err := d.db.Open(d.opts.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	dc := &duckConn{parent: d, Connection: conn}
	d.conns = append(d.conns, dc)

	// Create a new struct for cleanup
	cleanupConn := new(duckConn)
	*cleanupConn = *dc
	runtime.AddCleanup(dc, func(conn *duckConn) { conn.Close() }, cleanupConn)

	return dc, nil
}

// Close closes the DuckDB database and all open connections.
func (d *DuckDB) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, c := range d.conns {
		c.Close()
	}
	d.conns = nil
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
func (d *DuckDB) Path() string {
	return d.opts.Path
}

// --- duckConn methods to implement the Connection interface ---

// Exec executes a statement that doesn't produce a result set.
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

// Query executes a SQL query and returns a RecordReader.
func (c *duckConn) Query(ctx context.Context, sql string) (array.RecordReader, error) {
	stmt, err := c.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	rr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, err
	}
	return newWrappedRecordReader(rr, stmt), nil
}

// GetTableSchema returns the Arrow schema of a table.
func (c *duckConn) GetTableSchema(ctx context.Context, catalog, schema *string, table string) (*arrow.Schema, error) {
	return c.Connection.GetTableSchema(ctx, catalog, schema, table)
}

// Close closes the connection, removing it from the parent's tracking.
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

// --- recordReaderWrapper wraps a RecordReader and its Statement ---

type recordReaderWrapper struct {
	rr   array.RecordReader
	stmt adbc.Statement
}

func (w *recordReaderWrapper) Schema() *arrow.Schema {
	return w.rr.Schema()
}

func (w *recordReaderWrapper) Next() bool {
	return w.rr.Next()
}

func (w *recordReaderWrapper) Record() arrow.Record {
	return w.rr.Record()
}

func (w *recordReaderWrapper) Err() error {
	return w.rr.Err()
}

func (w *recordReaderWrapper) Release() {
	w.rr.Release()
}

func (w *recordReaderWrapper) Retain() {
	w.rr.Retain()
}

func (w *recordReaderWrapper) Close() error {
	err := w.stmt.Close()
	w.rr.Release()
	return err
}

func newWrappedRecordReader(rr array.RecordReader, stmt adbc.Statement) array.RecordReader {
	return &recordReaderWrapper{
		rr:   rr,
		stmt: stmt,
	}
}

// GetPartitionWhereClause generates a WHERE clause for partitioned tables in DuckDB.
func (c *duckConn) GetPartitionWhereClause(ctx context.Context, table string, partition string) (string, error) {
	if partition == "" {
		return "", nil // No partition filtering needed
	}

	// Retrieve schema to find partition key
	schema, err := c.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve schema for partition detection: %w", err)
	}

	partitionKey := detectPartitionKey(schema)
	if partitionKey == "" {
		return "", fmt.Errorf("no partition key found for table '%s'", table)
	}

	return fmt.Sprintf("WHERE %s = '%s'", partitionKey, partition), nil
}

// GetRowCount retrieves the row count for a given table (or partition).
func (c *duckConn) GetRowCount(ctx context.Context, table string, partition string) (int64, error) {
	whereClause, err := c.GetPartitionWhereClause(ctx, table, partition)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", table, whereClause)

	rr, err := c.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rr.Release()

	if rr.Next() {
		rec := rr.Record()
		if rec.NumCols() < 1 || rec.NumRows() < 1 {
			return 0, fmt.Errorf("invalid row count result")
		}

		// Extract row count value
		return rec.Column(0).(*array.Int64).Value(0), nil
	}
	return 0, fmt.Errorf("no rows returned for COUNT query")
}

// GetAggregate computes an aggregate function (SUM, AVG, etc.) on a column.
func (c *duckConn) GetAggregate(ctx context.Context, table, column, function, partition string) (float64, error) {
	whereClause, err := c.GetPartitionWhereClause(ctx, table, partition)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf("SELECT %s(%s) FROM %s %s", function, column, table, whereClause)

	rr, err := c.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rr.Release()

	if rr.Next() {
		rec := rr.Record()
		if rec.NumCols() < 1 || rec.NumRows() < 1 {
			return 0, fmt.Errorf("invalid aggregate result")
		}

		// Convert result to float
		switch v := rec.Column(0).(*array.Float64); {
		case v != nil:
			return v.Value(0), nil
		default:
			return 0, fmt.Errorf("unexpected data type in aggregate result")
		}
	}

	return 0, fmt.Errorf("no rows returned for aggregate query")
}

// detectPartitionKey inspects the schema to determine the likely partition key.
func detectPartitionKey(schema *arrow.Schema) string {
	for _, field := range schema.Fields() {
		if isLikelyPartitionKey(field) {
			return field.Name
		}
	}
	return ""
}

// isLikelyPartitionKey determines if a column is a suitable partition key.
func isLikelyPartitionKey(field arrow.Field) bool {
	switch field.Type.ID() {
	case arrow.INT32, arrow.INT64, arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.STRING:
		return true
	default:
		return false
	}
}

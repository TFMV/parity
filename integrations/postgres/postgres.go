// postgres.go
package postgres

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

// Ensure Postgres implements Database.
var _ integrations.Database = (*Postgres)(nil)

// Ensure pgConn implements Connection.
var _ integrations.Connection = (*pgConn)(nil)

// Postgres is the primary struct managing a PostgreSQL database via ADBC.
type Postgres struct {
	mu     sync.Mutex
	db     adbc.Database
	driver adbc.Driver
	opts   integrations.Options
	conns  []*pgConn // track open connections
}

// pgConn is a wrapper holding an open connection.
type pgConn struct {
	parent *Postgres
	adbc.Connection
}

// NewPostgres creates a new Postgres instance.
func NewPostgres(options ...integrations.Option) (*Postgres, error) {
	var opts integrations.Options
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
			dPath = "/usr/local/lib/libadbc_driver_postgresql.dylib"
		case "linux":
			dPath = "/usr/local/lib/libadbc_driver_postgresql.so"
		case "windows":
			if home, err := os.UserHomeDir(); err == nil {
				dPath = home + "/Downloads/postgresql-windows-amd64/postgresql.dll"
			}
		}
	}

	dbOpts := map[string]string{
		"driver":          dPath,
		adbc.OptionKeyURI: opts.Path,
	}

	driver := drivermgr.Driver{}
	db, err := driver.NewDatabase(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("error creating new PostgreSQL database: %w", err)
	}

	pg := &Postgres{
		db:     db,
		driver: driver,
		opts:   opts,
	}

	cleanupPg := pg
	runtime.AddCleanup(pg, func(db *Postgres) { db.Close() }, cleanupPg)

	return pg, nil
}

// OpenConnection creates a new connection to Postgres.
func (p *Postgres) OpenConnection() (integrations.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, err := p.db.Open(p.opts.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	pc := &pgConn{parent: p, Connection: conn}
	cleanupConn := pc
	p.conns = append(p.conns, pc)

	runtime.AddCleanup(pc, func(conn *pgConn) { conn.Close() }, cleanupConn)

	return pc, nil
}

// Close closes the Postgres database and all open connections.
func (p *Postgres) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
	p.db.Close()
	p.db = nil
}

// ConnCount returns the current number of open connections.
func (p *Postgres) ConnCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// URI returns the connection URI (using opts.Path).
func (p *Postgres) URI() string {
	return p.opts.Path
}

// --- pgConn methods to implement the Connection interface ---

// Exec executes a statement that doesn't produce a result set.
func (c *pgConn) Exec(ctx context.Context, sql string) (int64, error) {
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
func (c *pgConn) Query(ctx context.Context, sql string) (array.RecordReader, error) {
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

// GetTableSchema returns the Arrow schema for a given table.
func (c *pgConn) GetTableSchema(ctx context.Context, catalog, schema *string, table string) (*arrow.Schema, error) {
	return c.Connection.GetTableSchema(ctx, catalog, schema, table)
}

// Close closes the connection, removing it from the parent's tracking.
func (c *pgConn) Close() {
	c.parent.mu.Lock()
	defer c.parent.mu.Unlock()

	for i, cc := range c.parent.conns {
		if cc == c {
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

// GetPartitionWhereClause generates the WHERE clause for partitioned tables in PostgreSQL.
// GetPartitionWhereClause generates the WHERE clause for partitioned tables in PostgreSQL.
func (c *pgConn) GetPartitionWhereClause(ctx context.Context, table string, partition string) (string, error) {
	if partition == "" {
		return "", nil // No partition filtering needed
	}

	// Query the partition key from PostgreSQL system catalogs
	partitionKey, err := c.detectPartitionKeyFromDB(ctx, table)
	if err != nil {
		return "", fmt.Errorf("failed to determine partition key: %w", err)
	}

	return fmt.Sprintf("WHERE %s = '%s'", partitionKey, partition), nil
}

// GetRowCount retrieves the row count for a given table (or partition).
func (c *pgConn) GetRowCount(ctx context.Context, table string, partition string) (int64, error) {
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

		// Extract count value
		return rec.Column(0).(*array.Int64).Value(0), nil
	}
	return 0, fmt.Errorf("no rows returned for COUNT query")
}

// GetAggregate computes an aggregate function on a column.
func (c *pgConn) GetAggregate(ctx context.Context, table, column, function, partition string) (float64, error) {
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

// detectPartitionKeyFromDB queries PostgreSQL to determine the partition key.
func (c *pgConn) detectPartitionKeyFromDB(ctx context.Context, table string) (string, error) {
	query := `
		SELECT a.attname 
		FROM pg_partitioned_table pt
		JOIN pg_class c ON pt.partrelid = c.oid
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs)
		WHERE c.relname = $1;
	`

	rr, err := c.Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve partition key: %w", err)
	}
	defer rr.Release()

	if rr.Next() {
		rec := rr.Record()
		if rec.NumCols() < 1 || rec.NumRows() < 1 {
			return "", fmt.Errorf("partition key not found for table '%s'", table)
		}

		// Extract partition key
		return rec.Column(0).(*array.String).Value(0), nil
	}

	return "", fmt.Errorf("no partition key detected for table '%s'", table)
}

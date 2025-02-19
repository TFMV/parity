package integrations

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Postgres is the primary struct managing a PostgreSQL database via ADBC.
// Use NewPostgres(...) to construct.
type Postgres struct {
	mu     sync.Mutex
	db     adbc.Database
	driver adbc.Driver
	opts   Options
	conns  []*pgConn // track open connections
}

// pgConn is a simple wrapper holding an open connection
type pgConn struct {
	parent *Postgres
	adbc.Connection
}

// NewPostgres creates a new Postgres instance.
func NewPostgres(options ...Option) (*Postgres, error) {
	var opts Options
	for _, opt := range options {
		opt(&opts)
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}

	// Auto-detect driver if empty
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
	return pg, nil
}

// OpenConnection opens a new connection to Postgres.
func (p *Postgres) OpenConnection() (*pgConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, err := p.db.Open(p.opts.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	pc := &pgConn{parent: p, Connection: conn}
	p.conns = append(p.conns, pc)
	return pc, nil
}

// Close closes the Postgres database and all open connections.
func (p *Postgres) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.conns {
		c.Connection.Close()
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

// Connection methods
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

func (c *pgConn) Query(ctx context.Context, sql string) (array.RecordReader, adbc.Statement, int64, error) {
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

// Close closes the connection, removing it from the parent Postgres's tracking.
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

// PostgresSource handles connection to a PostgreSQL database using ADBC.
type PostgresSource struct {
	conn adbc.Connection
}

// NewPostgresSource creates a new PostgresSource with an open ADBC connection.
func NewPostgresSource(ctx context.Context, dbURL string) (*PostgresSource, error) {
	drv := drivermgr.Driver{}
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: dbURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	return &PostgresSource{conn: conn}, nil
}

// PostgresRecordReader reads records from a PostgreSQL table.
type PostgresRecordReader struct {
	ctx       context.Context
	stmt      adbc.Statement
	recordSet array.RecordReader
}

func (p *PostgresSource) GetPostgresRecordReader(ctx context.Context, tableName string) (*PostgresRecordReader, error) {
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	// Force PostgreSQL to return the correct Arrow types.
	// TODO: Select * is a placeholder. We need to select the correct columns.
	query := fmt.Sprintf(`
		SELECT *
		FROM %s`, tableName)

	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	recordSet, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &PostgresRecordReader{
		ctx:       ctx,
		stmt:      stmt,
		recordSet: recordSet,
	}, nil
}

// Read reads the next record from the PostgreSQL table.
func (r *PostgresRecordReader) Read() (arrow.Record, error) {
	if !r.recordSet.Next() {
		if err := r.recordSet.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		return nil, io.EOF
	}

	record := r.recordSet.Record()
	record.Retain()
	return record, nil
}

// Schema returns the schema of the records being read.
func (r *PostgresRecordReader) Schema() *arrow.Schema {
	return r.recordSet.Schema()
}

// Close releases resources associated with the PostgresRecordReader.
func (r *PostgresRecordReader) Close() error {
	r.recordSet.Release()
	return r.stmt.Close()
}

// Close closes the ADBC connection associated with PostgresSource.
func (p *PostgresSource) Close() error {
	return p.conn.Close()
}

// URI returns the database connection URI.
func (p *Postgres) URI() string {
	return p.opts.Path // Path is used for URI in PostgreSQL's case
}

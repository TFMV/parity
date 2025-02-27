// Package integrations provides a common interface for databases and connections
// that can be used for comparison.
package integrations

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Options holds configuration options for database connections
type Options struct {
	Context    context.Context
	Path       string
	DriverPath string
}

// Option is a function that configures Options
type Option func(*Options)

// WithContext sets the context for database operations
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// WithPath sets the database connection path/URI
func WithPath(path string) Option {
	return func(o *Options) {
		o.Path = path
	}
}

// WithDriverPath sets the path to the database driver
func WithDriverPath(path string) Option {
	return func(o *Options) {
		o.DriverPath = path
	}
}

// Database represents any database that can be used for comparison
type Database interface {
	// OpenConnection creates a new connection to the database
	OpenConnection() (Connection, error)
	// Close closes the database and all its connections
	Close()
	// ConnCount returns number of open connections
	ConnCount() int
}

// Connection represents a database connection that can execute queries
type Connection interface {
	// Exec executes a query that doesn't return results
	Exec(ctx context.Context, sql string) (int64, error)

	// Query executes a query and returns results
	Query(ctx context.Context, sql string) (array.RecordReader, error)

	// GetTableSchema returns the schema for a table
	GetTableSchema(ctx context.Context, catalog, schema *string, table string) (*arrow.Schema, error)

	// GetPartitionWhereClause returns the appropriate WHERE clause for querying a partitioned table.
	GetPartitionWhereClause(ctx context.Context, table string, partition string) (string, error)

	// GetRowCount returns the row count of a table (or partition).
	GetRowCount(ctx context.Context, table string, partition string) (int64, error)

	// GetAggregate executes an aggregate query (SUM, AVG, etc.) for a given column.
	GetAggregate(ctx context.Context, table, column, function, partition string) (float64, error)

	// Close closes the connection
	Close()
}

// Integration represents a pair of databases to be compared
type Integration interface {
	// Source1 returns the first database
	Source1() Database
	// Source2 returns the second database
	Source2() Database
	// Close closes both databases
	Close()
	// ConnCount returns total number of open connections
	ConnCount() int
}

// DatabasePair implements Integration for any two databases
type DatabasePair struct {
	db1 Database
	db2 Database
}

func NewDatabasePair(db1, db2 Database) Integration {
	return &DatabasePair{
		db1: db1,
		db2: db2,
	}
}

func (p *DatabasePair) Source1() Database {
	return p.db1
}

func (p *DatabasePair) Source2() Database {
	return p.db2
}

func (p *DatabasePair) Close() {
	p.db1.Close()
	p.db2.Close()
}

// ConnCount returns the total number of open connections in both databases
func (p *DatabasePair) ConnCount() int {
	return p.db1.ConnCount() + p.db2.ConnCount()
}

package readers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/TFMV/parity/pkg/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// Import DuckDB driver
	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckDBReader implements a reader for DuckDB using standard SQL interface.
type DuckDBReader struct {
	db        *sql.DB
	query     string
	schema    *arrow.Schema
	batchSize int64
	alloc     memory.Allocator
	rows      *sql.Rows
	mu        sync.Mutex
	closed    bool
}

// NewDuckDBReader creates a new DuckDB reader.
func NewDuckDBReader(config core.ReaderConfig) (core.DatasetReader, error) {
	if config.Path == "" && config.ConnectionString == "" {
		return nil, errors.New("either path or connection string is required for DuckDB reader")
	}

	// Set default batch size if not specified
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	// Get the connection string
	dbPath := config.Path
	if config.ConnectionString != "" {
		dbPath = config.ConnectionString
	}

	// Construct query if not provided
	query := config.Query
	if query == "" && config.Table != "" {
		query = fmt.Sprintf("SELECT * FROM %s", config.Table)
	}

	if query == "" {
		return nil, errors.New("either query or table is required for DuckDB reader")
	}

	// Open database using standard SQL interface
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB database: %w", err)
	}

	// Create a dummy query to get schema
	schemaQuery := fmt.Sprintf("SELECT * FROM (%s) LIMIT 0", query)
	rows, err := db.QueryContext(context.Background(), schemaQuery)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute schema query: %w", err)
	}
	defer rows.Close()

	// Get column types to infer Arrow schema
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Create schema from SQL column types
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		nullable, _ := colType.Nullable()
		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     sqlTypeToArrowType(colType),
			Nullable: nullable,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	return &DuckDBReader{
		db:        db,
		query:     query,
		schema:    schema,
		batchSize: batchSize,
		alloc:     memory.NewGoAllocator(),
	}, nil
}

// sqlTypeToArrowType converts SQL column type to Arrow data type
func sqlTypeToArrowType(colType *sql.ColumnType) arrow.DataType {
	// Default to string for unknown types
	typeName := colType.DatabaseTypeName()

	switch typeName {
	case "INTEGER", "INT", "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "REAL", "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE", "NUMERIC", "DECIMAL":
		return arrow.PrimitiveTypes.Float64
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIME":
		return arrow.FixedWidthTypes.Time64ns
	case "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_ns
	default:
		return arrow.BinaryTypes.String
	}
}

// Read returns the next batch of records.
func (r *DuckDBReader) Read(ctx context.Context) (arrow.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if context is canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Check if closed
	if r.closed {
		return nil, io.EOF
	}

	// Lazy initialization of rows if not already opened
	if r.rows == nil {
		rows, err := r.db.QueryContext(ctx, r.query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
		r.rows = rows
	}

	// Check if there are more rows
	if !r.rows.Next() {
		r.rows.Close()
		r.closed = true
		if err := r.rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
		return nil, io.EOF
	}

	// Create record builders
	columnCount := len(r.schema.Fields())
	builders := make([]array.Builder, columnCount)
	for i, field := range r.schema.Fields() {
		builders[i] = array.NewBuilder(r.alloc, field.Type)
		defer builders[i].Release()
	}

	// Prepare for scanning
	values := make([]interface{}, columnCount)
	scanValues := make([]interface{}, columnCount)
	for i := range values {
		scanValues[i] = &values[i]
	}

	// Process batch of rows
	rowCount := int64(0)
	maxRows := r.batchSize

	// Process current row
	if err := r.rows.Scan(scanValues...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// Add values to builders
	appendValuesToBuilders(builders, values)
	rowCount++

	// Process remaining rows in batch
	for rowCount < maxRows && r.rows.Next() {
		// Check for context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if err := r.rows.Scan(scanValues...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		appendValuesToBuilders(builders, values)
		rowCount++
	}

	// If rows are exhausted, close
	if !r.rows.Next() {
		r.rows.Close()
		r.closed = true
	}

	// Build column arrays
	columns := make([]arrow.Array, columnCount)
	for i, builder := range builders {
		columns[i] = builder.NewArray()
		defer columns[i].Release()
	}

	// Create record batch
	record := array.NewRecord(r.schema, columns, rowCount)
	return record, nil
}

// appendValuesToBuilders appends SQL values to Arrow builders
func appendValuesToBuilders(builders []array.Builder, values []interface{}) {
	for i, val := range values {
		if val == nil {
			builders[i].AppendNull()
			continue
		}

		switch builder := builders[i].(type) {
		case *array.Int8Builder:
			if v, ok := val.(int8); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.Int16Builder:
			if v, ok := val.(int16); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.Int32Builder:
			if v, ok := val.(int32); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.Int64Builder:
			if v, ok := val.(int64); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.Float32Builder:
			if v, ok := val.(float32); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.Float64Builder:
			if v, ok := val.(float64); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.BooleanBuilder:
			if v, ok := val.(bool); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		case *array.StringBuilder:
			if v, ok := val.(string); ok {
				builder.Append(v)
			} else {
				builder.AppendNull()
			}
		default:
			// For unsupported types, append null
			builders[i].AppendNull()
		}
	}
}

// Schema returns the schema of the dataset.
func (r *DuckDBReader) Schema() *arrow.Schema {
	return r.schema
}

// Close closes the reader and releases resources.
func (r *DuckDBReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	var err error
	if r.rows != nil {
		err = r.rows.Close()
	}

	if r.db != nil {
		if dbErr := r.db.Close(); dbErr != nil && err == nil {
			err = dbErr
		}
	}

	r.closed = true
	return err
}

// File: validator_test.go
package validation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/TFMV/parity/integrations"
	"github.com/TFMV/parity/metrics"
	"github.com/TFMV/parity/report"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ----- Fake Implementations for Testing -----

// fakeConnection implements integrations.Connection.
type fakeConnection struct {
	rowCount int64
	schema   *arrow.Schema
}

func (fc *fakeConnection) Exec(ctx context.Context, sql string) (int64, error) {
	return 0, nil
}

func (fc *fakeConnection) Query(ctx context.Context, sql string) (array.RecordReader, error) {
	return nil, nil
}

func (fc *fakeConnection) GetTableSchema(ctx context.Context, catalog, schema *string, table string) (*arrow.Schema, error) {
	return fc.schema, nil
}

func (fc *fakeConnection) Close() {
	// No-op for fake connection.
}

func (fc *fakeConnection) GetPartitionWhereClause(ctx context.Context, table string, partition string) (string, error) {
	return "", nil
}

func (fc *fakeConnection) GetRowCount(ctx context.Context, table string, partition string) (int64, error) {
	return fc.rowCount, nil
}

func (fc *fakeConnection) GetAggregate(ctx context.Context, table, column, function, partition string) (float64, error) {
	return 5000.0, nil // Return dummy value for testing
}

// fakeDB implements integrations.Database.
type fakeDB struct {
	conn *fakeConnection
}

func (db *fakeDB) OpenConnection() (integrations.Connection, error) {
	return db.conn, nil
}

func (db *fakeDB) Close() {}

func (db *fakeDB) ConnCount() int {
	return 1
}

// fakeIntegration implements integrations.Integration.
type fakeIntegration struct {
	db1 integrations.Database
	db2 integrations.Database
}

func (fi *fakeIntegration) Source1() integrations.Database {
	return fi.db1
}

func (fi *fakeIntegration) Source2() integrations.Database {
	return fi.db2
}

func (fi *fakeIntegration) Close() {}

func (fi *fakeIntegration) ConnCount() int {
	return fi.db1.ConnCount() + fi.db2.ConnCount()
}

// newFakeSchema creates a simple Arrow schema for testing.
func newFakeSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}
	return arrow.NewSchema(fields, nil)
}

// ----- Tests -----

func TestValidator_ValidateSuccess(t *testing.T) {
	// Create a fake schema.
	schema := newFakeSchema()

	// Create two fake connections with the same schema.
	fakeConn1 := &fakeConnection{rowCount: 1000, schema: schema}
	fakeConn2 := &fakeConnection{rowCount: 1000, schema: schema}

	// Create fake databases.
	db1 := &fakeDB{conn: fakeConn1}
	db2 := &fakeDB{conn: fakeConn2}

	// Create a fake integration pair.
	fakeInt := &fakeIntegration{db1: db1, db2: db2}

	// Create a production Zap logger.
	logger, err := zap.NewProduction()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Set validation thresholds.
	thresholds := metrics.ValidationThresholds{
		NumericDifferenceTolerance: 0.01,
		RowCountTolerance:          0.005, // 0.5% tolerance
		SamplingConfidenceLevel:    0.95,
	}

	validator := NewValidator(fakeInt, "orders", thresholds, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reportResult, err := validator.Validate(ctx)
	if err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	// Check that all validations passed.
	if !reportResult.SchemaResult.Status {
		t.Errorf("schema validation did not pass")
	}
	if !reportResult.RowCountResult.Status {
		t.Errorf("row count validation did not pass")
	}
	if len(reportResult.AggregateResults) == 0 {
		t.Errorf("expected aggregate results")
	}
	if !reportResult.ValueResult.Status {
		t.Errorf("row values validation did not pass")
	}
}

func TestValidator_GenerateReports(t *testing.T) {
	// Use a fake report result.
	now := time.Now()
	reportResult := metrics.ValidationReport{
		DBMetadata: metrics.DatabaseMetadata{
			SourceDBName:         "src",
			DestinationDBName:    "dest",
			Engine:               "SQL",
			Version:              "1.0",
			SchemaName:           "public",
			StartTime:            now.Add(-time.Minute),
			EndTime:              now,
			Duration:             time.Minute,
			ConnectionString:     "",
			ValidationThresholds: metrics.ValidationThresholds{},
			TotalPartitions:      1,
			TotalShards:          1,
		},
		TableMetadata: metrics.TableMetadata{
			ObjectType:        metrics.Table,
			Name:              "orders",
			PrimaryKeys:       []string{"id"},
			NumColumns:        3,
			ColumnDataTypes:   map[string]string{"id": "int64", "amount": "float64", "price": "float64"},
			PartitionStrategy: "hash",
			ShardingStrategy:  "range",
		},
		RowCountResult: metrics.RowCountResult{
			SourceCount:      1000,
			DestinationCount: 1000,
			Difference:       0,
			Status:           true,
		},
		AggregateResults: []metrics.AggregateResult{
			{
				ColumnName:       "amount",
				AggType:          metrics.Sum,
				SourceValue:      5000,
				DestinationValue: 5000,
				Difference:       0,
				Status:           true,
			},
		},
		ValueResult: metrics.ValueResult{
			Mode:               metrics.Full,
			SamplingPercentage: 100,
			SampledRows:        100,
			MismatchedRows:     0,
			ColumnsCompared:    []string{"amount", "price"},
			MismatchedData:     map[string]interface{}{},
			Status:             true,
		},
		SchemaResult: metrics.SchemaResult{
			Result:                true,
			MissingInDestination:  []string{},
			MissingInSource:       []string{},
			DataTypeMismatches:    map[string]string{},
			PrimaryKeyDifferences: []string{},
			Status:                true,
		},
		PartitionMetrics: []metrics.PartitionResult{},
		ShardMetrics:     []metrics.ShardResult{},
	}

	// Create temporary files for reports.
	tempDir := t.TempDir()
	jsonPath := filepath.Join(tempDir, "report.json")
	htmlPath := filepath.Join(tempDir, "report.html")

	// Create a dummy validator for report generation.
	validator := &Validator{
		JSONReportGenerator: &report.JSONReportGenerator{},
		HTMLReportGenerator: &report.HTMLReportGenerator{},
	}

	if err := validator.GenerateReports(reportResult, jsonPath, htmlPath); err != nil {
		t.Fatalf("failed to generate reports: %v", err)
	}

	// Check that files exist.
	if _, err := os.Stat(jsonPath); err != nil {
		t.Errorf("JSON report file not created: %v", err)
	}
	if _, err := os.Stat(htmlPath); err != nil {
		t.Errorf("HTML report file not created: %v", err)
	}
}

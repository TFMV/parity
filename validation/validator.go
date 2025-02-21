// Package validation implements a high-performance data validator.
package validation

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/TFMV/parity/integrations"
	"github.com/TFMV/parity/metrics"
	"github.com/TFMV/parity/report"
)

// Validator manages the configuration and validation logic.
type Validator struct {
	Integration integrations.Integration // Pair of databases to compare
	TableName   string                   // Table to validate
	Thresholds  metrics.ValidationThresholds

	// Row value validation mode and sampling percentage.
	ValueValidationMode metrics.ValueValidationMode
	SamplingPercentage  float64

	// Logger for structured logging.
	Logger *zap.Logger

	// Metrics collector (stubbed Prometheus integration).
	MetricsCollector *metrics.PrometheusMetricsCollector

	// Report generators.
	JSONReportGenerator report.ReportGenerator
	HTMLReportGenerator report.ReportGenerator
}

// NewValidator constructs a new Validator instance.
func NewValidator(integration integrations.Integration, tableName string, thresholds metrics.ValidationThresholds, logger *zap.Logger) *Validator {
	return &Validator{
		Integration:         integration,
		TableName:           tableName,
		Thresholds:          thresholds,
		ValueValidationMode: metrics.Full,
		SamplingPercentage:  100.0,
		Logger:              logger,
		MetricsCollector:    &metrics.PrometheusMetricsCollector{},
		JSONReportGenerator: &report.JSONReportGenerator{},
		HTMLReportGenerator: &report.HTMLReportGenerator{},
	}
}

// Validate runs all validations concurrently and returns a detailed ValidationReport.
func (v *Validator) Validate(ctx context.Context) (metrics.ValidationReport, error) {
	startTime := time.Now()
	v.Logger.Info("Starting validation", zap.String("table", v.TableName))
	v.MetricsCollector.RecordValidationStart(metrics.DatabaseMetadata{
		StartTime: startTime,
	})

	var (
		wg              sync.WaitGroup
		errCh           = make(chan error, 4)
		schemaResult    metrics.SchemaResult
		rowCountResult  metrics.RowCountResult
		aggregateResult []metrics.AggregateResult
		valueResult     metrics.ValueResult
	)

	// Run schema, row count, aggregates, and row values validations concurrently.
	wg.Add(4)

	go func() {
		defer wg.Done()
		res, err := v.validateSchema(ctx)
		if err != nil {
			errCh <- fmt.Errorf("schema validation failed: %w", err)
			return
		}
		schemaResult = res
		v.Logger.Info("Schema validation completed", zap.Bool("status", res.Status))
	}()

	go func() {
		defer wg.Done()
		res, err := v.validateRowCount(ctx)
		if err != nil {
			errCh <- fmt.Errorf("row count validation failed: %w", err)
			return
		}
		rowCountResult = res
		v.MetricsCollector.RecordRowCountDifference(res.Difference)
		v.Logger.Info("Row count validation completed", zap.Int64("source", res.SourceCount), zap.Int64("destination", res.DestinationCount))
	}()

	go func() {
		defer wg.Done()
		res, err := v.validateAggregates(ctx)
		if err != nil {
			errCh <- fmt.Errorf("aggregate validation failed: %w", err)
			return
		}
		aggregateResult = res
		v.Logger.Info("Aggregate validation completed", zap.Int("columns", len(res)))
	}()

	go func() {
		defer wg.Done()
		res, err := v.validateRowValues(ctx)
		if err != nil {
			errCh <- fmt.Errorf("row values validation failed: %w", err)
			return
		}
		valueResult = res
		v.Logger.Info("Row values validation completed", zap.Int64("mismatches", res.MismatchedRows))
	}()

	wg.Wait()
	close(errCh)

	// Check for any error from the goroutines.
	var validationErr error
	for err := range errCh {
		if err != nil {
			validationErr = err
			v.Logger.Error("Validation error", zap.Error(err))
			break
		}
	}

	// Validate partitions and shards sequentially (or concurrently if needed).
	partitionMetrics, err := v.validatePartitions(ctx)
	if err != nil {
		v.Logger.Error("Partition validation failed", zap.Error(err))
	}
	shardMetrics, err := v.validateShards(ctx)
	if err != nil {
		v.Logger.Error("Shard validation failed", zap.Error(err))
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Assemble metadata.
	dbMeta := metrics.DatabaseMetadata{
		SourceDBName:         "source_db",      // Placeholder; normally determined from the integration.
		DestinationDBName:    "destination_db", // Placeholder
		Engine:               "Parity",         // Example engine type.
		Version:              "0.1.0",
		SchemaName:           "public", // Example schema.
		StartTime:            startTime,
		EndTime:              endTime,
		Duration:             duration,
		ConnectionString:     "", // Omitted for security.
		ValidationThresholds: v.Thresholds,
		TotalPartitions:      len(partitionMetrics),
		TotalShards:          len(shardMetrics),
	}

	tableMeta := metrics.TableMetadata{
		ObjectType:        metrics.Table,
		Name:              v.TableName,
		PrimaryKeys:       []string{"id"}, // Placeholder; adjust as needed.
		NumColumns:        0,              // Could be populated based on schema.
		ColumnDataTypes:   map[string]string{},
		PartitionStrategy: "hash",  // Example strategy.
		ShardingStrategy:  "range", // Example strategy.
	}

	reportResult := metrics.ValidationReport{
		DBMetadata:       dbMeta,
		TableMetadata:    tableMeta,
		RowCountResult:   rowCountResult,
		AggregateResults: aggregateResult,
		ValueResult:      valueResult,
		SchemaResult:     schemaResult,
		PartitionMetrics: partitionMetrics,
		ShardMetrics:     shardMetrics,
	}

	v.MetricsCollector.RecordValidationEnd(duration)
	v.Logger.Info("Validation complete", zap.Duration("duration", duration))

	return reportResult, validationErr
}

// validateSchema compares the table schemas between source and destination.
func (v *Validator) validateSchema(ctx context.Context) (metrics.SchemaResult, error) {
	srcConn, err := v.Integration.Source1().OpenConnection()
	if err != nil {
		return metrics.SchemaResult{}, err
	}
	defer srcConn.Close()

	destConn, err := v.Integration.Source2().OpenConnection()
	if err != nil {
		return metrics.SchemaResult{}, err
	}
	defer destConn.Close()

	// For simplicity, catalog and schema are nil.
	var catalog, sch *string
	srcSchema, err := srcConn.GetTableSchema(ctx, catalog, sch, v.TableName)
	if err != nil {
		return metrics.SchemaResult{}, err
	}
	destSchema, err := destConn.GetTableSchema(ctx, catalog, sch, v.TableName)
	if err != nil {
		return metrics.SchemaResult{}, err
	}

	// Compare fields.
	missingInDest := []string{}
	missingInSrc := []string{}
	dataTypeMismatches := make(map[string]string)

	srcFields := make(map[string]string)
	for _, f := range srcSchema.Fields() {
		srcFields[f.Name] = f.Type.String()
	}
	destFields := make(map[string]string)
	for _, f := range destSchema.Fields() {
		destFields[f.Name] = f.Type.String()
	}
	for name, srcType := range srcFields {
		if destType, ok := destFields[name]; !ok {
			missingInDest = append(missingInDest, name)
		} else if srcType != destType {
			dataTypeMismatches[name] = fmt.Sprintf("source: %s, destination: %s", srcType, destType)
		}
	}
	for name := range destFields {
		if _, ok := srcFields[name]; !ok {
			missingInSrc = append(missingInSrc, name)
		}
	}

	status := len(missingInDest) == 0 && len(missingInSrc) == 0 && len(dataTypeMismatches) == 0

	return metrics.SchemaResult{
		Result:                status,
		MissingInDestination:  missingInDest,
		MissingInSource:       missingInSrc,
		DataTypeMismatches:    dataTypeMismatches,
		PrimaryKeyDifferences: []string{}, // Not implemented in this example.
		Status:                status,
	}, nil
}

// validateRowCount compares row counts between source and destination.
func (v *Validator) validateRowCount(ctx context.Context) (metrics.RowCountResult, error) {
	srcConn, err := v.Integration.Source1().OpenConnection()
	if err != nil {
		return metrics.RowCountResult{}, err
	}
	defer srcConn.Close()

	destConn, err := v.Integration.Source2().OpenConnection()
	if err != nil {
		return metrics.RowCountResult{}, err
	}
	defer destConn.Close()

	srcCount, err := srcConn.GetRowCount(ctx, v.TableName, "")
	if err != nil {
		return metrics.RowCountResult{}, err
	}
	destCount, err := destConn.GetRowCount(ctx, v.TableName, "")
	if err != nil {
		return metrics.RowCountResult{}, err
	}

	diff := srcCount - destCount
	allowedDiff := int64(v.Thresholds.RowCountTolerance * float64(srcCount))
	status := (diff >= -allowedDiff && diff <= allowedDiff)

	return metrics.RowCountResult{
		SourceCount:      srcCount,
		DestinationCount: destCount,
		Difference:       diff,
		Status:           status,
	}, nil
}

// getRowCount simulates a COUNT(*) query; in production, this would execute a SQL query.
func (v *Validator) getRowCount(ctx context.Context, db integrations.Database) (int64, error) {
	conn, err := db.OpenConnection()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Simulated value; replace with actual query logic.
	return 1000, nil
}

// validateAggregates compares aggregate metrics (e.g. SUM) for select numeric columns.
func (v *Validator) validateAggregates(ctx context.Context) ([]metrics.AggregateResult, error) {
	srcConn, err := v.Integration.Source1().OpenConnection()
	if err != nil {
		return nil, err
	}
	defer srcConn.Close()

	destConn, err := v.Integration.Source2().OpenConnection()
	if err != nil {
		return nil, err
	}
	defer destConn.Close()

	columns := []string{"amount", "price"}
	results := make([]metrics.AggregateResult, 0, len(columns))

	for _, col := range columns {
		srcAgg, err := srcConn.GetAggregate(ctx, v.TableName, col, "SUM", "")
		if err != nil {
			return nil, err
		}

		destAgg, err := destConn.GetAggregate(ctx, v.TableName, col, "SUM", "")
		if err != nil {
			return nil, err
		}

		diff := srcAgg - destAgg
		status := math.Abs(diff) <= v.Thresholds.NumericDifferenceTolerance

		results = append(results, metrics.AggregateResult{
			ColumnName:       col,
			AggType:          metrics.Sum,
			SourceValue:      srcAgg,
			DestinationValue: destAgg,
			Difference:       diff,
			Status:           status,
		})
	}
	return results, nil
}

// getAggregate simulates an aggregate query; replace with real query execution and parsing.
func (v *Validator) getAggregate(ctx context.Context, db integrations.Database, column, aggType string) (float64, error) {
	conn, err := db.OpenConnection()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Simulated value; in production, run a query like "SELECT SUM(column) FROM table".
	return 5000.0, nil
}

// validateRowValues simulates a row-level data validation using sampling or full comparison.
func (v *Validator) validateRowValues(ctx context.Context) (metrics.ValueResult, error) {
	// In production, sample rows from both databases and compare.
	// Here, we simulate sampling 100 rows with 2 mismatches.
	sampledRows := int64(100)
	mismatchedRows := int64(2)
	status := (float64(mismatchedRows) / float64(sampledRows)) <= (1.0 - v.Thresholds.SamplingConfidenceLevel)
	return metrics.ValueResult{
		Mode:               v.ValueValidationMode,
		SamplingPercentage: v.SamplingPercentage,
		SampledRows:        sampledRows,
		MismatchedRows:     mismatchedRows,
		ColumnsCompared:    []string{"amount", "price"},
		MismatchedData:     map[string]interface{}{"row_id": []int64{10, 50}},
		Status:             status,
	}, nil
}

// validatePartitions simulates validation on partitioned tables.
func (v *Validator) validatePartitions(ctx context.Context) ([]metrics.PartitionResult, error) {
	// For example, assume the table is partitioned by date.
	partitions := []string{"2023-01-01", "2023-01-02", "2023-01-03"}
	results := make([]metrics.PartitionResult, 0, len(partitions))
	var wg sync.WaitGroup
	var mu sync.Mutex
	errCh := make(chan error, len(partitions))

	for _, part := range partitions {
		wg.Add(1)
		partition := part
		go func() {
			defer wg.Done()
			// Simulate partition validation.
			rowDiff := int64(0)
			aggDiffs := make(map[string]metrics.AggregateResult)
			aggDiffs["amount"] = metrics.AggregateResult{
				ColumnName:       "amount",
				AggType:          metrics.Sum,
				SourceValue:      2000,
				DestinationValue: 2000,
				Difference:       0,
				Status:           true,
			}
			res := metrics.PartitionResult{
				PartitionColumn:      partition,
				RowCountDifferences:  map[string]int64{"row_count": rowDiff},
				AggregateDifferences: aggDiffs,
				Status:               true,
			}
			mu.Lock()
			results = append(results, res)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		return nil, <-errCh
	}
	return results, nil
}

// validateShards simulates validation on sharded tables.
func (v *Validator) validateShards(ctx context.Context) ([]metrics.ShardResult, error) {
	// Assume the table is sharded into two shards.
	shards := []string{"shard1", "shard2"}
	results := make([]metrics.ShardResult, 0, len(shards))
	var wg sync.WaitGroup
	var mu sync.Mutex
	errCh := make(chan error, len(shards))

	for _, shard := range shards {
		wg.Add(1)
		sh := shard
		go func() {
			defer wg.Done()
			rowDiff := int64(0)
			aggDiffs := make(map[string]metrics.AggregateResult)
			aggDiffs["price"] = metrics.AggregateResult{
				ColumnName:       "price",
				AggType:          metrics.Sum,
				SourceValue:      3000,
				DestinationValue: 3000,
				Difference:       0,
				Status:           true,
			}
			res := metrics.ShardResult{
				ShardID:              sh,
				RowCountDifference:   rowDiff,
				AggregateDifferences: aggDiffs,
				Status:               true,
			}
			mu.Lock()
			results = append(results, res)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		return nil, <-errCh
	}
	return results, nil
}

// GenerateReports creates JSON and HTML reports and saves them to the specified file paths.
func (v *Validator) GenerateReports(run metrics.ValidationReport, jsonPath, htmlPath string) error {
	return report.SaveReports(run, jsonPath, htmlPath)
}

// absFloat returns the absolute value of a float64.
func absFloat(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}

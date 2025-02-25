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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

	// First, obtain database metadata to be included in the report
	dbMeta, tableMeta, err := v.extractMetadata(ctx)
	if err != nil {
		return metrics.ValidationReport{}, fmt.Errorf("failed to extract metadata: %w", err)
	}

	// Update metadata with timing information
	dbMeta.StartTime = startTime

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
		res, err := v.validateRowValues()
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
	partitionMetrics, err := v.validatePartitions()
	if err != nil {
		v.Logger.Error("Partition validation failed", zap.Error(err))
	}
	shardMetrics, err := v.validateShards()
	if err != nil {
		v.Logger.Error("Shard validation failed", zap.Error(err))
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Update metadata with timing information
	dbMeta.EndTime = endTime
	dbMeta.Duration = duration
	dbMeta.TotalPartitions = len(partitionMetrics)
	dbMeta.TotalShards = len(shardMetrics)
	dbMeta.ValidationThresholds = v.Thresholds

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

	// Get primary key information from both databases
	srcPKs, err := getPrimaryKeys(ctx, srcConn, v.TableName)
	if err != nil {
		v.Logger.Warn("Failed to get source primary keys",
			zap.String("table", v.TableName),
			zap.Error(err))
	}

	destPKs, err := getPrimaryKeys(ctx, destConn, v.TableName)
	if err != nil {
		v.Logger.Warn("Failed to get destination primary keys",
			zap.String("table", v.TableName),
			zap.Error(err))
	}

	// Find differences in primary keys
	pkDiffs := findPrimaryKeyDifferences(srcPKs, destPKs)

	// Overall status depends on all checks
	status := len(missingInDest) == 0 &&
		len(missingInSrc) == 0 &&
		len(dataTypeMismatches) == 0 &&
		len(pkDiffs) == 0

	return metrics.SchemaResult{
		Result:                status,
		MissingInDestination:  missingInDest,
		MissingInSource:       missingInSrc,
		DataTypeMismatches:    dataTypeMismatches,
		PrimaryKeyDifferences: pkDiffs,
		Status:                status,
	}, nil
}

// getPrimaryKeys attempts to retrieve primary key information for a table
func getPrimaryKeys(ctx context.Context, conn integrations.Connection, tableName string) ([]string, error) {
	// This query is database-specific and would need to be adapted
	// based on the type of database you're connecting to
	query := fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = '%s'
		AND constraint_name IN (
			SELECT constraint_name 
			FROM information_schema.table_constraints 
			WHERE table_name = '%s' 
			AND constraint_type = 'PRIMARY KEY'
		)
		ORDER BY ordinal_position
	`, tableName, tableName)

	reader, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	// Safely handle nil record reader
	if reader == nil {
		return nil, fmt.Errorf("query returned nil record reader")
	}

	defer reader.Release()

	var primaryKeys []string
	for reader.Next() {
		record := reader.Record()
		if record == nil {
			continue // Skip nil records
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				continue // Skip nil columns
			}

			if strCol, ok := col.(*array.String); ok {
				for i := 0; i < strCol.Len(); i++ {
					primaryKeys = append(primaryKeys, strCol.Value(i))
				}
			}
		}
	}

	return primaryKeys, nil
}

// findPrimaryKeyDifferences identifies differences between primary keys
func findPrimaryKeyDifferences(srcPKs, destPKs []string) []string {
	var differences []string

	// Convert to maps for easier comparison
	srcPKMap := make(map[string]bool)
	for _, pk := range srcPKs {
		srcPKMap[pk] = true
	}

	destPKMap := make(map[string]bool)
	for _, pk := range destPKs {
		destPKMap[pk] = true
	}

	// Check primary keys in source not in destination
	for _, pk := range srcPKs {
		if !destPKMap[pk] {
			differences = append(differences, fmt.Sprintf("PK '%s' in source but not in destination", pk))
		}
	}

	// Check primary keys in destination not in source
	for _, pk := range destPKs {
		if !srcPKMap[pk] {
			differences = append(differences, fmt.Sprintf("PK '%s' in destination but not in source", pk))
		}
	}

	// Check if primary key order is different
	if len(srcPKs) == len(destPKs) && len(differences) == 0 {
		for i := range srcPKs {
			if srcPKs[i] != destPKs[i] {
				differences = append(differences, "Primary key columns are in different order")
				break
			}
		}
	}

	return differences
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

// validateRowValues simulates a row-level data validation using sampling or full comparison.
func (v *Validator) validateRowValues() (metrics.ValueResult, error) {
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
func (v *Validator) validatePartitions() ([]metrics.PartitionResult, error) {
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

// validatePartition validates a single partition comparing row counts and aggregates
func (v *Validator) validatePartition(ctx context.Context, partition string) (map[string]int64, map[string]metrics.AggregateResult, error) {
	srcConn, err := v.Integration.Source1().OpenConnection()
	if err != nil {
		return nil, nil, err
	}
	defer srcConn.Close()

	destConn, err := v.Integration.Source2().OpenConnection()
	if err != nil {
		return nil, nil, err
	}
	defer destConn.Close()

	// Validate row count for this partition
	srcCount, err := srcConn.GetRowCount(ctx, v.TableName, partition)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get source row count: %w", err)
	}

	destCount, err := destConn.GetRowCount(ctx, v.TableName, partition)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get destination row count: %w", err)
	}

	rowCountDiffs := map[string]int64{
		"row_count": srcCount - destCount,
	}

	// Get schema to determine columns for aggregate validation
	var catalog, schema *string
	srcSchema, err := srcConn.GetTableSchema(ctx, catalog, schema, v.TableName)
	if err != nil {
		return rowCountDiffs, nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Choose numeric columns for aggregates
	numericColumns := []string{}
	for _, field := range srcSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
			arrow.FLOAT32, arrow.FLOAT64:
			numericColumns = append(numericColumns, field.Name)
		}
	}

	// Calculate aggregates for each numeric column
	aggDiffs := make(map[string]metrics.AggregateResult)
	for _, col := range numericColumns {
		srcAgg, err := srcConn.GetAggregate(ctx, v.TableName, col, "SUM", partition)
		if err != nil {
			v.Logger.Warn("Failed to get aggregate for column",
				zap.String("column", col),
				zap.String("partition", partition),
				zap.Error(err))
			continue
		}

		destAgg, err := destConn.GetAggregate(ctx, v.TableName, col, "SUM", partition)
		if err != nil {
			v.Logger.Warn("Failed to get aggregate for column",
				zap.String("column", col),
				zap.String("partition", partition),
				zap.Error(err))
			continue
		}

		diff := srcAgg - destAgg
		status := math.Abs(diff) <= v.Thresholds.NumericDifferenceTolerance

		aggDiffs[col] = metrics.AggregateResult{
			ColumnName:       col,
			AggType:          metrics.Sum,
			SourceValue:      srcAgg,
			DestinationValue: destAgg,
			Difference:       diff,
			Status:           status,
		}
	}

	return rowCountDiffs, aggDiffs, nil
}

// validateShards simulates validation on sharded tables.
func (v *Validator) validateShards() ([]metrics.ShardResult, error) {
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

// extractMetadata obtains metadata about the databases and table being validated
func (v *Validator) extractMetadata(ctx context.Context) (metrics.DatabaseMetadata, metrics.TableMetadata, error) {
	// Initialize default metadata that will be returned as fallback in case of errors
	defaultDBMeta := metrics.DatabaseMetadata{
		SourceDBName:      "source_db",
		DestinationDBName: "destination_db",
		Engine:            "Unknown",
		Version:           "Unknown",
		SchemaName:        "public",
		ConnectionString:  "", // Omitted for security
	}

	// Default empty table metadata
	defaultTableMeta := metrics.TableMetadata{
		ObjectType:        metrics.Table,
		Name:              v.TableName,
		PrimaryKeys:       []string{"id"}, // Common default
		NumColumns:        0,
		ColumnDataTypes:   make(map[string]string),
		PartitionStrategy: "none",
		ShardingStrategy:  "none",
	}

	srcConn, err := v.Integration.Source1().OpenConnection()
	if err != nil {
		v.Logger.Warn("Failed to open source connection", zap.Error(err))
		return defaultDBMeta, defaultTableMeta, nil // Don't fail, use defaults
	}
	defer srcConn.Close()

	destConn, err := v.Integration.Source2().OpenConnection()
	if err != nil {
		v.Logger.Warn("Failed to open destination connection", zap.Error(err))
		return defaultDBMeta, defaultTableMeta, nil // Don't fail, use defaults
	}
	defer destConn.Close()

	// Get database names
	srcDBName, err := getDatabaseName(ctx, srcConn)
	if err != nil {
		srcDBName = "source_db" // Fallback
		v.Logger.Warn("Failed to get source database name", zap.Error(err))
	}

	destDBName, err := getDatabaseName(ctx, destConn)
	if err != nil {
		destDBName = "destination_db" // Fallback
		v.Logger.Warn("Failed to get destination database name", zap.Error(err))
	}

	// Get database engine and version
	engine, version, err := getDatabaseEngineVersion(ctx, srcConn)
	if err != nil {
		engine = "Unknown"
		version = "Unknown"
		v.Logger.Warn("Failed to get database engine/version", zap.Error(err))
	}

	// Get schema name if not specified
	schemaName, err := getSchemaName(ctx, srcConn, v.TableName)
	if err != nil {
		schemaName = "public" // Default schema
		v.Logger.Warn("Failed to get schema name", zap.Error(err))
	}

	// Get destination schema name for comparison
	destSchemaName, err := getSchemaName(ctx, destConn, v.TableName)
	if err != nil {
		destSchemaName = "public" // Default schema
		v.Logger.Warn("Failed to get destination schema name", zap.Error(err))
	}

	// Build database metadata
	dbMeta := metrics.DatabaseMetadata{
		SourceDBName:      srcDBName,
		DestinationDBName: destDBName,
		Engine:            engine,
		Version:           version,
		SchemaName:        schemaName,
		ConnectionString:  "", // Omitted for security
	}

	// Gather schema information from source
	var catalog, schema *string
	srcSchema, err := srcConn.GetTableSchema(ctx, catalog, schema, v.TableName)
	if err != nil {
		v.Logger.Warn("Failed to get source table schema", zap.Error(err))
		return dbMeta, defaultTableMeta, nil // Return what we have so far
	}

	// Gather schema information from destination for complete metadata
	destSchema, err := destConn.GetTableSchema(ctx, catalog, schema, v.TableName)
	if err != nil {
		v.Logger.Warn("Failed to get destination table schema", zap.Error(err))
		// We can continue with source schema if destination schema retrieval fails
	}

	// Get primary keys with robust error handling
	var primaryKeys []string
	pkErr := func() error {
		primaryKeys, err = getPrimaryKeys(ctx, srcConn, v.TableName)
		return err
	}()

	if pkErr != nil {
		v.Logger.Warn("Failed to get source primary keys", zap.Error(pkErr))
		primaryKeys = []string{"id"} // Fallback to common primary key
	}

	// Get destination primary keys for comparison
	var destPrimaryKeys []string
	destPkErr := func() error {
		destPrimaryKeys, err = getPrimaryKeys(ctx, destConn, v.TableName)
		return err
	}()

	if destPkErr != nil {
		v.Logger.Warn("Failed to get destination primary keys", zap.Error(destPkErr))
		destPrimaryKeys = []string{"id"} // Fallback to common primary key
	}

	// Log primary key differences
	if !equalStringSlices(primaryKeys, destPrimaryKeys) {
		v.Logger.Warn("Primary key differences detected",
			zap.Strings("source_pks", primaryKeys),
			zap.Strings("destination_pks", destPrimaryKeys))
	}

	// Determine if table is a view with robust error handling
	isView := false
	viewErr := func() error {
		var err error
		isView, err = isTableView(ctx, srcConn, v.TableName)
		return err
	}()

	if viewErr != nil {
		v.Logger.Warn("Failed to determine if object is view", zap.Error(viewErr))
		isView = false // Default to table
	}

	objectType := metrics.Table
	if isView {
		objectType = metrics.View
	}

	// Build column data types map
	columnDataTypes := make(map[string]string)
	if srcSchema != nil { // Protect against nil schema
		for _, field := range srcSchema.Fields() {
			columnDataTypes[field.Name] = field.Type.String()
		}
	}

	// Build destination column data types map
	destColumnDataTypes := make(map[string]string)
	if destSchema != nil { // Protect against nil schema
		for _, field := range destSchema.Fields() {
			destColumnDataTypes[field.Name] = field.Type.String()
		}
	}

	// Log schema differences
	logSchemaDifferences(v.Logger, columnDataTypes, destColumnDataTypes)

	// Determine partitioning strategy with robust error handling
	partitionStrategy := "none"
	partErr := func() error {
		var err error
		partitionStrategy, err = getPartitionStrategy(ctx, srcConn, v.TableName)
		return err
	}()

	if partErr != nil {
		v.Logger.Warn("Failed to determine partition strategy", zap.Error(partErr))
		partitionStrategy = "none"
	}

	// Determine sharding strategy with robust error handling
	shardingStrategy := "none"
	shardErr := func() error {
		var err error
		shardingStrategy, err = getShardingStrategy(ctx, srcConn, v.TableName)
		return err
	}()

	if shardErr != nil {
		v.Logger.Warn("Failed to determine sharding strategy", zap.Error(shardErr))
		shardingStrategy = "none"
	}

	// Build table metadata
	numColumns := 0
	if srcSchema != nil {
		numColumns = srcSchema.NumFields()
	}

	// Store destination num columns
	destNumColumns := 0
	if destSchema != nil {
		destNumColumns = destSchema.NumFields()
	}

	tableMeta := metrics.TableMetadata{
		ObjectType:        objectType,
		Name:              v.TableName,
		PrimaryKeys:       primaryKeys,
		NumColumns:        numColumns,
		ColumnDataTypes:   columnDataTypes,
		PartitionStrategy: partitionStrategy,
		ShardingStrategy:  shardingStrategy,
	}

	// We currently can't add fields to the TableMetadata struct,
	// but we can log the destination schema information for analysis
	v.Logger.Info("Destination schema information",
		zap.String("schema_name", destSchemaName),
		zap.Int("num_columns", destNumColumns),
		zap.Strings("primary_keys", destPrimaryKeys),
		zap.Any("column_data_types", destColumnDataTypes))

	return dbMeta, tableMeta, nil
}

// Helper functions for comparing schema information

// equalStringSlices compares two string slices for equality
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// logSchemaDifferences logs differences between source and destination schemas
func logSchemaDifferences(logger *zap.Logger, srcColumns, destColumns map[string]string) {
	// Check for columns in source not in destination
	for colName, srcType := range srcColumns {
		if destType, exists := destColumns[colName]; !exists {
			logger.Warn("Column in source but not in destination",
				zap.String("column", colName),
				zap.String("type", srcType))
		} else if srcType != destType {
			logger.Warn("Column type mismatch",
				zap.String("column", colName),
				zap.String("source_type", srcType),
				zap.String("dest_type", destType))
		}
	}

	// Check for columns in destination not in source
	for colName, destType := range destColumns {
		if _, exists := srcColumns[colName]; !exists {
			logger.Warn("Column in destination but not in source",
				zap.String("column", colName),
				zap.String("type", destType))
		}
	}
}

// Helper functions to extract database information

// getDatabaseName gets the current database name
func getDatabaseName(ctx context.Context, conn integrations.Connection) (string, error) {
	// This query works for PostgreSQL, MySQL, etc.
	rr, err := conn.Query(ctx, "SELECT current_database()")
	if err != nil {
		// Try alternative queries for different databases
		rr, err = conn.Query(ctx, "SELECT DATABASE()")
		if err != nil {
			return "", err
		}
	}

	// Safely handle nil record reader
	if rr == nil {
		return "", fmt.Errorf("query returned nil record reader")
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return "", fmt.Errorf("nil record returned")
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				return "", fmt.Errorf("nil column returned")
			}

			if strCol, ok := col.(*array.String); ok && strCol.Len() > 0 {
				return strCol.Value(0), nil
			}
		}
	}

	return "", fmt.Errorf("could not determine database name")
}

// getDatabaseEngineVersion gets the database engine and version
func getDatabaseEngineVersion(ctx context.Context, conn integrations.Connection) (string, string, error) {
	// Try PostgreSQL version query
	rr, err := conn.Query(ctx, "SELECT version()")
	if err != nil {
		return "", "", err
	}

	// Safely handle nil record reader
	if rr == nil {
		return "", "", fmt.Errorf("query returned nil record reader")
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return "", "", fmt.Errorf("nil record returned")
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				return "", "", fmt.Errorf("nil column returned")
			}

			if strCol, ok := col.(*array.String); ok && strCol.Len() > 0 {
				versionStr := strCol.Value(0)
				// Simple parsing - production code would be more robust
				if len(versionStr) > 10 {
					return "PostgreSQL", versionStr[:10], nil
				}
				return "PostgreSQL", versionStr, nil
			}
		}
	}

	return "", "", fmt.Errorf("could not determine database engine/version")
}

// getSchemaName gets the schema name for a table
func getSchemaName(ctx context.Context, conn integrations.Connection, tableName string) (string, error) {
	query := fmt.Sprintf(`
		SELECT table_schema
		FROM information_schema.tables
		WHERE table_name = '%s'
		LIMIT 1
	`, tableName)

	rr, err := conn.Query(ctx, query)
	if err != nil {
		return "", err
	}

	// Safely handle nil record reader
	if rr == nil {
		return "", fmt.Errorf("query returned nil record reader")
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return "", fmt.Errorf("nil record returned")
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				return "", fmt.Errorf("nil column returned")
			}

			if strCol, ok := col.(*array.String); ok && strCol.Len() > 0 {
				return strCol.Value(0), nil
			}
		}
	}

	return "", fmt.Errorf("could not determine schema name")
}

// isTableView determines if an object is a view
func isTableView(ctx context.Context, conn integrations.Connection, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT table_type
		FROM information_schema.tables
		WHERE table_name = '%s'
		LIMIT 1
	`, tableName)

	rr, err := conn.Query(ctx, query)
	if err != nil {
		return false, err
	}

	// Safely handle nil record reader
	if rr == nil {
		return false, fmt.Errorf("query returned nil record reader")
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return false, fmt.Errorf("nil record returned")
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				return false, fmt.Errorf("nil column returned")
			}

			if strCol, ok := col.(*array.String); ok && strCol.Len() > 0 {
				tableType := strCol.Value(0)
				return tableType == "VIEW", nil
			}
		}
	}

	return false, fmt.Errorf("could not determine if object is view")
}

// getPartitionStrategy attempts to determine table partitioning
func getPartitionStrategy(ctx context.Context, conn integrations.Connection, tableName string) (string, error) {
	// PostgreSQL-specific query
	query := fmt.Sprintf(`
		SELECT partitioning_type 
		FROM information_schema.partitions
		WHERE table_name = '%s'
		LIMIT 1
	`, tableName)

	rr, err := conn.Query(ctx, query)
	if err != nil {
		return "none", nil // Assume no partitioning if query fails
	}

	// Safely handle nil record reader
	if rr == nil {
		return "none", nil // Assume no partitioning if record reader is nil
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return "none", nil // Assume no partitioning if record is nil
		}

		if record.NumCols() > 0 && record.NumRows() > 0 {
			col := record.Column(0)
			if col == nil {
				return "none", nil // Assume no partitioning if column is nil
			}

			if strCol, ok := col.(*array.String); ok && strCol.Len() > 0 {
				return strCol.Value(0), nil
			}
		}
	}

	return "none", nil
}

// getShardingStrategy attempts to determine table sharding
func getShardingStrategy(ctx context.Context, conn integrations.Connection, tableName string) (string, error) {
	// This is highly database-specific
	// For simplicity, we'll check if the table has a shard key column
	query := fmt.Sprintf(`
		SELECT column_name 
		FROM information_schema.columns
		WHERE table_name = '%s'
		AND column_name LIKE '%%shard%%'
		LIMIT 1
	`, tableName)

	rr, err := conn.Query(ctx, query)
	if err != nil {
		return "none", nil
	}

	// Safely handle nil record reader
	if rr == nil {
		return "none", nil // Assume no sharding if record reader is nil
	}

	defer rr.Release()

	if rr.Next() {
		record := rr.Record()
		if record == nil {
			return "none", nil // Assume no sharding if record is nil
		}

		return "hash", nil // Assume hash sharding if we find a shard column
	}

	return "none", nil
}

// validator.go
package validation

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/TFMV/parity/integrations"
	"github.com/TFMV/parity/logger"
	"github.com/TFMV/parity/metrics"
	"github.com/TFMV/parity/report"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.uber.org/zap"
)

// Validator encapsulates a pair of databases, validation thresholds, and a report generator.
type Validator struct {
	integration     integrations.Integration
	thresholds      metrics.ValidationThresholds
	reportGenerator report.ReportGenerator
}

// NewValidator creates a new Validator instance.
func NewValidator(integration integrations.Integration, thresholds metrics.ValidationThresholds, reportGenerator report.ReportGenerator) *Validator {
	return &Validator{
		integration:     integration,
		thresholds:      thresholds,
		reportGenerator: reportGenerator,
	}
}

// ValidateAll runs all validations (schema, row count, aggregates, and values) concurrently and assembles a ValidationReport.
func (v *Validator) ValidateAll(ctx context.Context, tableName string, columns []string, valueMode metrics.ValueValidationMode, samplePercentage float64) (metrics.ValidationReport, error) {
	log := logger.GetLogger()
	startTime := time.Now()
	log.Info("Starting full validation", zap.String("table", tableName))

	var wg sync.WaitGroup
	var schemaResult metrics.SchemaResult
	var rowCountResult metrics.RowCountResult
	var aggResults []metrics.AggregateResult
	var valueResult metrics.ValueResult

	var schemaErr, rowCountErr, aggErr, valueErr error

	// Run validations concurrently
	wg.Add(4)

	go func() {
		defer wg.Done()
		schemaResult, schemaErr = v.ValidateSchema(ctx, tableName)
	}()

	go func() {
		defer wg.Done()
		rowCountResult, rowCountErr = v.ValidateRowCounts(ctx, tableName)
	}()

	go func() {
		defer wg.Done()
		aggResults, aggErr = v.ValidateAggregates(ctx, tableName, columns)
	}()

	go func() {
		defer wg.Done()
		valueResult, valueErr = v.ValidateValues(ctx, tableName, columns, valueMode, samplePercentage)
	}()

	wg.Wait()

	// Aggregate errors
	if schemaErr != nil || rowCountErr != nil || aggErr != nil || valueErr != nil {
		log.Error("Validation errors encountered",
			zap.Error(schemaErr), zap.Error(rowCountErr), zap.Error(aggErr), zap.Error(valueErr))
		return metrics.ValidationReport{}, fmt.Errorf("validation errors encountered: %v, %v, %v, %v",
			schemaErr, rowCountErr, aggErr, valueErr)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Construct metadata
	dbMeta := metrics.DatabaseMetadata{
		SourceDBName:         "SourceDB", // Extract actual DB info if available
		DestinationDBName:    "DestinationDB",
		Engine:               "Apache Arrow ADBC",
		Version:              "1.0",
		SchemaName:           "",
		StartTime:            startTime,
		EndTime:              endTime,
		Duration:             duration,
		ConnectionString:     "",
		ValidationThresholds: v.thresholds,
		TotalPartitions:      0,
		TotalShards:          0,
	}
	tableMeta := metrics.TableMetadata{
		ObjectType:        metrics.Table,
		Name:              tableName,
		PrimaryKeys:       []string{},
		NumColumns:        0,
		ColumnDataTypes:   map[string]string{},
		PartitionStrategy: "",
		ShardingStrategy:  "",
	}

	// Construct validation report
	reportData := metrics.ValidationReport{
		DBMetadata:       dbMeta,
		TableMetadata:    tableMeta,
		RowCountResult:   rowCountResult,
		AggregateResults: aggResults,
		ValueResult:      valueResult,
		SchemaResult:     schemaResult,
		PartitionMetrics: []metrics.PartitionResult{},
		ShardMetrics:     []metrics.ShardResult{},
	}

	// Generate and log report
	if rep, err := v.reportGenerator.GenerateValidationReport(reportData); err == nil {
		log.Info("Validation report generated", zap.ByteString("report", rep))
	} else {
		log.Error("Failed to generate report", zap.Error(err))
	}

	log.Info("Validation completed", zap.Duration("duration", duration))
	return reportData, nil
}

// ValidateSchema compares the table schema between source and destination.
func (v *Validator) ValidateSchema(ctx context.Context, tableName string) (metrics.SchemaResult, error) {
	log := logger.GetLogger()
	log.Info("Starting schema validation", zap.String("table", tableName))

	srcConn, err := v.integration.Source1().OpenConnection()
	if err != nil {
		log.Error("Failed to open source connection", zap.Error(err))
		return metrics.SchemaResult{}, err
	}
	defer srcConn.Close()

	dstConn, err := v.integration.Source2().OpenConnection()
	if err != nil {
		log.Error("Failed to open destination connection", zap.Error(err))
		return metrics.SchemaResult{}, err
	}
	defer dstConn.Close()

	srcSchema, err := srcConn.GetTableSchema(ctx, nil, nil, tableName)
	if err != nil {
		log.Error("Failed to get source schema", zap.Error(err))
		return metrics.SchemaResult{}, err
	}
	dstSchema, err := dstConn.GetTableSchema(ctx, nil, nil, tableName)
	if err != nil {
		log.Error("Failed to get destination schema", zap.Error(err))
		return metrics.SchemaResult{}, err
	}

	srcFields := srcSchema.Fields()
	dstFields := dstSchema.Fields()

	var missingInDest []string
	var missingInSource []string
	dataTypeMismatches := make(map[string]string)

	srcMap := make(map[string]*arrow.Field)
	dstMap := make(map[string]*arrow.Field)

	for _, f := range srcFields {
		srcMap[f.Name] = &f
	}
	for _, f := range dstFields {
		dstMap[f.Name] = &f
	}

	for name, srcField := range srcMap {
		if dstField, ok := dstMap[name]; !ok {
			missingInDest = append(missingInDest, name)
		} else {
			if srcField.Type.String() != dstField.Type.String() {
				dataTypeMismatches[name] = fmt.Sprintf("source: %s, destination: %s", srcField.Type, dstField.Type)
			}
		}
	}
	for name := range dstMap {
		if _, ok := srcMap[name]; !ok {
			missingInSource = append(missingInSource, name)
		}
	}

	status := len(missingInDest) == 0 && len(missingInSource) == 0 && len(dataTypeMismatches) == 0
	if status {
		log.Info("Schema validation passed", zap.String("table", tableName))
	} else {
		log.Warn("Schema validation failed", zap.String("table", tableName),
			zap.Any("missingInDestination", missingInDest),
			zap.Any("missingInSource", missingInSource),
			zap.Any("dataTypeMismatches", dataTypeMismatches))
	}

	return metrics.SchemaResult{
		Result:                status,
		MissingInDestination:  missingInDest,
		MissingInSource:       missingInSource,
		DataTypeMismatches:    dataTypeMismatches,
		PrimaryKeyDifferences: []string{}, // Extend if primary key info is available.
		Status:                status,
	}, nil
}

// ValidateRowCounts compares the row counts between source and destination.
func (v *Validator) ValidateRowCounts(ctx context.Context, tableName string) (metrics.RowCountResult, error) {
	log := logger.GetLogger()
	log.Info("Starting row count validation", zap.String("table", tableName))

	srcConn, err := v.integration.Source1().OpenConnection()
	if err != nil {
		log.Error("Failed to open source connection", zap.Error(err))
		return metrics.RowCountResult{}, err
	}
	defer srcConn.Close()

	dstConn, err := v.integration.Source2().OpenConnection()
	if err != nil {
		log.Error("Failed to open destination connection", zap.Error(err))
		return metrics.RowCountResult{}, err
	}
	defer dstConn.Close()

	srcCount, err := v.getRowCount(ctx, srcConn, tableName)
	if err != nil {
		log.Error("Failed to get source row count", zap.Error(err))
		return metrics.RowCountResult{}, err
	}
	dstCount, err := v.getRowCount(ctx, dstConn, tableName)
	if err != nil {
		log.Error("Failed to get destination row count", zap.Error(err))
		return metrics.RowCountResult{}, err
	}

	diff := srcCount - dstCount
	status := math.Abs(float64(diff)) <= v.thresholds.RowCountTolerance

	if status {
		log.Info("Row count validation passed", zap.Int64("source", srcCount), zap.Int64("destination", dstCount))
	} else {
		log.Warn("Row count validation failed", zap.Int64("source", srcCount), zap.Int64("destination", dstCount), zap.Int64("difference", diff))
	}

	return metrics.RowCountResult{
		SourceCount:      srcCount,
		DestinationCount: dstCount,
		Difference:       diff,
		Status:           status,
	}, nil
}

// getRowCount executes a COUNT(*) query and returns the count.
func (v *Validator) getRowCount(ctx context.Context, conn integrations.Connection, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	rr, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rr.Release()

	if rr.Next() {
		rec := rr.Record()
		if rec.NumCols() < 1 || rec.NumRows() < 1 {
			return 0, fmt.Errorf("invalid count result")
		}
		col := rec.Column(0)
		// For simplicity, assume COUNT(*) returns int64.
		intArr, ok := col.(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("unexpected data type for count")
		}
		return intArr.Value(0), nil
	}
	return 0, fmt.Errorf("no data returned for count query")
}

// ValidateAggregates compares aggregates (SUM, AVG, MIN, MAX, COUNT) for each specified column.
func (v *Validator) ValidateAggregates(ctx context.Context, tableName string, columns []string) ([]metrics.AggregateResult, error) {
	log := logger.GetLogger()
	log.Info("Starting aggregate validation", zap.String("table", tableName))
	var results []metrics.AggregateResult

	aggTypes := []metrics.AggregationType{metrics.Sum, metrics.Avg, metrics.Min, metrics.Max, metrics.Count}

	for _, col := range columns {
		query := fmt.Sprintf("SELECT SUM(%s), AVG(%s), MIN(%s), MAX(%s), COUNT(%s) FROM %s",
			col, col, col, col, col, tableName)

		srcConn, err := v.integration.Source1().OpenConnection()
		if err != nil {
			log.Error("Failed to open source connection", zap.Error(err))
			return nil, err
		}
		defer srcConn.Close()

		srcAgg, err := v.getAggregates(ctx, srcConn, query)
		if err != nil {
			log.Error("Failed to get source aggregates", zap.String("column", col), zap.Error(err))
			return nil, err
		}

		dstConn, err := v.integration.Source2().OpenConnection()
		if err != nil {
			log.Error("Failed to open destination connection", zap.Error(err))
			return nil, err
		}
		defer dstConn.Close()

		dstAgg, err := v.getAggregates(ctx, dstConn, query)
		if err != nil {
			log.Error("Failed to get destination aggregates", zap.String("column", col), zap.Error(err))
			return nil, err
		}

		for i, aggType := range aggTypes {
			srcVal := srcAgg[i]
			dstVal := dstAgg[i]
			diff := srcVal - dstVal
			status := math.Abs(diff) <= v.thresholds.NumericDifferenceTolerance

			if status {
				log.Info("Aggregate validation passed", zap.String("column", col), zap.String("aggType", string(aggType)))
			} else {
				log.Warn("Aggregate validation failed", zap.String("column", col), zap.String("aggType", string(aggType)),
					zap.Float64("src", srcVal), zap.Float64("dst", dstVal), zap.Float64("diff", diff))
			}

			results = append(results, metrics.AggregateResult{
				ColumnName:       col,
				AggType:          aggType,
				SourceValue:      srcVal,
				DestinationValue: dstVal,
				Difference:       diff,
				Status:           status,
			})
		}
	}

	return results, nil
}

// getAggregates runs the provided aggregate query on the given database and extracts 5 numeric values.
func (v *Validator) getAggregates(ctx context.Context, conn integrations.Connection, query string) ([]float64, error) {
	rr, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	if rr.Next() {
		rec := rr.Record()
		if rec.NumCols() < 5 || rec.NumRows() < 1 {
			return nil, fmt.Errorf("invalid aggregate result")
		}
		var aggregates []float64
		for i := 0; i < 5; i++ {
			col := rec.Column(i)
			val, err := extractNumericValue(col, 0)
			if err != nil {
				return nil, err
			}

			// Convert different numeric types to float64
			switch v := val.(type) {
			case float64:
				aggregates = append(aggregates, v)
			case float32:
				aggregates = append(aggregates, float64(v))
			case int64:
				aggregates = append(aggregates, float64(v))
			case int32:
				aggregates = append(aggregates, float64(v))
			case *decimalValue:
				// Convert decimal to float64 for aggregates
				f, _ := v.value.Float64()
				if v.scale > 0 {
					f = f / math.Pow10(int(v.scale))
				}
				aggregates = append(aggregates, f)
			default:
				return nil, fmt.Errorf("unsupported aggregate type: %T", val)
			}
		}
		return aggregates, nil
	}
	return nil, fmt.Errorf("no data returned for aggregate query")
}

// extractNumericValue returns the numeric value at the given row for supported Arrow types.
func extractNumericValue(arr arrow.Array, row int) (interface{}, error) {
	if arr.IsNull(row) {
		return nil, nil // Return nil for NULL values instead of 0
	}

	switch arr.DataType().ID() {
	case arrow.INT64:
		return arr.(*array.Int64).Value(row), nil
	case arrow.INT32:
		return arr.(*array.Int32).Value(row), nil
	case arrow.FLOAT64:
		return arr.(*array.Float64).Value(row), nil
	case arrow.FLOAT32:
		return arr.(*array.Float32).Value(row), nil
	case arrow.DECIMAL128:
		// Preserve Decimal128 precision using big.Int
		decArr := arr.(*array.Decimal128)
		dec := decArr.Value(row)
		scale := arr.DataType().(*arrow.Decimal128Type).Scale
		return &decimalValue{
			value: dec.BigInt(),
			scale: scale,
		}, nil
	case arrow.DECIMAL256:
		// Preserve Decimal256 precision using big.Int
		decArr := arr.(*array.Decimal256)
		dec := decArr.Value(row)
		scale := arr.DataType().(*arrow.Decimal256Type).Scale
		return &decimalValue{
			value: dec.BigInt(),
			scale: scale,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported data type for numeric extraction: %s", arr.DataType().Name())
	}
}

// decimalValue represents a decimal number with arbitrary precision
type decimalValue struct {
	value *big.Int
	scale int32
}

// compareValues updated to handle decimal comparisons:
func compareValues(val1, val2 interface{}, tolerance float64) bool {
	switch v1 := val1.(type) {
	case int64:
		v2, ok := val2.(int64)
		if !ok {
			return false
		}
		return v1 == v2
	case int32:
		v2, ok := val2.(int32)
		if !ok {
			return false
		}
		return v1 == v2
	case float64:
		v2, ok := val2.(float64)
		if !ok {
			return false
		}
		return math.Abs(v1-v2) <= tolerance
	case float32:
		v2, ok := val2.(float32)
		if !ok {
			return false
		}
		return math.Abs(float64(v1)-float64(v2)) <= tolerance
	case string:
		v2, ok := val2.(string)
		if !ok {
			return false
		}
		return v1 == v2
	case *decimalValue:
		v2, ok := val2.(*decimalValue)
		if !ok {
			return false
		}
		// Compare with matching scales
		if v1.scale == v2.scale {
			return v1.value.Cmp(v2.value) == 0
		}
		// Adjust scales if needed for comparison
		return adjustAndCompareDecimals(v1, v2)
	default:
		return val1 == val2
	}
}

// adjustAndCompareDecimals compares two decimal values by adjusting their scales
func adjustAndCompareDecimals(d1, d2 *decimalValue) bool {
	if d1.scale == d2.scale {
		return d1.value.Cmp(d2.value) == 0
	}

	// Create copies to avoid modifying originals
	v1 := new(big.Int).Set(d1.value)
	v2 := new(big.Int).Set(d2.value)

	// Scale up the number with lower scale
	if d1.scale < d2.scale {
		scale := d2.scale - d1.scale
		v1.Mul(v1, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	} else {
		scale := d1.scale - d2.scale
		v2.Mul(v2, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	}

	return v1.Cmp(v2) == 0
}

// ValidateValues performs full or sampled row‑level validation for specified columns.
func (v *Validator) ValidateValues(ctx context.Context, tableName string, columns []string, mode metrics.ValueValidationMode, samplePercentage float64) (metrics.ValueResult, error) {
	log := logger.GetLogger()
	log.Info("Starting value-level validation", zap.String("table", tableName), zap.String("mode", string(mode)))

	srcConn, err := v.integration.Source1().OpenConnection()
	if err != nil {
		log.Error("Failed to open source connection", zap.Error(err))
		return metrics.ValueResult{}, err
	}
	defer srcConn.Close()

	dstConn, err := v.integration.Source2().OpenConnection()
	if err != nil {
		log.Error("Failed to open destination connection", zap.Error(err))
		return metrics.ValueResult{}, err
	}
	defer dstConn.Close()

	var query string
	if mode == metrics.Full {
		query = fmt.Sprintf("SELECT * FROM %s", tableName)
	} else {
		// For sampling, use a LIMIT calculated from the row count.
		totalRows, err := v.getRowCount(ctx, srcConn, tableName)
		if err != nil {
			log.Error("Failed to get row count for sampling", zap.Error(err))
			return metrics.ValueResult{}, err
		}
		sampleRows := int64(float64(totalRows) * (samplePercentage / 100.0))
		if sampleRows == 0 {
			sampleRows = 1
		}
		query = fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, sampleRows)
	}

	srcRR, err := srcConn.Query(ctx, query)
	if err != nil {
		log.Error("Failed to query source for values", zap.Error(err))
		return metrics.ValueResult{}, err
	}
	defer srcRR.Release()

	dstRR, err := dstConn.Query(ctx, query)
	if err != nil {
		log.Error("Failed to query destination for values", zap.Error(err))
		return metrics.ValueResult{}, err
	}
	defer dstRR.Release()

	sampledRows := int64(0)
	mismatchedRows := int64(0)
	mismatches := make(map[string]interface{})
	rowIndex := 0

	// Process each batch from both record readers.
	for srcRR.Next() && dstRR.Next() {
		srcRec := srcRR.Record()
		dstRec := dstRR.Record()

		numRows := int(srcRec.NumRows())
		for r := 0; r < numRows; r++ {
			sampledRows++
			for _, colName := range columns {
				srcColIdx := findColumnIndex(srcRec.Schema(), colName)
				dstColIdx := findColumnIndex(dstRec.Schema(), colName)
				if srcColIdx < 0 || dstColIdx < 0 {
					continue
				}
				srcVal, err := getValueFromArray(srcRec.Column(srcColIdx), r)
				if err != nil {
					log.Error("Failed to extract value from source", zap.String("column", colName), zap.Error(err))
					continue
				}
				dstVal, err := getValueFromArray(dstRec.Column(dstColIdx), r)
				if err != nil {
					log.Error("Failed to extract value from destination", zap.String("column", colName), zap.Error(err))
					continue
				}
				if !compareValues(srcVal, dstVal, v.thresholds.NumericDifferenceTolerance) {
					mismatchedRows++
					key := fmt.Sprintf("row_%d_column_%s", rowIndex+r, colName)
					mismatches[key] = map[string]interface{}{
						"source":      srcVal,
						"destination": dstVal,
					}
				}
			}
		}
		rowIndex += numRows
	}

	status := (mismatchedRows == 0)
	if status {
		log.Info("Value-level validation passed", zap.Int64("sampledRows", sampledRows))
	} else {
		log.Warn("Value-level validation found mismatches", zap.Int64("mismatchedRows", mismatchedRows))
	}

	return metrics.ValueResult{
		Mode:               mode,
		SamplingPercentage: samplePercentage,
		SampledRows:        sampledRows,
		MismatchedRows:     mismatchedRows,
		ColumnsCompared:    columns,
		MismatchedData:     mismatches,
		Status:             status,
	}, nil
}

// findColumnIndex returns the index of the given column in the Arrow schema.
func findColumnIndex(schema *arrow.Schema, columnName string) int {
	for i, field := range schema.Fields() {
		if field.Name == columnName {
			return i
		}
	}
	return -1
}

// getValueFromArray extracts the value at the specified row from an Arrow array.
func getValueFromArray(arr arrow.Array, row int) (interface{}, error) {
	switch arr.DataType().ID() {
	case arrow.INT64:
		a := arr.(*array.Int64)
		return a.Value(row), nil
	case arrow.INT32:
		a := arr.(*array.Int32)
		return a.Value(row), nil
	case arrow.FLOAT64:
		a := arr.(*array.Float64)
		return a.Value(row), nil
	case arrow.FLOAT32:
		a := arr.(*array.Float32)
		return a.Value(row), nil
	case arrow.STRING:
		a := arr.(*array.String)
		return a.Value(row), nil
	default:
		return nil, fmt.Errorf("unsupported data type for value extraction: %s", arr.DataType().Name())
	}
}

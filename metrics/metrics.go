package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// -----------------------------
// Domain Types & Metadata
// -----------------------------

// ObjectType distinguishes between a table and a view.
type ObjectType string

const (
	Table ObjectType = "table"
	View  ObjectType = "view"
)

// DatabaseMetadata captures high-level context for a validation run.
type DatabaseMetadata struct {
	SourceDBName         string               `json:"source_db_name"`
	DestinationDBName    string               `json:"destination_db_name"`
	Engine               string               `json:"engine"`
	Version              string               `json:"version"`
	SchemaName           string               `json:"schema_name"`
	StartTime            time.Time            `json:"start_time"`
	EndTime              time.Time            `json:"end_time"`
	Duration             time.Duration        `json:"duration"`
	ConnectionString     string               `json:"connection_string"`
	ValidationThresholds ValidationThresholds `json:"validation_thresholds"`
	TotalPartitions      int                  `json:"total_partitions"`
	TotalShards          int                  `json:"total_shards"`
}

// TableMetadata holds details about the object being validated.
type TableMetadata struct {
	ObjectType        ObjectType        `json:"object_type"`
	Name              string            `json:"name"`
	PrimaryKeys       []string          `json:"primary_keys"`
	NumColumns        int               `json:"num_columns"`
	ColumnDataTypes   map[string]string `json:"column_data_types"`
	PartitionStrategy string            `json:"partition_strategy"`
	ShardingStrategy  string            `json:"sharding_strategy"`
}

// -----------------------------
// Validation Result Types
// -----------------------------

// RowCountResult holds metrics for row count validation.
type RowCountResult struct {
	SourceCount      int64 `json:"source_count"`
	DestinationCount int64 `json:"destination_count"`
	Difference       int64 `json:"difference"`
	Status           bool  `json:"status"`
}

// AggregationType defines the aggregation functions.
type AggregationType string

const (
	Sum   AggregationType = "SUM"
	Avg   AggregationType = "AVG"
	Min   AggregationType = "MIN"
	Max   AggregationType = "MAX"
	Count AggregationType = "COUNT"
)

// AggregateResult captures aggregate metric comparisons.
type AggregateResult struct {
	ColumnName       string          `json:"column_name"`
	AggType          AggregationType `json:"agg_type"`
	SourceValue      float64         `json:"source_value"`
	DestinationValue float64         `json:"destination_value"`
	Difference       float64         `json:"difference"`
	Status           bool            `json:"status"`
}

// ValueValidationMode determines validation method.
type ValueValidationMode string

const (
	Sampling ValueValidationMode = "Sampling"
	Full     ValueValidationMode = "Full"
)

// ValueResult holds row-level data validation results.
type ValueResult struct {
	Mode               ValueValidationMode    `json:"mode"`
	SamplingPercentage float64                `json:"sampling_percentage"`
	SampledRows        int64                  `json:"sampled_rows"`
	MismatchedRows     int64                  `json:"mismatched_rows"`
	ColumnsCompared    []string               `json:"columns_compared"`
	MismatchedData     map[string]interface{} `json:"mismatched_data"`
	Status             bool                   `json:"status"`
}

// SchemaResult captures the results of schema validation.
type SchemaResult struct {
	Result                bool              `json:"result"`
	MissingInDestination  []string          `json:"missing_in_destination"`
	MissingInSource       []string          `json:"missing_in_source"`
	DataTypeMismatches    map[string]string `json:"data_type_mismatches"`
	PrimaryKeyDifferences []string          `json:"primary_key_differences"`
	Status                bool              `json:"status"`
}

// PartitionResult holds validation metrics for partitioned tables.
type PartitionResult struct {
	PartitionColumn      string                     `json:"partition_column"`
	RowCountDifferences  map[string]int64           `json:"row_count_differences"`
	AggregateDifferences map[string]AggregateResult `json:"aggregate_differences"`
	Status               bool                       `json:"status"`
}

// ShardResult holds validation metrics for sharded tables.
type ShardResult struct {
	ShardID              string                     `json:"shard_id"`
	RowCountDifference   int64                      `json:"row_count_difference"`
	AggregateDifferences map[string]AggregateResult `json:"aggregate_differences"`
	Status               bool                       `json:"status"`
}

// ValidationReport aggregates validation results.
type ValidationReport struct {
	DBMetadata       DatabaseMetadata  `json:"db_metadata"`
	TableMetadata    TableMetadata     `json:"table_metadata"`
	RowCountResult   RowCountResult    `json:"row_count_result"`
	AggregateResults []AggregateResult `json:"aggregate_results"`
	ValueResult      ValueResult       `json:"value_result"`
	SchemaResult     SchemaResult      `json:"schema_result"`
	PartitionMetrics []PartitionResult `json:"partition_metrics"`
	ShardMetrics     []ShardResult     `json:"shard_metrics"`
}

// -----------------------------
// Metrics Storage
// -----------------------------

// MetricsStore abstracts validation result storage.
type MetricsStore interface {
	Save(run ValidationReport) error
	SaveWithContext(ctx context.Context, run ValidationReport) error
}

// JSONMetricsStore stores results as JSON.
type JSONMetricsStore struct {
	FilePath string
}

func (j *JSONMetricsStore) Save(run ValidationReport) error {
	data, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return err
	}
	if j.FilePath != "" {
		return os.WriteFile(j.FilePath, data, 0644)
	}
	fmt.Println(string(data))
	return nil
}

func (j *JSONMetricsStore) SaveWithContext(ctx context.Context, run ValidationReport) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return j.Save(run)
	}
}

// -----------------------------
// Validation & Metrics
// -----------------------------

// ValidationThresholds defines numeric tolerances.
type ValidationThresholds struct {
	NumericDifferenceTolerance float64 `json:"numeric_difference_tolerance"`
	RowCountTolerance          float64 `json:"row_count_tolerance"`
	SamplingConfidenceLevel    float64 `json:"sampling_confidence_level"`
}

// ValidationStatus holds validation status details.
type ValidationStatus struct {
	Passed    bool      `json:"passed"`
	ErrorCode string    `json:"error_code,omitempty"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// -----------------------------
// Error Handling
// -----------------------------

type ValidationError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("Validation Error [%s]: %s", e.Code, e.Message)
}

// -----------------------------
// Report Generation
// -----------------------------

type ReportGenerator interface {
	GenerateValidationReport(run ValidationReport) ([]byte, error)
	GenerateAlertNotification(run ValidationReport) ([]byte, error)
}

// JSONReportGenerator implementation.
type JSONReportGenerator struct{}

func (j *JSONReportGenerator) GenerateValidationReport(run ValidationReport) ([]byte, error) {
	return json.MarshalIndent(run, "", "  ")
}

// Stub for Prometheus integration.
type PrometheusMetricsCollector struct{}

func (p *PrometheusMetricsCollector) RecordValidationStart(dbMeta DatabaseMetadata) {}
func (p *PrometheusMetricsCollector) RecordValidationEnd(duration time.Duration)    {}
func (p *PrometheusMetricsCollector) RecordRowCountDifference(diff int64)           {}

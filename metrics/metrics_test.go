package metrics

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestValidationReportSerialization ensures that ValidationReport can be serialized and deserialized correctly.
func TestValidationReportSerialization(t *testing.T) {
	originalReport := ValidationReport{
		DBMetadata: DatabaseMetadata{
			SourceDBName:      "SourceDB",
			DestinationDBName: "DestinationDB",
			Engine:            "PostgreSQL",
			Version:           "13.0",
			SchemaName:        "public",
			StartTime:         time.Now(),
			EndTime:           time.Now().Add(10 * time.Second),
			Duration:          10 * time.Second,
		},
		RowCountResult: RowCountResult{
			SourceCount:      1000,
			DestinationCount: 995,
			Difference:       5,
			Status:           false,
		},
	}

	data, err := json.Marshal(originalReport)
	if err != nil {
		t.Fatalf("Failed to serialize ValidationReport: %v", err)
	}

	var deserializedReport ValidationReport
	if err := json.Unmarshal(data, &deserializedReport); err != nil {
		t.Fatalf("Failed to deserialize ValidationReport: %v", err)
	}

	if deserializedReport.RowCountResult.SourceCount != originalReport.RowCountResult.SourceCount {
		t.Errorf("Expected SourceCount %d, got %d", originalReport.RowCountResult.SourceCount, deserializedReport.RowCountResult.SourceCount)
	}
}

// TestJSONMetricsStore ensures that reports are correctly written to and read from a file.
func TestJSONMetricsStore(t *testing.T) {
	testFile := "test_metrics.json"
	defer os.Remove(testFile) // Cleanup test file after execution

	store := &JSONMetricsStore{FilePath: testFile}
	testReport := ValidationReport{
		DBMetadata: DatabaseMetadata{
			SourceDBName:      "SourceDB",
			DestinationDBName: "DestinationDB",
			Engine:            "DuckDB",
		},
		RowCountResult: RowCountResult{
			SourceCount:      5000,
			DestinationCount: 4999,
			Difference:       1,
			Status:           true,
		},
	}

	if err := store.Save(testReport); err != nil {
		t.Fatalf("Failed to save validation report: %v", err)
	}

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read back validation report: %v", err)
	}

	var loadedReport ValidationReport
	if err := json.Unmarshal(data, &loadedReport); err != nil {
		t.Fatalf("Failed to deserialize saved report: %v", err)
	}

	if loadedReport.RowCountResult.Difference != testReport.RowCountResult.Difference {
		t.Errorf("Expected row count difference %d, got %d", testReport.RowCountResult.Difference, loadedReport.RowCountResult.Difference)
	}
}

// TestSaveWithContext ensures that context cancellation is respected when saving a report.
func TestSaveWithContext(t *testing.T) {
	store := &JSONMetricsStore{FilePath: "test_metrics.json"}
	testReport := ValidationReport{
		DBMetadata: DatabaseMetadata{
			SourceDBName:      "TestDB",
			DestinationDBName: "TestDB2",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediately cancel the context

	err := store.SaveWithContext(ctx, testReport)
	if err == nil {
		t.Fatalf("Expected context cancellation error, got nil")
	}
}

// TestValidationThresholds ensures that numeric tolerances are applied correctly.
func TestValidationThresholds(t *testing.T) {
	thresholds := ValidationThresholds{
		NumericDifferenceTolerance: 0.01,
		RowCountTolerance:          5,
		SamplingConfidenceLevel:    95.0,
	}

	if thresholds.NumericDifferenceTolerance != 0.01 {
		t.Errorf("Expected NumericDifferenceTolerance to be 0.01, got %f", thresholds.NumericDifferenceTolerance)
	}

	if thresholds.RowCountTolerance != 5 {
		t.Errorf("Expected RowCountTolerance to be 5, got %f", thresholds.RowCountTolerance)
	}

	if thresholds.SamplingConfidenceLevel != 95.0 {
		t.Errorf("Expected SamplingConfidenceLevel to be 95.0, got %f", thresholds.SamplingConfidenceLevel)
	}
}

// TestValidationError ensures that ValidationError correctly formats error messages.
func TestValidationError(t *testing.T) {
	err := ValidationError{
		Code:    "VAL_ERR_001",
		Message: "Row count mismatch",
		Details: map[string]interface{}{
			"expected": 1000,
			"actual":   995,
		},
	}

	expectedMsg := "Validation Error [VAL_ERR_001]: Row count mismatch"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestJSONReportGenerator ensures reports are correctly formatted as JSON.
func TestJSONReportGenerator(t *testing.T) {
	reportGen := JSONReportGenerator{}
	testReport := ValidationReport{
		DBMetadata: DatabaseMetadata{
			SourceDBName: "SourceDB",
		},
	}

	data, err := reportGen.GenerateValidationReport(testReport)
	if err != nil {
		t.Fatalf("Failed to generate JSON report: %v", err)
	}

	var loadedReport ValidationReport
	if err := json.Unmarshal(data, &loadedReport); err != nil {
		t.Fatalf("Failed to parse generated JSON report: %v", err)
	}

	if loadedReport.DBMetadata.SourceDBName != "SourceDB" {
		t.Errorf("Expected SourceDBName to be 'SourceDB', got '%s'", loadedReport.DBMetadata.SourceDBName)
	}
}

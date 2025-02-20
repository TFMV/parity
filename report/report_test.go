package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/TFMV/parity/metrics"
)

func TestJSONReportGenerator_GenerateValidationReport(t *testing.T) {
	// Create test report data
	report := createTestReport()
	generator := &JSONReportGenerator{}

	// Generate report
	data, err := generator.GenerateValidationReport(report)
	if err != nil {
		t.Fatalf("Failed to generate report: %v", err)
	}

	// Verify JSON is valid
	var decoded metrics.ValidationReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	// Verify content
	if decoded.DBMetadata.SourceDBName != "TestDB" {
		t.Errorf("Expected source DB name 'TestDB', got %s", decoded.DBMetadata.SourceDBName)
	}
}

func TestJSONReportGenerator_SaveReportToFile(t *testing.T) {
	report := createTestReport()
	generator := &JSONReportGenerator{}

	// Create temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_report.json")

	// Save report
	if err := generator.SaveReportToFile(report, filePath); err != nil {
		t.Fatalf("Failed to save report: %v", err)
	}

	// Verify file exists and contains valid JSON
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	var decoded metrics.ValidationReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Saved file contains invalid JSON: %v", err)
	}
}

func TestHTMLReportGenerator_GenerateValidationReport(t *testing.T) {
	report := createTestReport()
	generator := &HTMLReportGenerator{}

	// Generate HTML report
	data, err := generator.GenerateValidationReport(report)
	if err != nil {
		t.Fatalf("Failed to generate HTML report: %v", err)
	}

	// Basic validation of HTML content
	html := string(data)
	expectedElements := []string{
		"<!DOCTYPE html>",
		"<title>Parity Validation Report</title>",
		"TestDB", // Source database name
		"PASS",   // Status indicator
	}

	for _, expected := range expectedElements {
		if !contains(html, expected) {
			t.Errorf("HTML report missing expected content: %s", expected)
		}
	}
}

func TestSaveReports(t *testing.T) {
	report := createTestReport()
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "report.json")
	htmlPath := filepath.Join(tmpDir, "report.html")

	// Save both reports
	err := SaveReports(report, jsonPath, htmlPath)
	if err != nil {
		t.Fatalf("Failed to save reports: %v", err)
	}

	// Verify both files exist
	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		t.Error("JSON report file was not created")
	}
	if _, err := os.Stat(htmlPath); os.IsNotExist(err) {
		t.Error("HTML report file was not created")
	}
}

func TestReportFromFilePath(t *testing.T) {
	report := createTestReport()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_report.json")

	// Save report first
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal report: %v", err)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Load report
	loaded, err := ReportFromFilePath(filePath)
	if err != nil {
		t.Fatalf("Failed to load report: %v", err)
	}

	// Verify content
	if loaded.DBMetadata.SourceDBName != report.DBMetadata.SourceDBName {
		t.Errorf("Loaded report data mismatch: expected %s, got %s",
			report.DBMetadata.SourceDBName, loaded.DBMetadata.SourceDBName)
	}
}

// Helper function to create test report data
func createTestReport() metrics.ValidationReport {
	now := time.Now()
	return metrics.ValidationReport{
		DBMetadata: metrics.DatabaseMetadata{
			SourceDBName:      "TestDB",
			DestinationDBName: "TestDestDB",
			StartTime:         now,
			EndTime:           now.Add(time.Second),
		},
		RowCountResult: metrics.RowCountResult{
			SourceCount:      100,
			DestinationCount: 100,
			Difference:       0,
			Status:           true,
		},
		SchemaResult: metrics.SchemaResult{
			Status: true,
		},
		ValueResult: metrics.ValueResult{
			Mode:            metrics.Full,
			SampledRows:     100,
			MismatchedRows:  0,
			ColumnsCompared: []string{"id", "name"},
			Status:          true,
		},
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

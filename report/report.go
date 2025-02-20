package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"time"

	"github.com/TFMV/parity/metrics"
)

// -----------------------------
// Report Generator Interfaces
// -----------------------------

// ReportGenerator defines the methods for generating reports.
type ReportGenerator interface {
	GenerateValidationReport(run metrics.ValidationReport) ([]byte, error)
	GenerateAlertNotification(run metrics.ValidationReport) ([]byte, error)
	SaveReportToFile(run metrics.ValidationReport, filePath string) error
	ReportFromFilePath(filePath string) (metrics.ValidationReport, error)
}

// -----------------------------
// JSON Report Generator
// -----------------------------

// JSONReportGenerator generates JSON reports.
type JSONReportGenerator struct{}

// GenerateValidationReport serializes the ValidationReport to JSON.
func (j *JSONReportGenerator) GenerateValidationReport(run metrics.ValidationReport) ([]byte, error) {
	return json.MarshalIndent(run, "", "  ")
}

// GenerateAlertNotification generates an alert message in JSON format.
func (j *JSONReportGenerator) GenerateAlertNotification(run metrics.ValidationReport) ([]byte, error) {
	alert := map[string]interface{}{
		"alert":     "Validation Failed",
		"database":  run.DBMetadata.SourceDBName,
		"message":   "Discrepancies detected in validation.",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	return json.MarshalIndent(alert, "", "  ")
}

// SaveReportToFile saves the JSON report to a file.
func (j *JSONReportGenerator) SaveReportToFile(run metrics.ValidationReport, filePath string) error {
	data, err := j.GenerateValidationReport(run)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// ReportFromFilePath loads and generates a report from a file
func (j *JSONReportGenerator) ReportFromFilePath(path string) (metrics.ValidationReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return metrics.ValidationReport{}, err
	}

	var report metrics.ValidationReport
	if err := json.Unmarshal(data, &report); err != nil {
		return metrics.ValidationReport{}, err
	}
	return report, nil
}

// -----------------------------
// HTML Report Generator
// -----------------------------

// HTMLReportGenerator generates HTML reports.
type HTMLReportGenerator struct{}

// HTML template for the report.
const htmlTemplate = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Validation Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f4f4f4; }
        .status-pass { color: green; }
        .status-fail { color: red; }
    </style>
</head>
<body>
    <h1>Validation Report</h1>
    <p><strong>Source Database:</strong> {{.DBMetadata.SourceDBName}}</p>
    <p><strong>Destination Database:</strong> {{.DBMetadata.DestinationDBName}}</p>
    <p><strong>Schema:</strong> {{.DBMetadata.SchemaName}}</p>
    <p><strong>Validation Date:</strong> {{.DBMetadata.StartTime}}</p>
    
    <h2>Row Count Validation</h2>
    <table>
        <tr>
            <th>Source Count</th>
            <th>Destination Count</th>
            <th>Difference</th>
            <th>Status</th>
        </tr>
        <tr>
            <td>{{.RowCountResult.SourceCount}}</td>
            <td>{{.RowCountResult.DestinationCount}}</td>
            <td>{{.RowCountResult.Difference}}</td>
            <td class="{{if .RowCountResult.Status}}status-pass{{else}}status-fail{{end}}">
                {{if .RowCountResult.Status}}PASS{{else}}FAIL{{end}}
            </td>
        </tr>
    </table>

    <h2>Schema Validation</h2>
    <p><strong>Status:</strong> {{if .SchemaResult.Status}}<span class="status-pass">PASS</span>{{else}}<span class="status-fail">FAIL</span>{{end}}</p>
    <h3>Missing in Destination:</h3>
    <ul>
        {{range .SchemaResult.MissingInDestination}}<li>{{.}}</li>{{else}}<li>None</li>{{end}}
    </ul>
    <h3>Missing in Source:</h3>
    <ul>
        {{range .SchemaResult.MissingInSource}}<li>{{.}}</li>{{else}}<li>None</li>{{end}}
    </ul>

    <h2>Aggregate Validation</h2>
    <table>
        <tr>
            <th>Column</th>
            <th>Aggregation Type</th>
            <th>Source Value</th>
            <th>Destination Value</th>
            <th>Difference</th>
            <th>Status</th>
        </tr>
        {{range .AggregateResults}}
        <tr>
            <td>{{.ColumnName}}</td>
            <td>{{.AggType}}</td>
            <td>{{.SourceValue}}</td>
            <td>{{.DestinationValue}}</td>
            <td>{{.Difference}}</td>
            <td class="{{if .Status}}status-pass{{else}}status-fail{{end}}">
                {{if .Status}}PASS{{else}}FAIL{{end}}
            </td>
        </tr>
        {{end}}
    </table>

    <h2>Partition & Shard Validation</h2>
    <p><strong>Total Partitions:</strong> {{.DBMetadata.TotalPartitions}}</p>
    <p><strong>Total Shards:</strong> {{.DBMetadata.TotalShards}}</p>

    <footer>
        <p>Generated on {{.DBMetadata.EndTime}}</p>
    </footer>
</body>
</html>
`

// GenerateValidationReport generates an HTML report from the validation run.
func (h *HTMLReportGenerator) GenerateValidationReport(run metrics.ValidationReport) ([]byte, error) {
	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, run)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GenerateAlertNotification generates an HTML alert.
func (h *HTMLReportGenerator) GenerateAlertNotification(run metrics.ValidationReport) ([]byte, error) {
	alertHTML := fmt.Sprintf(
		`<html><body><h3>Validation Failed</h3><p>Discrepancies detected in validation for database %s.</p></body></html>`,
		run.DBMetadata.SourceDBName,
	)
	return []byte(alertHTML), nil
}

// SaveReportToFile saves the HTML report to a file.
func (h *HTMLReportGenerator) SaveReportToFile(run metrics.ValidationReport, filePath string) error {
	data, err := h.GenerateValidationReport(run)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// SaveReports saves both JSON and HTML reports.
func SaveReports(run metrics.ValidationReport, jsonPath, htmlPath string) error {
	jsonGen := JSONReportGenerator{}
	htmlGen := HTMLReportGenerator{}

	// Save JSON report
	if err := jsonGen.SaveReportToFile(run, jsonPath); err != nil {
		return err
	}

	// Save HTML report
	if err := htmlGen.SaveReportToFile(run, htmlPath); err != nil {
		return err
	}

	return nil
}

func ReportFromFilePath(filePath string) (metrics.ValidationReport, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return metrics.ValidationReport{}, err
	}
	var report metrics.ValidationReport
	if err := json.Unmarshal(data, &report); err != nil {
		return metrics.ValidationReport{}, err
	}
	return report, nil
}

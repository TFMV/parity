# Parity

[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/parity)](https://goreportcard.com/report/github.com/TFMV/parity)

Parity is a high-performance data validation framework designed to ensure data consistency across different databases. It leverages Apache Arrow for in-memory data processing and supports PostgreSQL, DuckDB, and other databases.

## Features

| Feature | Description |
|---------|-------------|
| Schema Validation | Compare column structures between databases. |
| Row Count Validation | Ensure record counts match between source and destination. |
| Aggregate Validation | Compare SUM, AVG, MIN, MAX, and COUNT values. |
| Value-Level Validation | Perform full or sampled row comparisons. |
| Partition & Shard Validation | Validate partitioned and sharded datasets. |
| Parallelized Execution | Runs validations concurrently for maximum efficiency. |
| Structured Logging | Uses Zap for high-performance logging. |
| Extensible Reporting | Generates JSON and HTML validation reports. |

## Installation

```sh
git clone https://github.com/TFMV/parity.git
cd parity
go mod tidy
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "github.com/TFMV/parity/integrations"
    "github.com/TFMV/parity/logger"
    "github.com/TFMV/parity/metrics"
    "github.com/TFMV/parity/report"
    "github.com/TFMV/parity/validation"
)

func main() {
    logger.InitLogger()

    // Define integration
    integration := integrations.NewDatabasePair(sourceDB, destinationDB)

    // Define validation thresholds
    thresholds := metrics.ValidationThresholds{
        NumericDifferenceTolerance: 0.01,
        RowCountTolerance: 5,
        SamplingConfidenceLevel: 95.0,
    }

    // Create validator
    validator := validation.NewValidator(integration, thresholds, &report.JSONReportGenerator{})

    // Run full validation
    ctx := context.Background()
    report, err := validator.ValidateAll(ctx, "users", []string{"id", "amount"}, metrics.Full, 100.0)
    if err != nil {
        fmt.Println("Validation failed:", err)
    } else {
        fmt.Println("Validation successful:", report)
    }
}
```

## Supported Databases

- PostgreSQL
- DuckDB
- (Planned: BigQuery, Snowflake, MySQL, MongoDB)

## Validation Types

| Type | Description |
|------|-------------|
| Schema Validation | Checks for column mismatches, missing fields, and type differences. |
| Row Count Validation | Ensures the total number of rows match. |
| Aggregate Validation | Compares column-level aggregates like SUM, AVG, MIN, MAX. |
| Value-Level Validation | Performs row-wise data comparison (full or sampled). |
| Partition Validation | Validates partitioned tables individually. |
| Shard Validation | Ensures correct distribution of sharded data. |

## Reports & Logging

- Generates structured JSON and HTML reports.
- Logs all validation events using Zap.

## Example Report

```json
{
    "db_metadata": {
        "source_db_name": "SourceDB",
        "destination_db_name": "DestinationDB",
        "engine": "Apache Arrow ADBC",
        "version": "1.0",
        "schema_name": "",
        "start_time": "2024-02-20T10:00:00Z",
        "end_time": "2024-02-20T10:00:01Z",
        "duration": 1000000000,
        "validation_thresholds": {
            "numeric_difference_tolerance": 0.001,
            "row_count_tolerance": 0,
            "sampling_confidence_level": 95.0
        }
    },
    "row_count_result": {
        "source_count": 1000,
        "destination_count": 1000,
        "difference": 0,
        "status": true
    },
    "aggregate_results": [
        {
            "column_name": "amount",
            "agg_type": "SUM",
            "source_value": 50000.00,
            "destination_value": 50000.00,
            "difference": 0,
            "status": true
        }
    ],
    "schema_result": {
        "result": true,
        "missing_in_destination": null,
        "missing_in_source": null,
        "data_type_mismatches": {},
        "status": true
    },
    "value_result": {
        "mode": "Full",
        "sampling_percentage": 100,
        "sampled_rows": 1000,
        "mismatched_rows": 0,
        "columns_compared": ["id", "amount"],
        "mismatched_data": {},
        "status": true
    }
}
```

The validation report includes detailed metadata about the databases being compared, results from row count validation, aggregate comparisons, schema validation, and value-level validation. Reports can be generated in both JSON and HTML formats.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

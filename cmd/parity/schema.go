package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/TFMV/parity/pkg/schema"
	"github.com/spf13/cobra"
)

// SchemaOptions represents the options for the schema command.
type SchemaOptions struct {
	// Input file options
	SourcePath string
	SourceType string
	TargetPath string
	TargetType string

	// Validation options
	ValidationLevel      string
	RequiredFields       []string
	NonNullableFields    []string
	RequireDictEncoding  bool
	RequireUTCTimestamps bool
	TimestampFields      []string
	DateFields           []string

	// Output options
	OutputFormat string
}

// newSchemaCommand creates a new schema command.
func newSchemaCommand() *cobra.Command {
	options := &SchemaOptions{
		SourceType:          "auto",
		TargetType:          "auto",
		ValidationLevel:     "compatible",
		RequireDictEncoding: false,
		OutputFormat:        "text",
	}

	cmd := &cobra.Command{
		Use:   "schema [flags] <source-path> [target-path]",
		Short: "Validate Apache Arrow schemas",
		Long: `Validate and compare Apache Arrow schemas.

This command provides schema validation capabilities for datasets. It supports:
- Single file validation against predefined rules
- Comparison between two schemas (source and target)
- Multiple validation levels (strict, compatible, relaxed)
- Customizable validation rules for field types, nullability, and encoding`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Set source path from args
			options.SourcePath = args[0]

			// Set target path if provided
			if len(args) > 1 {
				options.TargetPath = args[1]
			}

			return runSchemaValidation(options)
		},
	}

	// Add flags
	// Input file options
	cmd.Flags().StringVar(&options.SourceType, "source-type", options.SourceType, "Source file type (auto, arrow, parquet, csv)")
	cmd.Flags().StringVar(&options.TargetType, "target-type", options.TargetType, "Target file type (auto, arrow, parquet, csv)")

	// Validation options
	cmd.Flags().StringVar(&options.ValidationLevel, "level", options.ValidationLevel, "Validation level (strict, compatible, relaxed)")
	cmd.Flags().StringSliceVar(&options.RequiredFields, "required", []string{}, "Fields that must be present in the schema")
	cmd.Flags().StringSliceVar(&options.NonNullableFields, "non-nullable", []string{}, "Fields that must not be nullable")
	cmd.Flags().BoolVar(&options.RequireDictEncoding, "require-dict-encoding", options.RequireDictEncoding, "Require dictionary encoding for string fields")
	cmd.Flags().BoolVar(&options.RequireUTCTimestamps, "require-utc", options.RequireUTCTimestamps, "Require UTC timezone for timestamp fields")
	cmd.Flags().StringSliceVar(&options.TimestampFields, "timestamp-fields", []string{}, "Fields that should be timestamps")
	cmd.Flags().StringSliceVar(&options.DateFields, "date-fields", []string{}, "Fields that should be date32 type")

	// Output options
	cmd.Flags().StringVarP(&options.OutputFormat, "format", "f", options.OutputFormat, "Output format (text, json)")

	return cmd
}

// runSchemaValidation executes the schema validation with the given options.
func runSchemaValidation(options *SchemaOptions) error {
	// Create context
	ctx := context.Background()

	// Determine source type if auto
	sourceType := options.SourceType
	if sourceType == "auto" {
		ext := strings.ToLower(filepath.Ext(options.SourcePath))
		switch ext {
		case ".parquet":
			sourceType = "parquet"
		case ".arrow":
			sourceType = "arrow"
		case ".csv":
			sourceType = "csv"
		default:
			return fmt.Errorf("could not determine source type from extension: %s", ext)
		}
	}

	// Create source reader
	sourceReader, err := createReader(ctx, options.SourcePath, sourceType)
	if err != nil {
		return fmt.Errorf("failed to create source reader: %w", err)
	}
	defer sourceReader.Close()

	// Create validator
	validator := createValidator(options)

	// If target path is provided, compare schemas
	if options.TargetPath != "" {
		// Determine target type if auto
		targetType := options.TargetType
		if targetType == "auto" {
			ext := strings.ToLower(filepath.Ext(options.TargetPath))
			switch ext {
			case ".parquet":
				targetType = "parquet"
			case ".arrow":
				targetType = "arrow"
			case ".csv":
				targetType = "csv"
			default:
				return fmt.Errorf("could not determine target type from extension: %s", ext)
			}
		}

		// Create target reader
		targetReader, err := createReader(ctx, options.TargetPath, targetType)
		if err != nil {
			return fmt.Errorf("failed to create target reader: %w", err)
		}
		defer targetReader.Close()

		// For CSV reader, we need to read a batch to populate the schema
		if targetType == "csv" {
			_, err := targetReader.Read(ctx)
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed to read first batch from target: %w", err)
			}
		}

		// Compare schemas
		result := validator.ValidateAgainstTarget(sourceReader.Schema(), targetReader.Schema())

		// Output comparison
		if options.OutputFormat == "json" {
			// JSON output would be implemented here
			// Using simple text output for now
			fmt.Println(schema.PrintValidationResult(result))
		} else {
			// Print comparison
			fmt.Println(schema.CompareSchemas(sourceReader.Schema(), targetReader.Schema()))
			fmt.Println(schema.PrintValidationResult(result))
		}
	} else {
		// Validate single schema
		result := validator.ValidateSchema(sourceReader.Schema())

		// Output validation result
		if options.OutputFormat == "json" {
			// JSON output would be implemented here
			// Using simple text output for now
			fmt.Println(schema.PrintValidationResult(result))
		} else {
			// Print schema
			fmt.Println(schema.SchemaToString(sourceReader.Schema()))
			fmt.Println(schema.PrintValidationResult(result))
		}
	}

	return nil
}

// createReader creates a dataset reader based on the file type.
func createReader(ctx context.Context, path, fileType string) (core.DatasetReader, error) {
	config := core.ReaderConfig{
		Path: path,
		Type: fileType,
	}

	// Create reader
	var reader core.DatasetReader
	var err error

	switch fileType {
	case "parquet":
		reader, err = readers.NewParquetReader(config)
	case "arrow":
		reader, err = readers.NewArrowReader(config)
	case "csv":
		reader, err = readers.NewCSVReader(config)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", fileType)
	}

	if err != nil {
		return nil, err
	}

	return reader, nil
}

// createValidator creates a schema validator with the specified options.
func createValidator(options *SchemaOptions) schema.SchemaValidator {
	var validator *schema.ArrowSchemaValidator

	// Create validator based on validation level
	switch options.ValidationLevel {
	case "strict":
		validator = schema.NewStrictValidator()
	case "relaxed":
		validator = schema.NewRelaxedValidator()
	default: // "compatible" is default
		validator = schema.NewCompatibleValidator()
	}

	// Add required fields rule if specified
	if len(options.RequiredFields) > 0 {
		validator.AddRule(&schema.RequiredFieldsRule{
			RequiredFields: options.RequiredFields,
		})
	}

	// Add non-nullable fields rule if specified
	if len(options.NonNullableFields) > 0 {
		validator.AddRule(&schema.NullabilityRule{
			NonNullableFields: options.NonNullableFields,
		})
	}

	// Add dictionary encoding rule if specified
	if options.RequireDictEncoding {
		validator.AddRule(&schema.DictionaryEncodingRule{
			StringFieldsRequireDictionary: true,
		})
	}

	// Add temporal format rule if specified
	if options.RequireUTCTimestamps || len(options.TimestampFields) > 0 || len(options.DateFields) > 0 {
		timezone := ""
		if options.RequireUTCTimestamps {
			timezone = "UTC"
		}

		validator.AddRule(&schema.TemporalFormatRule{
			DateFields:       options.DateFields,
			TimestampFields:  options.TimestampFields,
			RequiredTimezone: timezone,
		})
	}

	return validator
}

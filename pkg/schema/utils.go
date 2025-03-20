package schema

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// NewValidatorFromSchemaFile creates a validator from a schema definition file.
// This is a convenience function for creating a validator with predefined rules.
func NewValidatorFromSchemaFile(schemaPath string) (*ArrowSchemaValidator, error) {
	// This is just a placeholder for now. In a real implementation, this would
	// parse a schema definition file (e.g., JSON/YAML) and create a validator with rules
	// based on the file's contents.
	return NewArrowSchemaValidator(), nil
}

// ValidateDataType validates that a data type is of a specific Arrow type.
// Returns true if the type is valid, false otherwise.
func ValidateDataType(dataType arrow.DataType, expectedType arrow.DataType) bool {
	return arrow.TypeEqual(dataType, expectedType)
}

// SchemaToString converts an Arrow schema to a human-readable string.
func SchemaToString(schema *arrow.Schema) string {
	var builder strings.Builder
	builder.WriteString("Schema:\n")

	// Add fields
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		nullabilityStr := "NOT NULL"
		if field.Nullable {
			nullabilityStr = "NULL"
		}
		builder.WriteString(fmt.Sprintf("  %s: %s %s\n", field.Name, field.Type, nullabilityStr))
	}

	// Add metadata if present
	metadata := schema.Metadata()
	if metadata.Len() > 0 {
		builder.WriteString("\nMetadata:\n")
		for i, key := range metadata.Keys() {
			value := metadata.Values()[i]
			builder.WriteString(fmt.Sprintf("  %s: %s\n", key, value))
		}
	}

	return builder.String()
}

// PrintValidationResult prints a validation result in a human-readable format.
func PrintValidationResult(result ValidationResult) string {
	var builder strings.Builder

	if result.Valid {
		builder.WriteString("Schema validation passed.\n")
	} else {
		builder.WriteString("Schema validation failed!\n")
	}

	// Print errors
	if len(result.Errors) > 0 {
		builder.WriteString("\nErrors:\n")
		for ruleName, errors := range result.Errors {
			builder.WriteString(fmt.Sprintf("  Rule '%s':\n", ruleName))
			for _, err := range errors {
				builder.WriteString(fmt.Sprintf("    - %s\n", err))
			}
		}
	}

	// Print warnings
	if len(result.Warnings) > 0 {
		builder.WriteString("\nWarnings:\n")
		for ruleName, warnings := range result.Warnings {
			builder.WriteString(fmt.Sprintf("  Rule '%s':\n", ruleName))
			for _, warning := range warnings {
				builder.WriteString(fmt.Sprintf("    - %s\n", warning))
			}
		}
	}

	return builder.String()
}

// CreateCommonValidator creates a validator with commonly used rules.
func CreateCommonValidator() *ArrowSchemaValidator {
	validator := NewArrowSchemaValidator()

	// Add common rules here
	// For example, ensure timestamps use UTC timezone
	validator.AddRule(&TemporalFormatRule{
		TimestampFields:  []string{}, // Empty by default
		RequiredTimezone: "UTC",
	})

	// Recommend dictionary encoding for string fields
	validator.AddRule(&DictionaryEncodingRule{
		StringFieldsRequireDictionary: true,
		ExemptFields:                  []string{}, // Empty by default
	})

	return validator
}

// ValidateReader validates the schema of a DatasetReader.
// This is a convenience function for validating a reader against a set of rules.
func ValidateReader(reader interface{}, validator SchemaValidator) (ValidationResult, error) {
	// Try to get the Schema method using reflection
	schemaGetter, ok := reader.(interface {
		Schema() *arrow.Schema
	})

	if !ok {
		return ValidationResult{
			Valid: false,
			Errors: map[string][]string{
				"System": {"Reader does not implement Schema() method"},
			},
		}, fmt.Errorf("reader does not implement Schema() method")
	}

	schema := schemaGetter.Schema()
	if schema == nil {
		return ValidationResult{
			Valid: false,
			Errors: map[string][]string{
				"System": {"Reader returned nil schema"},
			},
		}, fmt.Errorf("reader returned nil schema")
	}

	return validator.ValidateSchema(schema), nil
}

// CompareSchemas compares two schemas and returns a human-readable report of the differences.
func CompareSchemas(sourceSchema, targetSchema *arrow.Schema) string {
	var builder strings.Builder
	builder.WriteString("Schema Comparison:\n\n")

	// Compare field count
	if sourceSchema.NumFields() != targetSchema.NumFields() {
		builder.WriteString(fmt.Sprintf("Field count differs: source=%d, target=%d\n\n",
			sourceSchema.NumFields(), targetSchema.NumFields()))
	}

	// Build maps for faster lookup
	sourceFields := make(map[string]arrow.Field)
	for i := 0; i < sourceSchema.NumFields(); i++ {
		field := sourceSchema.Field(i)
		sourceFields[field.Name] = field
	}

	targetFields := make(map[string]arrow.Field)
	for i := 0; i < targetSchema.NumFields(); i++ {
		field := targetSchema.Field(i)
		targetFields[field.Name] = field
	}

	// Fields only in source
	var onlyInSource []string
	for name := range sourceFields {
		if _, exists := targetFields[name]; !exists {
			onlyInSource = append(onlyInSource, name)
		}
	}

	if len(onlyInSource) > 0 {
		builder.WriteString("Fields only in source schema:\n")
		for _, name := range onlyInSource {
			field := sourceFields[name]
			builder.WriteString(fmt.Sprintf("  %s: %s\n", name, field.Type))
		}
		builder.WriteString("\n")
	}

	// Fields only in target
	var onlyInTarget []string
	for name := range targetFields {
		if _, exists := sourceFields[name]; !exists {
			onlyInTarget = append(onlyInTarget, name)
		}
	}

	if len(onlyInTarget) > 0 {
		builder.WriteString("Fields only in target schema:\n")
		for _, name := range onlyInTarget {
			field := targetFields[name]
			builder.WriteString(fmt.Sprintf("  %s: %s\n", name, field.Type))
		}
		builder.WriteString("\n")
	}

	// Common fields with differences
	builder.WriteString("Common fields with differences:\n")
	hasDifferences := false

	for name, sourceField := range sourceFields {
		targetField, exists := targetFields[name]
		if !exists {
			continue
		}

		differences := []string{}

		// Check type
		if !arrow.TypeEqual(sourceField.Type, targetField.Type) {
			differences = append(differences, fmt.Sprintf("type: %s -> %s",
				sourceField.Type, targetField.Type))
		}

		// Check nullability
		if sourceField.Nullable != targetField.Nullable {
			differences = append(differences, fmt.Sprintf("nullability: %t -> %t",
				sourceField.Nullable, targetField.Nullable))
		}

		if len(differences) > 0 {
			hasDifferences = true
			builder.WriteString(fmt.Sprintf("  %s: %s\n", name, strings.Join(differences, ", ")))
		}
	}

	if !hasDifferences {
		builder.WriteString("  None\n")
	}

	return builder.String()
}

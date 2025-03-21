package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"gopkg.in/yaml.v3"
)

// SchemaConfig represents the configuration loaded from the schema file
type SchemaConfig struct {
	// ValidationLevel determines how strict the validation should be
	ValidationLevel string `json:"validation_level" yaml:"validation_level"`

	// RequiredFields lists field names that must be present
	RequiredFields []string `json:"required_fields" yaml:"required_fields"`

	// NonNullableFields lists fields that must not be nullable
	NonNullableFields []string `json:"non_nullable_fields" yaml:"non_nullable_fields"`

	// FieldTypes maps field names to allowed type definitions
	FieldTypes map[string][]string `json:"field_types" yaml:"field_types"`

	// TimestampFields lists fields that should be timestamps
	TimestampFields []string `json:"timestamp_fields" yaml:"timestamp_fields"`

	// RequiredTimezone specifies required timezone for timestamp fields
	RequiredTimezone string `json:"required_timezone" yaml:"required_timezone"`

	// StringFieldsRequireDictionary indicates if string fields should use dictionary encoding
	StringFieldsRequireDictionary bool `json:"string_fields_require_dictionary" yaml:"string_fields_require_dictionary"`

	// DictionaryExemptFields lists fields exempt from dictionary encoding requirement
	DictionaryExemptFields []string `json:"dictionary_exempt_fields" yaml:"dictionary_exempt_fields"`

	// DecimalPrecisions maps decimal field names to [precision, scale] requirements
	DecimalPrecisions map[string][2]int32 `json:"decimal_precisions" yaml:"decimal_precisions"`

	// RequiredMetadataKeys lists metadata keys that must be present
	RequiredMetadataKeys []string `json:"required_metadata_keys" yaml:"required_metadata_keys"`
}

// NewValidatorFromSchemaFile creates a validator from a schema definition file.
// This is a convenience function for creating a validator with predefined rules.
// The file can be either JSON or YAML format.
func NewValidatorFromSchemaFile(schemaPath string) (*ArrowSchemaValidator, error) {
	// Check if file exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("schema file not found: %s", schemaPath)
	}

	// Read file content
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	// Parse configuration based on file extension
	var config SchemaConfig
	ext := strings.ToLower(filepath.Ext(schemaPath))

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON schema file: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML schema file: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported schema file format: %s (supported: .json, .yaml, .yml)", ext)
	}

	// Create validator and set validation level
	validator := NewArrowSchemaValidator()

	// Set validation level if specified
	if config.ValidationLevel != "" {
		switch strings.ToLower(config.ValidationLevel) {
		case "strict":
			validator.SetValidationLevel(ValidationLevelStrict)
		case "compatible":
			validator.SetValidationLevel(ValidationLevelCompatible)
		case "relaxed":
			validator.SetValidationLevel(ValidationLevelRelaxed)
		default:
			return nil, fmt.Errorf("unknown validation level: %s", config.ValidationLevel)
		}
	}

	// Add rules based on configuration
	// 1. Required fields rule
	if len(config.RequiredFields) > 0 {
		validator.AddRule(&RequiredFieldsRule{
			RequiredFields: config.RequiredFields,
		})
	}

	// 2. Non-nullable fields rule
	if len(config.NonNullableFields) > 0 {
		validator.AddRule(&NullabilityRule{
			NonNullableFields: config.NonNullableFields,
		})
	}

	// 3. Field types rule (if specified)
	if len(config.FieldTypes) > 0 {
		fieldTypeRule, err := createFieldTypeRule(config.FieldTypes)
		if err != nil {
			return nil, fmt.Errorf("failed to create field type rule: %w", err)
		}
		validator.AddRule(fieldTypeRule)
	}

	// 4. Temporal format rule
	if len(config.TimestampFields) > 0 || config.RequiredTimezone != "" {
		validator.AddRule(&TemporalFormatRule{
			TimestampFields:  config.TimestampFields,
			RequiredTimezone: config.RequiredTimezone,
		})
	}

	// 5. Dictionary encoding rule
	if config.StringFieldsRequireDictionary {
		validator.AddRule(&DictionaryEncodingRule{
			StringFieldsRequireDictionary: true,
			ExemptFields:                  config.DictionaryExemptFields,
		})
	}

	// 6. Decimal precision rule
	if len(config.DecimalPrecisions) > 0 {
		validator.AddRule(&DecimalPrecisionRule{
			FieldRequirements: config.DecimalPrecisions,
		})
	}

	// 7. Metadata rule
	if len(config.RequiredMetadataKeys) > 0 {
		validator.AddRule(&MetadataRule{
			RequiredKeys: config.RequiredMetadataKeys,
		})
	}

	return validator, nil
}

// createFieldTypeRule creates a FieldTypeRule from a map of field names to allowed type strings
func createFieldTypeRule(fieldTypeConfig map[string][]string) (*FieldTypeRule, error) {
	allowedTypes := make(map[string][]arrow.DataType)

	for fieldName, typeStrings := range fieldTypeConfig {
		var types []arrow.DataType

		for _, typeStr := range typeStrings {
			dataType, err := parseArrowType(typeStr)
			if err != nil {
				return nil, fmt.Errorf("invalid type specification for field '%s': %w", fieldName, err)
			}
			types = append(types, dataType)
		}

		allowedTypes[fieldName] = types
	}

	return &FieldTypeRule{
		AllowedTypes: allowedTypes,
	}, nil
}

// parseArrowType converts a string representation to an Arrow DataType
func parseArrowType(typeStr string) (arrow.DataType, error) {
	// Handle basic types
	switch strings.ToLower(typeStr) {
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "int32", "int":
		return arrow.PrimitiveTypes.Int32, nil
	case "uint32", "uint":
		return arrow.PrimitiveTypes.Uint32, nil
	case "int64", "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint64", "ulong":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string", "utf8":
		return arrow.BinaryTypes.String, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "timestamp":
		return &arrow.TimestampType{Unit: arrow.Second}, nil
	case "timestamp[ms]":
		return &arrow.TimestampType{Unit: arrow.Millisecond}, nil
	case "timestamp[us]":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "timestamp[ns]":
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
	default:
		// Handle decimal types (decimal(precision,scale))
		if strings.HasPrefix(strings.ToLower(typeStr), "decimal") {
			var precision, scale int
			if _, err := fmt.Sscanf(typeStr, "decimal(%d,%d)", &precision, &scale); err == nil {
				return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
			}
		}

		return nil, fmt.Errorf("unsupported Arrow type: %s", typeStr)
	}
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

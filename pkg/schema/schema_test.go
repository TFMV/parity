package schema

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestRequiredFieldsRule(t *testing.T) {
	// Create a schema with fields "id" and "name"
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Test valid case
	rule := &RequiredFieldsRule{
		RequiredFields: []string{"id", "name"},
	}
	valid, err := rule.Validate(schema)
	assert.True(t, valid)
	assert.NoError(t, err)

	// Test invalid case
	rule = &RequiredFieldsRule{
		RequiredFields: []string{"id", "name", "age"},
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "age")
}

func TestFieldTypeRule(t *testing.T) {
	// Create a schema with fields of various types
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Test valid case
	rule := &FieldTypeRule{
		AllowedTypes: map[string][]arrow.DataType{
			"id":    {arrow.PrimitiveTypes.Int64},
			"name":  {arrow.BinaryTypes.String},
			"value": {arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64},
		},
	}
	valid, err := rule.Validate(schema)
	assert.True(t, valid)
	assert.NoError(t, err)

	// Test invalid case
	rule = &FieldTypeRule{
		AllowedTypes: map[string][]arrow.DataType{
			"id":    {arrow.PrimitiveTypes.Int32}, // Wrong type
			"name":  {arrow.BinaryTypes.String},
			"value": {arrow.PrimitiveTypes.Float64},
		},
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field 'id'")
}

func TestNullabilityRule(t *testing.T) {
	// Create a schema with nullable and non-nullable fields
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Test valid case
	rule := &NullabilityRule{
		NonNullableFields: []string{"id"},
	}
	valid, err := rule.Validate(schema)
	assert.True(t, valid)
	assert.NoError(t, err)

	// Test invalid case
	rule = &NullabilityRule{
		NonNullableFields: []string{"id", "name"},
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name")
}

func TestMetadataRule(t *testing.T) {
	// Create a schema with metadata
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}
	metadata := arrow.NewMetadata(
		[]string{"creator", "version"},
		[]string{"test", "1.0"},
	)
	schema := arrow.NewSchema(fields, &metadata)

	// Test valid case
	rule := &MetadataRule{
		RequiredKeys: []string{"creator", "version"},
		KeyValidators: map[string]func(string) error{
			"version": func(value string) error {
				if value != "1.0" {
					return fmt.Errorf("expected version 1.0, got %s", value)
				}
				return nil
			},
		},
	}
	valid, err := rule.Validate(schema)
	assert.True(t, valid)
	assert.NoError(t, err)

	// Test invalid case - missing key
	rule = &MetadataRule{
		RequiredKeys: []string{"creator", "version", "timestamp"},
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timestamp")

	// Test invalid case - invalid value
	rule = &MetadataRule{
		RequiredKeys: []string{"creator", "version"},
		KeyValidators: map[string]func(string) error{
			"version": func(value string) error {
				if value != "2.0" {
					return fmt.Errorf("expected version 2.0, got %s", value)
				}
				return nil
			},
		},
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected version 2.0")
}

func TestArrowSchemaValidator(t *testing.T) {
	// Create a schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create a validator with multiple rules
	validator := NewArrowSchemaValidator()

	// Add rules
	validator.AddRule(&RequiredFieldsRule{
		RequiredFields: []string{"id", "name"},
	})
	validator.AddRule(&NullabilityRule{
		NonNullableFields: []string{"id"},
	})

	// Test valid schema
	result := validator.ValidateSchema(schema)
	assert.True(t, result.Valid)
	assert.Empty(t, result.Errors)

	// Test invalid schema by adding a failing rule
	validator.AddRule(&RequiredFieldsRule{
		RequiredFields: []string{"id", "name", "email"},
	})
	result = validator.ValidateSchema(schema)
	assert.False(t, result.Valid)
	assert.NotEmpty(t, result.Errors)
	assert.Contains(t, result.Errors["RequiredFieldsRule"][0], "email")
}

func TestValidateAgainstTarget_Strict(t *testing.T) {
	// Create source schema
	sourceFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	sourceSchema := arrow.NewSchema(sourceFields, nil)

	// Create identical target schema for strict validation
	targetFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	targetSchema := arrow.NewSchema(targetFields, nil)

	// Create validator with strict validation
	validator := NewStrictValidator()

	// Test identical schemas (should pass)
	result := validator.ValidateAgainstTarget(sourceSchema, targetSchema)
	assert.True(t, result.Valid)
	assert.Empty(t, result.Errors)

	// Create incompatible target schema
	incompatibleFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false}, // Different type
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	incompatibleSchema := arrow.NewSchema(incompatibleFields, nil)

	// Test incompatible schemas (should fail)
	result = validator.ValidateAgainstTarget(sourceSchema, incompatibleSchema)
	assert.False(t, result.Valid)
	assert.NotEmpty(t, result.Errors)
	assert.Contains(t, result.Errors["SchemaStructure"][0], "field type mismatch")
}

func TestValidateAgainstTarget_Compatible(t *testing.T) {
	// Create source schema
	sourceFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	sourceSchema := arrow.NewSchema(sourceFields, nil)

	// Create compatible target schema with added field and relaxed nullability
	targetFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},  // Relaxed nullability
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true}, // New field
	}
	targetSchema := arrow.NewSchema(targetFields, nil)

	// Create validator with compatible validation
	validator := NewCompatibleValidator()

	// Test compatible schemas (should pass with warnings)
	result := validator.ValidateAgainstTarget(sourceSchema, targetSchema)
	assert.True(t, result.Valid)
	assert.Empty(t, result.Errors)
	assert.NotEmpty(t, result.Warnings)
	assert.Len(t, result.Warnings["SchemaEvolution"], 2)
	assert.Contains(t, result.Warnings["SchemaEvolution"], "new field 'email' in target schema")
	assert.Contains(t, result.Warnings["SchemaEvolution"], "relaxed nullability for field 'name'")

	// Create incompatible target schema
	incompatibleFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false}, // New field
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false}, // New field
	}
	incompatibleSchema := arrow.NewSchema(incompatibleFields, nil)

	// Remove a field to test schema evolution
	result = validator.ValidateAgainstTarget(incompatibleSchema, sourceSchema)
	assert.True(t, result.Valid)
	assert.Empty(t, result.Errors)
	assert.NotEmpty(t, result.Warnings)
	assert.Contains(t, result.Warnings["SchemaEvolution"][0], "removed")
}

func TestValidateAgainstTarget_Relaxed(t *testing.T) {
	// Create source schema
	sourceFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}
	sourceSchema := arrow.NewSchema(sourceFields, nil)

	// Create target schema with different fields
	targetFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},     // Different field
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // Different field
	}
	targetSchema := arrow.NewSchema(targetFields, nil)

	// Create validator with relaxed validation
	validator := NewRelaxedValidator()

	// Test schemas with only some common fields (should pass with warnings)
	result := validator.ValidateAgainstTarget(sourceSchema, targetSchema)
	assert.True(t, result.Valid)
	assert.Empty(t, result.Errors)
	assert.NotEmpty(t, result.Warnings)
	assert.Len(t, result.Warnings["SchemaStructure"], 4)

	// Create incompatible target schema
	incompatibleFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false}, // Different type
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	incompatibleSchema := arrow.NewSchema(incompatibleFields, nil)

	// Test schemas with incompatible common fields (should fail)
	result = validator.ValidateAgainstTarget(sourceSchema, incompatibleSchema)
	assert.False(t, result.Valid)
	assert.NotEmpty(t, result.Errors)
	assert.Contains(t, result.Errors["CommonFields"][0], "incompatible type for common field 'id'")
}

func TestTemporalFormatRule(t *testing.T) {
	// Create fields with date and timestamp types
	dateType := arrow.Date32Type{}
	timestampType := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}
	wrongTimestampType := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/New_York"}

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "created_date", Type: &dateType, Nullable: true},
		{Name: "updated_at", Type: timestampType, Nullable: true},
		{Name: "processed_at", Type: wrongTimestampType, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	// Test valid case
	rule := &TemporalFormatRule{
		DateFields:       []string{"created_date"},
		TimestampFields:  []string{"updated_at"},
		RequiredTimezone: "UTC",
	}
	valid, err := rule.Validate(schema)
	assert.True(t, valid)
	assert.NoError(t, err)

	// Test invalid timezone
	rule = &TemporalFormatRule{
		DateFields:       []string{"created_date"},
		TimestampFields:  []string{"updated_at", "processed_at"},
		RequiredTimezone: "UTC",
	}
	valid, err = rule.Validate(schema)
	assert.False(t, valid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "should use timezone 'UTC'")
}

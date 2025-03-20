// Package schema provides validation capabilities for Apache Arrow schemas.
package schema

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ValidationLevel defines how strict the validation should be.
type ValidationLevel int

const (
	// Strict validation requires exact schema matches (types, names, and nullability).
	ValidationLevelStrict ValidationLevel = iota

	// Compatible validation allows schema evolution (adding fields, relaxing nullability).
	ValidationLevelCompatible

	// Relaxed validation just checks that common fields have compatible types.
	ValidationLevelRelaxed
)

// ValidationRule defines an interface for schema validation rules.
type ValidationRule interface {
	// Validate checks if the schema meets the rule's criteria.
	Validate(schema *arrow.Schema) (bool, error)

	// Name returns the human-readable name of the rule.
	Name() string

	// Description returns a detailed description of what the rule validates.
	Description() string
}

// ValidationResult represents the result of a schema validation.
type ValidationResult struct {
	// Valid indicates whether the schema is valid according to all rules.
	Valid bool

	// Errors contains validation errors grouped by rule name.
	Errors map[string][]string

	// Warnings contains validation warnings grouped by rule name.
	Warnings map[string][]string
}

// SchemaValidator defines an interface for schema validators.
type SchemaValidator interface {
	// ValidateSchema checks if a schema is valid according to the validator's rules.
	ValidateSchema(schema *arrow.Schema) ValidationResult

	// ValidateAgainstTarget checks if a schema is compatible with a target schema.
	ValidateAgainstTarget(schema, targetSchema *arrow.Schema) ValidationResult

	// AddRule adds a validation rule to the validator.
	AddRule(rule ValidationRule)

	// SetValidationLevel sets the validation level.
	SetValidationLevel(level ValidationLevel)
}

// FieldTypeRule is a validation rule that checks field types.
type FieldTypeRule struct {
	// AllowedTypes is the list of allowed arrow types for each field.
	// The map key is the field name, and the value is a list of allowed types.
	AllowedTypes map[string][]arrow.DataType
}

// Validate implements ValidationRule.Validate.
func (r *FieldTypeRule) Validate(schema *arrow.Schema) (bool, error) {
	if len(r.AllowedTypes) == 0 {
		return true, nil
	}

	var errors []string
	for fieldName, allowedTypes := range r.AllowedTypes {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			errors = append(errors, fmt.Sprintf("field '%s' not found in schema", fieldName))
			continue
		}

		field := schema.Field(i[0])
		found := false
		for _, allowedType := range allowedTypes {
			if arrow.TypeEqual(field.Type, allowedType) {
				found = true
				break
			}
		}

		if !found {
			typeNames := make([]string, len(allowedTypes))
			for i, t := range allowedTypes {
				typeNames[i] = t.String()
			}
			errors = append(
				errors,
				fmt.Sprintf(
					"field '%s' has type '%s', but expected one of: %s",
					fieldName, field.Type, strings.Join(typeNames, ", "),
				),
			)
		}
	}

	if len(errors) > 0 {
		return false, fmt.Errorf("field type validation failed: %s", strings.Join(errors, "; "))
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *FieldTypeRule) Name() string {
	return "FieldTypeRule"
}

// Description implements ValidationRule.Description.
func (r *FieldTypeRule) Description() string {
	return "Validates that fields have the expected data types"
}

// RequiredFieldsRule is a validation rule that checks for required fields.
type RequiredFieldsRule struct {
	// RequiredFields is the list of field names that must be present in the schema.
	RequiredFields []string
}

// Validate implements ValidationRule.Validate.
func (r *RequiredFieldsRule) Validate(schema *arrow.Schema) (bool, error) {
	if len(r.RequiredFields) == 0 {
		return true, nil
	}

	var missingFields []string
	for _, fieldName := range r.RequiredFields {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			missingFields = append(missingFields, fieldName)
		}
	}

	if len(missingFields) > 0 {
		return false, fmt.Errorf(
			"required fields missing: %s",
			strings.Join(missingFields, ", "),
		)
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *RequiredFieldsRule) Name() string {
	return "RequiredFieldsRule"
}

// Description implements ValidationRule.Description.
func (r *RequiredFieldsRule) Description() string {
	return "Validates that all required fields are present in the schema"
}

// NullabilityRule is a validation rule that checks field nullability.
type NullabilityRule struct {
	// NonNullableFields is the list of field names that must not be nullable.
	NonNullableFields []string
}

// Validate implements ValidationRule.Validate.
func (r *NullabilityRule) Validate(schema *arrow.Schema) (bool, error) {
	if len(r.NonNullableFields) == 0 {
		return true, nil
	}

	var nullableFields []string
	for _, fieldName := range r.NonNullableFields {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			// Field doesn't exist, skip it
			continue
		}

		field := schema.Field(i[0])
		if field.Nullable {
			nullableFields = append(nullableFields, fieldName)
		}
	}

	if len(nullableFields) > 0 {
		return false, fmt.Errorf(
			"fields that should not be nullable: %s",
			strings.Join(nullableFields, ", "),
		)
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *NullabilityRule) Name() string {
	return "NullabilityRule"
}

// Description implements ValidationRule.Description.
func (r *NullabilityRule) Description() string {
	return "Validates that specified fields are not nullable"
}

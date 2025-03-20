package schema

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowSchemaValidator implements the SchemaValidator interface.
type ArrowSchemaValidator struct {
	rules           []ValidationRule
	validationLevel ValidationLevel
}

// NewArrowSchemaValidator creates a new instance of ArrowSchemaValidator.
func NewArrowSchemaValidator() *ArrowSchemaValidator {
	return &ArrowSchemaValidator{
		rules:           []ValidationRule{},
		validationLevel: ValidationLevelCompatible, // Default to compatible validation
	}
}

// ValidateSchema checks if a schema is valid according to the validator's rules.
func (v *ArrowSchemaValidator) ValidateSchema(schema *arrow.Schema) ValidationResult {
	result := ValidationResult{
		Valid:    true,
		Errors:   make(map[string][]string),
		Warnings: make(map[string][]string),
	}

	for _, rule := range v.rules {
		valid, err := rule.Validate(schema)
		if !valid {
			result.Valid = false
			if err != nil {
				result.Errors[rule.Name()] = append(result.Errors[rule.Name()], err.Error())
			}
		}
	}

	return result
}

// ValidateAgainstTarget checks if a schema is compatible with a target schema.
func (v *ArrowSchemaValidator) ValidateAgainstTarget(schema, targetSchema *arrow.Schema) ValidationResult {
	result := ValidationResult{
		Valid:    true,
		Errors:   make(map[string][]string),
		Warnings: make(map[string][]string),
	}

	// Different validation logic based on the validation level
	switch v.validationLevel {
	case ValidationLevelStrict:
		if err := v.validateStrictCompatibility(schema, targetSchema, &result); err != nil {
			result.Valid = false
		}
	case ValidationLevelCompatible:
		if err := v.validateCompatible(schema, targetSchema, &result); err != nil {
			result.Valid = false
		}
	case ValidationLevelRelaxed:
		if err := v.validateRelaxed(schema, targetSchema, &result); err != nil {
			result.Valid = false
		}
	}

	return result
}

// validateStrictCompatibility checks if schemas match exactly.
func (v *ArrowSchemaValidator) validateStrictCompatibility(schema, targetSchema *arrow.Schema, result *ValidationResult) error {
	if schema.NumFields() != targetSchema.NumFields() {
		errMsg := fmt.Sprintf("schema field count mismatch: got %d, expected %d", schema.NumFields(), targetSchema.NumFields())
		result.Errors["SchemaStructure"] = append(result.Errors["SchemaStructure"], errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	var errors []string
	for i := 0; i < schema.NumFields(); i++ {
		sourceField := schema.Field(i)
		targetField := targetSchema.Field(i)

		if sourceField.Name != targetField.Name {
			errors = append(errors, fmt.Sprintf("field name mismatch at index %d: got '%s', expected '%s'",
				i, sourceField.Name, targetField.Name))
			continue
		}

		if !arrow.TypeEqual(sourceField.Type, targetField.Type) {
			errors = append(errors, fmt.Sprintf("field type mismatch for '%s': got '%s', expected '%s'",
				sourceField.Name, sourceField.Type, targetField.Type))
		}

		if sourceField.Nullable != targetField.Nullable {
			errors = append(errors, fmt.Sprintf("field nullability mismatch for '%s': got %v, expected %v",
				sourceField.Name, sourceField.Nullable, targetField.Nullable))
		}
	}

	if len(errors) > 0 {
		errMsg := strings.Join(errors, "; ")
		result.Errors["SchemaStructure"] = append(result.Errors["SchemaStructure"], errMsg)
		return fmt.Errorf("strict validation failed: %s", errMsg)
	}

	return nil
}

// validateCompatible checks if schemas are compatible (allow schema evolution).
func (v *ArrowSchemaValidator) validateCompatible(schema, targetSchema *arrow.Schema, result *ValidationResult) error {
	sourceFieldMap := make(map[string]arrow.Field)
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		sourceFieldMap[field.Name] = field
	}

	var errors []string
	var warnings []string

	// Check that all target fields exist in source with compatible types
	for i := 0; i < targetSchema.NumFields(); i++ {
		targetField := targetSchema.Field(i)
		sourceField, exists := sourceFieldMap[targetField.Name]

		if !exists {
			warnings = append(warnings, fmt.Sprintf("new field '%s' in target schema", targetField.Name))
			continue
		}

		if !arrow.TypeEqual(sourceField.Type, targetField.Type) {
			errors = append(errors, fmt.Sprintf("incompatible type for field '%s': got '%s', expected '%s'",
				targetField.Name, sourceField.Type, targetField.Type))
		}

		// Allow changing from non-nullable to nullable (more permissive), but not the reverse
		if !sourceField.Nullable && targetField.Nullable {
			warnings = append(warnings, fmt.Sprintf("relaxed nullability for field '%s'", targetField.Name))
		} else if sourceField.Nullable && !targetField.Nullable {
			errors = append(errors, fmt.Sprintf("increased nullability restriction for field '%s'", targetField.Name))
		}
	}

	// Check for removed fields in target
	for name := range sourceFieldMap {
		i := targetSchema.FieldIndices(name)
		if len(i) == 0 {
			warnings = append(warnings, fmt.Sprintf("field '%s' removed in target schema", name))
		}
	}

	if len(warnings) > 0 {
		result.Warnings["SchemaEvolution"] = warnings
	}

	if len(errors) > 0 {
		errMsg := strings.Join(errors, "; ")
		result.Errors["SchemaCompatibility"] = append(result.Errors["SchemaCompatibility"], errMsg)
		return fmt.Errorf("compatibility validation failed: %s", errMsg)
	}

	return nil
}

// validateRelaxed checks if common fields between schemas have compatible types.
func (v *ArrowSchemaValidator) validateRelaxed(schema, targetSchema *arrow.Schema, result *ValidationResult) error {
	sourceFieldMap := make(map[string]arrow.Field)
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		sourceFieldMap[field.Name] = field
	}

	var errors []string
	var warnings []string

	// Check only common fields for type compatibility
	for i := 0; i < targetSchema.NumFields(); i++ {
		targetField := targetSchema.Field(i)
		sourceField, exists := sourceFieldMap[targetField.Name]

		if !exists {
			// Skip fields that don't exist in source
			warnings = append(warnings, fmt.Sprintf("field '%s' exists only in target schema", targetField.Name))
			continue
		}

		if !arrow.TypeEqual(sourceField.Type, targetField.Type) {
			errors = append(errors, fmt.Sprintf("incompatible type for common field '%s': got '%s', expected '%s'",
				targetField.Name, sourceField.Type, targetField.Type))
		}
	}

	// Note fields that only exist in source
	for name := range sourceFieldMap {
		i := targetSchema.FieldIndices(name)
		if len(i) == 0 {
			warnings = append(warnings, fmt.Sprintf("field '%s' exists only in source schema", name))
		}
	}

	if len(warnings) > 0 {
		result.Warnings["SchemaStructure"] = warnings
	}

	if len(errors) > 0 {
		errMsg := strings.Join(errors, "; ")
		result.Errors["CommonFields"] = append(result.Errors["CommonFields"], errMsg)
		return fmt.Errorf("relaxed validation failed: %s", errMsg)
	}

	return nil
}

// AddRule adds a validation rule to the validator.
func (v *ArrowSchemaValidator) AddRule(rule ValidationRule) {
	v.rules = append(v.rules, rule)
}

// SetValidationLevel sets the validation level.
func (v *ArrowSchemaValidator) SetValidationLevel(level ValidationLevel) {
	v.validationLevel = level
}

// NewStrictValidator creates a validator with strict validation level.
func NewStrictValidator() *ArrowSchemaValidator {
	validator := NewArrowSchemaValidator()
	validator.SetValidationLevel(ValidationLevelStrict)
	return validator
}

// NewCompatibleValidator creates a validator with compatible validation level.
func NewCompatibleValidator() *ArrowSchemaValidator {
	validator := NewArrowSchemaValidator()
	validator.SetValidationLevel(ValidationLevelCompatible)
	return validator
}

// NewRelaxedValidator creates a validator with relaxed validation level.
func NewRelaxedValidator() *ArrowSchemaValidator {
	validator := NewArrowSchemaValidator()
	validator.SetValidationLevel(ValidationLevelRelaxed)
	return validator
}

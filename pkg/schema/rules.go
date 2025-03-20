package schema

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// TypeCompatibilityRule checks if field types are compatible with certain conversions allowed.
type TypeCompatibilityRule struct {
	// FieldTypeMappings contains mappings of field names to allowed type conversions.
	// Key is the field name, value is a map of source type to compatible target types.
	FieldTypeMappings map[string]map[arrow.DataType][]arrow.DataType
}

// Validate implements ValidationRule.Validate.
func (r *TypeCompatibilityRule) Validate(schema *arrow.Schema) (bool, error) {
	if len(r.FieldTypeMappings) == 0 {
		return true, nil
	}

	var errors []string
	for fieldName, typeMappings := range r.FieldTypeMappings {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			// Field doesn't exist, skip it
			continue
		}

		field := schema.Field(i[0])
		fieldType := field.Type

		// Check if this type is in our mappings
		allowedTypes, ok := typeMappings[fieldType]
		if !ok {
			errors = append(errors, fmt.Sprintf("field '%s' has type '%s' which is not in the compatibility map",
				fieldName, fieldType))
			continue
		}

		// Check if any of the allowed types match
		compatibleFound := false
		for _, allowedType := range allowedTypes {
			if arrow.TypeEqual(fieldType, allowedType) {
				compatibleFound = true
				break
			}
		}

		if !compatibleFound {
			typeStrings := make([]string, len(allowedTypes))
			for i, t := range allowedTypes {
				typeStrings[i] = t.String()
			}
			errors = append(errors, fmt.Sprintf("field '%s' has type '%s' which is not compatible with any of: %s",
				fieldName, fieldType, strings.Join(typeStrings, ", ")))
		}
	}

	if len(errors) > 0 {
		return false, fmt.Errorf("type compatibility validation failed: %s", strings.Join(errors, "; "))
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *TypeCompatibilityRule) Name() string {
	return "TypeCompatibilityRule"
}

// Description implements ValidationRule.Description.
func (r *TypeCompatibilityRule) Description() string {
	return "Validates that field types are compatible with defined conversion rules"
}

// MetadataRule checks for required metadata in the schema.
type MetadataRule struct {
	// RequiredKeys is a list of metadata keys that must be present.
	RequiredKeys []string

	// KeyValidators contains validators for specific metadata keys.
	// The key is the metadata key, and the value is a function that validates the metadata value.
	KeyValidators map[string]func(string) error
}

// Validate implements ValidationRule.Validate.
func (r *MetadataRule) Validate(schema *arrow.Schema) (bool, error) {
	metadata := schema.Metadata()

	// First check for required keys
	var missingKeys []string
	for _, key := range r.RequiredKeys {
		if metadata.FindKey(key) < 0 {
			missingKeys = append(missingKeys, key)
		}
	}

	if len(missingKeys) > 0 {
		return false, fmt.Errorf("required metadata keys missing: %s", strings.Join(missingKeys, ", "))
	}

	// Then validate values for keys with validators
	var validationErrors []string
	for key, validator := range r.KeyValidators {
		keyIndex := metadata.FindKey(key)
		if keyIndex < 0 {
			// Skip keys that don't exist
			continue
		}

		value := metadata.Values()[keyIndex]
		if err := validator(value); err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("validation failed for key '%s': %s", key, err.Error()))
		}
	}

	if len(validationErrors) > 0 {
		return false, fmt.Errorf("metadata validation failed: %s", strings.Join(validationErrors, "; "))
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *MetadataRule) Name() string {
	return "MetadataRule"
}

// Description implements ValidationRule.Description.
func (r *MetadataRule) Description() string {
	return "Validates that schema metadata contains required keys with valid values"
}

// DictionaryEncodingRule checks for proper dictionary encoding on string fields.
type DictionaryEncodingRule struct {
	// StringFieldsRequireDictionary indicates whether string fields should use dictionary encoding.
	StringFieldsRequireDictionary bool

	// ExemptFields lists fields that are exempt from the dictionary encoding requirement.
	ExemptFields []string
}

// Validate implements ValidationRule.Validate.
func (r *DictionaryEncodingRule) Validate(schema *arrow.Schema) (bool, error) {
	if !r.StringFieldsRequireDictionary {
		return true, nil
	}

	// Create a map of exempt fields for fast lookup
	exemptMap := make(map[string]bool)
	for _, field := range r.ExemptFields {
		exemptMap[field] = true
	}

	var nonDictionaryFields []string
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)

		// Skip exempt fields
		if exemptMap[field.Name] {
			continue
		}

		// Check if it's a string field
		if arrow.TypeEqual(field.Type, arrow.BinaryTypes.String) ||
			arrow.TypeEqual(field.Type, arrow.BinaryTypes.LargeString) {

			// Check if it's dictionary encoded
			_, isDictionary := field.Type.(*arrow.DictionaryType)
			if !isDictionary {
				nonDictionaryFields = append(nonDictionaryFields, field.Name)
			}
		}
	}

	if len(nonDictionaryFields) > 0 {
		return false, fmt.Errorf(
			"string fields should use dictionary encoding: %s",
			strings.Join(nonDictionaryFields, ", "),
		)
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *DictionaryEncodingRule) Name() string {
	return "DictionaryEncodingRule"
}

// Description implements ValidationRule.Description.
func (r *DictionaryEncodingRule) Description() string {
	return "Validates that string fields use dictionary encoding for better performance"
}

// TemporalFormatRule checks for proper temporal types and formats.
type TemporalFormatRule struct {
	// DateFields specifies fields that should use Date32 type.
	DateFields []string

	// TimestampFields specifies fields that should use Timestamp type.
	TimestampFields []string

	// RequiredTimezone specifies a required timezone for timestamp fields.
	RequiredTimezone string
}

// Validate implements ValidationRule.Validate.
func (r *TemporalFormatRule) Validate(schema *arrow.Schema) (bool, error) {
	var errors []string

	// Check date fields
	for _, fieldName := range r.DateFields {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			// Field doesn't exist, skip it
			continue
		}

		field := schema.Field(i[0])
		_, isDate32 := field.Type.(*arrow.Date32Type)
		if !isDate32 {
			errors = append(errors, fmt.Sprintf("field '%s' should use Date32 type, got '%s'",
				fieldName, field.Type))
		}
	}

	// Check timestamp fields
	for _, fieldName := range r.TimestampFields {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			// Field doesn't exist, skip it
			continue
		}

		field := schema.Field(i[0])
		tsType, isTimestamp := field.Type.(*arrow.TimestampType)
		if !isTimestamp {
			errors = append(errors, fmt.Sprintf("field '%s' should use Timestamp type, got '%s'",
				fieldName, field.Type))
			continue
		}

		// Check timezone if required
		if r.RequiredTimezone != "" && tsType.TimeZone != r.RequiredTimezone {
			errors = append(errors, fmt.Sprintf("field '%s' should use timezone '%s', got '%s'",
				fieldName, r.RequiredTimezone, tsType.TimeZone))
		}
	}

	if len(errors) > 0 {
		return false, fmt.Errorf("temporal format validation failed: %s", strings.Join(errors, "; "))
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *TemporalFormatRule) Name() string {
	return "TemporalFormatRule"
}

// Description implements ValidationRule.Description.
func (r *TemporalFormatRule) Description() string {
	return "Validates that temporal fields use proper types and formats"
}

// DecimalPrecisionRule validates that decimal fields have the correct precision and scale.
type DecimalPrecisionRule struct {
	// FieldRequirements maps field names to required precision and scale.
	// The map key is the field name, and the value is a pair of [precision, scale].
	FieldRequirements map[string][2]int32
}

// Validate implements ValidationRule.Validate.
func (r *DecimalPrecisionRule) Validate(schema *arrow.Schema) (bool, error) {
	if len(r.FieldRequirements) == 0 {
		return true, nil
	}

	var errors []string
	for fieldName, requirements := range r.FieldRequirements {
		i := schema.FieldIndices(fieldName)
		if len(i) == 0 {
			// Field doesn't exist, skip it
			continue
		}

		field := schema.Field(i[0])

		// Check if it's a decimal type
		decimalType, isDecimal := field.Type.(*arrow.Decimal128Type)
		if !isDecimal {
			errors = append(errors, fmt.Sprintf("field '%s' should be a decimal type, got '%s'",
				fieldName, field.Type))
			continue
		}

		requiredPrecision := requirements[0]
		requiredScale := requirements[1]

		if decimalType.Precision != requiredPrecision || decimalType.Scale != requiredScale {
			errors = append(errors, fmt.Sprintf(
				"field '%s' has decimal precision/scale (%d,%d), but requires (%d,%d)",
				fieldName, decimalType.Precision, decimalType.Scale, requiredPrecision, requiredScale))
		}
	}

	if len(errors) > 0 {
		return false, fmt.Errorf("decimal precision validation failed: %s", strings.Join(errors, "; "))
	}

	return true, nil
}

// Name implements ValidationRule.Name.
func (r *DecimalPrecisionRule) Name() string {
	return "DecimalPrecisionRule"
}

// Description implements ValidationRule.Description.
func (r *DecimalPrecisionRule) Description() string {
	return "Validates that decimal fields have the correct precision and scale"
}

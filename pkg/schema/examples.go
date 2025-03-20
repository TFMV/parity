package schema

import (
	"context"
	"fmt"
	"log"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/apache/arrow-go/v18/arrow"
)

// This file contains example functions that demonstrate how to use
// the schema validator in various scenarios. These are not meant to be
// used in production code but serve as reference implementations.

// Example_ValidateParquetFile shows how to validate a Parquet file's schema.
func Example_ValidateParquetFile(filePath string) error {
	// Create a Parquet reader
	reader, err := readers.NewParquetReader(core.ReaderConfig{
		Path: filePath,
	})
	if err != nil {
		return fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer reader.Close()

	// Create a validator with custom rules
	validator := NewArrowSchemaValidator()

	// Add rules specific to this validation
	validator.AddRule(&RequiredFieldsRule{
		RequiredFields: []string{"id", "name", "timestamp"},
	})

	validator.AddRule(&NullabilityRule{
		NonNullableFields: []string{"id"},
	})

	validator.AddRule(&TemporalFormatRule{
		TimestampFields:  []string{"timestamp", "created_at", "updated_at"},
		RequiredTimezone: "UTC",
	})

	// Validate the schema
	result := validator.ValidateSchema(reader.Schema())

	// Print the result
	fmt.Println(PrintValidationResult(result))

	return nil
}

// Example_CompareSchemas shows how to compare schemas from different files.
func Example_CompareSchemas(sourcePath, targetPath string) error {
	// Create source reader
	sourceReader, err := readers.NewArrowReader(core.ReaderConfig{
		Path: sourcePath,
	})
	if err != nil {
		return fmt.Errorf("failed to create source reader: %w", err)
	}
	defer sourceReader.Close()

	// Create target reader
	targetReader, err := readers.NewCSVReader(core.ReaderConfig{
		Path: targetPath,
	})
	if err != nil {
		return fmt.Errorf("failed to create target reader: %w", err)
	}
	defer targetReader.Close()

	// Read a sample from CSV to ensure schema is populated
	if _, err := targetReader.Read(context.Background()); err != nil {
		if err != nil {
			return fmt.Errorf("failed to read from target: %w", err)
		}
	}

	// Create a validator with compatible validation level
	validator := NewCompatibleValidator()

	// Validate source schema against target
	result := validator.ValidateAgainstTarget(sourceReader.Schema(), targetReader.Schema())

	// Print schema comparison
	fmt.Println(CompareSchemas(sourceReader.Schema(), targetReader.Schema()))

	// Print validation result
	fmt.Println(PrintValidationResult(result))

	return nil
}

// Example_DynamicSchemaValidation shows how to dynamically build a validator based on data.
func Example_DynamicSchemaValidation(file1Path, file2Path string) error {
	// Create readers
	reader1, err := readers.NewArrowReader(core.ReaderConfig{
		Path: file1Path,
	})
	if err != nil {
		return fmt.Errorf("failed to create reader 1: %w", err)
	}
	defer reader1.Close()

	reader2, err := readers.NewArrowReader(core.ReaderConfig{
		Path: file2Path,
	})
	if err != nil {
		return fmt.Errorf("failed to create reader 2: %w", err)
	}
	defer reader2.Close()

	// Dynamically build a field type rule based on the first schema
	schema1 := reader1.Schema()
	allowedTypes := make(map[string][]arrow.DataType)

	for i := 0; i < schema1.NumFields(); i++ {
		field := schema1.Field(i)
		allowedTypes[field.Name] = []arrow.DataType{field.Type}
	}

	// Create a validator with the dynamic rule
	validator := NewArrowSchemaValidator()
	validator.AddRule(&FieldTypeRule{
		AllowedTypes: allowedTypes,
	})

	// Validate the second schema against the rules derived from the first
	result := validator.ValidateSchema(reader2.Schema())

	// Print the result
	fmt.Println(PrintValidationResult(result))

	return nil
}

// Example_ValidatorFactory demonstrates how to create and use validator factories.
func Example_ValidatorFactory() {
	// This example shows how to create a factory function for validators
	// that enforce specific rules for different datasets

	// Create a factory function for customer data validators
	createCustomerValidator := func() *ArrowSchemaValidator {
		validator := NewArrowSchemaValidator()

		// Add rules specific to customer data
		validator.AddRule(&RequiredFieldsRule{
			RequiredFields: []string{"customer_id", "name", "email"},
		})

		validator.AddRule(&NullabilityRule{
			NonNullableFields: []string{"customer_id", "email"},
		})

		// Ensure email and name are stored as dictionary-encoded strings for efficiency
		validator.AddRule(&DictionaryEncodingRule{
			StringFieldsRequireDictionary: true,
			ExemptFields:                  []string{"notes", "description"},
		})

		return validator
	}

	// Create a factory function for transaction data validators
	createTransactionValidator := func() *ArrowSchemaValidator {
		validator := NewArrowSchemaValidator()

		// Add rules specific to transaction data
		validator.AddRule(&RequiredFieldsRule{
			RequiredFields: []string{"transaction_id", "customer_id", "amount", "timestamp"},
		})

		validator.AddRule(&NullabilityRule{
			NonNullableFields: []string{"transaction_id", "customer_id", "amount", "timestamp"},
		})

		// Ensure timestamps use UTC
		validator.AddRule(&TemporalFormatRule{
			TimestampFields:  []string{"timestamp", "created_at", "updated_at"},
			RequiredTimezone: "UTC",
		})

		// Ensure decimals have the correct precision/scale
		validator.AddRule(&DecimalPrecisionRule{
			FieldRequirements: map[string][2]int32{
				"amount": {10, 2}, // 10 digits, 2 decimal places
				"fee":    {8, 4},  // 8 digits, 4 decimal places
			},
		})

		return validator
	}

	// Example usage
	customerValidator := createCustomerValidator()
	transactionValidator := createTransactionValidator()

	// Create fake schemas for demonstration
	customerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "customer_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	transactionSchema := arrow.NewSchema([]arrow.Field{
		{Name: "transaction_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "customer_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: false},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, Nullable: false},
	}, nil)

	// Validate schemas
	customerResult := customerValidator.ValidateSchema(customerSchema)
	transactionResult := transactionValidator.ValidateSchema(transactionSchema)

	// Print results
	log.Printf("Customer schema validation: %v\n", customerResult.Valid)
	log.Printf("Transaction schema validation: %v\n", transactionResult.Valid)
}

// IntegrateWithDiffer demonstrates how to integrate schema validation with the differ.
func IntegrateWithDiffer() {
	// This is a conceptual example showing how the schema validator could be
	// integrated with the existing differ implementation.
	// Note: This function is not meant to be executed but serves as documentation.

	/*
		// Pseudo-code for integration with the differ:

		func (d *ArrowDiffer) Diff(ctx context.Context, source, target core.DatasetReader, options core.DiffOptions) (*core.DiffResult, error) {
			// Get schemas for source and target
			sourceSchema := source.Schema()
			targetSchema := target.Schema()

			// Create a schema validator
			validator := schema.NewCompatibleValidator()

			// Add required rules based on options
			if len(options.KeyColumns) > 0 {
				validator.AddRule(&schema.RequiredFieldsRule{
					RequiredFields: options.KeyColumns,
				})
			}

			// Validate schemas for compatibility
			result := validator.ValidateAgainstTarget(sourceSchema, targetSchema)
			if !result.Valid {
				// If validation fails, return detailed error
				return nil, fmt.Errorf("schema validation failed: %s", schema.PrintValidationResult(result))
			}

			// Continue with existing diff logic
			// ...
		}
	*/
}

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
)

const (
	// Default values
	defaultSize    = 0.1 // 100MB
	defaultRows    = 1000000
	defaultOutDir  = "test_data"
	defaultOutFile = "test_data_source.parquet"
	defaultSeed    = 42

	// Constants for data generation
	maxStringLength = 100
	firstNames      = "John,Jane,Bob,Mary,Alice,David,Emma,Michael,Olivia,James,Sophia,William,Ava,Benjamin,Mia,Daniel,Charlotte,Matthew,Amelia,Henry"
	lastNames       = "Smith,Johnson,Williams,Jones,Brown,Davis,Miller,Wilson,Moore,Taylor,Anderson,Thomas,Jackson,White,Harris,Martin,Thompson,Garcia,Martinez,Robinson"
	domains         = "gmail.com,yahoo.com,hotmail.com,outlook.com,icloud.com,example.com,company.com,business.org,school.edu,local.net"
	statusValues    = "active,inactive,pending,suspended,deleted"
	stateValues     = "AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY"
)

// Configuration for the data generator
type Config struct {
	sizeGB            float64
	rowCount          int
	outputDir         string
	sourceFile        string
	targetFile        string
	randomSeed        int64
	errorRate         float64
	diffRate          float64
	nullRate          float64
	targetHasNewCols  bool
	targetMissingCols bool
}

// Storage for source IDs between file generations
var (
	globalSourceIDs       []string
	globalSourceIDIndices map[string]int
)

func main() {
	// Parse command-line flags
	config := parseFlags()

	// Reset global variables for this run to avoid issues with previously generated files
	globalSourceIDs = nil
	globalSourceIDIndices = make(map[string]int)

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Set up random source
	rnd := rand.New(rand.NewSource(config.randomSeed))

	// Generate source file
	sourcePath := filepath.Join(config.outputDir, config.sourceFile)
	log.Printf("Generating source file: %s (%.2f GB, approx. %d rows)", sourcePath, config.sizeGB, config.rowCount)
	if err := generateParquetFile(sourcePath, config, rnd, false); err != nil {
		log.Fatalf("Failed to generate source file: %v", err)
	}

	// Generate target file with differences
	targetPath := filepath.Join(config.outputDir, config.targetFile)
	log.Printf("Generating target file: %s with %.2f%% differences", targetPath, config.diffRate*100)
	if err := generateParquetFile(targetPath, config, rnd, true); err != nil {
		log.Fatalf("Failed to generate target file: %v", err)
	}

	log.Printf("Successfully generated test files:")
	log.Printf("  - Source: %s", sourcePath)
	log.Printf("  - Target: %s", targetPath)
}

// parseFlags parses command-line arguments and returns a Config
func parseFlags() Config {
	sizeGB := flag.Float64("size", defaultSize, "Size of the files to generate in GB")
	rowCount := flag.Int("rows", defaultRows, "Approximate number of rows to generate")
	outputDir := flag.String("outdir", defaultOutDir, "Output directory for generated files")
	sourceFile := flag.String("source", defaultOutFile, "Filename for the source Parquet file")
	targetFile := flag.String("target", "test_data_target.parquet", "Filename for the target Parquet file")
	seed := flag.Int64("seed", defaultSeed, "Random seed for data generation")
	errorRate := flag.Float64("errors", 0.01, "Rate of errors/corruptions in target file (0.0-1.0)")
	diffRate := flag.Float64("diffs", 0.1, "Percentage of rows that should differ between source and target (0.0-1.0)")
	nullRate := flag.Float64("nulls", 0.05, "Rate of NULL values in the data (0.0-1.0)")
	targetNewCols := flag.Bool("target-new-cols", true, "Whether target should have additional columns")
	targetMissingCols := flag.Bool("target-missing-cols", false, "Whether target should miss some columns from source")

	flag.Parse()

	return Config{
		sizeGB:            *sizeGB,
		rowCount:          *rowCount,
		outputDir:         *outputDir,
		sourceFile:        *sourceFile,
		targetFile:        *targetFile,
		randomSeed:        *seed,
		errorRate:         *errorRate,
		diffRate:          *diffRate,
		nullRate:          *nullRate,
		targetHasNewCols:  *targetNewCols,
		targetMissingCols: *targetMissingCols,
	}
}

// generateParquetFile creates a Parquet file with the specified parameters
func generateParquetFile(path string, config Config, rnd *rand.Rand, isTarget bool) error {
	// Create schema based on whether this is source or target
	schema := createSchema(config, isTarget)

	// Calculate batch size based on memory constraints - but limit it to 1000 for testing
	batchSize := calculateBatchSize(schema, config)
	if config.rowCount < 1000 {
		// For very small test files, use a small batch size
		batchSize = config.rowCount
	}

	// Calculate number of batches needed to reach desired size
	totalBatches := 1
	if config.rowCount > batchSize {
		totalBatches = (config.rowCount + batchSize - 1) / batchSize
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create Arrow writer
	mem := memory.NewGoAllocator()
	writer, err := pqarrow.NewFileWriter(schema, file, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Store source IDs if we're generating source file
	var sourceIDs []string
	sourceIDIndices := make(map[string]int)

	// For source file generation or target without overlap
	if !isTarget {
		// Generate data in batches
		uniqueIDs := make(map[string]bool)
		totalRows := 0

		for batchNum := 0; batchNum < totalBatches; batchNum++ {
			progress := float64(batchNum) / float64(totalBatches) * 100
			if batchNum%10 == 0 || batchNum == totalBatches-1 {
				log.Printf("  Progress: %.1f%% (batch %d/%d)", progress, batchNum+1, totalBatches)
			}

			// For the last batch, adjust batch size to match exactly the desired row count
			actualBatchSize := batchSize
			if batchNum == totalBatches-1 && config.rowCount%batchSize != 0 {
				actualBatchSize = config.rowCount % batchSize
				if actualBatchSize == 0 {
					actualBatchSize = batchSize
				}
			}

			// Generate a batch of data
			record, err := generateDataBatch(schema, actualBatchSize, uniqueIDs, rnd, config, isTarget, batchNum == 0)
			if err != nil {
				return fmt.Errorf("failed to generate data batch %d: %w", batchNum, err)
			}

			totalRows += int(record.NumRows())

			// Store IDs for potential reuse in target
			idCol := findColumnByName(record, "id")
			if idCol >= 0 {
				for i := 0; i < int(record.NumRows()); i++ {
					id := record.Column(idCol).(*array.String).Value(i)
					sourceIDs = append(sourceIDs, id)
					sourceIDIndices[id] = len(sourceIDs) - 1
				}
			}

			// Write the batch
			if err := writer.Write(record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write batch %d: %w", batchNum, err)
			}

			// Release the record
			record.Release()
		}

		// Store IDs to global variable for reuse in target
		globalSourceIDs = sourceIDs
		globalSourceIDIndices = sourceIDIndices

		log.Printf("Source file generated with %d actual rows", totalRows)

	} else {
		// For target file generation with overlap
		// Calculate overlap percentage based on diffRate
		overlapRate := 1.0 - config.diffRate

		// Get IDs from the previously generated source file
		if len(globalSourceIDs) == 0 {
			log.Println("Warning: No source IDs available, generating target without overlap")

			// Just generate a new file without overlap
			uniqueIDs := make(map[string]bool)
			for batchNum := 0; batchNum < totalBatches; batchNum++ {
				progress := float64(batchNum) / float64(totalBatches) * 100
				if batchNum%10 == 0 || batchNum == totalBatches-1 {
					log.Printf("  Progress: %.1f%% (batch %d/%d)", progress, batchNum+1, totalBatches)
				}

				// For the last batch, adjust batch size to match exactly the desired row count
				actualBatchSize := batchSize
				if batchNum == totalBatches-1 && config.rowCount%batchSize != 0 {
					actualBatchSize = config.rowCount % batchSize
					if actualBatchSize == 0 {
						actualBatchSize = batchSize
					}
				}

				record, err := generateDataBatch(schema, actualBatchSize, uniqueIDs, rnd, config, isTarget, batchNum == 0)
				if err != nil {
					return fmt.Errorf("failed to generate data batch %d: %w", batchNum, err)
				}

				// Write the batch
				if err := writer.Write(record); err != nil {
					record.Release()
					return fmt.Errorf("failed to write batch %d: %w", batchNum, err)
				}

				record.Release()
			}
		} else {
			// Determine how many IDs to reuse from source, but not more than the desired row count
			maxIDsToReuse := config.rowCount
			log.Printf("Source has %d IDs, target will reuse up to %d (%.1f%%) of them with modifications",
				len(globalSourceIDs), maxIDsToReuse, overlapRate*100)

			// Calculate actual number of IDs to reuse
			numIDsToReuse := int(float64(len(globalSourceIDs)) * overlapRate)
			if numIDsToReuse > maxIDsToReuse {
				numIDsToReuse = maxIDsToReuse
			}

			log.Printf("Will reuse %d IDs from source", numIDsToReuse)

			if numIDsToReuse > 0 {
				// Shuffle the source IDs to pick random ones to reuse
				reuseIndices := make([]int, len(globalSourceIDs))
				for i := range reuseIndices {
					reuseIndices[i] = i
				}

				// Shuffle
				for i := len(reuseIndices) - 1; i > 0; i-- {
					j := rnd.Intn(i + 1)
					reuseIndices[i], reuseIndices[j] = reuseIndices[j], reuseIndices[i]
				}

				// Take the first numIDsToReuse indices
				reuseIndices = reuseIndices[:numIDsToReuse]

				// Create map of IDs to reuse
				idsToReuse := make(map[string]bool)
				for _, idx := range reuseIndices {
					idsToReuse[globalSourceIDs[idx]] = true
				}

				// Generate target batches with reused IDs
				uniqueIDs := make(map[string]bool)

				// First, create records for reused IDs with modifications
				reusedIDs := make([]string, 0, numIDsToReuse)
				for id := range idsToReuse {
					reusedIDs = append(reusedIDs, id)
					uniqueIDs[id] = true
				}

				// Calculate batches for reused IDs, making sure not to exceed total rows
				reuseBatchSize := batchSize
				if reuseBatchSize > numIDsToReuse {
					reuseBatchSize = numIDsToReuse
				}

				reuseBatches := (numIDsToReuse + reuseBatchSize - 1) / reuseBatchSize
				log.Printf("Creating %d batches with reused IDs (batch size: %d)", reuseBatches, reuseBatchSize)

				for batchNum := 0; batchNum < reuseBatches; batchNum++ {
					progress := float64(batchNum) / float64(reuseBatches) * 100
					if batchNum%10 == 0 || batchNum == reuseBatches-1 {
						log.Printf("  Progress (reused IDs): %.1f%% (batch %d/%d)", progress, batchNum+1, reuseBatches)
					}

					// Calculate indices for this batch
					startIdx := batchNum * reuseBatchSize
					endIdx := startIdx + reuseBatchSize
					if endIdx > numIDsToReuse {
						endIdx = numIDsToReuse
					}

					batchIDs := reusedIDs[startIdx:endIdx]
					log.Printf("  Batch %d: reusing %d IDs", batchNum+1, len(batchIDs))

					record, err := generateModifiedBatch(schema, batchIDs, rnd, config)
					if err != nil {
						return fmt.Errorf("failed to generate modified batch %d: %w", batchNum, err)
					}

					// Write the batch
					if err := writer.Write(record); err != nil {
						record.Release()
						return fmt.Errorf("failed to write modified batch %d: %w", batchNum, err)
					}

					record.Release()
				}

				// Check if we need to generate additional new records
				newRecordsNeeded := config.rowCount - numIDsToReuse

				if newRecordsNeeded > 0 {
					newBatchSize := batchSize
					if newBatchSize > newRecordsNeeded {
						newBatchSize = newRecordsNeeded
					}

					newBatches := (newRecordsNeeded + newBatchSize - 1) / newBatchSize
					log.Printf("Creating %d batches with new IDs (batch size: %d, records needed: %d)",
						newBatches, newBatchSize, newRecordsNeeded)

					for batchNum := 0; batchNum < newBatches; batchNum++ {
						progress := float64(batchNum) / float64(newBatches) * 100
						if batchNum%10 == 0 || batchNum == newBatches-1 {
							log.Printf("  Progress (new IDs): %.1f%% (batch %d/%d)", progress, batchNum+1, newBatches)
						}

						// For the last batch, adjust batch size if needed
						actualBatchSize := newBatchSize
						if batchNum == newBatches-1 && newRecordsNeeded%newBatchSize != 0 {
							actualBatchSize = newRecordsNeeded % newBatchSize
							if actualBatchSize == 0 {
								actualBatchSize = newBatchSize
							}
						}

						log.Printf("  Batch %d: generating %d new records", batchNum+1, actualBatchSize)

						record, err := generateDataBatch(schema, actualBatchSize, uniqueIDs, rnd, config, isTarget, batchNum == 0)
						if err != nil {
							return fmt.Errorf("failed to generate new batch %d: %w", batchNum, err)
						}

						// Write the batch
						if err := writer.Write(record); err != nil {
							record.Release()
							return fmt.Errorf("failed to write new batch %d: %w", batchNum, err)
						}

						record.Release()
					}
				}
			} else {
				// Just generate a new file without overlap
				log.Println("No IDs to reuse (overlap is 0%), generating target without overlap")

				uniqueIDs := make(map[string]bool)
				for batchNum := 0; batchNum < totalBatches; batchNum++ {
					progress := float64(batchNum) / float64(totalBatches) * 100
					if batchNum%10 == 0 || batchNum == totalBatches-1 {
						log.Printf("  Progress: %.1f%% (batch %d/%d)", progress, batchNum+1, totalBatches)
					}

					// For the last batch, adjust batch size to match exactly the desired row count
					actualBatchSize := batchSize
					if batchNum == totalBatches-1 && config.rowCount%batchSize != 0 {
						actualBatchSize = config.rowCount % batchSize
						if actualBatchSize == 0 {
							actualBatchSize = batchSize
						}
					}

					record, err := generateDataBatch(schema, actualBatchSize, uniqueIDs, rnd, config, isTarget, batchNum == 0)
					if err != nil {
						return fmt.Errorf("failed to generate data batch %d: %w", batchNum, err)
					}

					// Write the batch
					if err := writer.Write(record); err != nil {
						record.Release()
						return fmt.Errorf("failed to write batch %d: %w", batchNum, err)
					}

					record.Release()
				}
			}
		}
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

// calculateBatchSize determines a reasonable batch size based on the schema
func calculateBatchSize(schema *arrow.Schema, config Config) int {
	// Estimate row size in bytes based on schema
	estimatedRowSizeBytes := estimateRowSize(schema)

	// Target batch size around 50MB to avoid excessive memory usage
	targetBatchSizeBytes := 50 * 1024 * 1024

	batchSize := targetBatchSizeBytes / estimatedRowSizeBytes
	if batchSize < 1000 {
		batchSize = 1000 // Minimum batch size
	}
	if batchSize > 1000000 {
		batchSize = 1000000 // Maximum batch size
	}

	return batchSize
}

// estimateRowSize provides a rough estimate of bytes per row based on the schema
func estimateRowSize(schema *arrow.Schema) int {
	size := 0
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		switch field.Type.ID() {
		case arrow.BOOL:
			size += 1
		case arrow.INT8, arrow.UINT8:
			size += 1
		case arrow.INT16, arrow.UINT16:
			size += 2
		case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
			size += 4
		case arrow.INT64, arrow.UINT64, arrow.FLOAT64, arrow.TIMESTAMP:
			size += 8
		case arrow.STRING, arrow.BINARY:
			size += 50 // Rough average length for strings
		case arrow.DECIMAL128:
			size += 16
		default:
			size += 8 // Default value for unknown types
		}
	}
	return size
}

// calculateBatchCount determines how many batches to generate
func calculateBatchCount(config Config, batchSize int) int {
	if config.sizeGB <= 0 {
		return config.rowCount/batchSize + 1
	}

	// Calculate based on desired file size
	targetSizeBytes := int64(config.sizeGB * 1024 * 1024 * 1024)
	rowsPerGB := 10000000 // Rough estimate, will vary based on schema

	totalRows := int(float64(targetSizeBytes) / 1024 / 1024 / 1024 * float64(rowsPerGB))
	return totalRows/batchSize + 1
}

// createSchema builds the Arrow schema for the data
func createSchema(config Config, isTarget bool) *arrow.Schema {
	// Base fields for both source and target
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "first_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "last_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "score", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "balance", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		{Name: "updated_at", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
	}

	// Add additional fields if we're generating the target file and it should have new columns
	if isTarget && config.targetHasNewCols {
		fields = append(fields,
			arrow.Field{Name: "state", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "lat", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			arrow.Field{Name: "long", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			arrow.Field{Name: "last_login", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		)
	}

	// Remove some fields if target should be missing columns
	if isTarget && config.targetMissingCols {
		// Remove a couple of fields
		fields = fields[:len(fields)-2]
	}

	// Create metadata
	metadata := arrow.NewMetadata(
		[]string{"created_by", "version", "timestamp"},
		[]string{"parity_test_generator", "1.0.0", time.Now().Format(time.RFC3339)},
	)

	return arrow.NewSchema(fields, &metadata)
}

// generateDataBatch creates a single batch of data
func generateDataBatch(
	schema *arrow.Schema,
	batchSize int,
	uniqueIDs map[string]bool,
	rnd *rand.Rand,
	config Config,
	isTarget bool,
	isFirstBatch bool,
) (arrow.Record, error) {
	// Create builders for each field
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		builders[i] = createBuilderForField(field.Type, mem)
	}

	// Generate data for each row
	for i := 0; i < batchSize; i++ {
		// Generate base values that will be used for most fields
		id := generateUniqueID(rnd, uniqueIDs)
		firstName := randomItemFromList(strings.Split(firstNames, ","), rnd)
		lastName := randomItemFromList(strings.Split(lastNames, ","), rnd)
		email := fmt.Sprintf("%s.%s@%s",
			strings.ToLower(firstName),
			strings.ToLower(lastName),
			randomItemFromList(strings.Split(domains, ","), rnd))
		age := rnd.Intn(80) + 18
		score := rnd.Float64() * 100
		balance := rnd.Float64() * 10000
		active := rnd.Float32() > 0.3
		status := randomItemFromList(strings.Split(statusValues, ","), rnd)
		createdAt := time.Now().Add(-time.Duration(rnd.Intn(1000)) * 24 * time.Hour)

		// Calculate days since creation, ensuring it's at least 1
		daysSinceCreation := int(time.Since(createdAt).Hours() / 24)
		if daysSinceCreation < 1 {
			daysSinceCreation = 1
		}
		updatedAt := createdAt.Add(time.Duration(rnd.Intn(daysSinceCreation)) * 24 * time.Hour)

		// Additional fields for target
		state := randomItemFromList(strings.Split(stateValues, ","), rnd)
		lat := rnd.Float64()*170 - 85
		long := rnd.Float64()*360 - 180
		lastLogin := updatedAt.Add(time.Duration(rnd.Intn(30)+1) * 24 * time.Hour)

		// Introduce errors in target file if needed
		if isTarget && rnd.Float64() < config.errorRate {
			switch rnd.Intn(4) {
			case 0:
				firstName += "X"
			case 1:
				age += rnd.Intn(5)
			case 2:
				score *= 1.1
			case 3:
				balance = -balance
			}
		}

		// Add values to builders
		for j := 0; j < schema.NumFields(); j++ {
			field := schema.Field(j)
			shouldBeNull := rnd.Float64() < config.nullRate && field.Nullable

			if shouldBeNull {
				appendNull(builders[j])
				continue
			}

			switch field.Name {
			case "id":
				builders[j].(*array.StringBuilder).Append(id)
			case "first_name":
				builders[j].(*array.StringBuilder).Append(firstName)
			case "last_name":
				builders[j].(*array.StringBuilder).Append(lastName)
			case "email":
				builders[j].(*array.StringBuilder).Append(email)
			case "age":
				builders[j].(*array.Int32Builder).Append(int32(age))
			case "score":
				appendDecimal(builders[j].(*array.Decimal128Builder), score)
			case "balance":
				builders[j].(*array.Float64Builder).Append(balance)
			case "active":
				builders[j].(*array.BooleanBuilder).Append(active)
			case "status":
				builders[j].(*array.StringBuilder).Append(status)
			case "created_at":
				builders[j].(*array.TimestampBuilder).Append(arrow.Timestamp(createdAt.UnixMilli()))
			case "updated_at":
				builders[j].(*array.TimestampBuilder).Append(arrow.Timestamp(updatedAt.UnixMilli()))
			case "state":
				builders[j].(*array.StringBuilder).Append(state)
			case "lat":
				builders[j].(*array.Float64Builder).Append(lat)
			case "long":
				builders[j].(*array.Float64Builder).Append(long)
			case "last_login":
				builders[j].(*array.TimestampBuilder).Append(arrow.Timestamp(lastLogin.UnixMilli()))
			}
		}
	}

	// Build column arrays
	cols := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		cols[i] = builder.NewArray()
		builder.Release()
	}

	// Create record batch
	return array.NewRecord(schema, cols, int64(batchSize)), nil
}

// generateModifiedBatch creates a batch with reused IDs but modified values
func generateModifiedBatch(schema *arrow.Schema, ids []string, rnd *rand.Rand, config Config) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		builders[i] = createBuilderForField(field.Type, mem)
	}

	// Process each ID
	for _, id := range ids {
		// For each field in the schema
		for i := 0; i < schema.NumFields(); i++ {
			field := schema.Field(i)
			shouldBeNull := rnd.Float64() < config.nullRate && field.Nullable

			if shouldBeNull {
				appendNull(builders[i])
				continue
			}

			if field.Name == "id" {
				// Keep the ID the same
				builders[i].(*array.StringBuilder).Append(id)
				continue
			}

			// For other fields, generate new values with some randomness
			// Use ID to get a consistent source of randomness for this row
			rowRnd := rand.New(rand.NewSource(rnd.Int63()))

			// Generate values similar to generateDataBatch but with some modifications
			switch field.Name {
			case "first_name":
				firstName := randomItemFromList(strings.Split(firstNames, ","), rowRnd)
				// Sometimes modify the name
				if rowRnd.Float64() < 0.5 {
					firstName += "X" // Simple modification for testing
				}
				builders[i].(*array.StringBuilder).Append(firstName)
			case "last_name":
				lastName := randomItemFromList(strings.Split(lastNames, ","), rowRnd)
				builders[i].(*array.StringBuilder).Append(lastName)
			case "email":
				email := fmt.Sprintf("modified_%s@example.com", id)
				builders[i].(*array.StringBuilder).Append(email)
			case "age":
				age := rowRnd.Intn(80) + 18
				// Sometimes modify the age
				if rowRnd.Float64() < 0.5 {
					age += rowRnd.Intn(5)
				}
				builders[i].(*array.Int32Builder).Append(int32(age))
			case "score":
				score := rowRnd.Float64() * 100
				appendDecimal(builders[i].(*array.Decimal128Builder), score)
			case "balance":
				balance := rowRnd.Float64() * 10000
				builders[i].(*array.Float64Builder).Append(balance)
			case "active":
				active := rowRnd.Float32() > 0.3
				builders[i].(*array.BooleanBuilder).Append(active)
			case "status":
				status := randomItemFromList(strings.Split(statusValues, ","), rowRnd)
				builders[i].(*array.StringBuilder).Append(status)
			case "created_at":
				createdAt := time.Now().Add(-time.Duration(rowRnd.Intn(1000)) * 24 * time.Hour)
				builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(createdAt.UnixMilli()))
			case "updated_at":
				updatedAt := time.Now().Add(-time.Duration(rowRnd.Intn(100)) * 24 * time.Hour)
				builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(updatedAt.UnixMilli()))
			case "state":
				state := randomItemFromList(strings.Split(stateValues, ","), rowRnd)
				builders[i].(*array.StringBuilder).Append(state)
			case "lat":
				lat := rowRnd.Float64()*170 - 85
				builders[i].(*array.Float64Builder).Append(lat)
			case "long":
				long := rowRnd.Float64()*360 - 180
				builders[i].(*array.Float64Builder).Append(long)
			case "last_login":
				lastLogin := time.Now().Add(-time.Duration(rowRnd.Intn(30)) * 24 * time.Hour)
				builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(lastLogin.UnixMilli()))
			default:
				// For unknown fields, append null
				appendNull(builders[i])
			}
		}
	}

	// Build arrays
	cols := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		cols[i] = builder.NewArray()
		builder.Release()
	}

	// Create record
	return array.NewRecord(schema, cols, int64(len(ids))), nil
}

// findColumnByName returns the index of a column by name, or -1 if not found
func findColumnByName(record arrow.Record, colName string) int {
	for i := 0; i < int(record.NumCols()); i++ {
		if record.Schema().Field(i).Name == colName {
			return i
		}
	}
	return -1
}

// createBuilderForField creates an appropriate builder for the field type
func createBuilderForField(dataType arrow.DataType, mem memory.Allocator) array.Builder {
	switch dataType.ID() {
	case arrow.STRING:
		return array.NewStringBuilder(mem)
	case arrow.INT32:
		return array.NewInt32Builder(mem)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(mem)
	case arrow.BOOL:
		return array.NewBooleanBuilder(mem)
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(mem, dataType.(*arrow.TimestampType))
	case arrow.DECIMAL128:
		return array.NewDecimal128Builder(mem, dataType.(*arrow.Decimal128Type))
	default:
		// This would panic if we've missed a type
		return array.NewBuilder(mem, dataType)
	}
}

// appendNull adds a null value to any builder type
func appendNull(builder array.Builder) {
	builder.AppendNull()
}

// appendValue copies a value from a source array to a builder
func appendValue(builder array.Builder, arr arrow.Array, idx int) {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return
	}

	switch bldr := builder.(type) {
	case *array.StringBuilder:
		bldr.Append(arr.(*array.String).Value(idx))
	case *array.Int32Builder:
		bldr.Append(arr.(*array.Int32).Value(idx))
	case *array.Float64Builder:
		bldr.Append(arr.(*array.Float64).Value(idx))
	case *array.BooleanBuilder:
		bldr.Append(arr.(*array.Boolean).Value(idx))
	case *array.TimestampBuilder:
		bldr.Append(arr.(*array.Timestamp).Value(idx))
	case *array.Decimal128Builder:
		bldr.Append(arr.(*array.Decimal128).Value(idx))
	default:
		// This would be a programming error
		builder.AppendNull()
	}
}

// appendDecimal adds a decimal value from a float64
func appendDecimal(builder *array.Decimal128Builder, value float64) {
	// Convert to decimal representation
	// This is a simple implementation that may lose precision
	decType := builder.Type().(*arrow.Decimal128Type)
	scale := decType.Scale
	scaleFactor := int64(1)
	for i := 0; i < int(scale); i++ {
		scaleFactor *= 10
	}

	intVal := int64(value * float64(scaleFactor))
	dec := decimal128.New(0, uint64(intVal))
	builder.Append(dec)
}

// generateUniqueID creates a unique ID
func generateUniqueID(rnd *rand.Rand, uniqueIDs map[string]bool) string {
	var id string
	for {
		id = uuid.New().String()
		if !uniqueIDs[id] {
			uniqueIDs[id] = true
			return id
		}
	}
}

// randomItemFromList returns a random item from a string slice
func randomItemFromList(items []string, rnd *rand.Rand) string {
	return items[rnd.Intn(len(items))]
}

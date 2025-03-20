package diff

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetLineitems(t *testing.T) {
	// Skip this test for quick runs
	if testing.Short() {
		t.Skip("Skipping lineitem parquet test in short mode")
	}

	// Paths to test parquet files
	sourcePath := filepath.Join("..", "..", "data", "lineitem1.parquet")
	targetPath := filepath.Join("..", "..", "data", "lineitem2.parquet")

	// Create readers for source and target
	sourceConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      sourcePath,
		BatchSize: 10000,
	}
	sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
	require.NoError(t, err)
	defer sourceReader.Close()

	targetConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      targetPath,
		BatchSize: 10000,
	}
	targetReader, err := readers.DefaultFactory.Create(targetConfig)
	require.NoError(t, err)
	defer targetReader.Close()

	// Create ArrowDiffer
	differ, err := NewArrowDiffer()
	require.NoError(t, err)
	defer differ.Close()

	// Set diff options
	diffOptions := core.DiffOptions{
		KeyColumns:    []string{"l_orderkey", "l_linenumber"},
		IgnoreColumns: []string{},
		BatchSize:     10000,
		Tolerance:     0.0001,
		Parallel:      true,
		NumWorkers:    4,
	}

	// Measure performance
	start := time.Now()

	// Run the diff
	ctx := context.Background()
	result, err := differ.Diff(ctx, sourceReader, targetReader, diffOptions)
	require.NoError(t, err)

	// Log performance
	elapsed := time.Since(start)
	t.Logf("Diff completed in %s", elapsed)

	// Basic assertions on the result
	assert.NotNil(t, result)
	assert.Equal(t, int64(441482), result.Summary.TotalSource)
	assert.Equal(t, int64(441505), result.Summary.TotalTarget)

	// Log summary
	t.Logf("Source records: %d", result.Summary.TotalSource)
	t.Logf("Target records: %d", result.Summary.TotalTarget)
	t.Logf("Added records: %d", result.Summary.Added)
	t.Logf("Deleted records: %d", result.Summary.Deleted)
	t.Logf("Modified records: %d", result.Summary.Modified)
}

// BenchmarkDiff benchmarks the Diff function on the lineitem Parquet files
func BenchmarkDiff(b *testing.B) {
	// Paths to test parquet files
	sourcePath := filepath.Join("..", "..", "data", "lineitem1.parquet")
	targetPath := filepath.Join("..", "..", "data", "lineitem2.parquet")

	// Create readers for source and target
	sourceConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      sourcePath,
		BatchSize: 10000,
	}

	targetConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      targetPath,
		BatchSize: 10000,
	}

	// Set diff options
	diffOptions := core.DiffOptions{
		KeyColumns:    []string{"l_orderkey", "l_linenumber"},
		IgnoreColumns: []string{},
		BatchSize:     10000,
		Tolerance:     0.0001,
		Parallel:      true,
		NumWorkers:    4,
	}

	// Reset the timer for each iteration
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create new readers for each iteration
		sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
		if err != nil {
			b.Fatalf("Failed to create source reader: %v", err)
		}

		targetReader, err := readers.DefaultFactory.Create(targetConfig)
		if err != nil {
			sourceReader.Close()
			b.Fatalf("Failed to create target reader: %v", err)
		}

		// Create ArrowDiffer
		differ, err := NewArrowDiffer()
		if err != nil {
			sourceReader.Close()
			targetReader.Close()
			b.Fatalf("Failed to create differ: %v", err)
		}

		ctx := context.Background()
		b.StartTimer()

		// Run the diff
		result, err := differ.Diff(ctx, sourceReader, targetReader, diffOptions)
		if err != nil {
			b.Fatalf("Diff failed: %v", err)
		}

		b.StopTimer()
		// Clean up
		sourceReader.Close()
		targetReader.Close()
		differ.Close()

		// Ensure result is not optimized away
		if result == nil {
			b.Fatalf("Unexpected nil result")
		}
	}
}

// BenchmarkDiffStreaming benchmarks the Diff function with streaming mode
func BenchmarkDiffStreaming(b *testing.B) {
	// Skip in CI
	if os.Getenv("CI") != "" {
		b.Skip("Skipping streaming diff benchmark in CI")
	}

	// Paths to test parquet files
	sourcePath := filepath.Join("..", "..", "data", "lineitem1.parquet")
	targetPath := filepath.Join("..", "..", "data", "lineitem2.parquet")

	// Create readers for source and target with streaming configuration
	sourceConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      sourcePath,
		BatchSize: 1000, // Smaller batch size for streaming
	}

	targetConfig := core.ReaderConfig{
		Type:      "parquet",
		Path:      targetPath,
		BatchSize: 1000, // Smaller batch size for streaming
	}

	// Set diff options
	diffOptions := core.DiffOptions{
		KeyColumns:    []string{"l_orderkey", "l_linenumber"},
		IgnoreColumns: []string{},
		BatchSize:     1000,
		Tolerance:     0.0001,
		Parallel:      true,
		NumWorkers:    4,
	}

	// Reset the timer for each iteration
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create new readers for each iteration
		sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
		if err != nil {
			b.Fatalf("Failed to create source reader: %v", err)
		}

		targetReader, err := readers.DefaultFactory.Create(targetConfig)
		if err != nil {
			sourceReader.Close()
			b.Fatalf("Failed to create target reader: %v", err)
		}

		// Create ArrowDiffer
		differ, err := NewArrowDiffer()
		if err != nil {
			sourceReader.Close()
			targetReader.Close()
			b.Fatalf("Failed to create differ: %v", err)
		}

		ctx := context.Background()
		b.StartTimer()

		// Run the diff
		result, err := differ.Diff(ctx, sourceReader, targetReader, diffOptions)
		if err != nil {
			b.Fatalf("Diff failed: %v", err)
		}

		b.StopTimer()
		// Clean up
		sourceReader.Close()
		targetReader.Close()
		differ.Close()

		// Ensure result is not optimized away
		if result == nil {
			b.Fatalf("Unexpected nil result")
		}
	}
}

// BenchmarkDiffFullLoad benchmarks the Diff function with full loading mode
func BenchmarkDiffFullLoad(b *testing.B) {
	// Paths to test parquet files
	sourcePath := filepath.Join("..", "..", "data", "lineitem1.parquet")
	targetPath := filepath.Join("..", "..", "data", "lineitem2.parquet")

	// Create full load wrapper for readall
	fullLoadWrapper := func(reader core.DatasetReader) core.DatasetReader {
		return &fullLoadReaderForTest{reader: reader}
	}

	// Reset the timer for each iteration
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create new readers for each iteration
		sourceConfig := core.ReaderConfig{
			Type:      "parquet",
			Path:      sourcePath,
			BatchSize: 100000, // Larger batch size for full load
		}
		sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
		if err != nil {
			b.Fatalf("Failed to create source reader: %v", err)
		}

		targetConfig := core.ReaderConfig{
			Type:      "parquet",
			Path:      targetPath,
			BatchSize: 100000, // Larger batch size for full load
		}
		targetReader, err := readers.DefaultFactory.Create(targetConfig)
		if err != nil {
			sourceReader.Close()
			b.Fatalf("Failed to create target reader: %v", err)
		}

		// Wrap readers to use ReadAll
		wrappedSourceReader := fullLoadWrapper(sourceReader)
		wrappedTargetReader := fullLoadWrapper(targetReader)

		// Create ArrowDiffer
		differ, err := NewArrowDiffer()
		if err != nil {
			sourceReader.Close()
			targetReader.Close()
			b.Fatalf("Failed to create differ: %v", err)
		}

		// Set diff options
		diffOptions := core.DiffOptions{
			KeyColumns:    []string{"l_orderkey", "l_linenumber"},
			IgnoreColumns: []string{},
			BatchSize:     100000,
			Tolerance:     0.0001,
			Parallel:      true,
			NumWorkers:    4,
		}

		ctx := context.Background()
		b.StartTimer()

		// Run the diff
		result, err := differ.Diff(ctx, wrappedSourceReader, wrappedTargetReader, diffOptions)
		if err != nil {
			b.Fatalf("Diff failed: %v", err)
		}

		b.StopTimer()
		// Clean up
		wrappedSourceReader.Close()
		wrappedTargetReader.Close()
		differ.Close()

		// Ensure result is not optimized away
		if result == nil {
			b.Fatalf("Unexpected nil result")
		}
	}
}

// fullLoadReaderForTest is a wrapper that forces the use of ReadAll for a reader
type fullLoadReaderForTest struct {
	reader     core.DatasetReader
	loadedData arrow.Record
	read       bool
}

func (f *fullLoadReaderForTest) Read(ctx context.Context) (arrow.Record, error) {
	if f.loadedData == nil {
		data, err := f.reader.ReadAll(ctx)
		if err != nil {
			return nil, err
		}
		f.loadedData = data
	}

	if f.read {
		return nil, io.EOF
	}

	f.read = true
	return f.loadedData, nil
}

func (f *fullLoadReaderForTest) ReadAll(ctx context.Context) (arrow.Record, error) {
	if f.loadedData == nil {
		data, err := f.reader.ReadAll(ctx)
		if err != nil {
			return nil, err
		}
		f.loadedData = data
	}

	return f.loadedData, nil
}

func (f *fullLoadReaderForTest) Schema() *arrow.Schema {
	return f.reader.Schema()
}

func (f *fullLoadReaderForTest) Close() error {
	if f.loadedData != nil {
		f.loadedData.Release()
	}
	return f.reader.Close()
}

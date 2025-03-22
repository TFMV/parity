package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/diff"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/TFMV/parity/pkg/writers"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"
)

// DiffOptions represents the options for the diff command.
type DiffOptions struct {
	SourcePath    string
	TargetPath    string
	SourceType    string
	TargetType    string
	KeyColumns    []string
	IgnoreColumns []string
	OutputPath    string
	OutputFormat  string
	Tolerance     float64
	Parallel      bool
	BatchSize     int64
	NumWorkers    int
	FullLoad      bool
	Stream        bool
	DifferType    string
}

// DiffCommand represents the diff command
type DiffCommand struct {
	Source           string   `help:"Source Parquet file" required:""`
	Target           string   `help:"Target Parquet file" required:""`
	KeyColumns       []string `help:"Key columns for record matching (required for large files)" short:"k" placeholder:"COLUMN"`
	BatchSize        int      `help:"Number of rows to process in each batch" default:"1000" short:"b"`
	IgnoreColumns    []string `help:"Columns to ignore in the comparison" short:"i" placeholder:"COLUMN"`
	Tolerance        float64  `help:"Floating point comparison tolerance" default:"0.0001"`
	Differ           string   `help:"Differ type (arrow, parquet, stream)" default:"arrow" enum:"arrow,parquet,stream"`
	FullLoad         bool     `help:"Load entire datasets into memory" default:"false"`
	Streaming        bool     `help:"Stream records instead of loading all at once" default:"false"`
	Parallelism      int      `help:"Number of worker threads" default:"4" short:"p"`
	OutputFormat     string   `help:"Output format (text, csv, json, parquet)" default:"text" enum:"text,csv,json,parquet" short:"o"`
	OutputPath       string   `help:"Output path (defaults to stdout for text output)" default:""`
	SummaryOnly      bool     `help:"Only output summary information" default:"false"`
	DetailedSummary  bool     `help:"Include more detailed information in summary" default:"false"`
	DisplayLimit     int      `help:"Limit the number of records to display" default:"100"`
	DisplayBatchSize int      `help:"Number of records to display at once" default:"20"`
}

// newDiffCommand creates a new diff command.
func newDiffCommand() *cobra.Command {
	options := &DiffOptions{
		SourceType:   "auto",
		TargetType:   "auto",
		OutputFormat: "parquet",
		Tolerance:    0.0001,
		Parallel:     true,
		BatchSize:    10000,
		NumWorkers:   4,
		FullLoad:     false,
		Stream:       false,
		DifferType:   "arrow",
	}

	cmd := &cobra.Command{
		Use:   "diff [flags] SOURCE TARGET",
		Short: "Compare two datasets and compute differences",
		Long: `The diff command compares two datasets and computes the differences between them.
		
It supports various input formats (Parquet, Arrow, CSV, database) and can output
the differences in multiple formats (Parquet, Arrow, JSON, Markdown, HTML).

Performance Tips:
- Use --key to specify key columns for efficient record matching (IMPORTANT for large files)
- Use --batch-size to control memory usage (lower for less memory, higher for speed)
- Use --stream to process in batches (good for very large files)
- Use --full-load for smaller datasets to improve performance`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Get source and target paths
			options.SourcePath = args[0]
			options.TargetPath = args[1]

			// Auto-detect source and target types if not specified
			if options.SourceType == "auto" {
				options.SourceType = detectType(options.SourcePath)
			}
			if options.TargetType == "auto" {
				options.TargetType = detectType(options.TargetPath)
			}

			// Validate mutually exclusive options
			if options.FullLoad && options.Stream {
				return fmt.Errorf("--full-load and --stream cannot be used together")
			}

			// Print warnings for common issues
			if len(options.KeyColumns) == 0 {
				fmt.Println("WARNING: No key columns specified. This may severely impact performance with large files.")
				fmt.Println("Hint: Specify key columns with --key for faster diffing, e.g., --key id,customer_id")
			}

			// Run diff
			return runDiff(options)
		},
	}

	// Add flags
	cmd.Flags().StringVar(&options.SourceType, "source-type", options.SourceType, "Source dataset type (parquet, arrow, csv, auto)")
	cmd.Flags().StringVar(&options.TargetType, "target-type", options.TargetType, "Target dataset type (parquet, arrow, csv, auto)")
	cmd.Flags().StringSliceVar(&options.KeyColumns, "key", nil, "Key columns to match records (highly recommended for large files)")
	cmd.Flags().StringSliceVar(&options.IgnoreColumns, "ignore", nil, "Columns to ignore in comparison")
	cmd.Flags().StringVarP(&options.OutputPath, "output", "o", "", "Output path for diff results")
	cmd.Flags().StringVarP(&options.OutputFormat, "format", "f", options.OutputFormat, "Output format (parquet, arrow, json)")
	cmd.Flags().Float64Var(&options.Tolerance, "tolerance", options.Tolerance, "Tolerance for floating point comparisons")
	cmd.Flags().BoolVar(&options.Parallel, "parallel", options.Parallel, "Use parallel processing")
	cmd.Flags().Int64VarP(&options.BatchSize, "batch-size", "b", options.BatchSize, "Batch size for processing")
	cmd.Flags().IntVar(&options.NumWorkers, "workers", options.NumWorkers, "Number of worker threads for parallel processing")
	cmd.Flags().BoolVar(&options.FullLoad, "full-load", options.FullLoad, "Load entire datasets into memory for faster processing")
	cmd.Flags().BoolVar(&options.Stream, "stream", options.Stream, "Process datasets in streaming mode to minimize memory usage")
	cmd.Flags().StringVar(&options.DifferType, "differ", options.DifferType, "Type of differ to use (arrow)")

	return cmd
}

// runDiff executes the diff command with the given options.
func runDiff(options *DiffOptions) error {
	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		fmt.Println("\nCancelling operation...")
		cancel()
	}()

	// Print information about the diffing operation
	fmt.Printf("Diffing:\n  Source: %s\n  Target: %s\n", options.SourcePath, options.TargetPath)

	if len(options.KeyColumns) > 0 {
		fmt.Printf("Using key columns: %s\n", strings.Join(options.KeyColumns, ", "))
	}

	fmt.Printf("Batch size: %d rows\n", options.BatchSize)

	// Set default memory management strategy if neither is specified
	if !options.FullLoad && !options.Stream {
		// By default, we'll use streaming for large batch sizes and full load for small ones
		if options.BatchSize > 100000 {
			options.Stream = true
			fmt.Println("Using streaming mode for large batch size")
		} else {
			options.FullLoad = true
			fmt.Println("Using full load mode for small batch size")
		}
	}

	// Create source reader
	sourceConfig := core.ReaderConfig{
		Type:      options.SourceType,
		Path:      options.SourcePath,
		BatchSize: options.BatchSize,
	}
	sourceReader, err := readers.DefaultFactory.Create(sourceConfig)
	if err != nil {
		return fmt.Errorf("failed to create source reader: %w", err)
	}
	defer sourceReader.Close()

	// Create target reader
	targetConfig := core.ReaderConfig{
		Type:      options.TargetType,
		Path:      options.TargetPath,
		BatchSize: options.BatchSize,
	}
	targetReader, err := readers.DefaultFactory.Create(targetConfig)
	if err != nil {
		return fmt.Errorf("failed to create target reader: %w", err)
	}
	defer targetReader.Close()

	// Create differ based on selected type
	var differ core.Differ
	switch options.DifferType {
	case "arrow":
		differ, err = diff.NewArrowDiffer()
		if err != nil {
			return fmt.Errorf("failed to create Arrow differ: %w", err)
		}
	default:
		return fmt.Errorf("unsupported differ type: %s", options.DifferType)
	}

	defer differ.Close()

	// Configure diff options
	diffOptions := core.DiffOptions{
		KeyColumns:    options.KeyColumns,
		IgnoreColumns: options.IgnoreColumns,
		BatchSize:     options.BatchSize,
		Tolerance:     options.Tolerance,
		Parallel:      options.Parallel,
		NumWorkers:    options.NumWorkers,
	}

	// If using full load mode, wrap the readers to use ReadAll
	var sourceReaderToUse, targetReaderToUse core.DatasetReader

	if options.FullLoad {
		fmt.Println("Loading datasets fully into memory...")

		// Create wrappers that use ReadAll
		sourceReaderToUse = &fullLoadReader{reader: sourceReader}
		targetReaderToUse = &fullLoadReader{reader: targetReader}
	} else if options.Stream {
		fmt.Println("Using streaming mode to minimize memory usage...")

		// Use the original readers directly
		sourceReaderToUse = sourceReader
		targetReaderToUse = targetReader
	} else {
		// Let the differ decide based on its detection of ReadAll support
		sourceReaderToUse = sourceReader
		targetReaderToUse = targetReader
	}

	// Compute diff
	fmt.Println("Computing differences...")
	result, err := differ.Diff(ctx, sourceReaderToUse, targetReaderToUse, diffOptions)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
	}

	// Print summary to stderr if we're using JSON format output to stdout
	if options.OutputFormat == "json" && options.OutputPath == "" {
		// In case of JSON format with no output file, print summary to stderr
		printSummaryToStream(result, os.Stderr)
	} else {
		// Otherwise print to stdout
		printSummary(result)
	}

	// Handle output - either to file or stdout
	if options.OutputPath != "" {
		// Write to specified output file
		if err := writeOutput(ctx, result, options); err != nil {
			return fmt.Errorf("failed to write output: %w", err)
		}
	} else if options.OutputFormat == "json" {
		// Write JSON to stdout if format is JSON and no output file specified
		if err := writeJSONToStdout(ctx, result); err != nil {
			return fmt.Errorf("failed to write JSON to stdout: %w", err)
		}
	}

	return nil
}

// fullLoadReader is a wrapper that forces the use of ReadAll for any reader
type fullLoadReader struct {
	reader     core.DatasetReader
	loadedData arrow.Record
	read       bool
}

func (f *fullLoadReader) Read(ctx context.Context) (arrow.Record, error) {
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

func (f *fullLoadReader) ReadAll(ctx context.Context) (arrow.Record, error) {
	if f.loadedData == nil {
		data, err := f.reader.ReadAll(ctx)
		if err != nil {
			return nil, err
		}
		f.loadedData = data
	}

	return f.loadedData, nil
}

func (f *fullLoadReader) Schema() *arrow.Schema {
	return f.reader.Schema()
}

func (f *fullLoadReader) Close() error {
	if f.loadedData != nil {
		f.loadedData.Release()
	}
	return f.reader.Close()
}

// printSummary prints a summary of the diff results to stdout.
func printSummary(result *core.DiffResult) {
	printSummaryToStream(result, os.Stdout)
}

// printSummaryToStream prints a summary of the diff results to the specified writer.
func printSummaryToStream(result *core.DiffResult, writer io.Writer) {
	summary := result.Summary
	fmt.Fprintln(writer, "\nDiff Summary:")
	fmt.Fprintf(writer, "  Source records: %d\n", summary.TotalSource)
	fmt.Fprintf(writer, "  Target records: %d\n", summary.TotalTarget)
	fmt.Fprintf(writer, "  Added records:   %d\n", summary.Added)
	fmt.Fprintf(writer, "  Deleted records: %d\n", summary.Deleted)
	fmt.Fprintf(writer, "  Modified records: %d\n", summary.Modified)

	if len(summary.Columns) > 0 {
		fmt.Fprintln(writer, "\nModified columns:")
		for col, count := range summary.Columns {
			fmt.Fprintf(writer, "  %s: %d modifications\n", col, count)
		}
	}
}

// writeJSONToStdout writes the diff results to stdout in JSON format.
func writeJSONToStdout(ctx context.Context, result *core.DiffResult) error {
	// Create a custom stdout writer
	writer := &stdoutJSONWriter{
		encoder:  json.NewEncoder(os.Stdout),
		isArray:  true,
		firstRow: true,
	}
	writer.encoder.SetIndent("", "  ")

	// Write opening bracket for array
	if _, err := os.Stdout.WriteString("[\n"); err != nil {
		return fmt.Errorf("failed to write opening bracket: %w", err)
	}

	// Write records
	if result.Added != nil && result.Added.NumRows() > 0 {
		if err := writer.Write(ctx, result.Added); err != nil {
			return fmt.Errorf("failed to write added records: %w", err)
		}
	}

	if result.Deleted != nil && result.Deleted.NumRows() > 0 {
		if err := writer.Write(ctx, result.Deleted); err != nil {
			return fmt.Errorf("failed to write deleted records: %w", err)
		}
	}

	if result.Modified != nil && result.Modified.NumRows() > 0 {
		if err := writer.Write(ctx, result.Modified); err != nil {
			return fmt.Errorf("failed to write modified records: %w", err)
		}
	}

	// Write closing bracket for array
	if _, err := os.Stdout.WriteString("\n]"); err != nil {
		return fmt.Errorf("failed to write closing bracket: %w", err)
	}

	return nil
}

// stdoutJSONWriter implements a JSON writer for stdout.
type stdoutJSONWriter struct {
	encoder  *json.Encoder
	isArray  bool
	firstRow bool
}

// Write writes a record to stdout in JSON format.
func (w *stdoutJSONWriter) Write(ctx context.Context, record arrow.Record) error {
	// Check if context is canceled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Convert records to JSON
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// Iterate through rows
	for i := 0; i < numRows; i++ {
		// Create a map for the row
		row := make(map[string]interface{})

		// Add field values
		for j := 0; j < numCols; j++ {
			col := record.Column(j)
			field := record.Schema().Field(j)

			// Get value based on type
			var value interface{}
			switch col := col.(type) {
			case *array.Int8:
				value = col.Value(i)
			case *array.Int16:
				value = col.Value(i)
			case *array.Int32:
				value = col.Value(i)
			case *array.Int64:
				value = col.Value(i)
			case *array.Uint8:
				value = col.Value(i)
			case *array.Uint16:
				value = col.Value(i)
			case *array.Uint32:
				value = col.Value(i)
			case *array.Uint64:
				value = col.Value(i)
			case *array.Float32:
				value = col.Value(i)
			case *array.Float64:
				value = col.Value(i)
			case *array.Boolean:
				value = col.Value(i)
			case *array.String:
				value = col.Value(i)
			default:
				value = nil
			}

			row[field.Name] = value
		}

		// If not the first row, write a comma
		if !w.firstRow {
			if _, err := os.Stdout.WriteString(",\n"); err != nil {
				return fmt.Errorf("failed to write comma: %w", err)
			}
		} else {
			w.firstRow = false
		}

		// Write the row
		if err := w.encoder.Encode(row); err != nil {
			return fmt.Errorf("failed to encode row: %w", err)
		}
	}

	return nil
}

// writeOutput writes the diff results to the specified output format.
func writeOutput(ctx context.Context, result *core.DiffResult, options *DiffOptions) error {
	// Create writer
	writerConfig := core.WriterConfig{
		Type: options.OutputFormat,
		Path: options.OutputPath,
	}
	writer, err := writers.DefaultFactory.Create(writerConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer writer.Close()

	// Write added records
	if result.Added != nil && result.Added.NumRows() > 0 {
		fmt.Println("Writing added records...")
		if err := writer.Write(ctx, result.Added); err != nil {
			return fmt.Errorf("failed to write added records: %w", err)
		}
	}

	// Write deleted records
	if result.Deleted != nil && result.Deleted.NumRows() > 0 {
		fmt.Println("Writing deleted records...")
		if err := writer.Write(ctx, result.Deleted); err != nil {
			return fmt.Errorf("failed to write deleted records: %w", err)
		}
	}

	// Write modified records
	if result.Modified != nil && result.Modified.NumRows() > 0 {
		fmt.Println("Writing modified records...")
		if err := writer.Write(ctx, result.Modified); err != nil {
			return fmt.Errorf("failed to write modified records: %w", err)
		}
	}

	fmt.Printf("Diff results written to %s\n", options.OutputPath)
	return nil
}

// detectType detects the type of a file based on its extension.
func detectType(path string) string {
	lowercase := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lowercase, ".parquet"):
		return "parquet"
	case strings.HasSuffix(lowercase, ".arrow"):
		return "arrow"
	case strings.HasSuffix(lowercase, ".csv"):
		return "csv"
	case strings.HasSuffix(lowercase, ".db") || strings.HasSuffix(lowercase, ".duckdb"):
		return "duckdb"
	default:
		// Default to parquet
		return "parquet"
	}
}

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/TFMV/parity/pkg/core"
	"github.com/TFMV/parity/pkg/diff"
	"github.com/TFMV/parity/pkg/readers"
	"github.com/TFMV/parity/pkg/writers"
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
	}

	cmd := &cobra.Command{
		Use:   "diff [flags] SOURCE TARGET",
		Short: "Compare two datasets and compute differences",
		Long: `The diff command compares two datasets and computes the differences between them.
		
It supports various input formats (Parquet, Arrow, CSV, database) and can output
the differences in multiple formats (Parquet, Arrow, JSON, Markdown, HTML).`,
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

			// Run diff
			return runDiff(options)
		},
	}

	// Add flags
	cmd.Flags().StringVar(&options.SourceType, "source-type", options.SourceType, "Source dataset type (parquet, arrow, csv, duckdb, auto)")
	cmd.Flags().StringVar(&options.TargetType, "target-type", options.TargetType, "Target dataset type (parquet, arrow, csv, duckdb, auto)")
	cmd.Flags().StringSliceVar(&options.KeyColumns, "key", nil, "Key columns to match records")
	cmd.Flags().StringSliceVar(&options.IgnoreColumns, "ignore", nil, "Columns to ignore in comparison")
	cmd.Flags().StringVarP(&options.OutputPath, "output", "o", "", "Output path for diff results")
	cmd.Flags().StringVarP(&options.OutputFormat, "format", "f", options.OutputFormat, "Output format (parquet, arrow, json, markdown, html)")
	cmd.Flags().Float64Var(&options.Tolerance, "tolerance", options.Tolerance, "Tolerance for floating point comparisons")
	cmd.Flags().BoolVar(&options.Parallel, "parallel", options.Parallel, "Use parallel processing")
	cmd.Flags().Int64Var(&options.BatchSize, "batch-size", options.BatchSize, "Batch size for processing")

	return cmd
}

// runDiff runs the diff operation with the given options.
func runDiff(options *DiffOptions) error {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		fmt.Println("Received signal, cancelling...")
		cancel()
	}()

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

	// Create differ
	differ, err := diff.NewDuckDBDiffer()
	if err != nil {
		return fmt.Errorf("failed to create differ: %w", err)
	}
	defer differ.Close()

	// Configure diff options
	diffOptions := core.DiffOptions{
		KeyColumns:    options.KeyColumns,
		IgnoreColumns: options.IgnoreColumns,
		BatchSize:     options.BatchSize,
		Tolerance:     options.Tolerance,
		Parallel:      options.Parallel,
	}

	// Compute diff
	fmt.Println("Computing differences...")
	result, err := differ.Diff(ctx, sourceReader, targetReader, diffOptions)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
	}

	// Print summary
	printSummary(result)

	// Write output if requested
	if options.OutputPath != "" {
		if err := writeOutput(ctx, result, options); err != nil {
			return fmt.Errorf("failed to write output: %w", err)
		}
	}

	return nil
}

// printSummary prints a summary of the diff results.
func printSummary(result *core.DiffResult) {
	summary := result.Summary
	fmt.Println("\nDiff Summary:")
	fmt.Printf("  Source records: %d\n", summary.TotalSource)
	fmt.Printf("  Target records: %d\n", summary.TotalTarget)
	fmt.Printf("  Added records:   %d\n", summary.Added)
	fmt.Printf("  Deleted records: %d\n", summary.Deleted)
	fmt.Printf("  Modified records: %d\n", summary.Modified)

	if len(summary.Columns) > 0 {
		fmt.Println("\nModified columns:")
		for col, count := range summary.Columns {
			fmt.Printf("  %s: %d modifications\n", col, count)
		}
	}
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

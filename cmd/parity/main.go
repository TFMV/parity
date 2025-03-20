// Package main provides the entry point for the Parity dataset comparison tool.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Main entry point for the Parity tool
func main() {
	// Create root command
	rootCmd := &cobra.Command{
		Use:   "parity",
		Short: "Parity is a high-performance dataset comparison tool",
		Long: `Parity is a dataset comparison tool for large datasets.
It leverages Apache Arrow and DuckDB to efficiently compute differences
between datasets in various formats (Parquet, Arrow, CSV, databases).`,
	}

	// Add version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number of Parity",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Parity v0.1.0")
		},
	})

	// Add subcommands
	rootCmd.AddCommand(newDiffCommand())

	// Execute the command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

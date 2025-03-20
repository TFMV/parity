package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: inspect_parquet <file>")
		os.Exit(1)
	}

	filePath := os.Args[1]
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Create parquet file reader
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		fmt.Printf("Error creating parquet reader: %v\n", err)
		os.Exit(1)
	}
	defer parquetReader.Close()

	// Print file metadata
	fmt.Printf("File: %s\n", filePath)
	fmt.Printf("Number of rows: %d\n", parquetReader.NumRows())
	fmt.Printf("Number of row groups: %d\n", parquetReader.NumRowGroups())

	// Create Arrow reader
	alloc := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, alloc)
	if err != nil {
		fmt.Printf("Error creating arrow reader: %v\n", err)
		os.Exit(1)
	}

	// Get schema
	schema, err := arrowReader.Schema()
	if err != nil {
		fmt.Printf("Error getting schema: %v\n", err)
		os.Exit(1)
	}

	// Print schema
	fmt.Println("\nSchema:")
	for i, field := range schema.Fields() {
		fmt.Printf("  Field %d: %s (%s)\n", i, field.Name, field.Type)
	}

	// Read first 5 rows
	fmt.Println("\nFirst 5 rows:")

	ctx := context.Background()

	// For small files, read the entire file
	if parquetReader.NumRows() < 1000 {
		table, err := arrowReader.ReadTable(ctx)
		if err != nil {
			fmt.Printf("Error reading table: %v\n", err)
			os.Exit(1)
		}
		defer table.Release()

		printRows(table, 5)
	} else {
		// For larger files, read just the first row group
		if parquetReader.NumRowGroups() > 0 {
			rgReader := arrowReader.RowGroup(0)
			table, err := rgReader.ReadTable(ctx, nil)
			if err != nil {
				fmt.Printf("Error reading row group: %v\n", err)
				os.Exit(1)
			}
			defer table.Release()

			printRows(table, 5)
		}
	}
}

func printRows(table arrow.Table, maxRows int) {
	// Create record reader from table
	reader := array.NewTableReader(table, int64(maxRows))
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		numRows := int(record.NumRows())
		if numRows > maxRows {
			numRows = maxRows
		}

		for i := 0; i < numRows; i++ {
			fmt.Printf("Row %d: [", i)
			for j, col := range record.Columns() {
				if j > 0 {
					fmt.Print(", ")
				}
				if col.IsNull(i) {
					fmt.Print("NULL")
				} else {
					fmt.Printf("%v", col.GetOneForMarshal(i))
				}
			}
			fmt.Println("]")
		}
	}
}

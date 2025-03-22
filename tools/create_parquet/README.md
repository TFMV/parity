# Create Parquet Tool

A utility for generating Parquet test files suitable for testing the Parity data diffing tool. This tool generates a pair of Parquet files (source and target) with configurable differences to facilitate testing of data comparison functionality.

## Features

- Generate Parquet files of specified size (in GB)
- Control the number of rows in the generated files
- Configure the percentage of differences between source and target files
- Add or remove columns in the target file compared to source
- Set specific error rates, null rates, and randomization seed

## Building

```bash
cd parity/tools/create_parquet
go build -o create_parquet
```

## Usage

```bash
./create_parquet [options]
```

### Options

```
-size float
    Size of the files to generate in GB (default: 0.1)
-rows int
    Approximate number of rows to generate (default: 1000000)
-outdir string
    Output directory for generated files (default: "test_data")
-source string
    Filename for the source Parquet file (default: "test_data_source.parquet")
-target string
    Filename for the target Parquet file (default: "test_data_target.parquet")
-seed int
    Random seed for data generation (default: 42)
-errors float
    Rate of errors/corruptions in target file (0.0-1.0) (default: 0.01)
-diffs float
    Percentage of rows that should differ between source and target (0.0-1.0) (default: 0.1)
-nulls float
    Rate of NULL values in the data (0.0-1.0) (default: 0.05)
-target-new-cols
    Whether target should have additional columns (default: true)
-target-missing-cols
    Whether target should miss some columns from source (default: false)
```

## Examples

### Generate default test files (100MB each)

```bash
./create_parquet
```

### Generate larger test files (1GB each)

```bash
./create_parquet -size 1.0
```

### Generate files with 20% differences

```bash
./create_parquet -diffs 0.2
```

### Generate files with different schemas (target missing columns)

```bash
./create_parquet -target-new-cols=false -target-missing-cols=true
```

### Generate files with a custom output location

```bash
./create_parquet -outdir ./my_test_data -source my_source.parquet -target my_target.parquet
```

## Schema Information

The generated files contain the following fields:

### Base Schema (both source and target)

- `id` (string, not nullable): Unique identifier
- `first_name` (string): Person's first name
- `last_name` (string): Person's last name
- `email` (string): Email address
- `age` (int32): Person's age
- `score` (decimal(10,2)): Numeric score
- `balance` (float64): Account balance
- `active` (boolean): Whether account is active
- `status` (string): Status value (active, inactive, pending, etc.)
- `created_at` (timestamp): Creation timestamp
- `updated_at` (timestamp): Last update timestamp

### Additional fields in target (when target-new-cols=true)

- `state` (string): US state code
- `lat` (float64): Latitude
- `long` (float64): Longitude
- `last_login` (timestamp): Last login timestamp

## How It Works

1. The tool creates Arrow schemas for both source and target files based on configuration options
2. Data is generated in batches to manage memory usage
3. For each batch:
   - Random, but realistic data is generated for each field
   - The first batch of the target file includes rows from the source to ensure partial matches
   - Errors and differences are introduced in the target file at the specified rates
4. The data is written to Parquet files using Arrow as the intermediate representation

## Using with Parity

After generating the test files, you can run Parity's diff command:

```bash
parity diff test_data/test_data_source.parquet test_data/test_data_target.parquet
```

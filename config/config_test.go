package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateConfig(t *testing.T) {
	validConfig := &Config{
		DB1: DatabaseConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			User:     "user",
			Password: "password",
			DBName:   "db1",
			Tables: []TableConfig{
				{Name: "table1", PrimaryKey: "id"},
			},
		},
		DB2: DatabaseConfig{
			Type: "duckdb",
			Path: "/path/to/db2.db",
			Tables: []TableConfig{
				{Name: "table2", PrimaryKey: "id"},
			},
		},
	}

	err := ValidateConfig(validConfig)
	assert.NoError(t, err)

	invalidConfig := &Config{
		DB1: DatabaseConfig{},
		DB2: DatabaseConfig{},
	}
	err = ValidateConfig(invalidConfig)
	assert.Error(t, err)
}

func TestValidateTableConfig(t *testing.T) {
	validTable := TableConfig{Name: "table1", PrimaryKey: "id"}
	err := ValidateTableConfig(&validTable)
	assert.NoError(t, err)

	invalidTable := TableConfig{}
	err = ValidateTableConfig(&invalidTable)
	assert.Error(t, err)
}

func TestValidateShardingConfig(t *testing.T) {
	validSharding := ShardingConfig{Key: "shard_key"}
	err := ValidateShardingConfig(&validSharding)
	assert.NoError(t, err)

	invalidSharding := ShardingConfig{}
	err = ValidateShardingConfig(&invalidSharding)
	assert.Error(t, err)
}

func TestValidatePartitioningConfig(t *testing.T) {
	validPartitioning := PartitioningConfig{Key: "partition_key", Type: "range"}
	err := ValidatePartitioningConfig(&validPartitioning)
	assert.NoError(t, err)

	invalidPartitioning := PartitioningConfig{}
	err = ValidatePartitioningConfig(&invalidPartitioning)
	assert.Error(t, err)
}

func TestValidateSamplingConfig(t *testing.T) {
	validSampling := SamplingConfig{Enabled: true, SampleRate: 0.5, SampleColumn: "id"}
	err := ValidateSamplingConfig(&validSampling)
	assert.NoError(t, err)

	invalidSampling := SamplingConfig{Enabled: true, SampleRate: 1.5}
	err = ValidateSamplingConfig(&invalidSampling)
	assert.Error(t, err)
}

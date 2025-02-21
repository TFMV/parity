package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// --- Configuration Structs ---

type ShardingConfig struct {
	Key      string `yaml:"key"`
	Strategy string `yaml:"strategy"`
}

type PartitioningConfig struct {
	Key  string `yaml:"key"`
	Type string `yaml:"type"`
}

type TableConfig struct {
	Name         string              `yaml:"name"`
	PrimaryKey   string              `yaml:"primary_key"`
	Sharding     *ShardingConfig     `yaml:"sharding,omitempty"`
	Partitioning *PartitioningConfig `yaml:"partitioning,omitempty"`
	Sampling     bool                `yaml:"sampling"`
	SampleRate   float64             `yaml:"sample_rate"`
	SampleColumn string              `yaml:"sample_column"`
}

type DBConfig struct {
	Type     string        `yaml:"type"`
	Host     string        `yaml:"host,omitempty"`
	Port     int           `yaml:"port,omitempty"`
	User     string        `yaml:"user,omitempty"`
	Password string        `yaml:"password,omitempty"`
	DBName   string        `yaml:"dbname,omitempty"`
	Path     string        `yaml:"path,omitempty"` //For SQLite
	Tables   []TableConfig `yaml:"tables"`
}

type Config struct {
	DB1 DBConfig `yaml:"DB1"`
	DB2 DBConfig `yaml:"DB2"`
}

// --- Load Configuration ---

func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// --- Validation Functions ---

// validate is a helper function to reduce repetition.
func validate(condition bool, format string, a ...any) error {
	if !condition {
		return fmt.Errorf(format, a...)
	}
	return nil
}

func (c *Config) Validate() error { // Method on Config
	if err := c.DB1.Validate(); err != nil {
		return fmt.Errorf("DB1 validation failed: %w", err)
	}
	if err := c.DB2.Validate(); err != nil {
		return fmt.Errorf("DB2 validation failed: %w", err)
	}
	return nil
}
func (dbc *DBConfig) Validate() error {
	if err := validate(dbc.Type != "", "DB type is required"); err != nil {
		return err
	}
	for i, table := range dbc.Tables {
		if err := table.Validate(); err != nil {
			return fmt.Errorf("table '%s' validation failed: %w", dbc.Tables[i].Name, err)
		}
	}
	return nil
}

func (tc *TableConfig) Validate() error {
	if err := validate(tc.Name != "", "table name is required"); err != nil {
		return err
	}
	if err := validate(tc.PrimaryKey != "", "primary key is required"); err != nil {
		return err
	}

	if tc.Sharding != nil {
		if err := tc.Sharding.Validate(); err != nil {
			return fmt.Errorf("sharding configuration error: %w", err)
		}
	}
	if tc.Partitioning != nil {
		if err := tc.Partitioning.Validate(); err != nil {
			return fmt.Errorf("partitioning configuration error: %w", err)
		}
	}
	if tc.Sampling {
		if err := validate(tc.SampleRate > 0 && tc.SampleRate <= 1, "sample rate must be between 0 and 1"); err != nil {
			return err
		}
		if err := validate(tc.SampleColumn != "", "sample column is required when sampling is enabled"); err != nil {
			return err
		}
	}
	return nil
}

func (sc *ShardingConfig) Validate() error {
	return validate(sc.Key != "", "sharding key is required")
}

func (pc *PartitioningConfig) Validate() error {
	if err := validate(pc.Key != "", "partitioning key is required"); err != nil {
		return err
	}
	return validate(pc.Type != "", "partitioning type is required")
}

// --- Top-Level Validation ---  (Optional, but good practice)

func ValidateConfig(cfg *Config) error { //Kept for backward compatibility since it is exported.
	return cfg.Validate()
}

package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	Type     string        `mapstructure:"type"`
	Host     string        `mapstructure:"host"`
	Port     int           `mapstructure:"port"`
	User     string        `mapstructure:"user"`
	Password string        `mapstructure:"password"`
	DBName   string        `mapstructure:"dbname"`
	Path     string        `mapstructure:"path"`
	Tables   []TableConfig `mapstructure:"tables"`
}

type TableConfig struct {
	Name         string              `mapstructure:"name"`
	PrimaryKey   string              `mapstructure:"primary_key"`
	Sharding     *ShardingConfig     `mapstructure:"sharding"`
	Partitioning *PartitioningConfig `mapstructure:"partitioning"`
	Sampling     *SamplingConfig     `mapstructure:"sampling"`
}

type ShardingConfig struct {
	Key      string `mapstructure:"key"`
	Strategy string `mapstructure:"strategy"`
}

type PartitioningConfig struct {
	Key  string `mapstructure:"key"`
	Type string `mapstructure:"type"`
}

type SamplingConfig struct {
	Enabled      bool    `mapstructure:"enabled"`
	SampleRate   float64 `mapstructure:"sample_rate"`
	SampleColumn string  `mapstructure:"sample_column"`
}

type Config struct {
	DB1 DatabaseConfig `mapstructure:"DB1"`
	DB2 DatabaseConfig `mapstructure:"DB2"`
}

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

func ValidateConfig(cfg *Config) error {
	if cfg.DB1.Type == "" {
		return fmt.Errorf("DB1 type is required")
	}
	if cfg.DB2.Type == "" {
		return fmt.Errorf("DB2 type is required")
	}
	for _, table := range cfg.DB1.Tables {
		if err := ValidateTableConfig(&table); err != nil {
			return err
		}
	}
	for _, table := range cfg.DB2.Tables {
		if err := ValidateTableConfig(&table); err != nil {
			return err
		}
	}
	return nil
}

func ValidateTableConfig(cfg *TableConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("table name is required")
	}
	if cfg.PrimaryKey == "" {
		return fmt.Errorf("primary key is required")
	}
	if cfg.Sharding != nil {
		if err := ValidateShardingConfig(cfg.Sharding); err != nil {
			return err
		}
	}
	if cfg.Partitioning != nil {
		if err := ValidatePartitioningConfig(cfg.Partitioning); err != nil {
			return err
		}
	}
	if cfg.Sampling != nil {
		if err := ValidateSamplingConfig(cfg.Sampling); err != nil {
			return err
		}
	}
	return nil
}

func ValidateShardingConfig(cfg *ShardingConfig) error {
	if cfg.Key == "" {
		return fmt.Errorf("sharding key is required")
	}
	return nil
}

func ValidatePartitioningConfig(cfg *PartitioningConfig) error {
	if cfg.Key == "" {
		return fmt.Errorf("partitioning key is required")
	}
	if cfg.Type == "" {
		return fmt.Errorf("partitioning type is required")
	}
	return nil
}

func ValidateSamplingConfig(cfg *SamplingConfig) error {
	if cfg.Enabled {
		if cfg.SampleRate <= 0 || cfg.SampleRate > 1 {
			return fmt.Errorf("sample rate must be between 0 and 1")
		}
		if cfg.SampleColumn == "" {
			return fmt.Errorf("sample column is required")
		}
	}
	return nil
}

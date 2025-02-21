package config

import (
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			cfg: Config{
				DB1: DBConfig{
					Type: "postgres",
					Tables: []TableConfig{{
						Name:       "users",
						PrimaryKey: "id",
					}},
				},
				DB2: DBConfig{
					Type: "postgres",
					Tables: []TableConfig{{
						Name:       "users",
						PrimaryKey: "id",
					}},
				},
			},
			wantErr: false,
		},
		{
			name: "missing DB1 type",
			cfg: Config{
				DB1: DBConfig{
					Tables: []TableConfig{{
						Name:       "users",
						PrimaryKey: "id",
					}},
				},
				DB2: DBConfig{
					Type: "postgres",
					Tables: []TableConfig{{
						Name:       "users",
						PrimaryKey: "id",
					}},
				},
			},
			wantErr: true,
			errMsg:  "DB1 validation failed: DB type is required",
		},
		{
			name: "invalid table config",
			cfg: Config{
				DB1: DBConfig{
					Type: "postgres",
					Tables: []TableConfig{{
						Name: "users", // missing primary key
					}},
				},
				DB2: DBConfig{
					Type: "postgres",
					Tables: []TableConfig{{
						Name:       "users",
						PrimaryKey: "id",
					}},
				},
			},
			wantErr: true,
			errMsg:  "DB1 validation failed: table 'users' validation failed: primary key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("Config.Validate() error message = %v, want %v", err, tt.errMsg)
			}
		})
	}
}

func TestTableConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     TableConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid table config",
			cfg: TableConfig{
				Name:       "users",
				PrimaryKey: "id",
			},
			wantErr: false,
		},
		{
			name: "valid table config with sharding",
			cfg: TableConfig{
				Name:       "users",
				PrimaryKey: "id",
				Sharding: &ShardingConfig{
					Key:      "user_id",
					Strategy: "hash",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid sampling config",
			cfg: TableConfig{
				Name:       "users",
				PrimaryKey: "id",
				Sampling:   true,
				// Missing sample rate and column
			},
			wantErr: true,
			errMsg:  "sample rate must be between 0 and 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("TableConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("TableConfig.Validate() error message = %v, want %v", err, tt.errMsg)
			}
		})
	}
}

func TestShardingConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ShardingConfig
		wantErr bool
	}{
		{
			name: "valid sharding config",
			cfg: ShardingConfig{
				Key:      "user_id",
				Strategy: "hash",
			},
			wantErr: false,
		},
		{
			name: "missing key",
			cfg: ShardingConfig{
				Strategy: "hash",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ShardingConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPartitioningConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PartitioningConfig
		wantErr bool
	}{
		{
			name: "valid partitioning config",
			cfg: PartitioningConfig{
				Key:  "user_id",
				Type: "hash",
			},
			wantErr: false,
		},
		{
			name: "missing key",
			cfg: PartitioningConfig{
				Type: "hash",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("PartitioningConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package store

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

// StoreType represents different storage backend types
type StoreType string

const (
	StoreTypePostgres        StoreType = "postgres"
	StoreTypePostgresSharded StoreType = "postgres-sharded"
	StoreTypeSQLite          StoreType = "sqlite-file"
	StoreTypeSQLiteMem       StoreType = "sqlite-memory"
)

// Config holds store configuration
type Config struct {
	Type            StoreType
	Postgres        *PostgresConfig
	PostgresSharded *PostgresShardedConfig
	SQLite          *SQLiteConfig
}

// PostgresConfig holds PostgreSQL-specific configuration
type PostgresConfig struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	User               string `json:"user"`
	Password           string `json:"password"`
	Database           string `json:"database"`
	SSLMode            string `json:"ssl_mode"`
	SchemaMode         string `json:"schema_mode"`
	MaxConns           int    `json:"max_conns"`
	MinConns           int    `json:"min_conns"`
	MaxConnAge         string `json:"max_conn_age"`
	MaxIdleTime        string `json:"max_idle_time"`
	SlimEvents         bool   `json:"slim_events"`
	QueueClaimMode     string `json:"queue_claim_mode"`
	DeferEvents        bool   `json:"defer_events"`
	EventBatchSize     int    `json:"event_batch_size"`
	PrefetchEnabled    bool   `json:"prefetch_enabled"`
	PrefetchBufferSize int    `json:"prefetch_buffer_size"`
	PrefetchBatchSize  int    `json:"prefetch_batch_size"`
}

// PostgresShardedConfig holds static shard-map Postgres configuration.
type PostgresShardedConfig struct {
	ShardMapFile          string `json:"shard_map_file"`
	NodeID                string `json:"node_id"`
	AdvertiseAddr         string `json:"advertise_addr"`
	ShardMapRefreshPeriod string `json:"shard_map_refresh_period"`
	SchemaMode            string `json:"schema_mode"`
	MaxConns              int    `json:"max_conns"`
	MinConns              int    `json:"min_conns"`
	MaxConnAge            string `json:"max_conn_age"`
	MaxIdleTime           string `json:"max_idle_time"`
	SlimEvents            bool   `json:"slim_events"`
	QueueClaimMode        string `json:"queue_claim_mode"`
	DeferEvents           bool   `json:"defer_events"`
	EventBatchSize        int    `json:"event_batch_size"`
	PrefetchEnabled       bool   `json:"prefetch_enabled"`
	PrefetchBufferSize    int    `json:"prefetch_buffer_size"`
	PrefetchBatchSize     int    `json:"prefetch_batch_size"`
}

// NewStore creates a new store based on the configuration
func NewStore(ctx context.Context, cfg *Config, logger *zap.Logger) (agent.AgentSpace, error) {
	if cfg == nil {
		return nil, fmt.Errorf("store config is nil")
	}

	switch normalizeStoreType(cfg.Type) {
	case StoreTypePostgres:
		if cfg.Postgres == nil {
			return nil, fmt.Errorf("postgres config is nil")
		}
		return NewPostgresStore(ctx, cfg.Postgres, logger)
	case StoreTypePostgresSharded:
		if cfg.PostgresSharded == nil {
			return nil, fmt.Errorf("postgres sharded config is nil")
		}
		return NewPostgresShardedStore(ctx, cfg.PostgresSharded, logger)
	case StoreTypeSQLite, StoreTypeSQLiteMem:
		sqliteCfg := cfg.SQLite
		if sqliteCfg == nil {
			sqliteCfg = DefaultSQLiteConfig()
		}
		if cfg.Type == StoreTypeSQLiteMem {
			sqliteCfg.InMemory = true
		}
		return NewSQLiteStore(ctx, sqliteCfg, logger)

	default:
		return nil, fmt.Errorf("unsupported store type: %s (supported: postgres, sqlite-file, sqlite-memory)", cfg.Type)
	}
}

// DefaultConfig returns default store configuration
func DefaultConfig() *Config {
	return &Config{
		Type:            StoreTypePostgres,
		SQLite:          DefaultSQLiteConfig(),
		Postgres:        DefaultPostgresConfig(),
		PostgresSharded: DefaultPostgresShardedConfig(),
	}
}

// DefaultPostgresConfig returns default PostgreSQL configuration
func DefaultPostgresConfig() *PostgresConfig {
	return &PostgresConfig{
		Host:           "localhost",
		Port:           5432,
		User:           "postgres",
		Password:       "",
		Database:       "agent_space",
		SSLMode:        "disable",
		SchemaMode:     "migrate",
		MaxConns:       50,
		MinConns:       15,
		MaxConnAge:     "30m",
		MaxIdleTime:    "15m",
		SlimEvents:     false,
		QueueClaimMode: "strict",
	}
}

// DefaultPostgresShardedConfig returns default static shard-map configuration.
func DefaultPostgresShardedConfig() *PostgresShardedConfig {
	return &PostgresShardedConfig{
		ShardMapFile:          "./config/shard_map.json",
		NodeID:                "",
		AdvertiseAddr:         "",
		ShardMapRefreshPeriod: "0s",
		SchemaMode:            "migrate",
		MaxConns:              50,
		MinConns:              15,
		MaxConnAge:            "30m",
		MaxIdleTime:           "15m",
		SlimEvents:            false,
		QueueClaimMode:        "strict",
	}
}

// SQLiteConfig holds SQLite-specific configuration.
type SQLiteConfig struct {
	Path        string
	InMemory    bool
	SchemaMode  string
	BusyTimeout string
	JournalMode string
	Synchronous string
	CacheSizeKB int
}

// DefaultSQLiteConfig returns default SQLite configuration.
func DefaultSQLiteConfig() *SQLiteConfig {
	return &SQLiteConfig{
		Path:        "./data/sqlite/agent_space.db",
		InMemory:    false,
		SchemaMode:  "migrate",
		BusyTimeout: "5s",
		JournalMode: "WAL",
		Synchronous: "NORMAL",
		CacheSizeKB: 16384,
	}
}

func normalizeStoreType(value StoreType) StoreType {
	switch StoreType(strings.TrimSpace(strings.ToLower(string(value)))) {
	case "", StoreTypePostgres:
		return StoreTypePostgres
	case StoreTypePostgresSharded:
		return StoreTypePostgresSharded
	case StoreTypeSQLite:
		return StoreTypeSQLite
	case StoreTypeSQLiteMem:
		return StoreTypeSQLiteMem
	default:
		return value
	}
}

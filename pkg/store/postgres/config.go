package postgres

import (
	"fmt"
	"strings"
)

type QueueClaimMode string

const (
	QueueClaimModeStrict   QueueClaimMode = "strict"
	QueueClaimModeParallel QueueClaimMode = "parallel"
)

func ParseQueueClaimMode(raw string) (QueueClaimMode, error) {
	mode := QueueClaimMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return QueueClaimModeStrict, nil
	}
	switch mode {
	case QueueClaimModeStrict, QueueClaimModeParallel:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid postgres queue claim mode %q (supported: strict, parallel)", raw)
	}
}

// PostgresConfig holds PostgreSQL-specific configuration.
type PostgresConfig struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	User           string `json:"user"`
	Password       string `json:"password"`
	Database       string `json:"database"`
	ShardID        string `json:"shard_id"`
	SSLMode        string `json:"ssl_mode"`
	SchemaMode     string `json:"schema_mode"`
	MaxConns       int    `json:"max_conns"`
	MinConns       int    `json:"min_conns"`
	MaxConnAge     string `json:"max_conn_age"`
	MaxIdleTime    string `json:"max_idle_time"`
	SlimEvents     bool   `json:"slim_events"`
	QueueClaimMode string `json:"queue_claim_mode"`
	DeferEvents        bool   `json:"defer_events"`
	EventBatchSize     int    `json:"event_batch_size"`
	PrefetchEnabled    bool   `json:"prefetch_enabled"`
	PrefetchBufferSize int    `json:"prefetch_buffer_size"`
	PrefetchBatchSize  int    `json:"prefetch_batch_size"`
}

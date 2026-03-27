package store

import (
	"context"

	"go.uber.org/zap"

	postgresbackend "github.com/urobora-ai/agentspaces/pkg/store/postgres"
)

type PostgresStore = postgresbackend.PostgresStore

func NewPostgresStore(ctx context.Context, cfg *PostgresConfig, logger *zap.Logger) (*PostgresStore, error) {
	var backendCfg *postgresbackend.PostgresConfig
	if cfg != nil {
		backendCfg = &postgresbackend.PostgresConfig{
			Host:               cfg.Host,
			Port:               cfg.Port,
			User:               cfg.User,
			Password:           cfg.Password,
			Database:           cfg.Database,
			SSLMode:            cfg.SSLMode,
			SchemaMode:         cfg.SchemaMode,
			MaxConns:           cfg.MaxConns,
			MinConns:           cfg.MinConns,
			MaxConnAge:         cfg.MaxConnAge,
			MaxIdleTime:        cfg.MaxIdleTime,
			SlimEvents:         cfg.SlimEvents,
			QueueClaimMode:     cfg.QueueClaimMode,
			DeferEvents:        cfg.DeferEvents,
			EventBatchSize:     cfg.EventBatchSize,
			PrefetchEnabled:    cfg.PrefetchEnabled,
			PrefetchBufferSize: cfg.PrefetchBufferSize,
			PrefetchBatchSize:  cfg.PrefetchBatchSize,
		}
	}
	return postgresbackend.NewPostgresStore(ctx, backendCfg, logger)
}

package store

import (
	"context"

	"go.uber.org/zap"

	shardedbackend "github.com/urobora-ai/agentspaces/pkg/store/postgressharded"
)

type PostgresShardedStore = shardedbackend.PostgresShardedStore

func NewPostgresShardedStore(ctx context.Context, cfg *PostgresShardedConfig, logger *zap.Logger) (*PostgresShardedStore, error) {
	var backendCfg *shardedbackend.Config
	if cfg != nil {
		backendCfg = &shardedbackend.Config{
			ShardMapFile:          cfg.ShardMapFile,
			NodeID:                cfg.NodeID,
			AdvertiseAddr:         cfg.AdvertiseAddr,
			ShardMapRefreshPeriod: cfg.ShardMapRefreshPeriod,
			SchemaMode:            cfg.SchemaMode,
			MaxConns:              cfg.MaxConns,
			MinConns:              cfg.MinConns,
			MaxConnAge:            cfg.MaxConnAge,
			MaxIdleTime:           cfg.MaxIdleTime,
			SlimEvents:            cfg.SlimEvents,
			QueueClaimMode:        cfg.QueueClaimMode,
			DeferEvents:           cfg.DeferEvents,
			EventBatchSize:        cfg.EventBatchSize,
			PrefetchEnabled:       cfg.PrefetchEnabled,
			PrefetchBufferSize:    cfg.PrefetchBufferSize,
			PrefetchBatchSize:     cfg.PrefetchBatchSize,
		}
	}
	return shardedbackend.New(ctx, backendCfg, logger)
}

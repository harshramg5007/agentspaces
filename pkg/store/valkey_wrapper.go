package store

import (
	"context"

	"go.uber.org/zap"

	valkeybackend "github.com/urobora-ai/agentspaces/pkg/store/valkey"
)

type ValkeyStore = valkeybackend.ValkeyStore

func NewValkeyStore(ctx context.Context, cfg *ValkeyConfig, logger *zap.Logger) (*ValkeyStore, error) {
	var backendCfg *valkeybackend.ValkeyConfig
	if cfg != nil {
		shards := append([]string(nil), cfg.Shards...)
		backendCfg = &valkeybackend.ValkeyConfig{
			Addr:              cfg.Addr,
			Shards:            shards,
			PublishPubSub:     cfg.PublishPubSub,
			LeaseReapInterval: cfg.LeaseReapInterval,
			LeaseReapBatch:    cfg.LeaseReapBatch,
			SlimEvents:        cfg.SlimEvents,
			DeferEvents:       cfg.DeferEvents,
			EventBatchSize:    cfg.EventBatchSize,
			ShardMapFile:      cfg.ShardMapFile,
			NodeID:            cfg.NodeID,
			AdvertiseAddr:     cfg.AdvertiseAddr,
		}
	}
	return valkeybackend.NewValkeyStore(ctx, backendCfg, logger)
}

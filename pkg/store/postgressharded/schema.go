package postgressharded

import (
	"context"
	"fmt"

	"github.com/urobora-ai/agentspaces/pkg/store/migrations"
	postgresbackend "github.com/urobora-ai/agentspaces/pkg/store/postgres"
)

type SchemaActionResult struct {
	Backend      string                        `json:"backend"`
	Version      int64                         `json:"version"`
	Epoch        int64                         `json:"epoch,omitempty"`
	Compatible   bool                          `json:"compatible"`
	ShardResults map[string]*migrations.Result `json:"shard_results"`
}

func ExecuteSchemaAction(ctx context.Context, cfg *Config, action string, requestedVersion int64) (*SchemaActionResult, error) {
	if cfg == nil {
		return nil, fmt.Errorf("postgres sharded config is nil")
	}
	shardMap, err := loadShardMap(cfg.ShardMapFile)
	if err != nil {
		return nil, err
	}
	result := &SchemaActionResult{
		Backend:      "postgres-sharded",
		Version:      shardMap.Version,
		Epoch:        shardMap.Epoch,
		Compatible:   true,
		ShardResults: make(map[string]*migrations.Result, len(shardMap.Shards)),
	}
	for _, shard := range shardMap.Shards {
		pgCfg, err := postgresConfigFromShardSpec(cfg, shard)
		if err != nil {
			return nil, err
		}
		shardResult, err := postgresbackend.ExecuteSchemaAction(ctx, pgCfg, action, requestedVersion)
		if err != nil {
			return nil, fmt.Errorf("shard %s %s failed: %w", shard.ShardID, action, err)
		}
		result.ShardResults[shard.ShardID] = shardResult
		if !shardResult.Compatible {
			result.Compatible = false
		}
	}
	return result, nil
}

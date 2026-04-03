package postgressharded

import (
	"context"
	"testing"
)

func TestShardMapInfoRedactsWriterCredentials(t *testing.T) {
	store := &PostgresShardedStore{
		cfg: &Config{
			NodeID:        "node-a",
			AdvertiseAddr: "http://node-a:8080",
		},
		shardMap: &ShardMap{
			Version: 1,
			Epoch:   2,
		},
		shards: []*shardHandle{
			{
				spec: ShardSpec{
					ShardID:       "shard-a",
					OwnerNodeID:   "node-a",
					AdvertiseAddr: "http://node-a:8080",
					WriterDSN:     "postgres://agent_user:super-secret@db.internal:5432/agent_space?sslmode=require",
				},
			},
		},
	}

	info, err := store.ShardMapInfo(context.Background())
	if err != nil {
		t.Fatalf("ShardMapInfo() error = %v", err)
	}

	shards, ok := info["shards"].([]ShardSummary)
	if !ok {
		t.Fatalf("expected []ShardSummary, got %T", info["shards"])
	}
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard summary, got %d", len(shards))
	}
	if shards[0].WriterEndpoint == "" {
		t.Fatalf("expected writer endpoint to be populated")
	}
	if shards[0].WriterEndpoint == store.shards[0].spec.WriterDSN {
		t.Fatalf("expected writer endpoint to differ from raw DSN")
	}
	if got := shards[0].WriterEndpoint; got != "postgres://db.internal:5432/agent_space?sslmode=require" {
		t.Fatalf("unexpected redacted endpoint %q", got)
	}
}

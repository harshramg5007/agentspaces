package store

import "testing"

func TestDefaultValkeyConfig(t *testing.T) {
	cfg := DefaultValkeyConfig()
	if cfg == nil {
		t.Fatal("DefaultValkeyConfig() returned nil")
	}
	if cfg.Addr != "localhost:6379" {
		t.Fatalf("DefaultValkeyConfig().Addr = %q, want %q", cfg.Addr, "localhost:6379")
	}
	if cfg.PublishPubSub {
		t.Fatalf("DefaultValkeyConfig().PublishPubSub = true, want false")
	}
	if cfg.LeaseReapInterval != "1s" {
		t.Fatalf("DefaultValkeyConfig().LeaseReapInterval = %q, want %q", cfg.LeaseReapInterval, "1s")
	}
	if cfg.LeaseReapBatch != 200 {
		t.Fatalf("DefaultValkeyConfig().LeaseReapBatch = %d, want 200", cfg.LeaseReapBatch)
	}
	if len(cfg.Shards) != 0 {
		t.Fatalf("DefaultValkeyConfig().Shards = %#v, want nil/empty", cfg.Shards)
	}
	if cfg.SlimEvents {
		t.Fatalf("DefaultValkeyConfig().SlimEvents = true, want false")
	}
	if cfg.DeferEvents {
		t.Fatalf("DefaultValkeyConfig().DeferEvents = true, want false")
	}
	if cfg.EventBatchSize != 100 {
		t.Fatalf("DefaultValkeyConfig().EventBatchSize = %d, want 100", cfg.EventBatchSize)
	}
	if cfg.ShardMapFile != "" {
		t.Fatalf("DefaultValkeyConfig().ShardMapFile = %q, want empty", cfg.ShardMapFile)
	}
	if cfg.NodeID != "" {
		t.Fatalf("DefaultValkeyConfig().NodeID = %q, want empty", cfg.NodeID)
	}
	if cfg.AdvertiseAddr != "" {
		t.Fatalf("DefaultValkeyConfig().AdvertiseAddr = %q, want empty", cfg.AdvertiseAddr)
	}
}

func TestDefaultConfigIncludesValkeyDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Valkey == nil {
		t.Fatal("DefaultConfig().Valkey returned nil")
	}
	if cfg.Valkey.Addr != "localhost:6379" {
		t.Fatalf("DefaultConfig().Valkey.Addr = %q, want %q", cfg.Valkey.Addr, "localhost:6379")
	}
}

func TestNormalizeStoreTypeValkey(t *testing.T) {
	tests := []struct {
		name  string
		input StoreType
		want  StoreType
	}{
		{name: "valkey", input: StoreTypeValkey, want: StoreTypeValkey},
		{name: "trim whitespace", input: StoreType("  valkey  "), want: StoreTypeValkey},
		{name: "empty defaults postgres", input: StoreType(""), want: StoreTypePostgres},
		{name: "preserve unsupported", input: StoreType("valkey-sharded"), want: StoreType("valkey-sharded")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeStoreType(tt.input); got != tt.want {
				t.Fatalf("normalizeStoreType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

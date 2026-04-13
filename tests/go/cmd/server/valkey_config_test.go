package main

import (
	"context"
	"testing"

	"github.com/spf13/viper"

	"github.com/urobora-ai/agentspaces/pkg/store"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

type telemetryStoreWithNamespace struct{}

func (telemetryStoreWithNamespace) RecordSpans(context.Context, []telemetry.Span) error { return nil }
func (telemetryStoreWithNamespace) ListSpans(context.Context, string, string, int, int) ([]telemetry.Span, error) {
	return nil, nil
}

type telemetryStoreWithoutNamespace struct{}

func (telemetryStoreWithoutNamespace) RecordSpans(context.Context, []telemetry.Span) error {
	return nil
}
func (telemetryStoreWithoutNamespace) ListSpans(context.Context, string, int, int) ([]telemetry.Span, error) {
	return nil, nil
}

func TestValkeyConfigFromViper(t *testing.T) {
	viper.Set("valkey.addr", "127.0.0.1:6390")
	viper.Set("valkey.shards", "127.0.0.1:6390, 127.0.0.1:6391,127.0.0.1:6390")
	viper.Set("valkey.shard_map_file", "/tmp/valkey-shards.json")
	viper.Set("valkey.node_id", "node-a")
	viper.Set("valkey.advertise_addr", "http://127.0.0.1:8080")
	viper.Set("valkey.publish_pubsub", true)
	viper.Set("valkey.lease_reap_interval", "3s")
	viper.Set("valkey.lease_reap_batch", 77)
	viper.Set("events.slim", true)
	viper.Set("events.defer", true)
	viper.Set("events.batch_size", 55)
	t.Cleanup(func() {
		viper.Set("valkey.addr", store.DefaultValkeyConfig().Addr)
		viper.Set("valkey.shards", "")
		viper.Set("valkey.shard_map_file", "")
		viper.Set("valkey.node_id", "")
		viper.Set("valkey.advertise_addr", "")
		viper.Set("valkey.publish_pubsub", false)
		viper.Set("valkey.lease_reap_interval", store.DefaultValkeyConfig().LeaseReapInterval)
		viper.Set("valkey.lease_reap_batch", store.DefaultValkeyConfig().LeaseReapBatch)
		viper.Set("events.slim", false)
		viper.Set("events.defer", true)
		viper.Set("events.batch_size", 100)
	})

	cfg := valkeyConfigFromViper()
	if cfg.Addr != "127.0.0.1:6390" {
		t.Fatalf("valkeyConfigFromViper().Addr = %q, want %q", cfg.Addr, "127.0.0.1:6390")
	}
	if got, want := len(cfg.Shards), 2; got != want {
		t.Fatalf("valkeyConfigFromViper().Shards len = %d, want %d (%v)", got, want, cfg.Shards)
	}
	if cfg.Shards[0] != "127.0.0.1:6390" || cfg.Shards[1] != "127.0.0.1:6391" {
		t.Fatalf("valkeyConfigFromViper().Shards = %#v, want deduped shard list", cfg.Shards)
	}
	if !cfg.PublishPubSub {
		t.Fatalf("valkeyConfigFromViper().PublishPubSub = false, want true")
	}
	if cfg.LeaseReapInterval != "3s" {
		t.Fatalf("valkeyConfigFromViper().LeaseReapInterval = %q, want %q", cfg.LeaseReapInterval, "3s")
	}
	if cfg.LeaseReapBatch != 77 {
		t.Fatalf("valkeyConfigFromViper().LeaseReapBatch = %d, want 77", cfg.LeaseReapBatch)
	}
	if cfg.ShardMapFile != "/tmp/valkey-shards.json" {
		t.Fatalf("valkeyConfigFromViper().ShardMapFile = %q, want %q", cfg.ShardMapFile, "/tmp/valkey-shards.json")
	}
	if cfg.NodeID != "node-a" {
		t.Fatalf("valkeyConfigFromViper().NodeID = %q, want %q", cfg.NodeID, "node-a")
	}
	if cfg.AdvertiseAddr != "http://127.0.0.1:8080" {
		t.Fatalf("valkeyConfigFromViper().AdvertiseAddr = %q, want %q", cfg.AdvertiseAddr, "http://127.0.0.1:8080")
	}
	if !cfg.SlimEvents {
		t.Fatalf("valkeyConfigFromViper().SlimEvents = false, want true")
	}
	if !cfg.DeferEvents {
		t.Fatalf("valkeyConfigFromViper().DeferEvents = false, want true")
	}
	if cfg.EventBatchSize != 55 {
		t.Fatalf("valkeyConfigFromViper().EventBatchSize = %d, want 55", cfg.EventBatchSize)
	}
}

func TestTelemetryCapabilityRequiresNamespaceAwareListSpans(t *testing.T) {
	if _, ok := any(telemetryStoreWithNamespace{}).(telemetryCapability); !ok {
		t.Fatalf("namespace-aware telemetry store should satisfy telemetryCapability")
	}
	if _, ok := any(telemetryStoreWithoutNamespace{}).(telemetryCapability); ok {
		t.Fatalf("legacy telemetry store without namespace parameter should not satisfy telemetryCapability")
	}
}

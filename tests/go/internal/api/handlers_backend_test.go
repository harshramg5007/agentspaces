package api

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/store"
	"github.com/urobora-ai/agentspaces/pkg/store/shardadmin"
)

func TestGetBackendTypeRecognizesSupportedBackends(t *testing.T) {
	tests := []struct {
		name  string
		space agent.AgentSpace
		want  string
	}{
		{name: "postgres", space: (*store.PostgresStore)(nil), want: "postgres"},
		{name: "postgres-sharded", space: (*store.PostgresShardedStore)(nil), want: "postgres-sharded"},
		{name: "sqlite", space: (*store.SQLiteStore)(nil), want: "sqlite"},
		{name: "valkey", space: (*store.ValkeyStore)(nil), want: "valkey"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandlers(tt.space, zap.NewNop())
			if got := h.getBackendType(); got != tt.want {
				t.Fatalf("getBackendType() = %q, want %q", got, tt.want)
			}
		})
	}
}

type shardHealthProviderStub struct{}

func (shardHealthProviderStub) Get(string) (*agent.Agent, error) { return nil, nil }
func (shardHealthProviderStub) Out(*agent.Agent) error           { return nil }
func (shardHealthProviderStub) In(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}
func (shardHealthProviderStub) Read(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}
func (shardHealthProviderStub) Query(*agent.Query) ([]*agent.Agent, error)  { return nil, nil }
func (shardHealthProviderStub) Take(*agent.Query) (*agent.Agent, error)     { return nil, nil }
func (shardHealthProviderStub) Update(string, map[string]interface{}) error { return nil }
func (shardHealthProviderStub) Complete(string, string, string, map[string]interface{}) error {
	return nil
}
func (shardHealthProviderStub) CompleteAndOut(string, string, string, map[string]interface{}, []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return nil, nil, nil
}
func (shardHealthProviderStub) Release(string, string, string, string) error { return nil }
func (shardHealthProviderStub) RenewLease(string, string, string, time.Duration) (*agent.Agent, error) {
	return nil, nil
}
func (shardHealthProviderStub) Subscribe(*agent.EventFilter, agent.EventHandler) error { return nil }
func (shardHealthProviderStub) GetEvents(string) ([]*agent.Event, error)               { return nil, nil }
func (shardHealthProviderStub) GetGlobalEvents(int, int) ([]*agent.Event, error)       { return nil, nil }
func (shardHealthProviderStub) GetDAG(string) (*agent.DAG, error)                      { return nil, nil }
func (shardHealthProviderStub) Health(context.Context) error                           { return nil }
func (shardHealthProviderStub) ShardHealthInfo(context.Context) ([]shardadmin.ShardHealth, error) {
	return []shardadmin.ShardHealth{{ShardID: "valkey-0", Ready: true}}, nil
}

func TestShardHealthProviderUsesSharedShardHealthType(t *testing.T) {
	h := NewHandlers(shardHealthProviderStub{}, zap.NewNop())
	provider, ok := h.space.(shardHealthInfoProvider)
	if !ok {
		t.Fatalf("handler space does not expose shard health provider")
	}

	info, err := provider.ShardHealthInfo(context.Background())
	if err != nil {
		t.Fatalf("ShardHealthInfo() error = %v", err)
	}
	if len(info) != 1 || info[0].ShardID != "valkey-0" || !info[0].Ready {
		t.Fatalf("ShardHealthInfo() = %#v, want one ready shared-health entry", info)
	}
}

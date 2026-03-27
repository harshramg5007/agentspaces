package api

import (
	"testing"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/store"
	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func TestGetBackendTypeRecognizesSupportedBackends(t *testing.T) {
	tests := []struct {
		name  string
		space agent.AgentSpace
		want  string
	}{
		{name: "postgres", space: (*store.PostgresStore)(nil), want: "postgres"},
		{name: "sqlite", space: (*store.SQLiteStore)(nil), want: "sqlite"},
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

package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	storepkg "github.com/urobora-ai/agentspaces/pkg/store"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

func TestValkeyShardedTelemetryListSpans(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	cfg := valkeyConfigFromEnv()
	if cfg == nil || cfg.Valkey == nil || len(cfg.Valkey.Shards) < 2 {
		t.Skip("Skipping sharded Valkey telemetry test: VALKEY_SHARDS with at least two shards not set")
	}

	ctx := context.Background()
	logger := zap.NewNop()
	space, err := storepkg.NewStore(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("failed to create sharded valkey store: %v", err)
	}
	if closer, ok := space.(interface{ Close() error }); ok {
		t.Cleanup(func() {
			_ = closer.Close()
		})
	}

	recorder, ok := space.(interface {
		RecordSpans(ctx context.Context, spans []telemetry.Span) error
		ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error)
	})
	if !ok {
		t.Fatalf("sharded valkey store does not support telemetry recording")
	}

	traceID := uuid.New().String()
	now := time.Now().UTC()
	spans := []telemetry.Span{
		{
			TraceID:     traceID,
			NamespaceID: "ns-alpha",
			SpanID:      "span-alpha",
			Name:        "task.alpha",
			Kind:        "operation",
			TsStart:     now,
		},
		{
			TraceID:     traceID,
			NamespaceID: "ns-beta",
			SpanID:      "span-beta",
			Name:        "task.beta",
			Kind:        "operation",
			TsStart:     now.Add(time.Millisecond),
		},
	}

	if err := recorder.RecordSpans(ctx, spans); err != nil {
		t.Fatalf("failed to record telemetry spans: %v", err)
	}

	nsListed, err := recorder.ListSpans(ctx, traceID, "ns-alpha", 10, 0)
	if err != nil {
		t.Fatalf("failed namespace-routed ListSpans: %v", err)
	}
	if len(nsListed) == 0 {
		t.Fatalf("expected namespace-routed telemetry spans for trace %s", traceID)
	}

	scatterListed, err := recorder.ListSpans(ctx, traceID, "", 10, 0)
	if err != nil {
		t.Fatalf("failed scatter-gather ListSpans: %v", err)
	}
	if len(scatterListed) < len(spans) {
		t.Fatalf("scatter-gather ListSpans returned %d spans, want at least %d", len(scatterListed), len(spans))
	}
}

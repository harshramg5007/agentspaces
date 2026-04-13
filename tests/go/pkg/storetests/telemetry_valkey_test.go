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

func TestValkeyTelemetryInsertAndCleanup(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	cfg := valkeyConfigFromEnv()
	if cfg == nil {
		if backendRequired("valkey") {
			t.Fatalf("required backend valkey is not configured (VALKEY_ADDR/VALKEY_SHARDS empty)")
		}
		t.Skip("VALKEY_ADDR and VALKEY_SHARDS not set")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	space, err := storepkg.NewStore(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("failed to create valkey store: %v", err)
	}
	if closer, ok := space.(interface{ Close() error }); ok {
		t.Cleanup(func() {
			_ = closer.Close()
		})
	}

	recorder, ok := space.(interface {
		RecordSpans(ctx context.Context, spans []telemetry.Span) error
		ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error)
		CleanupSpansBefore(ctx context.Context, cutoff time.Time) (int64, error)
	})
	if !ok {
		t.Fatalf("valkey store does not support telemetry recording")
	}

	traceID := uuid.New().String()
	namespaceID := uuid.New().String()
	now := time.Now().UTC()
	firstEnd := now.Add(-90 * time.Second)
	secondEnd := now.Add(-30 * time.Second)
	spans := []telemetry.Span{
		{
			TraceID:     traceID,
			NamespaceID: namespaceID,
			SpanID:      "span-1",
			Name:        "task.run",
			Kind:        "operation",
			TsStart:     now.Add(-2 * time.Minute),
			TsEnd:       &firstEnd,
		},
		{
			TraceID:      traceID,
			NamespaceID:  namespaceID,
			SpanID:       "span-2",
			ParentSpanID: "span-1",
			Name:         "task.finish",
			Kind:         "operation",
			TsStart:      now.Add(-time.Minute),
			TsEnd:        &secondEnd,
		},
	}

	if err := recorder.RecordSpans(ctx, spans); err != nil {
		t.Fatalf("failed to record telemetry spans: %v", err)
	}

	listed, err := recorder.ListSpans(ctx, traceID, namespaceID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list telemetry spans: %v", err)
	}
	if len(listed) != len(spans) {
		t.Fatalf("ListSpans() returned %d spans, want %d", len(listed), len(spans))
	}

	deleted, err := recorder.CleanupSpansBefore(ctx, now)
	if err != nil {
		t.Fatalf("failed to cleanup telemetry spans: %v", err)
	}
	if deleted < int64(len(spans)) {
		t.Fatalf("CleanupSpansBefore() deleted %d spans, want at least %d", deleted, len(spans))
	}

	listed, err = recorder.ListSpans(ctx, traceID, namespaceID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list telemetry spans after cleanup: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("ListSpans() after cleanup returned %d spans, want 0", len(listed))
	}
}

package store_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	storepkg "github.com/urobora-ai/agentspaces/pkg/store"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

func TestPostgresTelemetryInsert(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}
	if os.Getenv("POSTGRES_HOST") == "" {
		if backendRequired("postgres") {
			t.Fatalf("required backend postgres is not configured (POSTGRES_HOST empty)")
		}
		t.Skip("POSTGRES_HOST not set")
	}

	ctx := context.Background()
	logger := zap.NewNop()
	pgPort := 5432
	if rawPort := os.Getenv("POSTGRES_PORT"); rawPort != "" {
		if parsedPort, err := strconv.Atoi(rawPort); err == nil && parsedPort > 0 {
			pgPort = parsedPort
		}
	}

	pgCfg := &storepkg.Config{
		Type: storepkg.StoreTypePostgres,
		Postgres: &storepkg.PostgresConfig{
			Host:     os.Getenv("POSTGRES_HOST"),
			Port:     pgPort,
			User:     os.Getenv("POSTGRES_USER"),
			Password: os.Getenv("POSTGRES_PASSWORD"),
			Database: os.Getenv("POSTGRES_DATABASE"),
			SSLMode:  "disable",
		},
	}

	space, err := storepkg.NewStore(ctx, pgCfg, logger)
	if err != nil {
		t.Fatalf("failed to create postgres store: %v", err)
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
		t.Fatalf("postgres store does not support telemetry recording")
	}

	traceID := uuid.New().String()
	valueTrue := true
	spans := []telemetry.Span{
		{
			TraceID: traceID,
			Name:    "metrics.task_success",
			Kind:    "metric",
			TsStart: time.Now().UTC(),
			Metric: &telemetry.Metric{
				Key:       "metrics.task_success",
				ValueBool: &valueTrue,
			},
		},
	}

	if err := recorder.RecordSpans(ctx, spans); err != nil {
		t.Fatalf("failed to record telemetry spans: %v", err)
	}

	listed, err := recorder.ListSpans(ctx, traceID, "", 10, 0)
	if err != nil {
		t.Fatalf("failed to list telemetry spans: %v", err)
	}
	if len(listed) == 0 {
		t.Fatalf("expected telemetry span for trace %s", traceID)
	}
}

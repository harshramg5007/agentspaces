package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	storepkg "github.com/urobora-ai/agentspaces/pkg/store"
)

func TestValkeyCreateAndQueueTakeRegression(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	space := newValkeyRegressionStore(t)
	namespaceID := "valkey-regression-create-" + uuid.NewString()
	missionID := "mission-" + uuid.NewString()
	kind := "drone.task.search"

	first := &agent.Agent{
		ID:          uuid.NewString(),
		Kind:        kind,
		Status:      agent.StatusNew,
		NamespaceID: namespaceID,
		Payload: map[string]interface{}{
			"task_id": "task-nil-tags",
		},
		Metadata: map[string]string{
			"queue":      "task-high",
			"priority":   "high",
			"mission_id": missionID,
			"lease_sec":  "2",
		},
	}
	if err := space.Out(first); err != nil {
		t.Fatalf("Out(first) failed: %v", err)
	}

	second := &agent.Agent{
		ID:          uuid.NewString(),
		Kind:        kind,
		Status:      agent.StatusNew,
		NamespaceID: namespaceID,
		Tags:        []string{},
		Payload: map[string]interface{}{
			"task_id": "task-explicit-collections",
		},
		Metadata: map[string]string{
			"queue":      "task-medium",
			"priority":   "medium",
			"mission_id": missionID,
			"lease_sec":  "2",
			"mode":       "explicit",
		},
	}
	if err := space.Out(second); err != nil {
		t.Fatalf("Out(second) failed: %v", err)
	}

	results, err := space.Query(&agent.Query{
		Kind:        kind,
		NamespaceID: namespaceID,
		Metadata: map[string]string{
			"mission_id": missionID,
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query() failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Query() returned %d agents, want 2", len(results))
	}

	claimed, err := space.Take(&agent.Query{
		Kind:        kind,
		Status:      agent.StatusNew,
		NamespaceID: namespaceID,
		AgentID:     "worker-alpha",
		Metadata: map[string]string{
			"queue": "task-high",
		},
	})
	if err != nil {
		t.Fatalf("Take() failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("Take() returned nil, want claimed agent")
	}
	if claimed.ID != first.ID {
		t.Fatalf("Take() claimed %s, want %s", claimed.ID, first.ID)
	}
	if claimed.Owner != "worker-alpha" {
		t.Fatalf("Take() owner = %q, want %q", claimed.Owner, "worker-alpha")
	}
	if claimed.LeaseToken == "" {
		t.Fatal("Take() returned empty lease token")
	}
	if claimed.Status != agent.StatusInProgress {
		t.Fatalf("Take() status = %q, want %q", claimed.Status, agent.StatusInProgress)
	}
}

func TestValkeyStateClaimUpdateAndRenewRegression(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	space := newValkeyRegressionStore(t)
	namespaceID := "valkey-regression-state-" + uuid.NewString()
	missionID := "mission-" + uuid.NewString()
	agentID := "alpha"
	stateID := uuid.NewString()

	state := &agent.Agent{
		ID:          stateID,
		Kind:        "drone.state",
		Status:      agent.StatusNew,
		NamespaceID: namespaceID,
		Tags:        []string{},
		Payload: map[string]interface{}{
			"agent_id": agentID,
			"status":   "idle",
			"battery":  100.0,
			"position": map[string]interface{}{"x": 34.05, "y": -118.25},
		},
		Metadata: map[string]string{
			"queue":      "state-" + agentID,
			"agent_id":   agentID,
			"mission_id": missionID,
			"role":       "state",
			"lease_sec":  "2",
		},
	}
	if err := space.Out(state); err != nil {
		t.Fatalf("Out(state) failed: %v", err)
	}

	claimed, err := space.Take(&agent.Query{
		Kind:        "drone.state",
		Status:      agent.StatusNew,
		NamespaceID: namespaceID,
		AgentID:     agentID,
		Metadata: map[string]string{
			"queue": "state-" + agentID,
		},
	})
	if err != nil {
		t.Fatalf("Take(state) failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("Take(state) returned nil, want claimed tuple")
	}
	if claimed.ID != stateID {
		t.Fatalf("Take(state) claimed %s, want %s", claimed.ID, stateID)
	}
	if claimed.LeaseToken == "" {
		t.Fatal("Take(state) returned empty lease token")
	}

	if err := space.Update(stateID, map[string]interface{}{
		"payload": map[string]interface{}{
			"status":   "searching",
			"battery":  97.5,
			"position": map[string]interface{}{"x": 34.051, "y": -118.249},
		},
		"tags": []string{},
	}); err != nil {
		t.Fatalf("Update(state) failed: %v", err)
	}

	updated, err := space.Get(stateID)
	if err != nil {
		t.Fatalf("Get(updated state) failed: %v", err)
	}
	if updated == nil {
		t.Fatal("Get(updated state) returned nil")
	}
	if updated.Owner != agentID {
		t.Fatalf("updated owner = %q, want %q", updated.Owner, agentID)
	}
	if got := payloadString(updated.Payload, "status"); got != "searching" {
		t.Fatalf("updated payload status = %q, want %q", got, "searching")
	}

	renewed, err := space.RenewLease(stateID, agentID, claimed.LeaseToken, 3*time.Second)
	if err != nil {
		t.Fatalf("RenewLease(state) failed: %v", err)
	}
	if renewed == nil {
		t.Fatal("RenewLease(state) returned nil")
	}
	if renewed.LeaseToken != claimed.LeaseToken {
		t.Fatalf("renewed lease token = %q, want %q", renewed.LeaseToken, claimed.LeaseToken)
	}
	if renewed.LeaseUntil.IsZero() {
		t.Fatal("renewed lease_until is zero")
	}
	if !renewed.LeaseUntil.After(time.Now().UTC()) {
		t.Fatalf("renewed lease_until = %s, want future timestamp", renewed.LeaseUntil.Format(time.RFC3339Nano))
	}
}

func newValkeyRegressionStore(t *testing.T) agent.AgentSpace {
	t.Helper()

	cfg := valkeyConfigFromEnv()
	if cfg == nil {
		if backendRequired("valkey") {
			t.Fatalf("required backend valkey is not configured (VALKEY_ADDR/VALKEY_SHARDS empty)")
		}
		t.Skip("VALKEY_ADDR and VALKEY_SHARDS not set")
	}

	logger := zap.NewNop()
	space, err := storepkg.NewStore(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("failed to create valkey store: %v", err)
	}
	if closer, ok := space.(interface{ Close() error }); ok {
		t.Cleanup(func() {
			_ = closer.Close()
		})
	}
	return space
}

func payloadString(payload map[string]interface{}, key string) string {
	if payload == nil {
		return ""
	}
	if value, ok := payload[key].(string); ok {
		return value
	}
	return fmt.Sprint(payload[key])
}

package store_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

// Contract tests validate core agent space semantics across backends.

func TestContract_NoDoubleClaim(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testNoDoubleClaim(t, space, name)
		})
	}
}

func testNoDoubleClaim(t *testing.T, space agent.AgentSpace, backendName string) {
	t.Helper()

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("contract_claim_%s_%s", backendName, runID)
	numAgents := 20
	numWorkers := 8

	for i := 0; i < numAgents; i++ {
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"contract-claim", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Metadata:  map[string]string{"run_id": runID},
		}
		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
	}

	claimed := make(map[string]bool)
	var mu sync.Mutex
	errCh := make(chan error, numAgents)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			query := &agent.Query{
				Kind:    testKind,
				Status:  agent.StatusNew,
				Tags:    []string{runID},
				AgentID: fmt.Sprintf("worker-%d", worker),
			}

			for {
				tpl, err := space.Take(query)
				if err != nil {
					errCh <- fmt.Errorf("take failed: %w", err)
					return
				}
				if tpl == nil {
					return
				}

				mu.Lock()
				if claimed[tpl.ID] {
					mu.Unlock()
					errCh <- fmt.Errorf("duplicate claim detected for %s", tpl.ID)
					return
				}
				claimed[tpl.ID] = true
				mu.Unlock()

				if err := space.Complete(tpl.ID, query.AgentID, tpl.LeaseToken, nil); err != nil {
					errCh <- fmt.Errorf("failed to complete agent %s: %w", tpl.ID, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}

	if len(claimed) != numAgents {
		t.Errorf("Expected %d claimed agents, got %d", numAgents, len(claimed))
	}
}

func TestContract_ReleaseTokenEnforcement(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testReleaseTokenEnforcement(t, space, name)
		})
	}
}

func testReleaseTokenEnforcement(t *testing.T, space agent.AgentSpace, backendName string) {
	t.Helper()

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("contract_release_%s_%s", backendName, runID)

	tpl := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      testKind,
		Status:    agent.StatusNew,
		Tags:      []string{"contract-release", runID},
		Payload:   map[string]interface{}{"test": "release"},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Metadata:  map[string]string{"run_id": runID},
	}
	if err := space.Out(tpl); err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "agent-a",
	}

	taken, err := space.Take(query)
	if err != nil || taken == nil {
		t.Fatalf("Failed to take agent: %v", err)
	}

	if err := space.Release(taken.ID, "agent-b", taken.LeaseToken, ""); err == nil {
		t.Fatalf("Expected release to fail with wrong agent")
	}
	if err := space.Release(taken.ID, "agent-a", "bad-token", ""); err == nil {
		t.Fatalf("Expected release to fail with wrong token")
	}

	if err := space.Release(taken.ID, "agent-a", taken.LeaseToken, "test_release"); err != nil {
		t.Fatalf("Expected release to succeed with valid token: %v", err)
	}
}

func TestContract_LeaseReapReclaim(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testLeaseReapReclaim(t, space, name)
		})
	}
}

func testLeaseReapReclaim(t *testing.T, space agent.AgentSpace, backendName string) {
	t.Helper()

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("contract_reap_%s_%s", backendName, runID)

	tpl := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      testKind,
		Status:    agent.StatusNew,
		Tags:      []string{"contract-reap", runID},
		Payload:   map[string]interface{}{"test": "reap"},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Metadata: map[string]string{
			"run_id":    runID,
			"lease_sec": "1",
		},
	}
	if err := space.Out(tpl); err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "agent-reap-1",
	}

	taken, err := space.Take(query)
	if err != nil || taken == nil {
		t.Fatalf("Failed to take agent: %v", err)
	}

	sawResetState := false
	resetDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(resetDeadline) {
		current, err := space.Get(taken.ID)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if current.Status != agent.StatusNew {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if current.Owner != "" {
			t.Fatalf("Expected reaped agent owner to be empty, got %q", current.Owner)
		}
		if !current.OwnerTime.IsZero() {
			t.Fatalf("Expected reaped agent owner_time to be NULL/zero, got %s", current.OwnerTime)
		}
		if current.LeaseToken != "" {
			t.Fatalf("Expected reaped agent lease_token to be empty, got %q", current.LeaseToken)
		}
		if !current.LeaseUntil.IsZero() {
			t.Fatalf("Expected reaped agent lease_until to be NULL/zero, got %s", current.LeaseUntil)
		}
		sawResetState = true
		break
	}
	if !sawResetState {
		t.Fatalf("Agent did not transition back to NEW state after lease expiry")
	}

	reclaimQuery := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "agent-reap-2",
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tpl2, err := space.Take(reclaimQuery)
		if err != nil {
			t.Fatalf("Take failed while waiting for reaper: %v", err)
		}
		if tpl2 != nil {
			if err := space.Complete(tpl2.ID, reclaimQuery.AgentID, tpl2.LeaseToken, nil); err != nil {
				t.Fatalf("Failed to complete reclaimed agent: %v", err)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("Agent was not reclaimed after lease expiry")
}

func TestContract_StatusCounts(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testStatusCounts(t, space, name)
		})
	}
}

func TestContract_StaleCompletionRejected(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testStaleCompletionRejected(t, space, name)
		})
	}
}

func testStatusCounts(t *testing.T, space agent.AgentSpace, backendName string) {
	t.Helper()

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("contract_counts_%s_%s", backendName, runID)
	numAgents := 6

	for i := 0; i < numAgents; i++ {
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"contract-counts", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Metadata:  map[string]string{"run_id": runID},
		}
		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
	}

	counts := statusCounts(space, testKind, runID, 100)
	if counts[agent.StatusNew] != numAgents {
		t.Fatalf("Expected %d NEW agents, got %d", numAgents, counts[agent.StatusNew])
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "agent-counts",
	}
	taken, err := space.Take(query)
	if err != nil || taken == nil {
		t.Fatalf("Failed to take agent: %v", err)
	}
	if err := space.Complete(taken.ID, query.AgentID, taken.LeaseToken, nil); err != nil {
		t.Fatalf("Failed to complete agent: %v", err)
	}

	counts = statusCounts(space, testKind, runID, 100)
	total := counts[agent.StatusNew] + counts[agent.StatusInProgress] + counts[agent.StatusCompleted]
	if total != numAgents {
		t.Fatalf("Expected %d total agents, got %d", numAgents, total)
	}
	if counts[agent.StatusCompleted] != 1 {
		t.Fatalf("Expected 1 COMPLETED agent, got %d", counts[agent.StatusCompleted])
	}
}

func statusCounts(space agent.AgentSpace, kind, runID string, limit int) map[agent.Status]int {
	counts := map[agent.Status]int{
		agent.StatusNew:        0,
		agent.StatusInProgress: 0,
		agent.StatusCompleted:  0,
	}

	statuses := []agent.Status{
		agent.StatusNew,
		agent.StatusInProgress,
		agent.StatusCompleted,
	}

	for _, status := range statuses {
		agents, err := space.Query(&agent.Query{
			Kind:   kind,
			Status: status,
			Tags:   []string{runID},
			Limit:  limit,
		})
		if err != nil {
			continue
		}
		counts[status] = len(agents)
	}

	return counts
}

func testStaleCompletionRejected(t *testing.T, space agent.AgentSpace, backendName string) {
	t.Helper()

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("contract_stale_complete_%s_%s", backendName, runID)

	tpl := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      testKind,
		Status:    agent.StatusNew,
		Tags:      []string{"contract-stale-complete", runID},
		Payload:   map[string]interface{}{"test": "stale-complete"},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Metadata:  map[string]string{"run_id": runID},
	}

	if err := space.Out(tpl); err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "agent-1",
	}

	// Agent 1 takes and then releases
	taken1, err := space.Take(query)
	if err != nil || taken1 == nil {
		t.Fatalf("Agent 1 failed to take agent: %v", err)
	}
	token1 := taken1.LeaseToken

	if err := space.Release(taken1.ID, "agent-1", token1, ""); err != nil {
		t.Fatalf("Failed to release agent: %v", err)
	}

	// Agent 2 takes and completes
	query.AgentID = "agent-2"
	taken2, err := space.Take(query)
	if err != nil || taken2 == nil {
		t.Fatalf("Agent 2 failed to take agent: %v", err)
	}
	token2 := taken2.LeaseToken

	if err := space.Complete(taken2.ID, "agent-2", token2, map[string]interface{}{"result": "agent-2"}); err != nil {
		t.Fatalf("Agent 2 failed to complete agent: %v", err)
	}

	// Agent 1 tries to complete with stale token after completion
	err = space.Complete(taken2.ID, "agent-1", token1, map[string]interface{}{"result": "agent-1"})
	if err == nil {
		t.Fatalf("Expected stale completion to be rejected, but it succeeded")
	}
}

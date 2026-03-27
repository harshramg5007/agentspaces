package store_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	storepkg "github.com/urobora-ai/agentspaces/pkg/store"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

// FIFO Contract Tests
// These tests verify that all backends provide consistent FIFO ordering guarantees.

// TestFIFO_SingleConsumer verifies that a single consumer receives agents
// in CreatedAt order (oldest first).
func TestFIFO_SingleConsumer(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	// Run test against each configured backend
	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testFIFOSingleConsumer(t, space, name)
		})
	}
}

func testFIFOSingleConsumer(t *testing.T, space agent.AgentSpace, backendName string) {
	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fifo_test_%s_%s", backendName, runID)

	// Create agents with explicit ordering via sequence number
	numAgents := 10
	tupleIDs := make([]string, numAgents)

	for i := 0; i < numAgents; i++ {
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"fifo-test", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Metadata: map[string]string{
				"run_id":   runID,
				"sequence": fmt.Sprintf("%d", i),
			},
		}
		tupleIDs[i] = tpl.ID

		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}

		// Small delay to ensure distinct CreatedAt timestamps
		time.Sleep(5 * time.Millisecond)
	}

	// Take all agents and verify FIFO order
	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: "fifo-test-agent",
	}

	receivedSequences := make([]int, 0, numAgents)
	for i := 0; i < numAgents; i++ {
		tpl, err := space.Take(query)
		if err != nil {
			t.Fatalf("Failed to take agent %d: %v", i, err)
		}
		if tpl == nil {
			t.Fatalf("Got nil agent at position %d, expected more agents", i)
		}

		seq, ok := tpl.Payload["sequence"].(float64)
		if !ok {
			// Try int
			seqInt, ok := tpl.Payload["sequence"].(int)
			if !ok {
				t.Fatalf("Agent %d has invalid sequence type: %T", i, tpl.Payload["sequence"])
			}
			seq = float64(seqInt)
		}
		receivedSequences = append(receivedSequences, int(seq))

		// Complete to clean up
		if err := space.Complete(tpl.ID, "fifo-test-agent", tpl.LeaseToken, nil); err != nil {
			t.Logf("Warning: failed to complete agent: %v", err)
		}
	}

	// Verify FIFO order
	for i := 0; i < numAgents; i++ {
		if receivedSequences[i] != i {
			t.Errorf("FIFO violation at position %d: expected sequence %d, got %d. Full order: %v",
				i, i, receivedSequences[i], receivedSequences)
			break
		}
	}
}

// TestFIFO_MultiConsumer verifies that with multiple concurrent consumers,
// no agent with a later CreatedAt is claimed while an earlier one is still NEW.
func TestFIFO_MultiConsumer(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testFIFOMultiConsumer(t, space, name)
		})
	}
}

func testFIFOMultiConsumer(t *testing.T, space agent.AgentSpace, backendName string) {
	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fifo_multi_%s_%s", backendName, runID)

	// Create agents
	numAgents := 20
	for i := 0; i < numAgents; i++ {
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"fifo-multi-test", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Metadata: map[string]string{
				"run_id":   runID,
				"sequence": fmt.Sprintf("%d", i),
			},
		}

		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Run concurrent consumers
	numConsumers := 5
	var wg sync.WaitGroup
	claimedSequences := make(chan int, numAgents)

	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			query := &agent.Query{
				Kind:    testKind,
				Status:  agent.StatusNew,
				Tags:    []string{runID},
				AgentID: fmt.Sprintf("consumer-%d", consumerID),
			}

			for {
				tpl, err := space.Take(query)
				if err != nil || tpl == nil {
					return
				}

				seq, ok := tpl.Payload["sequence"].(float64)
				if !ok {
					seqInt, ok := tpl.Payload["sequence"].(int)
					if ok {
						seq = float64(seqInt)
					}
				}
				claimedSequences <- int(seq)

				// Complete
				space.Complete(tpl.ID, fmt.Sprintf("consumer-%d", consumerID), tpl.LeaseToken, nil)
			}
		}(c)
	}

	wg.Wait()
	close(claimedSequences)

	// Collect all claimed sequences
	claimed := make([]int, 0, numAgents)
	for seq := range claimedSequences {
		claimed = append(claimed, seq)
	}

	if len(claimed) != numAgents {
		t.Errorf("Expected %d agents, got %d", numAgents, len(claimed))
	}

	// Verify no gaps (all sequences present)
	seen := make(map[int]bool)
	for _, seq := range claimed {
		if seen[seq] {
			t.Errorf("Duplicate sequence %d claimed", seq)
		}
		seen[seq] = true
	}

	for i := 0; i < numAgents; i++ {
		if !seen[i] {
			t.Errorf("Sequence %d was not claimed", i)
		}
	}
}

// TestFIFO_ReleasePreservesPosition verifies that when a agent is released,
// it keeps its original position in the FIFO queue.
func TestFIFO_ReleasePreservesPosition(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testFIFOReleasePreservesPosition(t, space, name)
		})
	}
}

func testFIFOReleasePreservesPosition(t *testing.T, space agent.AgentSpace, backendName string) {
	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fifo_release_%s_%s", backendName, runID)
	agentID := "release-test-agent"

	// Create 3 agents: 1, 2, 3
	for i := 1; i <= 3; i++ {
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"fifo-release-test", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Metadata: map[string]string{
				"run_id":   runID,
				"sequence": fmt.Sprintf("%d", i),
			},
		}

		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		Tags:    []string{runID},
		AgentID: agentID,
	}

	// Take agent 1
	tpl1, err := space.Take(query)
	if err != nil || tpl1 == nil {
		t.Fatalf("Failed to take agent 1: %v", err)
	}

	seq1 := getSequence(tpl1)
	if seq1 != 1 {
		t.Fatalf("Expected first take to return sequence 1, got %d", seq1)
	}

	// Release agent 1 back to NEW status
	if err := space.Release(tpl1.ID, agentID, tpl1.LeaseToken, "test_release"); err != nil {
		t.Fatalf("Failed to release agent 1: %v", err)
	}

	// Next take should return agent 1 again (it keeps its original position)
	tpl1Again, err := space.Take(query)
	if err != nil || tpl1Again == nil {
		t.Fatalf("Failed to take after release: %v", err)
	}

	seq1Again := getSequence(tpl1Again)
	if seq1Again != 1 {
		t.Errorf("After release, expected sequence 1 (original position), got %d", seq1Again)
	}

	// Clean up
	space.Complete(tpl1Again.ID, agentID, tpl1Again.LeaseToken, nil)

	// Take remaining agents
	for i := 2; i <= 3; i++ {
		tpl, err := space.Take(query)
		if err != nil || tpl == nil {
			t.Fatalf("Failed to take agent %d: %v", i, err)
		}
		space.Complete(tpl.ID, agentID, tpl.LeaseToken, nil)
	}
}

// TestFIFO_QueueStressOrdering verifies queue-scoped FIFO ordering under high concurrency.
func TestFIFO_QueueStressOrdering(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testFIFOQueueStressOrdering(t, space, name)
		})
	}
}

func TestFIFO_QueueHeadKindMismatchDoesNotBypassPostgresStrictQueueHead(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	space, ok := backends["postgres"]
	if !ok {
		t.Skip("Skipping Postgres queue-head test: Postgres backend is not configured")
	}

	testQueueHeadKindMismatchDoesNotBypass(t, space)
}

type claimEvent struct {
	timestamp time.Time
	sequence  int
}

func testFIFOQueueStressOrdering(t *testing.T, space agent.AgentSpace, backendName string) {
	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fifo_queue_%s_%s", backendName, runID)
	queueName := fmt.Sprintf("queue-%s", runID)

	taskCount := getEnvInt("FIFO_STRESS_TASKS", 10000)
	consumerCount := getEnvInt("FIFO_STRESS_CONSUMERS", 200)
	eventTimeoutSec := getEnvInt("FIFO_STRESS_EVENT_TIMEOUT_SEC", 120)

	seqByID := make(map[string]int, taskCount)
	baseTime := time.Now().UTC()

	var eventMu sync.Mutex
	claimedEvents := make([]claimEvent, 0, taskCount)

	err := space.Subscribe(&agent.EventFilter{
		Types: []agent.EventType{agent.EventClaimed},
		Kinds: []string{testKind},
	}, func(event *agent.Event) {
		if event == nil || event.TupleID == "" {
			return
		}
		seq, ok := seqByID[event.TupleID]
		if !ok {
			return
		}
		eventMu.Lock()
		claimedEvents = append(claimedEvents, claimEvent{
			timestamp: event.Timestamp,
			sequence:  seq,
		})
		eventMu.Unlock()
	})
	if err != nil {
		t.Fatalf("Failed to subscribe to events: %v", err)
	}

	for i := 0; i < taskCount; i++ {
		createdAt := baseTime.Add(time.Duration(i) * time.Millisecond)
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"fifo-queue-stress", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Metadata: map[string]string{
				"run_id":           runID,
				"queue":            queueName,
				"queue_created_ms": strconv.FormatInt(createdAt.UnixMilli(), 10),
			},
		}

		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
		seqByID[tpl.ID] = i
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		AgentID: "fifo-queue-stress",
		Metadata: map[string]string{
			"queue": queueName,
		},
	}

	var wg sync.WaitGroup
	var claimedCount int32
	var duplicateClaims int32
	claimedIDs := make(map[string]struct{})
	var claimedMu sync.Mutex

	for c := 0; c < consumerCount; c++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			consumerQuery := *query
			consumerQuery.AgentID = fmt.Sprintf("consumer-%d", consumerID)

			for {
				if atomic.LoadInt32(&claimedCount) >= int32(taskCount) {
					return
				}
				tpl, err := space.Take(&consumerQuery)
				if err != nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if tpl == nil {
					if atomic.LoadInt32(&claimedCount) >= int32(taskCount) {
						return
					}
					time.Sleep(2 * time.Millisecond)
					continue
				}

				claimedMu.Lock()
				if _, ok := claimedIDs[tpl.ID]; ok {
					atomic.AddInt32(&duplicateClaims, 1)
					claimedMu.Unlock()
				} else {
					claimedIDs[tpl.ID] = struct{}{}
					claimedMu.Unlock()
					atomic.AddInt32(&claimedCount, 1)
				}

				if err := space.Complete(tpl.ID, consumerQuery.AgentID, tpl.LeaseToken, nil); err != nil {
					t.Logf("Warning: failed to complete agent: %v", err)
				}
			}
		}(c)
	}

	wg.Wait()

	if duplicateClaims > 0 {
		t.Fatalf("Detected %d duplicate claims during stress test", duplicateClaims)
	}

	if claimedCount != int32(taskCount) {
		t.Fatalf("Expected %d claimed agents, got %d", taskCount, claimedCount)
	}

	deadline := time.Now().Add(time.Duration(eventTimeoutSec) * time.Second)
	for {
		eventMu.Lock()
		eventsCaptured := len(claimedEvents)
		eventMu.Unlock()
		if eventsCaptured >= taskCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Timed out waiting for claim events (%d/%d)", eventsCaptured, taskCount)
		}
		time.Sleep(100 * time.Millisecond)
	}

	eventMu.Lock()
	events := make([]claimEvent, len(claimedEvents))
	copy(events, claimedEvents)
	eventMu.Unlock()

	sort.Slice(events, func(i, j int) bool {
		if events[i].timestamp.Equal(events[j].timestamp) {
			return events[i].sequence < events[j].sequence
		}
		return events[i].timestamp.Before(events[j].timestamp)
	})

	for i := 1; i < len(events); i++ {
		if events[i].sequence < events[i-1].sequence {
			t.Fatalf("FIFO violation: sequence %d claimed after %d", events[i].sequence, events[i-1].sequence)
		}
	}
}

func testQueueHeadKindMismatchDoesNotBypass(t *testing.T, space agent.AgentSpace) {
	runID := uuid.New().String()[:8]
	queueName := fmt.Sprintf("queue-head-%s", runID)
	headKind := fmt.Sprintf("queue_head_a_%s", runID)
	followerKind := fmt.Sprintf("queue_head_b_%s", runID)
	baseTime := time.Now().UTC()

	headAgent := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      headKind,
		Status:    agent.StatusNew,
		Tags:      []string{"queue-head-kind-mismatch", runID},
		Payload:   map[string]interface{}{"sequence": 1},
		CreatedAt: baseTime,
		UpdatedAt: baseTime,
		Metadata: map[string]string{
			"run_id":           runID,
			"queue":            queueName,
			"queue_created_ms": strconv.FormatInt(baseTime.UnixMilli(), 10),
		},
	}
	followerTime := baseTime.Add(time.Millisecond)
	followerAgent := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      followerKind,
		Status:    agent.StatusNew,
		Tags:      []string{"queue-head-kind-mismatch", runID},
		Payload:   map[string]interface{}{"sequence": 2},
		CreatedAt: followerTime,
		UpdatedAt: followerTime,
		Metadata: map[string]string{
			"run_id":           runID,
			"queue":            queueName,
			"queue_created_ms": strconv.FormatInt(followerTime.UnixMilli(), 10),
		},
	}

	if err := space.Out(headAgent); err != nil {
		t.Fatalf("Failed to create queue head agent: %v", err)
	}
	if err := space.Out(followerAgent); err != nil {
		t.Fatalf("Failed to create follower agent: %v", err)
	}

	queryFollower := &agent.Query{
		Kind:    followerKind,
		Status:  agent.StatusNew,
		AgentID: "queue-head-agent-b",
		Metadata: map[string]string{
			"queue": queueName,
		},
	}
	takenFollower, err := space.Take(queryFollower)
	if err != nil {
		t.Fatalf("Failed to take follower-kind agent: %v", err)
	}
	if takenFollower != nil {
		t.Fatalf("Expected follower kind to remain blocked behind queue head, got agent %s", takenFollower.ID)
	}

	queryHead := &agent.Query{
		Kind:    headKind,
		Status:  agent.StatusNew,
		AgentID: "queue-head-agent-a",
		Metadata: map[string]string{
			"queue": queueName,
		},
	}
	takenHead, err := space.Take(queryHead)
	if err != nil || takenHead == nil {
		t.Fatalf("Failed to take queue head agent: %v", err)
	}
	if takenHead.ID != headAgent.ID {
		t.Fatalf("Expected queue head agent %s, got %s", headAgent.ID, takenHead.ID)
	}
	if err := space.Complete(takenHead.ID, "queue-head-agent-a", takenHead.LeaseToken, nil); err != nil {
		t.Fatalf("Failed to complete queue head agent: %v", err)
	}

	takenFollower, err = space.Take(queryFollower)
	if err != nil || takenFollower == nil {
		t.Fatalf("Failed to take follower agent after head advanced: %v", err)
	}
	if takenFollower.ID != followerAgent.ID {
		t.Fatalf("Expected follower agent %s after head advanced, got %s", followerAgent.ID, takenFollower.ID)
	}
	if err := space.Complete(takenFollower.ID, "queue-head-agent-b", takenFollower.LeaseToken, nil); err != nil {
		t.Fatalf("Failed to complete follower agent: %v", err)
	}
}

// TestPostgres_ParallelQueueClaim_NoDuplicateClaims verifies that Postgres parallel queue mode
// can process a single hot queue concurrently without duplicate claims.
func TestPostgres_ParallelQueueClaim_NoDuplicateClaims(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	pgHost := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if pgHost == "" {
		t.Skip("Skipping Postgres parallel queue test: POSTGRES_HOST is not set")
	}

	pgPort := 5432
	if rawPort := strings.TrimSpace(os.Getenv("POSTGRES_PORT")); rawPort != "" {
		if parsed, err := strconv.Atoi(rawPort); err == nil && parsed > 0 {
			pgPort = parsed
		}
	}

	pgUser := strings.TrimSpace(os.Getenv("POSTGRES_USER"))
	if pgUser == "" {
		pgUser = "postgres"
	}
	pgPassword := strings.TrimSpace(os.Getenv("POSTGRES_PASSWORD"))
	if pgPassword == "" {
		pgPassword = "postgres"
	}
	pgDatabase := strings.TrimSpace(os.Getenv("POSTGRES_DATABASE"))
	if pgDatabase == "" {
		pgDatabase = "agent_space"
	}

	ctx := context.Background()
	logger := zap.NewNop()
	storeCfg := &storepkg.Config{
		Type: storepkg.StoreTypePostgres,
		Postgres: &storepkg.PostgresConfig{
			Host:           pgHost,
			Port:           pgPort,
			User:           pgUser,
			Password:       pgPassword,
			Database:       pgDatabase,
			SSLMode:        "disable",
			QueueClaimMode: "parallel",
		},
	}
	space, err := storepkg.NewStore(ctx, storeCfg, logger)
	if err != nil {
		t.Fatalf("Failed to initialize Postgres store in parallel queue mode: %v", err)
	}
	if closer, ok := space.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fifo_parallel_queue_%s", runID)
	queueName := fmt.Sprintf("queue-%s", runID)

	taskCount := getEnvInt("POSTGRES_PARALLEL_QUEUE_TASKS", 5000)
	consumerCount := getEnvInt("POSTGRES_PARALLEL_QUEUE_CONSUMERS", 200)
	testDeadline := time.Now().Add(2 * time.Minute)

	for i := 0; i < taskCount; i++ {
		now := time.Now().UTC()
		tpl := &agent.Agent{
			ID:        uuid.New().String(),
			Kind:      testKind,
			Status:    agent.StatusNew,
			Tags:      []string{"postgres-parallel-queue", runID},
			Payload:   map[string]interface{}{"sequence": i},
			CreatedAt: now,
			UpdatedAt: now,
			Metadata: map[string]string{
				"run_id": runID,
				"queue":  queueName,
			},
		}
		if err := space.Out(tpl); err != nil {
			t.Fatalf("Failed to create agent %d: %v", i, err)
		}
	}

	query := &agent.Query{
		Kind:    testKind,
		Status:  agent.StatusNew,
		AgentID: "parallel-queue",
		Metadata: map[string]string{
			"queue": queueName,
		},
	}

	var wg sync.WaitGroup
	var claimedCount int32
	var duplicateClaims int32
	var takeErrCount int32
	var claimedMu sync.Mutex
	var takeErrMu sync.Mutex
	takeErrSamples := make([]string, 0, 5)
	claimedIDs := make(map[string]struct{}, taskCount)
	errCh := make(chan error, consumerCount)

	for c := 0; c < consumerCount; c++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			consumerQuery := *query
			consumerQuery.AgentID = fmt.Sprintf("parallel-consumer-%d", consumerID)

			for {
				if atomic.LoadInt32(&claimedCount) >= int32(taskCount) {
					return
				}
				if time.Now().After(testDeadline) {
					errCh <- fmt.Errorf("timed out draining queue, claimed=%d/%d", atomic.LoadInt32(&claimedCount), taskCount)
					return
				}

				tpl, err := space.Take(&consumerQuery)
				if err != nil {
					atomic.AddInt32(&takeErrCount, 1)
					takeErrMu.Lock()
					if len(takeErrSamples) < cap(takeErrSamples) {
						takeErrSamples = append(takeErrSamples, err.Error())
					}
					takeErrMu.Unlock()
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if tpl == nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}

				claimedMu.Lock()
				if _, exists := claimedIDs[tpl.ID]; exists {
					atomic.AddInt32(&duplicateClaims, 1)
				} else {
					claimedIDs[tpl.ID] = struct{}{}
					atomic.AddInt32(&claimedCount, 1)
				}
				claimedMu.Unlock()

				if err := space.Complete(tpl.ID, consumerQuery.AgentID, tpl.LeaseToken, nil); err != nil {
					errCh <- fmt.Errorf("failed to complete agent %s: %w", tpl.ID, err)
					return
				}
			}
		}(c)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		return
	}

	if duplicateClaims > 0 {
		t.Fatalf("Detected %d duplicate claims in Postgres parallel queue mode", duplicateClaims)
	}
	if takeErrCount > 0 {
		t.Fatalf("encountered %d take errors in Postgres parallel queue mode (samples=%v)", takeErrCount, takeErrSamples)
	}
	if claimedCount != int32(taskCount) {
		t.Fatalf("Expected %d claimed agents, got %d", taskCount, claimedCount)
	}
}

// TestFencing_StaleTokenRejected verifies that a stale lease token is rejected
// after the lease expires and the agent is reclaimed.
func TestFencing_StaleTokenRejected(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	backends := getConfiguredBackends(t, ctx, logger)
	for name, space := range backends {
		t.Run(name, func(t *testing.T) {
			testFencingStaleToken(t, space, name)
		})
	}
}

func testFencingStaleToken(t *testing.T, space agent.AgentSpace, backendName string) {
	runID := uuid.New().String()[:8]
	testKind := fmt.Sprintf("fencing_%s_%s", backendName, runID)

	// Create a agent
	tpl := &agent.Agent{
		ID:        uuid.New().String(),
		Kind:      testKind,
		Status:    agent.StatusNew,
		Tags:      []string{"fencing-test", runID},
		Payload:   map[string]interface{}{"test": "fencing"},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Metadata: map[string]string{
			"run_id": runID,
		},
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

	// Agent 1 takes the agent
	taken1, err := space.Take(query)
	if err != nil || taken1 == nil {
		t.Fatalf("Agent 1 failed to take agent: %v", err)
	}
	token1 := taken1.LeaseToken

	// Agent 1 releases (simulating timeout or voluntary release)
	if err := space.Release(taken1.ID, "agent-1", token1, ""); err != nil {
		t.Fatalf("Failed to release agent: %v", err)
	}

	// Agent 2 takes the agent
	query.AgentID = "agent-2"
	taken2, err := space.Take(query)
	if err != nil || taken2 == nil {
		t.Fatalf("Agent 2 failed to take agent: %v", err)
	}
	token2 := taken2.LeaseToken

	// Verify tokens are different
	if token1 == token2 {
		t.Error("Expected different lease tokens after release and re-take")
	}

	// Agent 1 tries to complete with stale token - should fail
	err = space.Complete(taken2.ID, "agent-1", token1, map[string]interface{}{"result": "from agent 1"})
	if err == nil {
		t.Error("Expected stale token to be rejected, but complete succeeded")
	}

	// Agent 2 completes with valid token - should succeed
	err = space.Complete(taken2.ID, "agent-2", token2, map[string]interface{}{"result": "from agent 2"})
	if err != nil {
		t.Errorf("Agent 2 with valid token should succeed: %v", err)
	}
}

// Helper functions

func getSequence(tpl *agent.Agent) int {
	if tpl == nil || tpl.Payload == nil {
		return -1
	}

	switch v := tpl.Payload["sequence"].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	default:
		return -1
	}
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	if parsed <= 0 {
		return defaultValue
	}
	return parsed
}

func getConfiguredBackends(t *testing.T, ctx context.Context, logger *zap.Logger) map[string]agent.AgentSpace {
	required := requiredBackends()
	backends := make(map[string]agent.AgentSpace)

	// SQLite in-memory backend (always available in integration environments)
	sqliteStore, err := storepkg.NewStore(ctx, &storepkg.Config{Type: storepkg.StoreTypeSQLiteMem}, logger)
	if err != nil {
		t.Logf("Skipping SQLite backend: %v", err)
	} else {
		backends["sqlite-memory"] = sqliteStore
	}

	// Postgres backend (if configured)
	pgHost := os.Getenv("POSTGRES_HOST")
	if pgHost != "" {
		pgCfg := &storepkg.Config{
			Type: storepkg.StoreTypePostgres,
			Postgres: &storepkg.PostgresConfig{
				Host:     pgHost,
				Port:     5432,
				User:     os.Getenv("POSTGRES_USER"),
				Password: os.Getenv("POSTGRES_PASSWORD"),
				Database: os.Getenv("POSTGRES_DATABASE"),
				SSLMode:  "disable",
			},
		}

		pgStore, err := storepkg.NewStore(ctx, pgCfg, logger)
		if err != nil {
			t.Logf("Skipping Postgres backend: %v", err)
		} else {
			backends["postgres"] = pgStore
		}
	}

	if len(backends) == 0 {
		t.Skip("No backends available for testing")
	}
	for backend := range required {
		if _, ok := backends[backend]; !ok {
			t.Fatalf("required integration backend %q unavailable (set env/service and retry)", backend)
		}
	}

	return backends
}

func requiredBackends() map[string]struct{} {
	out := make(map[string]struct{})
	raw := os.Getenv("REQUIRE_INTEGRATION_BACKENDS")
	for _, backend := range strings.Split(raw, ",") {
		name := normalizeBackendName(backend)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func backendRequired(name string) bool {
	_, ok := requiredBackends()[normalizeBackendName(name)]
	return ok
}

func normalizeBackendName(value string) string {
	name := strings.ToLower(strings.TrimSpace(value))
	switch name {
	case "sqlite", "sqlite-mem", "sqlite_memory":
		return "sqlite-memory"
	case "sqlite-file", "sqlite_file":
		return "sqlite-file"
	default:
		return name
	}
}

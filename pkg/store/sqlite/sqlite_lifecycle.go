package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
)

func (s *SQLiteStore) Out(t *agent.Agent) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("out", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()

	if t.ID == "" {
		t.ID = uuid.New().String()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	t.UpdatedAt = time.Now().UTC()
	if t.Version == 0 {
		t.Version = 1
	}
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}

	event := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventCreated,
		Kind:      t.Kind,
		Timestamp: time.Now().UTC(),
		AgentID:   t.Metadata["agent_id"],
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data: map[string]interface{}{
			"agent": t,
		},
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := upsertSQLiteAgent(ctx, tx, t); err != nil {
		return err
	}
	if err := insertSQLiteEvent(ctx, tx, event); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.notifySubscribers(event)
	success = true
	return nil
}

// Get retrieves a agent by ID (O(1) direct lookup).
func (s *SQLiteStore) Get(id string) (*agent.Agent, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("get", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()

	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	success = true
	return t, nil
}

// In takes a agent from the space (blocking, destructive read).

func (s *SQLiteStore) Take(query *agent.Query) (*agent.Agent, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("take", s.storeLabel, success, time.Since(start).Seconds())
	}()

	if query != nil && query.Status != "" && query.Status != agent.StatusNew {
		return nil, nil
	}

	agentID := ""
	if query != nil {
		agentID = query.AgentID
		if agentID == "" && query.Metadata != nil {
			agentID = query.Metadata["agent_id"]
		}
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	nowMs := time.Now().UTC().UnixMilli()
	where, args, needsFilter := buildSQLiteWhereClauses(query, true, nowMs)
	selectSQL := fmt.Sprintf("SELECT %s FROM agents WHERE %s ORDER BY created_at_ms ASC", sqliteAgentSelectColumns, where)
	if !needsFilter {
		selectSQL += " LIMIT 1"
	}

	rows, err := tx.QueryContext(ctx, selectSQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		t, err := scanSQLiteAgent(rows)
		if err != nil {
			s.logger.Warn("failed to scan agent", zap.Error(err))
			continue
		}
		if needsFilter && !matchesQueryFilters(t, query) {
			continue
		}

		now := time.Now().UTC()
		leaseToken := uuid.New().String()
		leaseUntil := now.Add(s.leaseDurationForAgent(t))

		t.Status = agent.StatusInProgress
		t.Owner = agentID
		t.OwnerTime = now
		t.LeaseToken = leaseToken
		t.LeaseUntil = leaseUntil
		t.UpdatedAt = now
		t.Version++

		updated, err := claimSQLiteAgent(ctx, tx, t, nowMs)
		if err != nil {
			return nil, err
		}
		if !updated {
			continue
		}

		event := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   t.ID,
			Type:      agent.EventClaimed,
			Kind:      t.Kind,
			Timestamp: time.Now().UTC(),
			AgentID:   agentID,
			TraceID:   t.TraceID,
			Version:   t.Version,
			Data:      map[string]interface{}{"agent_id": agentID},
		}
		if err := insertSQLiteEvent(ctx, tx, event); err != nil {
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			return nil, err
		}

		s.notifySubscribers(event)
		success = true
		return t, nil
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	success = true
	return nil, nil
}

// Update updates a agent in place.
func (s *SQLiteStore) Update(id string, updates map[string]interface{}) error {
	if updates == nil {
		return nil
	}
	_, err := s.UpdateAndGet(id, updates)
	return err
}

// UpdateAndGet updates a agent and returns the updated agent.
func (s *SQLiteStore) UpdateAndGet(id string, updates map[string]interface{}) (*agent.Agent, error) {
	if updates == nil {
		return s.Get(id)
	}

	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("update", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	applyUpdates(t, updates)
	t.UpdatedAt = time.Now().UTC()
	t.Version++

	if err := updateSQLiteAgent(ctx, tx, t); err != nil {
		return nil, err
	}

	event := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventUpdated,
		Kind:      t.Kind,
		Timestamp: time.Now().UTC(),
		AgentID:   getStringFromMap(updates, "agent_id"),
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      updates,
	}
	if err := insertSQLiteEvent(ctx, tx, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.notifySubscribers(event)
	success = true
	return t, nil
}

// Complete marks a agent as completed.
func (s *SQLiteStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	_, err := s.CompleteAndGet(id, agentID, leaseToken, result)
	return err
}

// CompleteAndGet marks a agent as completed and returns the updated agent.
func (s *SQLiteStore) CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}

	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("complete", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	if t.Status == agent.StatusCompleted {
		if !completionTokenMatches(t, leaseToken) {
			return nil, fmt.Errorf(tokenMismatchStaleErr)
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		success = true
		return t, nil
	}
	if t.Status != agent.StatusInProgress {
		return nil, fmt.Errorf("invalid status: %s", t.Status)
	}
	if t.Owner != agentID {
		return nil, fmt.Errorf("cannot complete: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return nil, fmt.Errorf("lease token mismatch")
	}

	now := time.Now().UTC()
	if result == nil {
		result = map[string]interface{}{}
	}
	if t.Payload == nil {
		t.Payload = make(map[string]interface{})
	}
	t.Payload["result"] = result
	setCompletionMetadata(t, agentID, leaseToken, now)

	t.Status = agent.StatusCompleted
	t.Owner = ""
	t.OwnerTime = time.Time{}
	t.LeaseToken = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = now
	t.Version++

	if err := updateSQLiteAgent(ctx, tx, t); err != nil {
		return nil, err
	}

	event := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventCompleted,
		Kind:      t.Kind,
		Timestamp: now,
		AgentID:   agentID,
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      result,
	}
	if err := insertSQLiteEvent(ctx, tx, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.notifySubscribers(event)
	success = true
	return t, nil
}

// CompleteAndOut atomically completes a agent and creates new output agents.
func (s *SQLiteStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	if agentID == "" {
		return nil, nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, nil, fmt.Errorf("lease token is required")
	}

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, nil, err
	}

	if t.Status == agent.StatusCompleted {
		if !completionTokenMatches(t, leaseToken) {
			return nil, nil, fmt.Errorf(tokenMismatchStaleErr)
		}
		if err := tx.Commit(); err != nil {
			return nil, nil, err
		}
		return t, nil, nil
	}
	if t.Status != agent.StatusInProgress {
		return nil, nil, fmt.Errorf("invalid status: %s", t.Status)
	}
	if t.Owner != agentID {
		return nil, nil, fmt.Errorf("cannot complete: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return nil, nil, fmt.Errorf("lease token mismatch")
	}

	now := time.Now().UTC()
	if result == nil {
		result = map[string]interface{}{}
	}
	if t.Payload == nil {
		t.Payload = make(map[string]interface{})
	}
	t.Payload["result"] = result
	setCompletionMetadata(t, agentID, leaseToken, now)

	t.Status = agent.StatusCompleted
	t.Owner = ""
	t.OwnerTime = time.Time{}
	t.LeaseToken = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = now
	t.Version++

	if err := updateSQLiteAgent(ctx, tx, t); err != nil {
		return nil, nil, err
	}

	completedEvent := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventCompleted,
		Kind:      t.Kind,
		Timestamp: now,
		AgentID:   agentID,
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      result,
	}
	if err := insertSQLiteEvent(ctx, tx, completedEvent); err != nil {
		return nil, nil, err
	}

	createdOutputs := make([]*agent.Agent, 0, len(outputs))
	createdEvents := make([]*agent.Event, 0, len(outputs))
	for _, out := range outputs {
		if out.ID == "" {
			out.ID = uuid.New().String()
		}
		if out.CreatedAt.IsZero() {
			out.CreatedAt = time.Now().UTC()
		}
		out.UpdatedAt = time.Now().UTC()
		if out.Version == 0 {
			out.Version = 1
		}
		if out.Metadata == nil {
			out.Metadata = make(map[string]string)
		}
		if out.Status == "" {
			out.Status = agent.StatusNew
		}
		if out.TraceID == "" && t.TraceID != "" {
			out.TraceID = t.TraceID
		}
		if out.ParentID == "" {
			out.ParentID = t.ID
		}

		if err := upsertSQLiteAgent(ctx, tx, out); err != nil {
			return nil, nil, fmt.Errorf("failed to create output agent: %w", err)
		}

		outEvent := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   out.ID,
			Type:      agent.EventCreated,
			Kind:      out.Kind,
			Timestamp: time.Now().UTC(),
			AgentID:   agentID,
			TraceID:   out.TraceID,
			Version:   out.Version,
			Data: map[string]interface{}{
				"agent":     out,
				"parent_id": t.ID,
			},
		}
		if err := insertSQLiteEvent(ctx, tx, outEvent); err != nil {
			return nil, nil, fmt.Errorf("failed to create output event: %w", err)
		}

		createdOutputs = append(createdOutputs, out)
		createdEvents = append(createdEvents, outEvent)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}

	s.notifySubscribers(completedEvent)
	for _, e := range createdEvents {
		s.notifySubscribers(e)
	}

	return t, createdOutputs, nil
}

// Release releases ownership of a agent.
func (s *SQLiteStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := s.ReleaseAndGet(id, agentID, leaseToken, reason)
	return err
}

// ReleaseAndGet releases ownership of a agent and returns the updated agent.
func (s *SQLiteStore) ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}

	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("release", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	if t.Status != agent.StatusInProgress {
		return nil, fmt.Errorf("invalid status: %s", t.Status)
	}
	if t.Owner != agentID {
		return nil, fmt.Errorf("cannot release: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return nil, fmt.Errorf("lease token mismatch")
	}

	previousOwner := t.Owner

	t.Status = agent.StatusNew
	t.Owner = ""
	t.OwnerTime = time.Time{}
	t.LeaseToken = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = time.Now().UTC()
	t.Version++

	if err := updateSQLiteAgent(ctx, tx, t); err != nil {
		return nil, err
	}

	eventData := map[string]interface{}{}
	if reason != "" {
		eventData["reap_reason"] = reason
		eventData["previous_owner"] = previousOwner
	}

	event := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventReleased,
		Kind:      t.Kind,
		Timestamp: time.Now().UTC(),
		AgentID:   agentID,
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      eventData,
	}
	if err := insertSQLiteEvent(ctx, tx, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.notifySubscribers(event)
	success = true
	return t, nil
}

// RenewLease extends the lease for an in-progress agent.
func (s *SQLiteStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=? AND %s", sqliteAgentSelectColumns, sqliteTTLClause()),
		id,
		nowMs,
	)
	t, err := scanSQLiteAgent(row)
	if err != nil {
		if sqliteIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	if t.Status != agent.StatusInProgress {
		return nil, fmt.Errorf("invalid status: %s", t.Status)
	}
	if t.Owner != agentID {
		return nil, fmt.Errorf("cannot renew: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return nil, fmt.Errorf("lease token mismatch")
	}

	if leaseDuration <= 0 {
		leaseDuration = s.leaseDurationForAgent(t)
	}

	now := time.Now().UTC()
	t.OwnerTime = now
	t.LeaseUntil = now.Add(leaseDuration)
	t.UpdatedAt = now

	if err := updateSQLiteAgent(ctx, tx, t); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

// Subscribe subscribes to events.

func (s *SQLiteStore) GetDAG(rootID string) (*agent.DAG, error) {
	dag := &agent.DAG{
		Nodes: make(map[string]*agent.DAGNode),
		Edges: []agent.Edge{},
	}

	visited := make(map[string]bool)
	queue := []string{rootID}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		if visited[id] {
			continue
		}
		visited[id] = true

		t, err := s.Get(id)
		if err != nil {
			continue
		}

		dag.Nodes[id] = &agent.DAGNode{
			TupleID:   t.ID,
			Kind:      t.Kind,
			Status:    t.Status,
			CreatedAt: t.CreatedAt,
			Metadata:  t.Metadata,
		}

		if t.ParentID != "" {
			dag.Edges = append(dag.Edges, agent.Edge{
				From: t.ParentID,
				To:   t.ID,
				Type: "parent",
			})
			queue = append(queue, t.ParentID)
		}

		children, err := s.Query(&agent.Query{ParentID: t.ID})
		if err != nil {
			continue
		}
		for _, child := range children {
			dag.Edges = append(dag.Edges, agent.Edge{
				From: t.ID,
				To:   child.ID,
				Type: "spawned",
			})
			queue = append(queue, child.ID)
		}
	}

	return dag, nil
}

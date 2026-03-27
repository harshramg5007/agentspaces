package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
)

func (s *PostgresStore) Out(t *agent.Agent) error {
	return s.out(context.Background(), t)
}

func (s *PostgresStore) out(ctx context.Context, t *agent.Agent) error {
	ctx = defaultContext(ctx)
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
	if t.ShardID == "" && s.shardID != "" {
		t.ShardID = s.shardID
	}

	eventData := map[string]interface{}{
		"agent": t,
	}
	if s.slimEvents {
		eventData = map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   t.Status,
		}
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
		Data:      eventData,
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := upsertAgent(ctx, tx, t); err != nil {
		return err
	}
	if !s.deferEvents {
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return err
		}
	}
	if err := queueWakeupNotify(ctx, tx, t.NamespaceID, t.Metadata[queueMetadataKey]); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if s.deferEvents {
		s.enqueueEvent(event, t.NamespaceID)
	}
	s.notifySubscribers(event)
	s.signalWakeups()
	return nil
}

// Get retrieves a agent by ID (O(1) direct lookup).
func (s *PostgresStore) Get(id string) (*agent.Agent, error) {
	return s.get(context.Background(), id)
}

func (s *PostgresStore) get(ctx context.Context, id string) (*agent.Agent, error) {
	ctx = defaultContext(ctx)
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=$1 AND %s", agentSelectColumns, ttlClause()),
		id,
	)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	return t, nil
}

// In takes a agent from the space (blocking, destructive read).

func (s *PostgresStore) Take(query *agent.Query) (*agent.Agent, error) {
	return s.take(context.Background(), query)
}

func (s *PostgresStore) take(ctx context.Context, query *agent.Query) (*agent.Agent, error) {
	if query != nil && query.Status != "" && query.Status != agent.StatusNew {
		return nil, nil
	}

	agentID := ""
	queue := ""
	if query != nil {
		agentID = query.AgentID
		if agentID == "" && query.Metadata != nil {
			agentID = query.Metadata["agent_id"]
		}
		if query.Metadata != nil {
			queue = query.Metadata[queueMetadataKey]
		}
	}

	ctx = defaultContext(ctx)
	where, args := buildWhereClauses(query, true)

	if queue != "" && s.queueClaimMode == QueueClaimModeStrict {
		return s.takeStrictQueueHead(ctx, query, queue, agentID)
	}

	// Try prefetch path: get a pre-selected candidate ID and claim it directly.
	if queue != "" && s.prefetch != nil {
		namespaceID := ""
		if query != nil {
			namespaceID = query.NamespaceID
		}
		if t, err := s.takePrefetched(ctx, namespaceID, queue, agentID); err == nil && t != nil {
			return t, nil
		}
		// Fall through to normal CTE path on miss.
	}

	atomicAgent, err := s.takeWithStrategy(ctx, queue, agentID, func(ctx context.Context, tx pgx.Tx, lockClause string) (*takeClaimResult, error) {
		return s.takeAtomicClaim(ctx, tx, where, args, lockClause, agentID)
	})
	if err == nil || !shouldFallbackToLegacyTake(err) {
		return atomicAgent, err
	}

	sqlState := takeClaimSQLState(err)
	s.logger.Warn("falling back to legacy postgres take claim",
		zap.String("sqlstate", sqlState),
		zap.String("queue_claim_mode", string(s.queueClaimMode)),
		zap.String("queue", queue),
		zap.Error(err),
	)

	legacyAgent, legacyErr := s.takeWithStrategy(ctx, queue, agentID, func(ctx context.Context, tx pgx.Tx, lockClause string) (*takeClaimResult, error) {
		return s.takeLegacyClaim(ctx, tx, where, args, lockClause, agentID)
	})
	if legacyErr != nil {
		return nil, fmt.Errorf("postgres take fallback failed after atomic error: atomic=%v fallback=%w", err, legacyErr)
	}
	return legacyAgent, nil
}

// takePrefetched tries to claim an agent using a pre-fetched candidate ID.
// Returns (nil, nil) if no candidate is available or all candidates are stale.
func (s *PostgresStore) takePrefetched(ctx context.Context, namespaceID, queue, agentID string) (*agent.Agent, error) {
	const maxAttempts = 3
	for i := 0; i < maxAttempts; i++ {
		candidateID := s.prefetch.tryGet(namespaceID, queue)
		if candidateID == "" {
			return nil, nil // buffer empty
		}
		t, err := s.claimByID(ctx, candidateID, agentID)
		if err != nil {
			return nil, err
		}
		if t != nil {
			return t, nil
		}
		// Stale candidate (already claimed), try next.
	}
	return nil, nil
}

// claimByID attempts to claim a specific agent by ID with a targeted UPDATE.
// Returns (nil, nil) if the agent is no longer available (already claimed or expired).
func (s *PostgresStore) claimByID(ctx context.Context, candidateID, agentID string) (*agent.Agent, error) {
	now := time.Now().UTC()
	leaseToken := uuid.New().String()
	defaultLeaseMs := int64(s.leaseDuration / time.Millisecond)
	if defaultLeaseMs <= 0 {
		defaultLeaseMs = int64(agent.DefaultLeaseOptions().LeaseDuration / time.Millisecond)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		UPDATE agents
		SET status = $1,
			owner = $2,
			owner_time = $3::timestamptz,
			lease_token = $4,
			lease_until = CASE
				WHEN (metadata->>'lease_sec') ~ '^[0-9]+$'
					AND (metadata->>'lease_sec')::bigint > 0
					THEN ($3::timestamptz) + ((metadata->>'lease_sec')::bigint * interval '1 second')
				WHEN (metadata->>'lease_ms') ~ '^[0-9]+$'
					AND (metadata->>'lease_ms')::bigint > 0
					THEN ($3::timestamptz) + ((metadata->>'lease_ms')::bigint * interval '1 millisecond')
				ELSE ($3::timestamptz) + ($5::bigint * interval '1 millisecond')
			END,
			updated_at = $3::timestamptz,
			version = version + 1
		WHERE id = $6
		  AND status = $7
		  AND `+ttlClause()+`
		RETURNING `+agentSelectColumns,
		string(agent.StatusInProgress),
		agentID,
		now,
		leaseToken,
		defaultLeaseMs,
		candidateID,
		string(agent.StatusNew),
	)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, nil // already claimed or expired
		}
		return nil, err
	}

	// Event handling — identical to takeWithStrategy.
	eventData := map[string]interface{}{"agent_id": agentID}
	if s.slimEvents {
		eventData = map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   string(agent.StatusInProgress),
		}
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
		Data:      eventData,
	}

	if s.deferEvents {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		s.enqueueEvent(event, t.NamespaceID)
	} else {
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	s.notifySubscribers(event)
	return t, nil
}

type takeClaimResult struct {
	agent              *agent.Agent
	candidateSelectSec float64
	claimTransitionSec float64
}

func (s *PostgresStore) takeWithStrategy(
	ctx context.Context,
	queue string,
	agentID string,
	claimFn func(context.Context, pgx.Tx, string) (*takeClaimResult, error),
) (*agent.Agent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	lockClause, proceed, err := s.resolveTakeLockClause(ctx, tx, queue)
	if err != nil {
		return nil, err
	}
	if !proceed {
		return nil, nil
	}

	claim, err := claimFn(ctx, tx, lockClause)
	if err != nil {
		return nil, err
	}
	if claim == nil || claim.agent == nil {
		return nil, nil
	}
	metrics.RecordOperationStage("take", "candidate_select", "postgres", claim.candidateSelectSec)
	metrics.RecordOperationStage("take", "claim_transition", "postgres", claim.claimTransitionSec)

	t := claim.agent
	eventData := map[string]interface{}{"agent_id": agentID}
	if s.slimEvents {
		eventData = map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   string(agent.StatusInProgress),
		}
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
		Data:      eventData,
	}

	if s.deferEvents {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		s.enqueueEvent(event, t.NamespaceID)
	} else {
		eventStart := time.Now()
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return nil, err
		}
		metrics.RecordOperationStage("take", "event_append", "postgres", time.Since(eventStart).Seconds())
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	s.notifySubscribers(event)
	return t, nil
}

func (s *PostgresStore) resolveTakeLockClause(ctx context.Context, tx pgx.Tx, queue string) (string, bool, error) {
	lockClause := "FOR UPDATE SKIP LOCKED"
	if queue != "" && s.queueClaimMode == QueueClaimModeStrict {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtext($1))", queue).Scan(&locked); err != nil {
			return "", false, err
		}
		if !locked {
			return "", false, nil
		}
		lockClause = "FOR UPDATE"
	}
	return lockClause, true, nil
}

func (s *PostgresStore) takeStrictQueueHead(ctx context.Context, query *agent.Query, queue string, agentID string) (*agent.Agent, error) {
	result, err := s.takeWithStrategy(ctx, queue, agentID, func(ctx context.Context, tx pgx.Tx, lockClause string) (*takeClaimResult, error) {
		return s.takeStrictHeadClaim(ctx, tx, query, queue, agentID, lockClause)
	})
	return result, err
}

func (s *PostgresStore) takeStrictHeadClaim(
	ctx context.Context,
	tx pgx.Tx,
	query *agent.Query,
	queue string,
	agentID string,
	lockClause string,
) (*takeClaimResult, error) {
	where, args := buildWhereClauses(query, true)

	now := time.Now().UTC()
	leaseToken := uuid.New().String()
	defaultLeaseMs := int64(s.leaseDuration / time.Millisecond)
	if defaultLeaseMs <= 0 {
		defaultLeaseMs = int64(agent.DefaultLeaseOptions().LeaseDuration / time.Millisecond)
	}

	statusArg := len(args) + 1
	ownerArg := statusArg + 1
	nowArg := ownerArg + 1
	leaseTokenArg := nowArg + 1
	defaultLeaseMsArg := leaseTokenArg + 1

	claimSQL := fmt.Sprintf(`
		WITH candidate AS (
			SELECT id AS candidate_id, metadata AS candidate_metadata
			FROM agents
			WHERE %s
			ORDER BY created_at ASC, id ASC
			LIMIT 1
			%s
		),
		claimed AS (
			UPDATE agents AS t
			SET status = $%d,
				owner = $%d,
				owner_time = $%d::timestamptz,
				lease_token = $%d,
				lease_until = CASE
					WHEN (candidate.candidate_metadata->>'lease_sec') ~ '^[0-9]+$'
						AND (candidate.candidate_metadata->>'lease_sec')::bigint > 0
						THEN ($%d::timestamptz) + ((candidate.candidate_metadata->>'lease_sec')::bigint * interval '1 second')
					WHEN (candidate.candidate_metadata->>'lease_ms') ~ '^[0-9]+$'
						AND (candidate.candidate_metadata->>'lease_ms')::bigint > 0
						THEN ($%d::timestamptz) + ((candidate.candidate_metadata->>'lease_ms')::bigint * interval '1 millisecond')
					ELSE ($%d::timestamptz) + ($%d::bigint * interval '1 millisecond')
				END,
				updated_at = $%d::timestamptz,
				version = t.version + 1
			FROM candidate
			WHERE t.id = candidate.candidate_id
			RETURNING %s
		)
		SELECT %s FROM claimed
	`,
		where,
		lockClause,
		statusArg,
		ownerArg,
		nowArg,
		leaseTokenArg,
		nowArg,
		nowArg,
		nowArg,
		defaultLeaseMsArg,
		nowArg,
		agentSelectColumns,
		agentSelectColumns,
	)

	claimArgs := append(args,
		string(agent.StatusInProgress),
		agentID,
		now,
		leaseToken,
		defaultLeaseMs,
	)

	selectStart := time.Now()
	row := tx.QueryRow(ctx, claimSQL, claimArgs...)
	claimed, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres take strict queue head claim failed: %w", err)
	}

	return &takeClaimResult{
		agent:              claimed,
		candidateSelectSec: time.Since(selectStart).Seconds(),
		claimTransitionSec: 0,
	}, nil
}

func (s *PostgresStore) takeAtomicClaim(
	ctx context.Context,
	tx pgx.Tx,
	where string,
	args []interface{},
	lockClause string,
	agentID string,
) (*takeClaimResult, error) {
	now := time.Now().UTC()
	leaseToken := uuid.New().String()
	defaultLeaseMs := int64(s.leaseDuration / time.Millisecond)
	if defaultLeaseMs <= 0 {
		defaultLeaseMs = int64(agent.DefaultLeaseOptions().LeaseDuration / time.Millisecond)
	}

	statusArg := len(args) + 1
	ownerArg := statusArg + 1
	nowArg := ownerArg + 1
	leaseTokenArg := nowArg + 1
	defaultLeaseMsArg := leaseTokenArg + 1

	claimSQL := fmt.Sprintf(`
		WITH candidate AS (
			SELECT id AS candidate_id, metadata AS candidate_metadata
			FROM agents
			WHERE %s
			ORDER BY created_at ASC, id ASC
			LIMIT 1
			%s
		),
		claimed AS (
			UPDATE agents AS t
			SET status = $%d,
				owner = $%d,
				owner_time = $%d::timestamptz,
				lease_token = $%d,
				lease_until = CASE
					WHEN (candidate.candidate_metadata->>'lease_sec') ~ '^[0-9]+$'
						AND (candidate.candidate_metadata->>'lease_sec')::bigint > 0
						THEN ($%d::timestamptz) + ((candidate.candidate_metadata->>'lease_sec')::bigint * interval '1 second')
					WHEN (candidate.candidate_metadata->>'lease_ms') ~ '^[0-9]+$'
						AND (candidate.candidate_metadata->>'lease_ms')::bigint > 0
						THEN ($%d::timestamptz) + ((candidate.candidate_metadata->>'lease_ms')::bigint * interval '1 millisecond')
					ELSE ($%d::timestamptz) + ($%d::bigint * interval '1 millisecond')
				END,
				updated_at = $%d::timestamptz,
				version = t.version + 1
			FROM candidate
			WHERE t.id = candidate.candidate_id
			RETURNING %s
		)
		SELECT %s FROM claimed
	`,
		where,
		lockClause,
		statusArg,
		ownerArg,
		nowArg,
		leaseTokenArg,
		nowArg,
		nowArg,
		nowArg,
		defaultLeaseMsArg,
		nowArg,
		agentSelectColumns,
		agentSelectColumns,
	)

	claimArgs := append(args,
		string(agent.StatusInProgress),
		agentID,
		now,
		leaseToken,
		defaultLeaseMs,
	)

	selectStart := time.Now()
	row := tx.QueryRow(ctx, claimSQL, claimArgs...)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres take atomic claim failed: %w", err)
	}

	return &takeClaimResult{
		agent:              t,
		candidateSelectSec: time.Since(selectStart).Seconds(),
		claimTransitionSec: 0,
	}, nil
}

func (s *PostgresStore) takeLegacyClaim(
	ctx context.Context,
	tx pgx.Tx,
	where string,
	args []interface{},
	lockClause string,
	agentID string,
) (*takeClaimResult, error) {
	selectSQL := fmt.Sprintf(
		"SELECT %s FROM agents WHERE %s ORDER BY created_at ASC, id ASC LIMIT 1 %s",
		agentSelectColumns,
		where,
		lockClause,
	)
	selectStart := time.Now()
	row := tx.QueryRow(ctx, selectSQL, args...)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres take legacy claim failed: %w", err)
	}
	candidateSec := time.Since(selectStart).Seconds()

	now := time.Now().UTC()
	leaseToken := uuid.New().String()
	leaseUntil := now.Add(s.leaseDurationForAgent(t))

	claimStart := time.Now()
	cmdTag, err := tx.Exec(ctx, `
		UPDATE agents SET
			status = $1,
			owner = $2,
			owner_time = $3,
			lease_token = $4,
			lease_until = $5,
			updated_at = $6,
			version = version + 1
		WHERE id = $7
	`,
		string(agent.StatusInProgress),
		agentID,
		now,
		leaseToken,
		leaseUntil,
		now,
		t.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres take legacy claim failed: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return nil, nil
	}
	claimSec := time.Since(claimStart).Seconds()

	t.Status = agent.StatusInProgress
	t.Owner = agentID
	t.OwnerTime = now
	t.LeaseToken = leaseToken
	t.LeaseUntil = leaseUntil
	t.UpdatedAt = now
	t.Version++

	return &takeClaimResult{
		agent:              t,
		candidateSelectSec: candidateSec,
		claimTransitionSec: claimSec,
	}, nil
}

// Update updates a agent in place.
func (s *PostgresStore) Update(id string, updates map[string]interface{}) error {
	if updates == nil {
		return nil
	}
	_, err := s.UpdateAndGet(id, updates)
	return err
}

// UpdateAndGet updates a agent and returns the updated agent.
func (s *PostgresStore) UpdateAndGet(id string, updates map[string]interface{}) (*agent.Agent, error) {
	if updates == nil {
		return s.Get(id)
	}

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=$1 AND %s FOR UPDATE", agentSelectColumns, ttlClause()),
		id,
	)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}

	applyUpdates(t, updates)
	t.UpdatedAt = time.Now().UTC()
	t.Version++

	if err := updateAgentRow(ctx, tx, t); err != nil {
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

	if s.deferEvents {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		s.enqueueEvent(event, t.NamespaceID)
	} else {
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	s.notifySubscribers(event)
	return t, nil
}

// Complete marks a agent as completed.
func (s *PostgresStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	_, err := s.completeAndGet(context.Background(), id, agentID, leaseToken, result)
	return err
}

// CompleteAndGet marks a agent as completed and returns the updated agent.
func (s *PostgresStore) CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	return s.completeAndGet(context.Background(), id, agentID, leaseToken, result)
}

func (s *PostgresStore) completeAndGet(ctx context.Context, id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	ctx = defaultContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	if result == nil {
		result = map[string]interface{}{}
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	completionMeta := &agent.Agent{Metadata: map[string]string{}}
	setCompletionMetadata(completionMeta, agentID, leaseToken, now)
	completionMetaJSON, err := json.Marshal(completionMeta.Metadata)
	if err != nil {
		return nil, err
	}
	claimStart := time.Now()
	row := tx.QueryRow(ctx, `
		UPDATE agents
		SET status = $1,
			payload = jsonb_set(COALESCE(payload, '{}'::jsonb), '{result}', $2::jsonb, true),
			owner = '',
			owner_time = NULL,
			lease_token = '',
			lease_until = NULL,
			updated_at = $3,
			version = version + 1,
			metadata = COALESCE(metadata, '{}'::jsonb) || $4::jsonb
		WHERE id = $5
		  AND status = $6
		  AND owner = $7
		  AND lease_token = $8
		  AND `+ttlClause()+`
		RETURNING `+agentSelectColumns,
		string(agent.StatusCompleted),
		resultJSON,
		now,
		completionMetaJSON,
		id,
		string(agent.StatusInProgress),
		agentID,
		leaseToken,
	)
	t, err := scanAgent(row)
	if err != nil {
		if !errorsIsNoRows(err) {
			return nil, err
		}
		loadStart := time.Now()
		current, loadErr := s.get(ctx, id)
		metrics.RecordOperationStage("complete", "load_current", "postgres", time.Since(loadStart).Seconds())
		if loadErr != nil {
			return nil, loadErr
		}
		if classifyErr := classifyCompletionConflict(current, agentID, leaseToken); classifyErr != nil {
			return nil, classifyErr
		}
		return current, nil
	}
	metrics.RecordOperationStage("complete", "claim_transition", "postgres", time.Since(claimStart).Seconds())

	completeEventData := map[string]interface{}(result)
	if s.slimEvents {
		completeEventData = map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   string(agent.StatusCompleted),
		}
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
		Data:      completeEventData,
	}

	if s.deferEvents {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		s.enqueueEvent(event, t.NamespaceID)
	} else {
		eventStart := time.Now()
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return nil, err
		}
		metrics.RecordOperationStage("complete", "event_append", "postgres", time.Since(eventStart).Seconds())
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	s.notifySubscribers(event)
	return t, nil
}

// CompleteAndOut atomically completes a agent and creates new output agents.
// This prevents partial workflow state where a agent is completed but
// downstream agents aren't created.
func (s *PostgresStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	if agentID == "" {
		return nil, nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, nil, fmt.Errorf("lease token is required")
	}

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	// Get and lock the agent
	row := tx.QueryRow(ctx,
		fmt.Sprintf("SELECT %s FROM agents WHERE id=$1 AND %s FOR UPDATE", agentSelectColumns, ttlClause()),
		id,
	)
	t, err := scanAgent(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, nil, err
	}

	// Validate ownership and status
	if t.Status == agent.StatusCompleted {
		if !completionTokenMatches(t, leaseToken) {
			return nil, nil, fmt.Errorf(tokenMismatchStaleErr)
		}
		// Already completed - commit and return the existing agent
		if err := tx.Commit(ctx); err != nil {
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

	// Update the agent to COMPLETED
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

	if err := updateAgentCompletionRow(ctx, tx, t); err != nil {
		return nil, nil, err
	}

	// Create COMPLETED event
	completedEventData := map[string]interface{}(result)
	if s.slimEvents {
		completedEventData = map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   string(agent.StatusCompleted),
		}
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
		Data:      completedEventData,
	}
	if !s.deferEvents {
		if err := insertEvent(ctx, tx, completedEvent, t.NamespaceID, s.shardID); err != nil {
			return nil, nil, err
		}
	}

	// Create output agents
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
		if out.ShardID == "" && s.shardID != "" {
			out.ShardID = s.shardID
		}
		// Inherit trace ID from parent if not set
		if out.TraceID == "" && t.TraceID != "" {
			out.TraceID = t.TraceID
		}
		if out.NamespaceID == "" && t.NamespaceID != "" {
			out.NamespaceID = t.NamespaceID
		}
		// Set parent ID to the completed agent if not set
		if out.ParentID == "" {
			out.ParentID = t.ID
		}

		if err := upsertAgent(ctx, tx, out); err != nil {
			return nil, nil, fmt.Errorf("failed to create output agent: %w", err)
		}
		if err := queueWakeupNotify(ctx, tx, out.NamespaceID, out.Metadata[queueMetadataKey]); err != nil {
			return nil, nil, fmt.Errorf("failed to queue output wakeup: %w", err)
		}

		outEventData := map[string]interface{}{
			"agent":     out,
			"parent_id": t.ID,
		}
		if s.slimEvents {
			outEventData = map[string]interface{}{
				"tuple_id": out.ID,
				"kind":     out.Kind,
				"status":   string(out.Status),
			}
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
			Data:      outEventData,
		}
		if !s.deferEvents {
			if err := insertEvent(ctx, tx, outEvent, out.NamespaceID, s.shardID); err != nil {
				return nil, nil, fmt.Errorf("failed to create output event: %w", err)
			}
		}

		createdOutputs = append(createdOutputs, out)
		createdEvents = append(createdEvents, outEvent)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}

	// Enqueue deferred events after commit
	if s.deferEvents {
		s.enqueueEvent(completedEvent, t.NamespaceID)
		for _, e := range createdEvents {
			// Use the output agent's namespace for its event
			ns := t.NamespaceID
			for _, out := range createdOutputs {
				if out.ID == e.TupleID {
					ns = out.NamespaceID
					break
				}
			}
			s.enqueueEvent(e, ns)
		}
	}

	// Notify subscribers
	s.notifySubscribers(completedEvent)
	for _, e := range createdEvents {
		s.notifySubscribers(e)
	}
	s.signalWakeups()

	return t, createdOutputs, nil
}

// Release releases ownership of a agent.
func (s *PostgresStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := s.releaseAndGet(context.Background(), id, agentID, leaseToken, reason)
	return err
}

// ReleaseAndGet releases ownership of a agent and returns the updated agent.
func (s *PostgresStore) ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	return s.releaseAndGet(context.Background(), id, agentID, leaseToken, reason)
}

func (s *PostgresStore) releaseAndGet(ctx context.Context, id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	ctx = defaultContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	row := tx.QueryRow(ctx, `
		UPDATE agents
		SET status = $1,
			owner = '',
			owner_time = NULL,
			lease_token = '',
			lease_until = NULL,
			updated_at = $2,
			version = version + 1
		WHERE id = $3
		  AND status = $4
		  AND owner = $5
		  AND lease_token = $6
		  AND `+ttlClause()+`
		RETURNING `+agentSelectColumns,
		string(agent.StatusNew),
		now,
		id,
		string(agent.StatusInProgress),
		agentID,
		leaseToken,
	)
	t, err := scanAgent(row)
	if err != nil {
		if !errorsIsNoRows(err) {
			return nil, err
		}
		current, loadErr := s.get(ctx, id)
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, classifyReleaseConflict(current, agentID, leaseToken)
	}

	// Build event data with reason if provided
	eventData := map[string]interface{}{}
	if reason != "" {
		eventData["reap_reason"] = reason
	}
	eventData["previous_owner"] = agentID

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
	if !s.deferEvents {
		if err := insertEvent(ctx, tx, event, t.NamespaceID, s.shardID); err != nil {
			return nil, err
		}
	}
	if err := queueWakeupNotify(ctx, tx, t.NamespaceID, t.Metadata[queueMetadataKey]); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	if s.deferEvents {
		s.enqueueEvent(event, t.NamespaceID)
	}
	s.notifySubscribers(event)
	s.signalWakeups()
	return t, nil
}

// RenewLease extends the lease for an in-progress agent.
func (s *PostgresStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return s.renewLease(context.Background(), id, agentID, leaseToken, leaseDuration)
}

func (s *PostgresStore) renewLease(ctx context.Context, id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	ctx = defaultContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now := time.Now().UTC()
	defaultLeaseMs := int64(leaseDuration / time.Millisecond)
	if defaultLeaseMs <= 0 {
		defaultLeaseMs = int64(s.leaseDuration / time.Millisecond)
	}
	row := tx.QueryRow(ctx, `
		UPDATE agents
		SET owner_time = $1::timestamptz,
			lease_until = CASE
				WHEN (metadata->>'lease_sec') ~ '^[0-9]+$'
					AND (metadata->>'lease_sec')::bigint > 0
					THEN ($1::timestamptz) + ((metadata->>'lease_sec')::bigint * interval '1 second')
				WHEN (metadata->>'lease_ms') ~ '^[0-9]+$'
					AND (metadata->>'lease_ms')::bigint > 0
					THEN ($1::timestamptz) + ((metadata->>'lease_ms')::bigint * interval '1 millisecond')
				ELSE ($1::timestamptz) + ($2::bigint * interval '1 millisecond')
			END,
			updated_at = $1::timestamptz
		WHERE id = $3
		  AND status = $4
		  AND owner = $5
		  AND lease_token = $6
		  AND `+ttlClause()+`
		RETURNING `+agentSelectColumns,
		now,
		defaultLeaseMs,
		id,
		string(agent.StatusInProgress),
		agentID,
		leaseToken,
	)
	t, err := scanAgent(row)
	if err != nil {
		if !errorsIsNoRows(err) {
			return nil, err
		}
		current, loadErr := s.get(ctx, id)
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, classifyRenewConflict(current, agentID, leaseToken)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return t, nil
}

func classifyCompletionConflict(current *agent.Agent, agentID string, leaseToken string) error {
	if current == nil {
		return fmt.Errorf("agent not found")
	}
	if current.Status == agent.StatusCompleted {
		if completionTokenMatches(current, leaseToken) {
			return nil
		}
		return fmt.Errorf(tokenMismatchStaleErr)
	}
	if current.Status != agent.StatusInProgress {
		return fmt.Errorf("invalid status: %s", current.Status)
	}
	if current.Owner != agentID {
		return fmt.Errorf("cannot complete: agent owned by %q, not %q", current.Owner, agentID)
	}
	if current.LeaseToken != leaseToken {
		return fmt.Errorf("lease token mismatch")
	}
	return fmt.Errorf("complete rejected")
}

func classifyReleaseConflict(current *agent.Agent, agentID string, leaseToken string) error {
	if current == nil {
		return fmt.Errorf("agent not found")
	}
	if current.Status != agent.StatusInProgress {
		return fmt.Errorf("invalid status: %s", current.Status)
	}
	if current.Owner != agentID {
		return fmt.Errorf("cannot release: agent owned by %q, not %q", current.Owner, agentID)
	}
	if current.LeaseToken != leaseToken {
		return fmt.Errorf("lease token mismatch")
	}
	return fmt.Errorf("release rejected")
}

func classifyRenewConflict(current *agent.Agent, agentID string, leaseToken string) error {
	if current == nil {
		return fmt.Errorf("agent not found")
	}
	if current.Status != agent.StatusInProgress {
		return fmt.Errorf("invalid status: %s", current.Status)
	}
	if current.Owner != agentID {
		return fmt.Errorf("cannot renew: agent owned by %q, not %q", current.Owner, agentID)
	}
	if current.LeaseToken != leaseToken {
		return fmt.Errorf("lease token mismatch")
	}
	return fmt.Errorf("renew rejected")
}

// Subscribe subscribes to events.

func (s *PostgresStore) GetDAG(rootID string) (*agent.DAG, error) {
	return s.getDAG(context.Background(), rootID)
}

func (s *PostgresStore) getDAG(ctx context.Context, rootID string) (*agent.DAG, error) {
	ctx = defaultContext(ctx)
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

		t, err := s.get(ctx, id)
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

		children, err := s.query(ctx, &agent.Query{ParentID: t.ID})
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

// BatchTake claims up to limit agents matching the query in a single transaction.
// Each claimed agent receives its own unique lease token.
// Only parallel mode (SKIP LOCKED) is used for batch claims.
func (s *PostgresStore) BatchTake(query *agent.Query, limit int) ([]*agent.Agent, error) {
	return s.batchTake(context.Background(), query, limit)
}

func (s *PostgresStore) batchTake(ctx context.Context, query *agent.Query, limit int) ([]*agent.Agent, error) {
	if limit <= 0 {
		return nil, nil
	}
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

	ctx = defaultContext(ctx)
	where, args := buildWhereClauses(query, true)

	now := time.Now().UTC()
	defaultLeaseMs := int64(s.leaseDuration / time.Millisecond)
	if defaultLeaseMs <= 0 {
		defaultLeaseMs = int64(agent.DefaultLeaseOptions().LeaseDuration / time.Millisecond)
	}

	ownerArg := len(args) + 1
	nowArg := ownerArg + 1
	defaultLeaseMsArg := nowArg + 1
	limitArg := defaultLeaseMsArg + 1

	claimSQL := fmt.Sprintf(`
		WITH candidates AS (
			SELECT id AS candidate_id, metadata AS candidate_metadata
			FROM agents
			WHERE %s
			ORDER BY created_at ASC, id ASC
			LIMIT $%d
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE agents AS t
			SET status = '%s',
				owner = $%d,
				owner_time = $%d::timestamptz,
				lease_token = gen_random_uuid()::text,
				lease_until = CASE
					WHEN (candidates.candidate_metadata->>'lease_sec') ~ '^[0-9]+$'
						AND (candidates.candidate_metadata->>'lease_sec')::bigint > 0
						THEN ($%d::timestamptz) + ((candidates.candidate_metadata->>'lease_sec')::bigint * interval '1 second')
					WHEN (candidates.candidate_metadata->>'lease_ms') ~ '^[0-9]+$'
						AND (candidates.candidate_metadata->>'lease_ms')::bigint > 0
						THEN ($%d::timestamptz) + ((candidates.candidate_metadata->>'lease_ms')::bigint * interval '1 millisecond')
					ELSE ($%d::timestamptz) + ($%d::bigint * interval '1 millisecond')
				END,
				updated_at = $%d::timestamptz,
				version = t.version + 1
			FROM candidates
			WHERE t.id = candidates.candidate_id
			RETURNING %s
		)
		SELECT %s FROM claimed
	`,
		where,
		limitArg,
		string(agent.StatusInProgress),
		ownerArg,
		nowArg,
		nowArg,
		nowArg,
		nowArg,
		defaultLeaseMsArg,
		nowArg,
		agentSelectColumns,
		agentSelectColumns,
	)

	claimArgs := append(args,
		agentID,
		now,
		defaultLeaseMs,
		limit,
	)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, claimSQL, claimArgs...)
	if err != nil {
		return nil, fmt.Errorf("postgres batch take failed: %w", err)
	}
	defer rows.Close()

	var claimed []*agent.Agent
	for rows.Next() {
		t, err := scanAgent(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres batch take scan failed: %w", err)
		}
		claimed = append(claimed, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(claimed) == 0 {
		return nil, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// Enqueue events after commit
	for _, t := range claimed {
		eventData := map[string]interface{}{"agent_id": agentID}
		if s.slimEvents {
			eventData = map[string]interface{}{
				"tuple_id": t.ID,
				"kind":     t.Kind,
				"status":   string(agent.StatusInProgress),
			}
		}
		event := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   t.ID,
			Type:      agent.EventClaimed,
			Kind:      t.Kind,
			Timestamp: now,
			AgentID:   agentID,
			TraceID:   t.TraceID,
			Version:   t.Version,
			Data:      eventData,
		}
		if s.deferEvents {
			s.enqueueEvent(event, t.NamespaceID)
		} else {
			batchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			batch := &pgx.Batch{}
			_ = queueInsertEvent(batch, event, t.NamespaceID, s.shardID)
			results := s.pool.SendBatch(batchCtx, batch)
			_, _ = results.Exec()
			results.Close()
			cancel()
		}
		s.notifySubscribers(event)
	}

	return claimed, nil
}

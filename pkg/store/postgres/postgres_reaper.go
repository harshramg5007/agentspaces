package postgres

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *PostgresStore) leaseDurationForAgent(t *agent.Agent) time.Duration {
	if t == nil || t.Metadata == nil {
		return agent.DefaultLeaseOptions().LeaseDuration
	}
	if leaseSec, ok := t.Metadata["lease_sec"]; ok {
		if sec, err := strconv.Atoi(leaseSec); err == nil && sec > 0 {
			return time.Duration(sec) * time.Second
		}
	}
	if leaseMs, ok := t.Metadata["lease_ms"]; ok {
		if ms, err := strconv.Atoi(leaseMs); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return agent.DefaultLeaseOptions().LeaseDuration
}

func (s *PostgresStore) leaseReaperLoop() {
	ticker := time.NewTicker(s.leaseReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.reapExpiredLeases()
		}
	}
}

// reapExpiredLeases finds agents with expired leases and releases them.
func (s *PostgresStore) reapExpiredLeases() {
	ctx := s.ctx
	now := time.Now().UTC()

	// Find and release expired leases in a single transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		s.logger.Debug("failed to begin reap transaction", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	// Find expired agents and update them atomically
	// This uses FOR UPDATE SKIP LOCKED to avoid conflicts with active workers
	rows, err := tx.Query(ctx, `
		WITH expired AS (
			SELECT id, trace_id, owner, namespace_id, queue
			FROM agents
			WHERE status = $3
			  AND lease_until IS NOT NULL
			  AND lease_until < $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE agents AS t
		SET status = $1,
			owner = '',
			owner_time = NULL,
			lease_token = '',
			lease_until = NULL,
			version = version + 1,
			updated_at = $2
		FROM expired
		WHERE t.id = expired.id
		RETURNING t.id, t.kind, expired.trace_id, expired.owner, t.version, expired.namespace_id, expired.queue
	`, string(agent.StatusNew), now, string(agent.StatusInProgress))
	if err != nil {
		s.logger.Debug("failed to reap expired leases", zap.Error(err))
		return
	}
	defer rows.Close()

	// Collect reaped agents and emit events
	type reapedAgent struct {
		id       string
		kind     string
		traceID  string
		oldOwner string
		version  int64
		namespaceID string
		queue    string
	}
	var reaped []reapedAgent

	for rows.Next() {
		var r reapedAgent
		if err := rows.Scan(&r.id, &r.kind, &r.traceID, &r.oldOwner, &r.version, &r.namespaceID, &r.queue); err != nil {
			s.logger.Debug("failed to scan reaped agent", zap.Error(err))
			continue
		}
		reaped = append(reaped, r)
	}

	if len(reaped) == 0 {
		return
	}

	events := make([]*agent.Event, 0, len(reaped))
	batch := &pgx.Batch{}

	// Queue RELEASED events for each reaped agent.
	for _, r := range reaped {
		event := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   r.id,
			Type:      agent.EventReleased,
			Kind:      r.kind,
			Timestamp: now,
			AgentID:   "reaper",
			TraceID:   r.traceID,
			Version:   r.version,
			Data: map[string]interface{}{
				"reap_reason":    "lease_expired",
				"previous_owner": r.oldOwner,
			},
		}
		if err := queueInsertEvent(batch, event, r.namespaceID, s.shardID); err != nil {
			s.logger.Debug("failed to queue reap event", zap.Error(err), zap.String("tuple_id", r.id))
			return
		}
		batch.Queue(`SELECT pg_notify($1, $2)`, postgresWakeupChannel, buildWakeupPayload(r.namespaceID, r.queue))
		events = append(events, event)
	}

	results := tx.SendBatch(ctx, batch)
	for i, event := range events {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			s.logger.Debug("failed to insert reap event", zap.Error(err), zap.String("tuple_id", event.TupleID), zap.Int("batch_index", i))
			return
		}
	}
	if err := results.Close(); err != nil {
		s.logger.Debug("failed to finalize reap event batch", zap.Error(err))
		return
	}

	for _, r := range reaped {
		s.logger.Debug("reaped expired lease",
			zap.String("tuple_id", r.id),
			zap.String("previous_owner", r.oldOwner),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		s.logger.Debug("failed to commit reap transaction", zap.Error(err))
		return
	}

	for _, event := range events {
		s.notifySubscribers(event)
	}
	s.signalWakeups()

	s.logger.Info("reaped expired leases", zap.Int("count", len(reaped)))
}

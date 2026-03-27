package sqlite

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *SQLiteStore) leaseDurationForAgent(t *agent.Agent) time.Duration {
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

func (s *SQLiteStore) leaseReaperLoop() {
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
func (s *SQLiteStore) reapExpiredLeases() {
	ctx := s.ctx
	now := time.Now().UTC()
	nowMs := now.UnixMilli()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Debug("failed to begin reap transaction", zap.Error(err))
		return
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, kind, trace_id, owner, version
		FROM agents
		WHERE status = ?
		  AND lease_until_ms IS NOT NULL
		  AND lease_until_ms < ?
	`, string(agent.StatusInProgress), nowMs)
	if err != nil {
		s.logger.Debug("failed to find expired leases", zap.Error(err))
		return
	}
	defer rows.Close()

	type reapedAgent struct {
		id       string
		kind     string
		traceID  string
		oldOwner string
		version  int64
	}
	var reaped []reapedAgent

	for rows.Next() {
		var r reapedAgent
		if err := rows.Scan(&r.id, &r.kind, &r.traceID, &r.oldOwner, &r.version); err != nil {
			s.logger.Debug("failed to scan reaped agent", zap.Error(err))
			continue
		}
		reaped = append(reaped, r)
	}
	if err := rows.Err(); err != nil {
		s.logger.Debug("failed to iterate reaped agents", zap.Error(err))
		return
	}

	if len(reaped) == 0 {
		return
	}

	for _, r := range reaped {
		_, err := tx.ExecContext(ctx, `
			UPDATE agents
			SET status = ?,
				owner = '',
				owner_time_ms = NULL,
				lease_token = '',
				lease_until_ms = NULL,
				version = version + 1,
				updated_at_ms = ?
			WHERE id = ?
		`, string(agent.StatusNew), nowMs, r.id)
		if err != nil {
			s.logger.Debug("failed to reap agent", zap.Error(err), zap.String("tuple_id", r.id))
			continue
		}

		event := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   r.id,
			Type:      agent.EventReleased,
			Kind:      r.kind,
			Timestamp: now,
			AgentID:   "reaper",
			TraceID:   r.traceID,
			Version:   r.version + 1,
			Data: map[string]interface{}{
				"reap_reason":    "lease_expired",
				"previous_owner": r.oldOwner,
			},
		}
		if err := insertSQLiteEvent(ctx, tx, event); err != nil {
			s.logger.Debug("failed to insert reap event", zap.Error(err), zap.String("tuple_id", r.id))
			continue
		}

		s.notifySubscribers(event)
	}

	if err := tx.Commit(); err != nil {
		s.logger.Debug("failed to commit reap transaction", zap.Error(err))
		return
	}

	if len(reaped) > 0 {
		s.logger.Info("reaped expired leases", zap.Int("count", len(reaped)))
	}
}

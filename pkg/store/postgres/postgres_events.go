package postgres

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *PostgresStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	s.subscriptions.Subscribe(filter, handler)
	return nil
}

// GetEvents returns events for a agent.
func (s *PostgresStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	return s.getEvents(context.Background(), tupleID)
}

func (s *PostgresStore) getEvents(ctx context.Context, tupleID string) ([]*agent.Event, error) {
	ctx = defaultContext(ctx)
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf("SELECT %s FROM agent_events WHERE tuple_id=$1 ORDER BY timestamp ASC", eventSelectColumns),
		tupleID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			s.logger.Warn("failed to scan event", zap.Error(err))
			continue
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

// GetGlobalEvents returns recent events across all agents.
func (s *PostgresStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	return s.getGlobalEvents(context.Background(), limit, offset)
}

func (s *PostgresStore) getGlobalEvents(ctx context.Context, limit, offset int) ([]*agent.Event, error) {
	if limit <= 0 {
		limit = 100
	}

	ctx = defaultContext(ctx)
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf("SELECT %s FROM agent_events ORDER BY timestamp DESC LIMIT $1 OFFSET $2", eventSelectColumns),
		limit,
		offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			s.logger.Warn("failed to scan event", zap.Error(err))
			continue
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

// QueryEvents returns events filtered by tuple_id, agent_id, type, and/or trace_id.
func (s *PostgresStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	return s.queryEvents(context.Background(), query)
}

func (s *PostgresStore) queryEvents(ctx context.Context, query *agent.EventQuery) (*agent.EventPage, error) {
	if query == nil {
		query = &agent.EventQuery{}
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}

	clauses := []string{"1=1"}
	args := []interface{}{}
	idx := 1

	if query.TupleID != "" {
		clauses = append(clauses, fmt.Sprintf("tuple_id = $%d", idx))
		args = append(args, query.TupleID)
		idx++
	}
	if query.AgentID != "" {
		clauses = append(clauses, fmt.Sprintf("agent_id = $%d", idx))
		args = append(args, query.AgentID)
		idx++
	}
	if query.Type != "" {
		clauses = append(clauses, fmt.Sprintf("type = $%d", idx))
		args = append(args, string(query.Type))
		idx++
	}
	if query.TraceID != "" {
		clauses = append(clauses, fmt.Sprintf("trace_id = $%d", idx))
		args = append(args, query.TraceID)
		idx++
	}

	where := strings.Join(clauses, " AND ")
	order := "timestamp DESC"
	if query.TupleID != "" {
		order = "timestamp ASC"
	}

	ctx = defaultContext(ctx)
	sql := fmt.Sprintf("SELECT %s FROM agent_events WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d",
		eventSelectColumns, where, order, idx, idx+1)
	argsWithPaging := append(args, limit, offset)

	rows, err := s.pool.Query(ctx, sql, argsWithPaging...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			s.logger.Warn("failed to scan event", zap.Error(err))
			continue
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM agent_events WHERE %s", where)
	var total int
	if err := s.pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, err
	}

	return &agent.EventPage{
		Events: events,
		Total:  total,
	}, nil
}

// RecordSpans persists telemetry spans (best-effort ingestion).

func (s *PostgresStore) generateSubKey(filter *agent.EventFilter) string {
	return common.GenerateSubKey(filter)
}

func (s *PostgresStore) notifySubscribers(event *agent.Event) {
	s.subscriptions.Notify(event)
}

func (s *PostgresStore) eventMatchesFilter(event *agent.Event, subKey string) bool {
	return common.EventMatchesFilter(event, subKey)
}

package sqlite

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *SQLiteStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	s.subscriptions.Subscribe(filter, handler)
	return nil
}

// GetEvents returns events for a agent.
func (s *SQLiteStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	ctx := context.Background()

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM agent_events WHERE tuple_id=? ORDER BY timestamp_ms ASC", sqliteEventSelectColumns),
		tupleID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanSQLiteEvent(rows)
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
func (s *SQLiteStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	if limit <= 0 {
		limit = 100
	}

	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM agent_events ORDER BY timestamp_ms DESC LIMIT ? OFFSET ?", sqliteEventSelectColumns),
		limit,
		offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanSQLiteEvent(rows)
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
func (s *SQLiteStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
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

	if query.TupleID != "" {
		clauses = append(clauses, "tuple_id = ?")
		args = append(args, query.TupleID)
	}
	if query.AgentID != "" {
		clauses = append(clauses, "agent_id = ?")
		args = append(args, query.AgentID)
	}
	if query.Type != "" {
		clauses = append(clauses, "type = ?")
		args = append(args, string(query.Type))
	}
	if query.TraceID != "" {
		clauses = append(clauses, "trace_id = ?")
		args = append(args, query.TraceID)
	}

	where := strings.Join(clauses, " AND ")
	order := "timestamp_ms DESC"
	if query.TupleID != "" {
		order = "timestamp_ms ASC"
	}

	sqlStr := fmt.Sprintf("SELECT %s FROM agent_events WHERE %s ORDER BY %s LIMIT ? OFFSET ?", sqliteEventSelectColumns, where, order)
	argsWithPaging := append(args, limit, offset)

	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, sqlStr, argsWithPaging...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*agent.Event
	for rows.Next() {
		event, err := scanSQLiteEvent(rows)
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
	row := s.db.QueryRowContext(ctx, countSQL, args...)
	var total int
	if err := row.Scan(&total); err != nil {
		return nil, err
	}

	return &agent.EventPage{
		Events: events,
		Total:  total,
	}, nil
}

// GetDAG returns the DAG of related agents.

func (s *SQLiteStore) notifySubscribers(event *agent.Event) {
	s.subscriptions.Notify(event)
}

package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/metrics"
	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *SQLiteStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		t, err := s.Take(query)
		if err != nil {
			return nil, err
		}
		if t != nil {
			return t, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}

	return nil, fmt.Errorf("timeout waiting for agent")
}

// Read reads a agent from the space (blocking, non-destructive).
func (s *SQLiteStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		q := &agent.Query{}
		if query != nil {
			*q = *query
		}
		q.Limit = 1

		agents, err := s.Query(q)
		if err != nil {
			return nil, err
		}
		if len(agents) > 0 {
			return agents[0], nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}

	return nil, fmt.Errorf("timeout waiting for agent")
}

// Query returns matching agents (non-blocking, non-destructive).
func (s *SQLiteStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("query", s.storeLabel, success, time.Since(start).Seconds())
	}()

	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()

	where, args, needsFilter := buildSQLiteWhereClauses(query, false, nowMs)
	sqlStr := fmt.Sprintf("SELECT %s FROM agents WHERE %s ORDER BY created_at_ms ASC", sqliteAgentSelectColumns, where)

	if !needsFilter {
		if query != nil && query.Limit > 0 {
			sqlStr += " LIMIT ?"
			args = append(args, query.Limit)
		}
		if query != nil && query.Offset > 0 {
			sqlStr += " OFFSET ?"
			args = append(args, query.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var agents []*agent.Agent
	for rows.Next() {
		t, err := scanSQLiteAgent(rows)
		if err != nil {
			s.logger.Warn("failed to scan agent", zap.Error(err))
			continue
		}
		if needsFilter && !matchesQueryFilters(t, query) {
			continue
		}
		agents = append(agents, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if needsFilter && query != nil {
		if query.Offset > 0 {
			if query.Offset >= len(agents) {
				success = true
				return []*agent.Agent{}, nil
			}
			agents = agents[query.Offset:]
		}
		if query.Limit > 0 && query.Limit < len(agents) {
			agents = agents[:query.Limit]
		}
	}

	success = true
	return agents, nil
}

// Count returns the number of agents matching the query without pagination.
func (s *SQLiteStore) Count(query *agent.Query) (int, error) {
	ctx := context.Background()
	nowMs := time.Now().UTC().UnixMilli()

	where, args, needsFilter := buildSQLiteWhereClauses(query, false, nowMs)
	if !needsFilter {
		row := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM agents WHERE %s", where), args...)
		var count int
		if err := row.Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}

	sqlStr := fmt.Sprintf("SELECT %s FROM agents WHERE %s ORDER BY created_at_ms ASC", sqliteAgentSelectColumns, where)
	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		t, err := scanSQLiteAgent(rows)
		if err != nil {
			s.logger.Warn("failed to scan agent", zap.Error(err))
			continue
		}
		if matchesQueryFilters(t, query) {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		return count, err
	}

	return count, nil
}

// Take takes a agent from the space (non-blocking, destructive).

func buildSQLiteWhereClauses(query *agent.Query, forceStatusNew bool, nowMs int64) (string, []interface{}, bool) {
	clauses := []string{sqliteTTLClause()}
	args := []interface{}{nowMs}
	needsFilter := false

	if query != nil {
		if forceStatusNew {
			clauses = append(clauses, "status = ?")
			args = append(args, string(agent.StatusNew))
		} else if query.Status != "" {
			clauses = append(clauses, "status = ?")
			args = append(args, string(query.Status))
		}

		if query.Kind != "" {
			clauses = append(clauses, "kind = ?")
			args = append(args, query.Kind)
		}

		if query.Owner != "" {
			clauses = append(clauses, "owner = ?")
			args = append(args, query.Owner)
		}

		if query.ParentID != "" {
			clauses = append(clauses, "parent_id = ?")
			args = append(args, query.ParentID)
		}

		if query.TraceID != "" {
			clauses = append(clauses, "trace_id = ?")
			args = append(args, query.TraceID)
		}

		if query.NamespaceID != "" {
			needsFilter = true
		}

		if len(query.Tags) > 0 {
			needsFilter = true
		}

		if query.Metadata != nil {
			for k := range query.Metadata {
				if k == "agent_id" {
					continue
				}
				needsFilter = true
				break
			}
		}
	}

	return strings.Join(clauses, " AND "), args, needsFilter
}

func sqliteTTLClause() string {
	return "(ttl_ms = 0 OR created_at_ms + ttl_ms > ?)"
}

type sqliteRow interface {
	Scan(dest ...interface{}) error
}

func matchesQueryFilters(t *agent.Agent, query *agent.Query) bool {
	if t == nil || query == nil {
		return true
	}
	if query.NamespaceID != "" {
		namespaceID := t.NamespaceID
		if namespaceID == "" && t.Metadata != nil {
			namespaceID = t.Metadata[namespaceMetadataKey]
		}
		if namespaceID != query.NamespaceID {
			return false
		}
	}
	if len(query.Tags) > 0 {
		for _, tag := range query.Tags {
			found := false
			for _, existing := range t.Tags {
				if existing == tag {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}

	if query.Metadata != nil {
		for k, v := range query.Metadata {
			if k == "agent_id" {
				continue
			}
			if t.Metadata == nil {
				return false
			}
			if val, ok := t.Metadata[k]; !ok || val != v {
				return false
			}
		}
	}

	return true
}

// leaseReaperLoop periodically checks for and reaps expired leases.

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func (s *PostgresStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return s.in(context.Background(), query, timeout)
}

func (s *PostgresStore) in(baseCtx context.Context, query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithTimeout(defaultContext(baseCtx), timeout)
	defer cancel()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		t, err := s.take(ctx, query)
		if err != nil {
			return nil, err
		}
		if t != nil {
			return t, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.currentWakeupChan():
		case <-time.After(s.pollInterval):
		}
	}

	return nil, fmt.Errorf("timeout waiting for agent")
}

// Read reads a agent from the space (blocking, non-destructive).
func (s *PostgresStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return s.read(context.Background(), query, timeout)
}

func (s *PostgresStore) read(baseCtx context.Context, query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithTimeout(defaultContext(baseCtx), timeout)
	defer cancel()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		q := &agent.Query{}
		if query != nil {
			*q = *query
		}
		q.Limit = 1

		agents, err := s.query(ctx, q)
		if err != nil {
			return nil, err
		}
		if len(agents) > 0 {
			return agents[0], nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.currentWakeupChan():
		case <-time.After(s.pollInterval):
		}
	}

	return nil, fmt.Errorf("timeout waiting for agent")
}

// Query returns matching agents (non-blocking, non-destructive).
func (s *PostgresStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	return s.query(context.Background(), query)
}

func (s *PostgresStore) query(ctx context.Context, query *agent.Query) ([]*agent.Agent, error) {
	ctx = defaultContext(ctx)
	where, args := buildWhereClauses(query, false)
	sql := fmt.Sprintf("SELECT %s FROM agents WHERE %s ORDER BY created_at ASC, id ASC", agentSelectColumns, where)

	idx := len(args) + 1
	if query != nil && query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT $%d", idx)
		args = append(args, query.Limit)
		idx++
	}
	if query != nil && query.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET $%d", idx)
		args = append(args, query.Offset)
	}

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var agents []*agent.Agent
	for rows.Next() {
		t, err := scanAgent(rows)
		if err != nil {
			s.logger.Warn("failed to scan agent", zap.Error(err))
			continue
		}
		agents = append(agents, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return agents, nil
}

// Count returns the number of agents matching the query without pagination.
func (s *PostgresStore) Count(query *agent.Query) (int, error) {
	return s.count(context.Background(), query)
}

func (s *PostgresStore) count(ctx context.Context, query *agent.Query) (int, error) {
	ctx = defaultContext(ctx)
	where, args := buildWhereClauses(query, false)
	sql := fmt.Sprintf("SELECT COUNT(*) FROM agents WHERE %s", where)

	var count int
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// CountByStatus returns grouped status counts for agents matching the query.
func (s *PostgresStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	return s.countByStatus(context.Background(), query)
}

func (s *PostgresStore) countByStatus(ctx context.Context, query *agent.Query) (map[agent.Status]int, error) {
	q := &agent.Query{}
	if query != nil {
		clone := *query
		clone.Status = ""
		clone.Limit = 0
		clone.Offset = 0
		q = &clone
	}

	where, args := buildWhereClauses(q, false)
	sql := fmt.Sprintf("SELECT status, COUNT(*) FROM agents WHERE %s GROUP BY status", where)

	ctx = defaultContext(ctx)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[agent.Status]int{
		agent.StatusNew:        0,
		agent.StatusInProgress: 0,
		agent.StatusCompleted:  0,
		agent.StatusFailed:     0,
		agent.StatusCancelled:  0,
	}

	for rows.Next() {
		var (
			statusRaw string
			count     int
		)
		if err := rows.Scan(&statusRaw, &count); err != nil {
			return nil, err
		}
		status := agent.Status(statusRaw)
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return counts, nil
}

// Take takes a agent from the space (non-blocking, destructive).

func buildWhereClauses(query *agent.Query, forceStatusNew bool) (string, []interface{}) {
	clauses := []string{ttlClause()}
	args := []interface{}{}
	idx := 1

	if query != nil {
		if forceStatusNew {
			clauses = append(clauses, fmt.Sprintf("status = $%d", idx))
			args = append(args, string(agent.StatusNew))
			idx++
		} else if query.Status != "" {
			clauses = append(clauses, fmt.Sprintf("status = $%d", idx))
			args = append(args, string(query.Status))
			idx++
		}

		if query.Kind != "" {
			clauses = append(clauses, fmt.Sprintf("kind = $%d", idx))
			args = append(args, query.Kind)
			idx++
		}

		if query.Owner != "" {
			clauses = append(clauses, fmt.Sprintf("owner = $%d", idx))
			args = append(args, query.Owner)
			idx++
		}

		if query.ParentID != "" {
			clauses = append(clauses, fmt.Sprintf("parent_id = $%d", idx))
			args = append(args, query.ParentID)
			idx++
		}

		if query.TraceID != "" {
			clauses = append(clauses, fmt.Sprintf("trace_id = $%d", idx))
			args = append(args, query.TraceID)
			idx++
		}

		if query.NamespaceID != "" {
			clauses = append(clauses, fmt.Sprintf("namespace_id = $%d", idx))
			args = append(args, query.NamespaceID)
			idx++
		}

		if len(query.Tags) > 0 {
			clauses = append(clauses, fmt.Sprintf("tags @> $%d", idx))
			args = append(args, query.Tags)
			idx++
		}

		if query.Metadata != nil {
			for k, v := range query.Metadata {
				if k == "agent_id" {
					continue
				}
				if k == queueMetadataKey {
					clauses = append(clauses, fmt.Sprintf("queue = $%d", idx))
					args = append(args, v)
					idx++
					continue
				}
				fragment, _ := json.Marshal(map[string]string{k: v})
				clauses = append(clauses, fmt.Sprintf("metadata @> $%d::jsonb", idx))
				args = append(args, string(fragment))
				idx++
			}
		}
	}

	return strings.Join(clauses, " AND "), args
}

func ttlClause() string {
	return "(ttl_ms = 0 OR created_at + (ttl_ms * interval '1 millisecond') > now())"
}

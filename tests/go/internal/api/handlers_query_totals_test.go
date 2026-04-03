package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type queryTotalSpace struct {
	agents []*agent.Agent
}

func (s *queryTotalSpace) Get(string) (*agent.Agent, error) {
	return nil, nil
}

func (s *queryTotalSpace) Out(*agent.Agent) error {
	return nil
}

func (s *queryTotalSpace) In(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *queryTotalSpace) Read(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *queryTotalSpace) Query(query *agent.Query) ([]*agent.Agent, error) {
	matches := make([]*agent.Agent, 0, len(s.agents))
	for _, item := range s.agents {
		if query != nil {
			if query.Kind != "" && item.Kind != query.Kind {
				continue
			}
			if query.Status != "" && item.Status != query.Status {
				continue
			}
		}
		copy := *item
		matches = append(matches, &copy)
	}
	if query == nil {
		return matches, nil
	}
	start := query.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(matches) {
		return []*agent.Agent{}, nil
	}
	end := len(matches)
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}
	return matches[start:end], nil
}

func (s *queryTotalSpace) Take(*agent.Query) (*agent.Agent, error) {
	return nil, nil
}

func (s *queryTotalSpace) Update(string, map[string]interface{}) error {
	return nil
}

func (s *queryTotalSpace) Complete(string, string, string, map[string]interface{}) error {
	return nil
}

func (s *queryTotalSpace) CompleteAndOut(string, string, string, map[string]interface{}, []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return nil, nil, nil
}

func (s *queryTotalSpace) Release(string, string, string, string) error {
	return nil
}

func (s *queryTotalSpace) RenewLease(string, string, string, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *queryTotalSpace) Subscribe(*agent.EventFilter, agent.EventHandler) error {
	return nil
}

func (s *queryTotalSpace) GetEvents(string) ([]*agent.Event, error) {
	return nil, nil
}

func (s *queryTotalSpace) GetGlobalEvents(int, int) ([]*agent.Event, error) {
	return nil, nil
}

func (s *queryTotalSpace) GetDAG(string) (*agent.DAG, error) {
	return nil, nil
}

func (s *queryTotalSpace) Count(query *agent.Query) (int, error) {
	items, err := s.Query(&agent.Query{
		Kind:   query.Kind,
		Status: query.Status,
	})
	if err != nil {
		return 0, err
	}
	return len(items), nil
}

func TestQueryAgentsReturnsTrueTotalAcrossPages(t *testing.T) {
	now := time.Now().UTC()
	space := &queryTotalSpace{
		agents: []*agent.Agent{
			{ID: "t1", Kind: "bench", Status: agent.StatusCompleted, CreatedAt: now, UpdatedAt: now},
			{ID: "t2", Kind: "bench", Status: agent.StatusCompleted, CreatedAt: now, UpdatedAt: now},
			{ID: "t3", Kind: "bench", Status: agent.StatusCompleted, CreatedAt: now, UpdatedAt: now},
			{ID: "t4", Kind: "bench", Status: agent.StatusCompleted, CreatedAt: now, UpdatedAt: now},
			{ID: "t5", Kind: "bench", Status: agent.StatusCompleted, CreatedAt: now, UpdatedAt: now},
		},
	}
	h := NewHandlers(space, zap.NewNop())

	body, err := json.Marshal(agent.Query{
		Kind:   "bench",
		Status: agent.StatusCompleted,
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/query", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.QueryAgents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	var payload struct {
		Agents []agent.Agent `json:"agents"`
		Total  int           `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(payload.Agents) != 2 {
		t.Fatalf("expected 2 agents in page, got %d", len(payload.Agents))
	}
	if payload.Total != 5 {
		t.Fatalf("expected total 5, got %d", payload.Total)
	}
}

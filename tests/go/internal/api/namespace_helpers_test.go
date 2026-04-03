package api

import (
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type quotaSpace struct {
	counts map[string]int
}

func (s *quotaSpace) key(query *agent.Query) string {
	if query == nil {
		return ""
	}
	namespaceID := ""
	queue := ""
	if query.Metadata != nil {
		namespaceID = query.Metadata[namespaceMetadataKey]
		queue = query.Metadata[queueMetadataKey]
	}
	return string(query.Status) + "|" + namespaceID + "|" + queue
}

func (s *quotaSpace) Count(query *agent.Query) (int, error) {
	return s.counts[s.key(query)], nil
}

func (s *quotaSpace) Get(id string) (*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Out(agent *agent.Agent) error {
	return nil
}

func (s *quotaSpace) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Query(query *agent.Query) ([]*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Take(query *agent.Query) (*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Update(id string, updates map[string]interface{}) error {
	return nil
}

func (s *quotaSpace) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	return nil
}

func (s *quotaSpace) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return nil, nil, nil
}

func (s *quotaSpace) Release(id string, agentID string, leaseToken string, reason string) error {
	return nil
}

func (s *quotaSpace) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *quotaSpace) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	return nil
}

func (s *quotaSpace) GetEvents(tupleID string) ([]*agent.Event, error) {
	return nil, nil
}

func (s *quotaSpace) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	return nil, nil
}

func (s *quotaSpace) GetDAG(rootID string) (*agent.DAG, error) {
	return nil, nil
}

func TestApplyPrincipalNamespaceToQuery(t *testing.T) {
	h := NewHandlers(&quotaSpace{}, zap.NewNop())
	req := httptest.NewRequest("POST", "/api/v1/agents/query", nil)
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "tenant-a"}))

	query := &agent.Query{
		NamespaceID: "wrong-client-value",
		Metadata: map[string]string{
			namespaceMetadataKey: "other",
		},
	}

	if err := h.applyPrincipalNamespaceToQuery(req, query); err != nil {
		t.Fatalf("applyPrincipalNamespaceToQuery() error = %v", err)
	}
	if query.NamespaceID != "tenant-a" {
		t.Fatalf("expected namespace_id=tenant-a, got %q", query.NamespaceID)
	}
	if query.Metadata[namespaceMetadataKey] != "tenant-a" {
		t.Fatalf("expected metadata.namespace_id=tenant-a, got %q", query.Metadata[namespaceMetadataKey])
	}
}

func TestAgentVisibleToPrincipal(t *testing.T) {
	h := NewHandlers(&quotaSpace{}, zap.NewNop())
	req := httptest.NewRequest("GET", "/api/v1/agents/t1", nil)
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "tenant-a"}))

	if !h.agentVisibleToPrincipal(req, &agent.Agent{NamespaceID: "tenant-a"}) {
		t.Fatalf("expected agent to be visible for matching namespace")
	}
	if h.agentVisibleToPrincipal(req, &agent.Agent{NamespaceID: "tenant-b"}) {
		t.Fatalf("expected agent to be hidden for mismatched namespace")
	}
	if !h.agentVisibleToPrincipal(req, &agent.Agent{Metadata: map[string]string{namespaceMetadataKey: "tenant-a"}}) {
		t.Fatalf("expected agent metadata namespace fallback to be visible")
	}
}

func TestAllowNamespaceAgentCreation_ActiveQuota(t *testing.T) {
	space := &quotaSpace{
		counts: map[string]int{
			string(agent.StatusNew) + "|tenant-a|":        1,
			string(agent.StatusInProgress) + "|tenant-a|": 1,
		},
	}
	h := NewHandlers(space, zap.NewNop())
	principal := AuthPrincipal{
		NamespaceID: "tenant-a",
		Limits: NamespaceLimits{
			MaxActiveAgents: 3,
		},
	}

	allowed, reason, err := h.allowNamespaceAgentCreation(principal, "tenant-a", nil, 1)
	if err != nil {
		t.Fatalf("allowNamespaceAgentCreation() error = %v", err)
	}
	if !allowed {
		t.Fatalf("expected quota check to allow one additional agent, reason=%q", reason)
	}

	allowed, reason, err = h.allowNamespaceAgentCreation(principal, "tenant-a", nil, 2)
	if err != nil {
		t.Fatalf("allowNamespaceAgentCreation() error = %v", err)
	}
	if allowed || reason != "active_agent_quota" {
		t.Fatalf("expected active_agent_quota rejection, got allowed=%v reason=%q", allowed, reason)
	}
}

func TestAllowNamespaceAgentCreation_QueueQuota(t *testing.T) {
	space := &quotaSpace{
		counts: map[string]int{
			string(agent.StatusNew) + "|tenant-a|jobs": 2,
		},
	}
	h := NewHandlers(space, zap.NewNop())
	principal := AuthPrincipal{
		NamespaceID: "tenant-a",
		Limits: NamespaceLimits{
			MaxQueueDepth: 2,
		},
	}

	allowed, reason, err := h.allowNamespaceAgentCreation(principal, "tenant-a", map[string]string{
		queueMetadataKey: "jobs",
	}, 1)
	if err != nil {
		t.Fatalf("allowNamespaceAgentCreation() error = %v", err)
	}
	if allowed || reason != "queue_depth_quota" {
		t.Fatalf("expected queue_depth_quota rejection, got allowed=%v reason=%q", allowed, reason)
	}
}

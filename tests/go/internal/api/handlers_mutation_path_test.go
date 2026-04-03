package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type mutationPathSpace struct {
	agents map[string]*agent.Agent

	getCalls      int
	takeCalls     int
	updateCalls   int
	completeCalls int
	releaseCalls  int
	takeResult    *agent.Agent
}

func newMutationPathSpace() *mutationPathSpace {
	return &mutationPathSpace{
		agents: map[string]*agent.Agent{
			"t1": {
				ID:         "t1",
				Kind:       "test_task",
				Status:     agent.StatusInProgress,
				Payload:    map[string]interface{}{},
				Owner:      "agent-1",
				LeaseToken: "lease-1",
				Metadata:   map[string]string{},
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
				Version:    1,
			},
		},
	}
}

func cloneAgent(src *agent.Agent) *agent.Agent {
	if src == nil {
		return nil
	}
	dst := *src
	if src.Payload != nil {
		dst.Payload = make(map[string]interface{}, len(src.Payload))
		for k, v := range src.Payload {
			dst.Payload[k] = v
		}
	}
	if src.Metadata != nil {
		dst.Metadata = make(map[string]string, len(src.Metadata))
		for k, v := range src.Metadata {
			dst.Metadata[k] = v
		}
	}
	if src.Tags != nil {
		dst.Tags = append([]string(nil), src.Tags...)
	}
	return &dst
}

func (s *mutationPathSpace) getAgent(id string) (*agent.Agent, error) {
	t, ok := s.agents[id]
	if !ok {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	return t, nil
}

func (s *mutationPathSpace) Get(id string) (*agent.Agent, error) {
	s.getCalls++
	t, err := s.getAgent(id)
	if err != nil {
		return nil, err
	}
	return cloneAgent(t), nil
}

func (s *mutationPathSpace) Out(_ *agent.Agent) error { return nil }

func (s *mutationPathSpace) In(_ *agent.Query, _ time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *mutationPathSpace) Read(_ *agent.Query, _ time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *mutationPathSpace) Query(_ *agent.Query) ([]*agent.Agent, error) { return nil, nil }

func (s *mutationPathSpace) Take(_ *agent.Query) (*agent.Agent, error) {
	s.takeCalls++
	if s.takeResult == nil {
		return nil, nil
	}
	return cloneAgent(s.takeResult), nil
}

func histogramSampleCount(t *testing.T, metricName string, labels map[string]string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := true
			for key, expected := range labels {
				found := false
				for _, pair := range metric.GetLabel() {
					if pair.GetName() == key && pair.GetValue() == expected {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched && metric.GetHistogram() != nil {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func (s *mutationPathSpace) Update(id string, updates map[string]interface{}) error {
	s.updateCalls++
	t, err := s.getAgent(id)
	if err != nil {
		return err
	}
	if payload, ok := updates["payload"].(map[string]interface{}); ok {
		if t.Payload == nil {
			t.Payload = make(map[string]interface{})
		}
		for k, v := range payload {
			t.Payload[k] = v
		}
	}
	t.UpdatedAt = time.Now().UTC()
	t.Version++
	return nil
}

func (s *mutationPathSpace) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	s.completeCalls++
	t, err := s.getAgent(id)
	if err != nil {
		return err
	}
	if t.Owner != agentID {
		return fmt.Errorf("cannot complete: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return fmt.Errorf("lease token mismatch")
	}
	if t.Payload == nil {
		t.Payload = make(map[string]interface{})
	}
	if result == nil {
		result = map[string]interface{}{}
	}
	t.Payload["result"] = result
	t.Status = agent.StatusCompleted
	t.LeaseToken = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = time.Now().UTC()
	t.Version++
	return nil
}

func (s *mutationPathSpace) CompleteAndOut(_ string, _ string, _ string, _ map[string]interface{}, _ []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return nil, nil, nil
}

func (s *mutationPathSpace) Release(id string, agentID string, leaseToken string, _ string) error {
	s.releaseCalls++
	t, err := s.getAgent(id)
	if err != nil {
		return err
	}
	if t.Owner != agentID {
		return fmt.Errorf("cannot release: agent owned by %q, not %q", t.Owner, agentID)
	}
	if t.LeaseToken != leaseToken {
		return fmt.Errorf("lease token mismatch")
	}
	t.Status = agent.StatusNew
	t.Owner = ""
	t.LeaseToken = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = time.Now().UTC()
	t.Version++
	return nil
}

func (s *mutationPathSpace) RenewLease(_ string, _ string, _ string, _ time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *mutationPathSpace) Subscribe(_ *agent.EventFilter, _ agent.EventHandler) error { return nil }

func (s *mutationPathSpace) GetEvents(_ string) ([]*agent.Event, error) { return nil, nil }

func (s *mutationPathSpace) GetGlobalEvents(_, _ int) ([]*agent.Event, error) { return nil, nil }

func (s *mutationPathSpace) GetDAG(_ string) (*agent.DAG, error) { return nil, nil }

type mutationPathSpaceWithReturn struct {
	*mutationPathSpace
	updateAndGetCalls   int
	completeAndGetCalls int
	releaseAndGetCalls  int
}

func (s *mutationPathSpaceWithReturn) UpdateAndGet(id string, updates map[string]interface{}) (*agent.Agent, error) {
	s.updateAndGetCalls++
	if err := s.mutationPathSpace.Update(id, updates); err != nil {
		return nil, err
	}
	t, err := s.getAgent(id)
	if err != nil {
		return nil, err
	}
	return cloneAgent(t), nil
}

func (s *mutationPathSpaceWithReturn) CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	s.completeAndGetCalls++
	if err := s.mutationPathSpace.Complete(id, agentID, leaseToken, result); err != nil {
		return nil, err
	}
	t, err := s.getAgent(id)
	if err != nil {
		return nil, err
	}
	return cloneAgent(t), nil
}

func (s *mutationPathSpaceWithReturn) ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	s.releaseAndGetCalls++
	if err := s.mutationPathSpace.Release(id, agentID, leaseToken, reason); err != nil {
		return nil, err
	}
	t, err := s.getAgent(id)
	if err != nil {
		return nil, err
	}
	return cloneAgent(t), nil
}

func decodeAgentResponse(t *testing.T, rr *httptest.ResponseRecorder) *agent.Agent {
	t.Helper()
	var out agent.Agent
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to decode agent response: %v", err)
	}
	return &out
}

func TestRequestPrefersReturnMinimal(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   bool
	}{
		{name: "empty", header: "", want: false},
		{name: "exact", header: "return=minimal", want: true},
		{name: "mixed directives", header: "respond-async, return=minimal", want: true},
		{name: "quoted value", header: "return=\"minimal\"", want: true},
		{name: "non minimal", header: "return=representation", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", nil)
			if tc.header != "" {
				req.Header.Set("Prefer", tc.header)
			}
			if got := requestPrefersReturnMinimal(req); got != tc.want {
				t.Fatalf("requestPrefersReturnMinimal() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUpdateAgentUsesUpdateAndGetWhenSupported(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPut, "/api/v1/agents/t1", strings.NewReader(`{"payload":{"x":1}}`))
	rr := httptest.NewRecorder()
	h.UpdateAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 1 {
		t.Fatalf("expected one Get call (pre-check only), got %d", space.getCalls)
	}
	if space.updateAndGetCalls != 1 {
		t.Fatalf("expected UpdateAndGet to be used once, got %d", space.updateAndGetCalls)
	}

	resp := decodeAgentResponse(t, rr)
	if got := resp.Payload["x"]; got != float64(1) {
		t.Fatalf("expected payload.x=1, got %#v", got)
	}
}

func TestUpdateAgentFallsBackToGetWhenCapabilityMissing(t *testing.T) {
	space := newMutationPathSpace()
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPut, "/api/v1/agents/t1", strings.NewReader(`{"payload":{"x":2}}`))
	rr := httptest.NewRecorder()
	h.UpdateAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 2 {
		t.Fatalf("expected two Get calls (pre-check + response fetch), got %d", space.getCalls)
	}
	if space.updateCalls != 1 {
		t.Fatalf("expected Update to be called once, got %d", space.updateCalls)
	}
}

func TestUpdateAgentRejectsLeaseSensitiveFields(t *testing.T) {
	space := newMutationPathSpace()
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPut, "/api/v1/agents/t1", strings.NewReader(`{"status":"COMPLETED","owner":"agent-2"}`))
	rr := httptest.NewRecorder()
	h.UpdateAgent(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.updateCalls != 0 {
		t.Fatalf("expected Update not to be called, got %d", space.updateCalls)
	}
	if space.getCalls != 1 {
		t.Fatalf("expected one pre-check Get call, got %d", space.getCalls)
	}
}

func TestTakeAgentPreferMinimalRecordsHandlerStageMetrics(t *testing.T) {
	space := newMutationPathSpace()
	space.takeResult = &agent.Agent{
		ID:         "take-1",
		Kind:       "test_task",
		Status:     agent.StatusInProgress,
		TraceID:    "trace-1",
		LeaseToken: "lease-1",
		Metadata:   map[string]string{},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
		Version:    1,
	}
	h := NewHandlers(space, zap.NewNop())

	labels := func(stage string) map[string]string {
		return map[string]string{
			"operation":  "http_take",
			"stage":      stage,
			"store_type": "unknown",
		}
	}
	decodeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("decode"))
	storeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("store"))
	encodeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("encode"))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/take", strings.NewReader(`{"query":{"kind":"test_task"}}`))
	req.Header.Set("Prefer", "return=minimal")
	rr := httptest.NewRecorder()
	h.TakeAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.takeCalls != 1 {
		t.Fatalf("expected Take to be called once, got %d", space.takeCalls)
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("decode")); got != decodeBefore+1 {
		t.Fatalf("expected decode stage sample count to increase by 1, got before=%d after=%d", decodeBefore, got)
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("store")); got != storeBefore+1 {
		t.Fatalf("expected store stage sample count to increase by 1, got before=%d after=%d", storeBefore, got)
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("encode")); got != encodeBefore+1 {
		t.Fatalf("expected encode stage sample count to increase by 1, got before=%d after=%d", encodeBefore, got)
	}
}

func TestCompleteAgentUsesCompleteAndGetWhenSupported(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", strings.NewReader(`{"result":{"ok":true}}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	rr := httptest.NewRecorder()
	h.CompleteAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 0 {
		t.Fatalf("expected zero Get calls without namespace principal, got %d", space.getCalls)
	}
	if space.completeAndGetCalls != 1 {
		t.Fatalf("expected CompleteAndGet to be used once, got %d", space.completeAndGetCalls)
	}

	resp := decodeAgentResponse(t, rr)
	if resp.Status != agent.StatusCompleted {
		t.Fatalf("expected COMPLETED status, got %s", resp.Status)
	}
}

func TestCompleteAgentFallsBackToGetWhenCapabilityMissing(t *testing.T) {
	space := newMutationPathSpace()
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", strings.NewReader(`{"result":{"ok":true}}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	rr := httptest.NewRecorder()
	h.CompleteAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 1 {
		t.Fatalf("expected one Get call (response fetch only), got %d", space.getCalls)
	}
	if space.completeCalls != 1 {
		t.Fatalf("expected Complete to be called once, got %d", space.completeCalls)
	}
}

func TestCompleteAgentPreferMinimalUsesCompleteWithoutRepresentation(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", strings.NewReader(`{"result":{"ok":true}}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	req.Header.Set("Prefer", "return=minimal")
	rr := httptest.NewRecorder()
	h.CompleteAgent(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("expected empty body for 204 response, got %q", rr.Body.String())
	}
	if space.getCalls != 0 {
		t.Fatalf("expected zero Get calls without namespace principal, got %d", space.getCalls)
	}
	if space.completeCalls != 1 {
		t.Fatalf("expected Complete to be called once, got %d", space.completeCalls)
	}
	if space.completeAndGetCalls != 0 {
		t.Fatalf("expected CompleteAndGet not to be used in minimal mode, got %d", space.completeAndGetCalls)
	}
}

func TestCompleteAgentPreferMinimalRecordsHandlerStageMetrics(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	h := NewHandlers(space, zap.NewNop())

	labels := func(stage string) map[string]string {
		return map[string]string{
			"operation":  "http_complete",
			"stage":      stage,
			"store_type": "unknown",
		}
	}
	decodeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("decode"))
	storeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("store"))
	encodeBefore := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("encode"))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", strings.NewReader(`{"result":{"ok":true}}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	req.Header.Set("Prefer", "return=minimal")
	rr := httptest.NewRecorder()
	h.CompleteAgent(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", rr.Code, rr.Body.String())
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("decode")); got != decodeBefore+1 {
		t.Fatalf("expected decode stage sample count to increase by 1, got before=%d after=%d", decodeBefore, got)
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("store")); got != storeBefore+1 {
		t.Fatalf("expected store stage sample count to increase by 1, got before=%d after=%d", storeBefore, got)
	}
	if got := histogramSampleCount(t, "agent_space_operation_stage_duration_seconds", labels("encode")); got != encodeBefore+1 {
		t.Fatalf("expected encode stage sample count to increase by 1, got before=%d after=%d", encodeBefore, got)
	}
}

func TestCompleteAgentWithNamespacePrincipalPerformsVisibilityPreCheck(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	space.agents["t1"].NamespaceID = "ns-1"
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/complete", strings.NewReader(`{"result":{"ok":true}}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "ns-1"}))
	rr := httptest.NewRecorder()
	h.CompleteAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 1 {
		t.Fatalf("expected one visibility pre-check Get call, got %d", space.getCalls)
	}
	if space.completeAndGetCalls != 1 {
		t.Fatalf("expected CompleteAndGet to be used once, got %d", space.completeAndGetCalls)
	}
}

func TestReleaseAgentUsesReleaseAndGetWhenSupported(t *testing.T) {
	space := &mutationPathSpaceWithReturn{mutationPathSpace: newMutationPathSpace()}
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/release", strings.NewReader(`{"reason":"test"}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	rr := httptest.NewRecorder()
	h.ReleaseAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 1 {
		t.Fatalf("expected one Get call (pre-check only), got %d", space.getCalls)
	}
	if space.releaseAndGetCalls != 1 {
		t.Fatalf("expected ReleaseAndGet to be used once, got %d", space.releaseAndGetCalls)
	}

	resp := decodeAgentResponse(t, rr)
	if resp.Status != agent.StatusNew {
		t.Fatalf("expected NEW status, got %s", resp.Status)
	}
}

func TestReleaseAgentFallsBackToGetWhenCapabilityMissing(t *testing.T) {
	space := newMutationPathSpace()
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/t1/release", strings.NewReader(`{"reason":"test"}`))
	req.Header.Set("X-Agent-ID", "agent-1")
	req.Header.Set("X-Lease-Token", "lease-1")
	rr := httptest.NewRecorder()
	h.ReleaseAgent(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.getCalls != 2 {
		t.Fatalf("expected two Get calls (pre-check + response fetch), got %d", space.getCalls)
	}
	if space.releaseCalls != 1 {
		t.Fatalf("expected Release to be called once, got %d", space.releaseCalls)
	}
}

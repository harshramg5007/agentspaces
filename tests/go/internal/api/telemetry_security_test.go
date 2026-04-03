package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

type telemetrySecuritySpace struct {
	agents             map[string]*agent.Agent
	recorded           []telemetry.Span
	listSpansNamespace string
	listedSpans        []telemetry.Span
}

func (s *telemetrySecuritySpace) Get(id string) (*agent.Agent, error) {
	t, ok := s.agents[id]
	if !ok {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	copy := *t
	if t.Metadata != nil {
		copy.Metadata = make(map[string]string, len(t.Metadata))
		for k, v := range t.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy, nil
}

func (s *telemetrySecuritySpace) Out(*agent.Agent) error { return nil }

func (s *telemetrySecuritySpace) In(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *telemetrySecuritySpace) Read(*agent.Query, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *telemetrySecuritySpace) Query(*agent.Query) ([]*agent.Agent, error) { return nil, nil }

func (s *telemetrySecuritySpace) Take(*agent.Query) (*agent.Agent, error) { return nil, nil }

func (s *telemetrySecuritySpace) Update(string, map[string]interface{}) error { return nil }

func (s *telemetrySecuritySpace) Complete(string, string, string, map[string]interface{}) error {
	return nil
}

func (s *telemetrySecuritySpace) CompleteAndOut(string, string, string, map[string]interface{}, []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return nil, nil, nil
}

func (s *telemetrySecuritySpace) Release(string, string, string, string) error { return nil }

func (s *telemetrySecuritySpace) RenewLease(string, string, string, time.Duration) (*agent.Agent, error) {
	return nil, nil
}

func (s *telemetrySecuritySpace) Subscribe(*agent.EventFilter, agent.EventHandler) error { return nil }

func (s *telemetrySecuritySpace) GetEvents(string) ([]*agent.Event, error) { return nil, nil }

func (s *telemetrySecuritySpace) GetGlobalEvents(int, int) ([]*agent.Event, error) { return nil, nil }

func (s *telemetrySecuritySpace) GetDAG(string) (*agent.DAG, error) { return nil, nil }

func (s *telemetrySecuritySpace) RecordSpans(_ context.Context, spans []telemetry.Span) error {
	s.recorded = append(s.recorded, spans...)
	return nil
}

func (s *telemetrySecuritySpace) ListSpans(_ context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error) {
	_ = traceID
	_ = limit
	_ = offset
	s.listSpansNamespace = namespaceID
	filtered := make([]telemetry.Span, 0, len(s.listedSpans))
	for _, span := range s.listedSpans {
		if namespaceID != "" && span.NamespaceID != namespaceID {
			continue
		}
		filtered = append(filtered, span)
	}
	return filtered, nil
}

func TestIngestTelemetrySpanScopesNamespaceToPrincipal(t *testing.T) {
	space := &telemetrySecuritySpace{
		agents: map[string]*agent.Agent{
			"agent-a": {ID: "agent-a", NamespaceID: "tenant-a"},
		},
	}
	h := NewHandlers(space, zap.NewNop())

	body := bytes.NewBufferString(`{"trace_id":"trace-1","tuple_id":"agent-a","namespace_id":"tenant-b","name":"worker.span"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/spans", body)
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "tenant-a"}))
	rr := httptest.NewRecorder()

	h.IngestTelemetrySpan(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(space.recorded) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(space.recorded))
	}
	if space.recorded[0].NamespaceID != "tenant-a" {
		t.Fatalf("expected namespace to be overwritten to tenant-a, got %q", space.recorded[0].NamespaceID)
	}
}

func TestIngestTelemetrySpanRejectsForeignTuple(t *testing.T) {
	space := &telemetrySecuritySpace{
		agents: map[string]*agent.Agent{
			"agent-b": {ID: "agent-b", NamespaceID: "tenant-b"},
		},
	}
	h := NewHandlers(space, zap.NewNop())

	body := bytes.NewBufferString(`{"trace_id":"trace-1","tuple_id":"agent-b","name":"worker.span"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/spans", body)
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "tenant-a"}))
	rr := httptest.NewRecorder()

	h.IngestTelemetrySpan(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for foreign tuple, got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(space.recorded) != 0 {
		t.Fatalf("expected no spans to be recorded, got %d", len(space.recorded))
	}
}

func TestListTelemetrySpansScopesByPrincipalNamespace(t *testing.T) {
	space := &telemetrySecuritySpace{
		listedSpans: []telemetry.Span{
			{TraceID: "trace-1", NamespaceID: "tenant-a", Name: "allowed"},
			{TraceID: "trace-1", NamespaceID: "tenant-b", Name: "blocked"},
		},
	}
	h := NewHandlers(space, zap.NewNop())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/telemetry/spans?trace_id=trace-1", nil)
	req = req.WithContext(withAuthPrincipal(req.Context(), AuthPrincipal{NamespaceID: "tenant-a"}))
	rr := httptest.NewRecorder()

	h.ListTelemetrySpans(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if space.listSpansNamespace != "tenant-a" {
		t.Fatalf("expected namespace scoped query, got %q", space.listSpansNamespace)
	}

	var payload struct {
		Spans []telemetry.Span `json:"spans"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode spans response: %v", err)
	}
	if len(payload.Spans) != 1 || payload.Spans[0].Name != "allowed" {
		t.Fatalf("expected only tenant-a span, got %#v", payload.Spans)
	}
}

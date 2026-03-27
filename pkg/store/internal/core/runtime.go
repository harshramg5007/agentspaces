package core

import (
	"context"
	"errors"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

var ErrCapabilityUnsupported = errors.New("store capability unsupported")

// Engine represents the mandatory store surface.
type Engine interface {
	agent.AgentSpace
}

// Optional capability interfaces.
type StatusCounter interface {
	CountByStatus(query *agent.Query) (map[agent.Status]int, error)
}

type DebugInvariants interface {
	DebugInvariants(ctx context.Context) (map[string]interface{}, error)
}

type Telemetry interface {
	RecordSpans(ctx context.Context, spans []telemetry.Span) error
	ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error)
}

// StoreRuntime binds an engine and discovers optional capabilities.
type StoreRuntime struct {
	Engine
	StatusCounter   StatusCounter
	DebugInvariants DebugInvariants
	Telemetry       Telemetry
}

func NewStoreRuntime(engine Engine) *StoreRuntime {
	r := &StoreRuntime{Engine: engine}
	if c, ok := engine.(StatusCounter); ok {
		r.StatusCounter = c
	}
	if d, ok := engine.(DebugInvariants); ok {
		r.DebugInvariants = d
	}
	if t, ok := engine.(Telemetry); ok {
		r.Telemetry = t
	}
	return r
}

func (r *StoreRuntime) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	if r == nil || r.StatusCounter == nil {
		return nil, ErrCapabilityUnsupported
	}
	return r.StatusCounter.CountByStatus(query)
}

func (r *StoreRuntime) GetDebugInvariants(ctx context.Context) (map[string]interface{}, error) {
	if r == nil || r.DebugInvariants == nil {
		return nil, ErrCapabilityUnsupported
	}
	return r.DebugInvariants.DebugInvariants(ctx)
}

func (r *StoreRuntime) RecordTelemetry(ctx context.Context, spans []telemetry.Span) error {
	if r == nil || r.Telemetry == nil {
		return ErrCapabilityUnsupported
	}
	return r.Telemetry.RecordSpans(ctx, spans)
}

func (r *StoreRuntime) ListTelemetry(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error) {
	if r == nil || r.Telemetry == nil {
		return nil, ErrCapabilityUnsupported
	}
	return r.Telemetry.ListSpans(ctx, traceID, namespaceID, limit, offset)
}

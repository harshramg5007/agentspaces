package api

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

type telemetryStore interface {
	RecordSpans(ctx context.Context, spans []telemetry.Span) error
	ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error)
}

type telemetrySpanBatch struct {
	Spans []telemetry.Span `json:"spans"`
}

func (h *Handlers) recordObs(ctx context.Context, tupleID string, traceID string, namespaceID string, obs *telemetry.Observation) {
	if h.telemetry == nil || obs == nil {
		return
	}

	spans := obsToSpans(tupleID, traceID, namespaceID, obs)
	if len(spans) == 0 {
		return
	}

	if err := h.telemetry.RecordSpans(ctx, spans); err != nil {
		h.logger.Warn("failed to record telemetry spans", zap.Error(err))
	}
}

func obsToSpans(tupleID string, traceID string, namespaceID string, obs *telemetry.Observation) []telemetry.Span {
	if obs == nil {
		return nil
	}

	baseTraceID := traceID
	var baseParent string
	baseAttrs := map[string]interface{}{}

	if obs.Trace != nil {
		if obs.Trace.TraceID != "" {
			baseTraceID = obs.Trace.TraceID
		}
		if obs.Trace.CallID != "" {
			baseAttrs["call_id"] = obs.Trace.CallID
		}
		if len(obs.Trace.Links) > 0 {
			baseAttrs["links"] = obs.Trace.Links
		}
	}

	if baseTraceID == "" {
		return nil
	}

	now := time.Now().UTC()
	spans := make([]telemetry.Span, 0)

	mergeAttrs := func(attrs map[string]interface{}) map[string]interface{} {
		if len(baseAttrs) == 0 && len(attrs) == 0 {
			return nil
		}
		merged := map[string]interface{}{}
		for k, v := range baseAttrs {
			merged[k] = v
		}
		for k, v := range attrs {
			merged[k] = v
		}
		return merged
	}

	// Anchor span for the agent operation (if span_id is provided).
	if obs.Trace != nil && obs.Trace.SpanID != "" {
		parent := obs.Trace.ParentSpanID
		if parent == "" && tupleID != "" {
			parent = tupleID
		}
		spans = append(spans, telemetry.Span{
			TraceID:      baseTraceID,
			SpanID:       obs.Trace.SpanID,
			ParentSpanID: parent,
			TupleID:      tupleID,
			NamespaceID:  namespaceID,
			Name:         "agent.op",
			Kind:         "agent_op",
			TsStart:      now,
			Attrs:        mergeAttrs(nil),
		})
		baseParent = obs.Trace.SpanID
	}

	if baseParent == "" && obs.Trace != nil {
		baseParent = obs.Trace.ParentSpanID
	}
	if baseParent == "" && tupleID != "" {
		baseParent = tupleID
	}

	// Treatments as an annotation span.
	if len(obs.Treatments) > 0 {
		spans = append(spans, telemetry.Span{
			TraceID:      baseTraceID,
			ParentSpanID: baseParent,
			TupleID:      tupleID,
			NamespaceID:  namespaceID,
			Name:         "trace.treatments",
			Kind:         "treatment",
			TsStart:      now,
			Attrs:        mergeAttrs(map[string]interface{}{"treatments": obs.Treatments}),
		})
	}

	// Metrics.
	for _, metric := range obs.Metrics {
		spans = append(spans, telemetry.Span{
			TraceID:      baseTraceID,
			ParentSpanID: baseParent,
			TupleID:      tupleID,
			NamespaceID:  namespaceID,
			Name:         metric.Key,
			Kind:         "metric",
			TsStart:      now,
			Metric:       &metric,
			Attrs:        mergeAttrs(nil),
		})
	}

	// Guardrails.
	for _, guard := range obs.Guardrails {
		attrs := map[string]interface{}{
			"rule_id":  guard.RuleID,
			"hit":      guard.Hit,
			"severity": guard.Severity,
		}
		if guard.EvidenceRef != "" {
			attrs["evidence_ref"] = guard.EvidenceRef
		}
		spans = append(spans, telemetry.Span{
			TraceID:      baseTraceID,
			ParentSpanID: baseParent,
			TupleID:      tupleID,
			NamespaceID:  namespaceID,
			Name:         guard.RuleID,
			Kind:         "guardrail",
			TsStart:      now,
			Attrs:        mergeAttrs(attrs),
		})
	}

	// Events.
	for _, event := range obs.Events {
		tsStart := event.TsStart
		if tsStart.IsZero() {
			tsStart = now
		}
		parent := event.ParentSpanID
		if parent == "" {
			parent = baseParent
		}
		spans = append(spans, telemetry.Span{
			TraceID:      baseTraceID,
			SpanID:       event.SpanID,
			ParentSpanID: parent,
			TupleID:      tupleID,
			NamespaceID:  namespaceID,
			Name:         event.Name,
			Kind:         defaultTelemetryKind(event.Kind, "event"),
			TsStart:      tsStart,
			TsEnd:        event.TsEnd,
			Status:       event.Status,
			Attrs:        mergeAttrs(event.Attrs),
		})
	}

	return spans
}

func defaultTelemetryKind(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

package telemetry

import "time"

// Observation captures inline telemetry annotations attached to agent mutations.
type Observation struct {
	Trace      *Trace                 `json:"trace,omitempty"`
	Treatments map[string]interface{} `json:"treatments,omitempty"`
	Metrics    []Metric               `json:"metrics,omitempty"`
	Guardrails []Guardrail            `json:"guardrails,omitempty"`
	Events     []Event                `json:"events,omitempty"`
}

// Trace provides trace/span linkage for observations.
type Trace struct {
	TraceID      string `json:"trace_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	CallID       string `json:"call_id,omitempty"`
	Links        []Link `json:"links,omitempty"`
}

// Link represents a relationship to another span.
type Link struct {
	SpanID string `json:"span_id,omitempty"`
	Type   string `json:"type,omitempty"`
}

// Metric is a typed metric payload.
type Metric struct {
	Key       string   `json:"key"`
	ValueBool *bool    `json:"value_bool,omitempty"`
	ValueNum  *float64 `json:"value_num,omitempty"`
	ValueStr  *string  `json:"value_str,omitempty"`
}

// Guardrail captures policy/guardrail outcomes.
type Guardrail struct {
	RuleID      string `json:"rule_id"`
	Hit         bool   `json:"hit"`
	Severity    string `json:"severity,omitempty"`
	EvidenceRef string `json:"evidence_ref,omitempty"`
}

// Event is a lightweight span-style record.
type Event struct {
	Name         string                 `json:"name"`
	Kind         string                 `json:"kind,omitempty"`
	TsStart      time.Time              `json:"ts_start"`
	TsEnd        *time.Time             `json:"ts_end,omitempty"`
	Status       string                 `json:"status,omitempty"`
	Attrs        map[string]interface{} `json:"attrs,omitempty"`
	SpanID       string                 `json:"span_id,omitempty"`
	ParentSpanID string                 `json:"parent_span_id,omitempty"`
}

// Span represents a telemetry span stored in analytics tables.
type Span struct {
	TraceID      string                 `json:"trace_id"`
	SpanID       string                 `json:"span_id,omitempty"`
	ParentSpanID string                 `json:"parent_span_id,omitempty"`
	TupleID      string                 `json:"tuple_id,omitempty"`
	NamespaceID  string                 `json:"namespace_id,omitempty"`
	Name         string                 `json:"name"`
	Kind         string                 `json:"kind,omitempty"`
	TsStart      time.Time              `json:"ts_start"`
	TsEnd        *time.Time             `json:"ts_end,omitempty"`
	Status       string                 `json:"status,omitempty"`
	Attrs        map[string]interface{} `json:"attrs,omitempty"`
	Metric       *Metric                `json:"metric,omitempty"`
	CallID       string                 `json:"call_id,omitempty"`
	Links        []Link                 `json:"links,omitempty"`
}

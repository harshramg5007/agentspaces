package api

import (
	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

// Request types

// CreateAgentRequest represents a request to create a new agent
type CreateAgentRequest struct {
	Kind        string                 `json:"kind"`
	Payload     map[string]interface{} `json:"payload"`
	Tags        []string               `json:"tags,omitempty"`
	ParentID    string                 `json:"parent_id,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
	NamespaceID string                 `json:"namespace_id,omitempty"`
	TTL         int                    `json:"ttl,omitempty"`
	Metadata    map[string]string      `json:"metadata,omitempty"`
	Obs         *telemetry.Observation `json:"obs,omitempty"`
}

// UpdateAgentRequest represents a request to update a agent
type UpdateAgentRequest struct {
	Payload  map[string]interface{} `json:"payload,omitempty"`
	Status   *string                `json:"status,omitempty"`
	Tags     []string               `json:"tags,omitempty"`
	Owner    *string                `json:"owner,omitempty"`
	Metadata map[string]string      `json:"metadata,omitempty"`
	Obs      *telemetry.Observation `json:"obs,omitempty"`
}

// TakeRequest represents a request to take a agent
type TakeRequest struct {
	Query agent.Query            `json:"query"`
	Obs   *telemetry.Observation `json:"obs,omitempty"`
}

// BatchTakeRequest represents a request to claim multiple agents at once.
type BatchTakeRequest struct {
	Query agent.Query `json:"query"`
	Limit int         `json:"limit"`
}

// InRequest represents a blocking take request
type InRequest struct {
	Query   agent.Query            `json:"query"`
	Timeout int                    `json:"timeout"` // seconds
	Obs     *telemetry.Observation `json:"obs,omitempty"`
}

// ReadRequest represents a read request (blocking or non-blocking)
type ReadRequest struct {
	Query    agent.Query `json:"query"`
	Timeout  int         `json:"timeout,omitempty"`  // seconds (blocking only)
	Blocking bool        `json:"blocking,omitempty"` // true for blocking read
}

// CompleteAgentRequest represents a request to complete a agent
type CompleteAgentRequest struct {
	Result  map[string]interface{} `json:"result"`
	ShardID string                 `json:"shard_id,omitempty"`
	Obs     *telemetry.Observation `json:"obs,omitempty"`
}

// CompleteAndOutRequest represents a request to complete a agent and create output agents atomically
type CompleteAndOutRequest struct {
	Result  map[string]interface{} `json:"result"`
	ShardID string                 `json:"shard_id,omitempty"`
	Outputs []CreateAgentRequest   `json:"outputs"`
	Obs     *telemetry.Observation `json:"obs,omitempty"`
}

// CompleteAndOutResponse represents the response from completing a agent and creating outputs
type CompleteAndOutResponse struct {
	Completed *agent.Agent   `json:"completed"`
	Created   []*agent.Agent `json:"created"`
}

// ReleaseRequest represents a request to release a agent
type ReleaseRequest struct {
	Reason  string                 `json:"reason,omitempty"`   // Optional reason (e.g., "lease_expired" for reaper)
	ShardID string                 `json:"shard_id,omitempty"` // Optional shard routing hint
	Obs     *telemetry.Observation `json:"obs,omitempty"`
}

// RenewLeaseRequest represents a request to renew a lease
type RenewLeaseRequest struct {
	LeaseSec int    `json:"lease_sec,omitempty"` // Optional lease duration override in seconds
	ShardID  string `json:"shard_id,omitempty"`  // Optional shard routing hint
}

// Response types

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string                 `json:"error"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

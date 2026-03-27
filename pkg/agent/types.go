package agent

import (
	"encoding/json"
	"time"
)

// Status represents the state of a agent
type Status string

const (
	StatusNew        Status = "NEW"
	StatusInProgress Status = "IN_PROGRESS"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
	StatusCancelled  Status = "CANCELLED"
)

// EventType represents different types of events in the system
type EventType string

const (
	EventCreated   EventType = "CREATED"
	EventClaimed   EventType = "CLAIMED"
	EventUpdated   EventType = "UPDATED"
	EventCompleted EventType = "COMPLETED"
	EventFailed    EventType = "FAILED"
	EventReleased  EventType = "RELEASED"
)

// Agent represents a unit of work or data in the agent space
type Agent struct {
	ID          string                 `json:"id"`
	Kind        string                 `json:"kind"` // Type/category of the agent
	Status      Status                 `json:"status"`
	ShardID     string                 `json:"shard_id,omitempty"`
	Payload     map[string]interface{} `json:"payload"`      // Actual data
	Tags        []string               `json:"tags"`         // For filtering
	Owner       string                 `json:"owner"`        // Agent that owns this agent
	ParentID    string                 `json:"parent_id"`    // For DAG relationships
	TraceID     string                 `json:"trace_id"`     // For distributed tracing
	NamespaceID string                 `json:"namespace_id"` // Isolation namespace enforced by server auth principal
	TTL         time.Duration          `json:"ttl"`          // Time to live
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	OwnerTime   time.Time              `json:"owner_time,omitempty"`  // When ownership was taken
	LeaseToken  string                 `json:"lease_token,omitempty"` // Fencing token for lease ownership
	LeaseUntil  time.Time              `json:"lease_until,omitempty"` // Lease expiry timestamp
	Version     int64                  `json:"version"`               // Monotonic version for idempotency
	Metadata    map[string]string      `json:"metadata"`              // Additional metadata
}

// Event represents a change to a agent
type Event struct {
	ID        string                 `json:"id"`
	TupleID   string                 `json:"tuple_id"`
	Type      EventType              `json:"type"`
	Kind      string                 `json:"kind"`
	Timestamp time.Time              `json:"timestamp"`
	AgentID   string                 `json:"agent_id"`
	Data      map[string]interface{} `json:"data"`
	TraceID   string                 `json:"trace_id"`
	Version   int64                  `json:"version"`
}

// Query represents a search query for agents
type Query struct {
	Kind        string            `json:"kind,omitempty"`
	Status      Status            `json:"status,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Owner       string            `json:"owner,omitempty"`
	ParentID    string            `json:"parent_id,omitempty"`
	TraceID     string            `json:"trace_id,omitempty"`
	NamespaceID string            `json:"namespace_id,omitempty"` // Server-enforced namespace scoping
	AgentID     string            `json:"agent_id,omitempty"`     // For ownership on take/in
	Metadata    map[string]string `json:"metadata,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	Offset      int               `json:"offset,omitempty"`
}

// AgentSpace defines the interface for agent space operations
type AgentSpace interface {
	// Get retrieves a agent by ID (O(1) direct lookup)
	Get(id string) (*Agent, error)

	// Out writes a agent to the space (non-blocking)
	Out(agent *Agent) error

	// In takes a agent from the space (blocking, destructive read)
	In(query *Query, timeout time.Duration) (*Agent, error)

	// Read reads a agent from the space (blocking, non-destructive)
	Read(query *Query, timeout time.Duration) (*Agent, error)

	// Query returns matching agents (non-blocking, non-destructive)
	Query(query *Query) ([]*Agent, error)

	// Take takes a agent from the space (non-blocking, destructive)
	Take(query *Query) (*Agent, error)

	// Update updates a agent in place
	Update(id string, updates map[string]interface{}) error

	// Complete marks a agent as completed
	Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error

	// CompleteAndOut atomically completes a agent and creates new output agents.
	// This prevents partial workflow state where a agent is completed but
	// downstream agents aren't created.
	// Returns the completed agent and the created output agents.
	CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*Agent) (*Agent, []*Agent, error)

	// Release releases ownership of a agent.
	// The agentID must match the current owner of the agent.
	// The reason parameter is optional and will be included in the RELEASED event data
	// (e.g., "lease_expired" for reaper releases, "" for normal releases).
	Release(id string, agentID string, leaseToken string, reason string) error

	// RenewLease extends the lease for an in-progress agent.
	// If leaseDuration is zero, the store's default lease duration for the agent is used.
	RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*Agent, error)

	// Subscribe subscribes to events
	Subscribe(filter *EventFilter, handler EventHandler) error

	// GetEvents returns events for a agent
	GetEvents(tupleID string) ([]*Event, error)

	// GetGlobalEvents returns recent events across all agents
	GetGlobalEvents(limit, offset int) ([]*Event, error)

	// GetDAG returns the DAG of related agents
	GetDAG(rootID string) (*DAG, error)
}

// UpdateAndGetProvider optionally supports atomic update-and-return semantics.
// Stores that implement this can avoid a follow-up Get() after Update().
type UpdateAndGetProvider interface {
	UpdateAndGet(id string, updates map[string]interface{}) (*Agent, error)
}

// CompleteAndGetProvider optionally supports atomic complete-and-return semantics.
// Stores that implement this can avoid a follow-up Get() after Complete().
type CompleteAndGetProvider interface {
	CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*Agent, error)
}

// ReleaseAndGetProvider optionally supports atomic release-and-return semantics.
// Stores that implement this can avoid a follow-up Get() after Release().
type ReleaseAndGetProvider interface {
	ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*Agent, error)
}

// BatchTakeProvider optionally supports claiming multiple agents in a single transaction.
// This reduces per-item transaction overhead under high concurrency.
type BatchTakeProvider interface {
	BatchTake(query *Query, limit int) ([]*Agent, error)
}

// Counter provides fast count support for agent queries.
// Implementations may optimize counts without paginating through agents.
type Counter interface {
	Count(query *Query) (int, error)
}

// StatusCounter provides grouped status counts for agent queries.
// Implementations may optimize this in a single backend call.
type StatusCounter interface {
	CountByStatus(query *Query) (map[Status]int, error)
}

// EventQuery represents a filtered event query with pagination.
type EventQuery struct {
	TupleID string
	AgentID string
	Type    EventType
	TraceID string
	Limit   int
	Offset  int
	Cursor  string
}

// EventPage is a paginated event response.
type EventPage struct {
	Events     []*Event
	Total      int
	NextCursor string
}

// EventQuerier provides filtered event queries with pagination.
type EventQuerier interface {
	QueryEvents(query *EventQuery) (*EventPage, error)
}

// EventFilter defines criteria for event subscriptions
type EventFilter struct {
	Types    []EventType
	TupleIDs []string
	Kinds    []string
	AgentIDs []string
	TraceIDs []string
}

// EventHandler is a callback for handling events
type EventHandler func(event *Event)

// DAG represents a directed acyclic graph of agents
type DAG struct {
	Nodes map[string]*DAGNode `json:"nodes"`
	Edges []Edge              `json:"edges"`
}

// DAGNode represents a node in the DAG
type DAGNode struct {
	TupleID   string            `json:"tuple_id"`
	Kind      string            `json:"kind"`
	Status    Status            `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
	Metadata  map[string]string `json:"metadata"`
}

// Edge represents a directed edge in the DAG
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"` // "parent", "spawned", "depends_on", etc.
}

// LeaseOptions represents options for agent leasing
type LeaseOptions struct {
	LeaseDuration time.Duration `json:"lease_duration"`
	AutoRenew     bool          `json:"auto_renew"`
}

// MarshalBinary implements encoding.BinaryMarshaler
func (t *Agent) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (t *Agent) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

// MarshalBinary implements encoding.BinaryMarshaler for Event
func (e *Event) MarshalBinary() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler for Event
func (e *Event) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, e)
}

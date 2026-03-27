package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// NewAgent creates a new agent with default values
func NewAgent(kind string, payload map[string]any) *Agent {
	now := time.Now()
	return &Agent{
		ID:        uuid.New().String(),
		Kind:      kind,
		TraceID:   uuid.New().String(), // New trace by default
		Status:    StatusNew,
		Payload:   payload,
		Tags:      []string{},
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
		Metadata:  make(map[string]string),
	}
}

// WithTraceID sets the trace ID for a agent
func (t *Agent) WithTraceID(traceID string) *Agent {
	t.TraceID = traceID
	return t
}

// WithParentID sets the parent ID for a agent
func (t *Agent) WithParentID(parentID string) *Agent {
	t.ParentID = parentID
	return t
}

// WithTags adds tags to a agent
func (t *Agent) WithTags(tags ...string) *Agent {
	t.Tags = append(t.Tags, tags...)
	return t
}

// WithTTL sets the TTL for a agent
func (t *Agent) WithTTL(ttl time.Duration) *Agent {
	t.TTL = ttl
	return t
}

// WithMetadata adds metadata to a agent
func (t *Agent) WithMetadata(key, value string) *Agent {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	t.Metadata[key] = value
	return t
}

// HasTag checks if a agent has a specific tag
func (t *Agent) HasTag(tag string) bool {
	for _, t := range t.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// HasAnyTag checks if a agent has any of the specified tags
func (t *Agent) HasAnyTag(tags []string) bool {
	for _, tag := range tags {
		if t.HasTag(tag) {
			return true
		}
	}
	return false
}

// HasAllTags checks if a agent has all of the specified tags
func (t *Agent) HasAllTags(tags []string) bool {
	for _, tag := range tags {
		if !t.HasTag(tag) {
			return false
		}
	}
	return true
}

// IsExpired checks if a agent has expired
func (t *Agent) IsExpired() bool {
	if t.TTL == 0 {
		return false
	}
	return time.Since(t.CreatedAt) > t.TTL
}

// IsLeaseExpired checks if a agent's lease has expired
func (t *Agent) IsLeaseExpired(leaseDuration time.Duration) bool {
	if t.Owner == "" || t.Status != StatusInProgress {
		return false
	}
	if !t.LeaseUntil.IsZero() {
		return time.Now().After(t.LeaseUntil)
	}
	return time.Since(t.OwnerTime) > leaseDuration
}

// MatchesQuery checks if a agent matches a query
func (t *Agent) MatchesQuery(q *Query) bool {
	if q == nil {
		return true
	}

	// Check kind (exact match only)
	if q.Kind != "" && t.Kind != q.Kind {
		return false
	}

	// Check status
	if q.Status != "" && t.Status != q.Status {
		return false
	}

	// Check owner
	if q.Owner != "" && t.Owner != q.Owner {
		return false
	}

	// Check trace ID
	if q.TraceID != "" && t.TraceID != q.TraceID {
		return false
	}

	// Check parent ID
	if q.ParentID != "" && t.ParentID != q.ParentID {
		return false
	}

	// Check namespace ID
	if q.NamespaceID != "" && t.NamespaceID != q.NamespaceID {
		return false
	}

	// Check tags
	if len(q.Tags) > 0 && !t.HasAllTags(q.Tags) {
		return false
	}

	return true
}

// ParseTag parses a tag in the format "key:value" or "key"
func ParseTag(tag string) (key, value string) {
	parts := strings.SplitN(tag, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return tag, ""
}

// MakeTag creates a tag from a key and value
func MakeTag(key, value string) string {
	if value == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", key, value)
}

// DefaultLeaseOptions returns default lease options
func DefaultLeaseOptions() *LeaseOptions {
	return &LeaseOptions{
		LeaseDuration: 5 * time.Minute,
		AutoRenew:     false,
	}
}

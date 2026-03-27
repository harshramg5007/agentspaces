package agent

import (
	"testing"
	"time"
)

func TestNewAgent(t *testing.T) {
	payload := map[string]any{
		"key": "value",
		"num": 42,
	}

	agent := NewAgent("test.kind", payload)

	if agent.ID == "" {
		t.Error("Expected agent to have an ID")
	}

	if agent.Kind != "test.kind" {
		t.Errorf("Expected kind to be 'test.kind', got %s", agent.Kind)
	}

	if agent.Status != StatusNew {
		t.Errorf("Expected status to be NEW, got %s", agent.Status)
	}

	if agent.TraceID == "" {
		t.Error("Expected agent to have a TraceID")
	}

	if len(agent.Payload) != 2 {
		t.Errorf("Expected payload to have 2 items, got %d", len(agent.Payload))
	}
}

func TestAgentBuilders(t *testing.T) {
	agent := NewAgent("test", nil).
		WithTraceID("trace123").
		WithParentID("parent456").
		WithTags("tag1", "tag2").
		WithTTL(5*time.Minute).
		WithMetadata("key", "value")

	if agent.TraceID != "trace123" {
		t.Errorf("Expected TraceID to be 'trace123', got %s", agent.TraceID)
	}

	if agent.ParentID != "parent456" {
		t.Errorf("Expected ParentID to be 'parent456', got %s", agent.ParentID)
	}

	if len(agent.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(agent.Tags))
	}

	if agent.TTL != 5*time.Minute {
		t.Errorf("Expected TTL to be 5 minutes, got %v", agent.TTL)
	}

	if agent.Metadata["key"] != "value" {
		t.Error("Expected metadata to contain key=value")
	}
}

func TestHasTag(t *testing.T) {
	agent := NewAgent("test", nil).WithTags("tag1", "tag2", "tag3")

	tests := []struct {
		tag      string
		expected bool
	}{
		{"tag1", true},
		{"tag2", true},
		{"tag3", true},
		{"tag4", false},
		{"", false},
	}

	for _, test := range tests {
		if result := agent.HasTag(test.tag); result != test.expected {
			t.Errorf("HasTag(%s) = %v, expected %v", test.tag, result, test.expected)
		}
	}
}

func TestHasAllTags(t *testing.T) {
	agent := NewAgent("test", nil).WithTags("tag1", "tag2", "tag3")

	tests := []struct {
		tags     []string
		expected bool
	}{
		{[]string{"tag1"}, true},
		{[]string{"tag1", "tag2"}, true},
		{[]string{"tag1", "tag2", "tag3"}, true},
		{[]string{"tag1", "tag4"}, false},
		{[]string{"tag4", "tag5"}, false},
		{[]string{}, true}, // Empty list should return true
	}

	for _, test := range tests {
		if result := agent.HasAllTags(test.tags); result != test.expected {
			t.Errorf("HasAllTags(%v) = %v, expected %v", test.tags, result, test.expected)
		}
	}
}

func TestMatchesQuery(t *testing.T) {
	agent := NewAgent("task.summarize", map[string]any{"doc": "test"}).
		WithTags("lang:en", "priority:high").
		WithTraceID("trace123").
		WithParentID("parent456")
	agent.Status = StatusInProgress
	agent.Owner = "agent1"
	agent.NamespaceID = "ns-a"

	tests := []struct {
		name     string
		query    *Query
		expected bool
	}{
		{
			name:     "match by kind",
			query:    &Query{Kind: "task.summarize"},
			expected: true,
		},
		{
			name:     "no match by kind",
			query:    &Query{Kind: "task.analyze"},
			expected: false,
		},
		{
			name:     "no match by kind wildcard literal",
			query:    &Query{Kind: "task.*"},
			expected: false,
		},
		{
			name:     "match by status",
			query:    &Query{Status: StatusInProgress},
			expected: true,
		},
		{
			name:     "match by owner",
			query:    &Query{Owner: "agent1"},
			expected: true,
		},
		{
			name:     "match by trace ID",
			query:    &Query{TraceID: "trace123"},
			expected: true,
		},
			{
				name:     "match by parent ID",
				query:    &Query{ParentID: "parent456"},
				expected: true,
			},
			{
				name:     "match by namespace ID",
				query:    &Query{NamespaceID: "ns-a"},
				expected: true,
			},
			{
				name:     "no match by namespace ID",
				query:    &Query{NamespaceID: "ns-b"},
				expected: false,
			},
			{
				name:     "match by tags",
				query:    &Query{Tags: []string{"lang:en"}},
				expected: true,
			},
		{
			name:     "no match - wrong tag",
			query:    &Query{Tags: []string{"lang:fr"}},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if result := agent.MatchesQuery(test.query); result != test.expected {
				t.Errorf("MatchesQuery() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestIsExpired(t *testing.T) {
	// Test agent without TTL
	agent1 := NewAgent("test", nil)
	if agent1.IsExpired() {
		t.Error("Agent without TTL should not be expired")
	}

	// Test agent with future TTL
	agent2 := NewAgent("test", nil).WithTTL(1 * time.Hour)
	if agent2.IsExpired() {
		t.Error("Agent with future TTL should not be expired")
	}

	// Test agent with past TTL
	agent3 := NewAgent("test", nil).WithTTL(1 * time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	if !agent3.IsExpired() {
		t.Error("Agent with past TTL should be expired")
	}
}

func TestParseTag(t *testing.T) {
	tests := []struct {
		tag           string
		expectedKey   string
		expectedValue string
	}{
		{"key:value", "key", "value"},
		{"key", "key", ""},
		{"key:value:extra", "key", "value:extra"},
		{"", "", ""},
		{":value", "", "value"},
	}

	for _, test := range tests {
		key, value := ParseTag(test.tag)
		if key != test.expectedKey || value != test.expectedValue {
			t.Errorf("ParseTag(%s) = (%s, %s), expected (%s, %s)",
				test.tag, key, value, test.expectedKey, test.expectedValue)
		}
	}
}

func TestMakeTag(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		expected string
	}{
		{"key", "value", "key:value"},
		{"key", "", "key"},
		{"", "value", ":value"},
		{"", "", ""},
	}

	for _, test := range tests {
		result := MakeTag(test.key, test.value)
		if result != test.expected {
			t.Errorf("MakeTag(%s, %s) = %s, expected %s",
				test.key, test.value, result, test.expected)
		}
	}
}

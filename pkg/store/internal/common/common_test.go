package common

import (
	"testing"
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func TestApplyUpdatesMutatesAgentFields(t *testing.T) {
	tpl := &agent.Agent{
		ID:       "id-1",
		Kind:     "task.kind",
		Status:   agent.StatusNew,
		Owner:    "",
		Payload:  map[string]interface{}{"a": "b"},
		Tags:     []string{"x"},
		Metadata: map[string]string{"m": "1"},
	}

	ApplyUpdates(tpl, map[string]interface{}{
		"status":   "IN_PROGRESS",
		"owner":    "agent-a",
		"payload":  map[string]interface{}{"done": true},
		"tags":     []string{"t1", "t2"},
		"metadata": map[string]interface{}{"queue": "q1"},
	})

	if tpl.Status != agent.StatusInProgress {
		t.Fatalf("expected status %s, got %s", agent.StatusInProgress, tpl.Status)
	}
	if tpl.Owner != "agent-a" {
		t.Fatalf("expected owner agent-a, got %s", tpl.Owner)
	}
	if v, ok := tpl.Payload["done"].(bool); !ok || !v {
		t.Fatalf("expected payload.done=true, got %#v", tpl.Payload)
	}
	if len(tpl.Tags) != 2 || tpl.Tags[0] != "t1" {
		t.Fatalf("unexpected tags: %#v", tpl.Tags)
	}
	if tpl.Metadata["queue"] != "q1" {
		t.Fatalf("expected metadata queue=q1, got %#v", tpl.Metadata)
	}
}

func TestCompletionMetadataAndTokenChecks(t *testing.T) {
	now := time.Now().UTC()
	tpl := &agent.Agent{Metadata: map[string]string{}}

	SetCompletionMetadata(tpl, "agent-a", "token-1", now)

	if !CompletionTokenRecorded(tpl) {
		t.Fatal("expected completion token to be recorded")
	}
	if !CompletionTokenMatches(tpl, "token-1") {
		t.Fatal("expected completion token match")
	}
	if CompletionTokenMatches(tpl, "token-2") {
		t.Fatal("expected completion token mismatch")
	}
}

func TestSubscriptionRegistryFilters(t *testing.T) {
	reg := NewSubscriptionRegistry()
	seen := make(chan *agent.Event, 1)

	reg.Subscribe(&agent.EventFilter{Kinds: []string{"demo.kind"}}, func(e *agent.Event) {
		seen <- e
	})

	reg.Notify(&agent.Event{Kind: "other.kind"})
	select {
	case <-seen:
		t.Fatal("unexpected event match")
	case <-time.After(20 * time.Millisecond):
	}

	e := &agent.Event{Kind: "demo.kind"}
	reg.Notify(e)
	select {
	case got := <-seen:
		if got.Kind != "demo.kind" {
			t.Fatalf("unexpected event: %#v", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected matching event")
	}
}

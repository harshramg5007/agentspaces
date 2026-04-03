package agent

import (
	"testing"
	"time"
)

func TestAgentWithTags(t *testing.T) {
	agent := NewAgent("test", nil).WithTags("tag1", "tag2", "tag3")

	if len(agent.Tags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(agent.Tags))
	}

	if agent.Tags[0] != "tag1" || agent.Tags[1] != "tag2" || agent.Tags[2] != "tag3" {
		t.Error("Tags not set correctly")
	}
}

func TestAgentWithParent(t *testing.T) {
	parentID := "parent-123"
	agent := NewAgent("child", nil).WithParentID(parentID)

	if agent.ParentID != parentID {
		t.Errorf("Expected parent ID to be %s, got %s", parentID, agent.ParentID)
	}
}

func TestAgentWithTTL(t *testing.T) {
	ttl := 5 * time.Minute
	agent := NewAgent("expiring", nil).WithTTL(ttl)

	if agent.TTL != ttl {
		t.Errorf("Expected TTL to be %v, got %v", ttl, agent.TTL)
	}

	if agent.IsExpired() {
		t.Error("New agent should not be expired")
	}
}

func TestAgentMarshalBinary(t *testing.T) {
	agent := NewAgent("test", map[string]interface{}{"key": "value"})

	data, err := agent.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal agent: %v", err)
	}

	var unmarshaled Agent
	if err := unmarshaled.UnmarshalBinary(data); err != nil {
		t.Fatalf("Failed to unmarshal agent: %v", err)
	}

	if unmarshaled.ID != agent.ID {
		t.Error("ID mismatch after marshal/unmarshal")
	}

	if unmarshaled.Kind != agent.Kind {
		t.Error("Kind mismatch after marshal/unmarshal")
	}
}

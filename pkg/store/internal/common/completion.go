package common

import (
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func SetCompletionMetadata(t *agent.Agent, agentID, leaseToken string, completedAt time.Time) {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if agentID != "" {
		t.Metadata[CompletedByMetadataKey] = agentID
	}
	if leaseToken != "" {
		t.Metadata[CompletedByTokenMetadataKey] = leaseToken
	}
	if !completedAt.IsZero() {
		t.Metadata[CompletedAtMetadataKey] = completedAt.UTC().Format(time.RFC3339Nano)
	}
}

func CompletionTokenMatches(t *agent.Agent, leaseToken string) bool {
	if t == nil || t.Metadata == nil || leaseToken == "" {
		return false
	}
	token := t.Metadata[CompletedByTokenMetadataKey]
	return token != "" && token == leaseToken
}

func CompletionTokenRecorded(t *agent.Agent) bool {
	if t == nil || t.Metadata == nil {
		return false
	}
	return t.Metadata[CompletedByTokenMetadataKey] != ""
}

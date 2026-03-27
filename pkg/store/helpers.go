package store

import (
	"time"

	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	"github.com/urobora-ai/agentspaces/pkg/agent"
)

const (
	completedByMetadataKey      = common.CompletedByMetadataKey
	completedByTokenMetadataKey = common.CompletedByTokenMetadataKey
	completedAtMetadataKey      = common.CompletedAtMetadataKey
	tokenMismatchStaleErr       = common.TokenMismatchStaleErr
	queueMetadataKey            = common.QueueMetadataKey
	queueCreatedMsKey           = common.QueueCreatedMsKey
	namespaceMetadataKey        = common.NamespaceMetadataKey
)

// getStringFromMap returns a string value for the provided key if present.
func getStringFromMap(m map[string]interface{}, key string) string {
	return common.GetStringFromMap(m, key)
}

func applyUpdates(t *agent.Agent, updates map[string]interface{}) {
	common.ApplyUpdates(t, updates)
}

func setCompletionMetadata(t *agent.Agent, agentID, leaseToken string, completedAt time.Time) {
	common.SetCompletionMetadata(t, agentID, leaseToken, completedAt)
}

func completionTokenMatches(t *agent.Agent, leaseToken string) bool {
	return common.CompletionTokenMatches(t, leaseToken)
}

func completionTokenRecorded(t *agent.Agent) bool {
	return common.CompletionTokenRecorded(t)
}

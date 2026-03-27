package sqlite

import (
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
)

const (
	queueMetadataKey      = common.QueueMetadataKey
	namespaceMetadataKey  = common.NamespaceMetadataKey
	tokenMismatchStaleErr = common.TokenMismatchStaleErr
)

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

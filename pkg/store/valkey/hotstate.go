package valkey

import (
	"strings"

	json "github.com/goccy/go-json"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type hotAgentState struct {
	ID           string       `json:"id"`
	Kind         string       `json:"kind"`
	Status       agent.Status `json:"status"`
	NamespaceID  string       `json:"namespace_id"`
	Queue        string       `json:"queue"`
	Owner        string       `json:"owner"`
	LeaseToken   string       `json:"lease_token"`
	LeaseMS      int64        `json:"lease_duration_ms"`
	LeaseUntilMS int64        `json:"lease_until_ms"`
	Version      int64        `json:"version"`
	CreatedMS    int64        `json:"created_ms"`
	QueueCreated int64        `json:"queue_created_ms"`
}

func decodeHotAgentState(raw string) (*hotAgentState, error) {
	var state hotAgentState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func queryRequiresFullAgent(query *agent.Query) bool {
	if query == nil {
		return false
	}
	if query.ParentID != "" || query.TraceID != "" || len(query.Tags) > 0 {
		return true
	}
	if query.Metadata != nil {
		for key := range query.Metadata {
			switch key {
			case "", "agent_id", queueMetadataKey, namespaceMetadataKey:
			default:
				return true
			}
		}
	}
	return false
}

func hotStateMatchesQuery(state *hotAgentState, query *agent.Query) bool {
	if state == nil || query == nil {
		return true
	}
	if query.Kind != "" && state.Kind != query.Kind {
		return false
	}
	if query.Status != "" && state.Status != query.Status {
		return false
	}
	if query.Owner != "" && state.Owner != query.Owner {
		return false
	}
	if query.NamespaceID != "" && state.NamespaceID != query.NamespaceID {
		return false
	}
	if query.Metadata != nil {
		if queue := strings.TrimSpace(query.Metadata[queueMetadataKey]); queue != "" && state.Queue != queue {
			return false
		}
		if namespaceID := strings.TrimSpace(query.Metadata[namespaceMetadataKey]); namespaceID != "" && state.NamespaceID != namespaceID {
			return false
		}
	}
	return true
}

func canUseQueueTakeScript(query *agent.Query) bool {
	if query == nil || query.Status != "" && query.Status != agent.StatusNew {
		return false
	}
	if strings.TrimSpace(query.NamespaceID) == "" {
		return false
	}
	if query == nil || query.Metadata == nil || strings.TrimSpace(query.Metadata[queueMetadataKey]) == "" {
		return false
	}
	return !queryRequiresFullAgent(query)
}

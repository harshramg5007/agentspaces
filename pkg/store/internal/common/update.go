package common

import (
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

// GetStringFromMap returns a string value for the provided key if present.
func GetStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// ApplyUpdates mutates agent fields from a generic update map.
func ApplyUpdates(t *agent.Agent, updates map[string]interface{}) {
	if updates == nil {
		return
	}
	if payload, ok := updates["payload"].(map[string]interface{}); ok {
		if t.Payload == nil {
			t.Payload = make(map[string]interface{})
		}
		for k, v := range payload {
			t.Payload[k] = v
		}
	}
	if status, ok := updates["status"].(string); ok {
		t.Status = agent.Status(status)
	}
	if owner, ok := updates["owner"].(string); ok {
		t.Owner = owner
	}
	if tags, ok := updates["tags"].([]string); ok {
		t.Tags = tags
	}
	if tags, ok := updates["tags"].([]interface{}); ok {
		converted := make([]string, 0, len(tags))
		for _, tag := range tags {
			if tagStr, ok := tag.(string); ok {
				converted = append(converted, tagStr)
			}
		}
		if converted != nil {
			t.Tags = converted
		}
	}
	if metadata, ok := updates["metadata"].(map[string]string); ok {
		if t.Metadata == nil {
			t.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			t.Metadata[k] = v
		}
	}
	if metadata, ok := updates["metadata"].(map[string]interface{}); ok {
		if t.Metadata == nil {
			t.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			if strVal, ok := v.(string); ok {
				t.Metadata[k] = strVal
			}
		}
	}
	if ownerTime, ok := updates["owner_time"].(time.Time); ok {
		t.OwnerTime = ownerTime
	}
	if ownerTime, ok := updates["owner_time"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339, ownerTime); err == nil {
			t.OwnerTime = parsed
		}
	}
	if leaseUntil, ok := updates["lease_until"].(time.Time); ok {
		t.LeaseUntil = leaseUntil
	}
	if leaseUntil, ok := updates["lease_until"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339, leaseUntil); err == nil {
			t.LeaseUntil = parsed
		}
	}
	if leaseToken, ok := updates["lease_token"].(string); ok {
		t.LeaseToken = leaseToken
	}
}

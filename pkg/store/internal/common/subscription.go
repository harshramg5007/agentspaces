package common

import (
	"strings"
	"sync"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

// SubscriptionRegistry is a threadsafe in-process event subscriber registry.
type SubscriptionRegistry struct {
	mu          sync.RWMutex
	subscribers map[string][]agent.EventHandler
}

func NewSubscriptionRegistry() *SubscriptionRegistry {
	return &SubscriptionRegistry{subscribers: make(map[string][]agent.EventHandler)}
}

func (r *SubscriptionRegistry) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) {
	if r == nil || handler == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	subKey := GenerateSubKey(filter)
	r.subscribers[subKey] = append(r.subscribers[subKey], handler)
}

func (r *SubscriptionRegistry) Notify(event *agent.Event) {
	if r == nil || event == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	for subKey, handlers := range r.subscribers {
		if EventMatchesFilter(event, subKey) {
			for _, handler := range handlers {
				go handler(event)
			}
		}
	}
}

func GenerateSubKey(filter *agent.EventFilter) string {
	parts := []string{"sub"}
	if filter == nil {
		return strings.Join(parts, ":")
	}

	if len(filter.Types) > 0 {
		types := make([]string, len(filter.Types))
		for i, t := range filter.Types {
			types[i] = string(t)
		}
		parts = append(parts, "types", strings.Join(types, ","))
	}
	if len(filter.Kinds) > 0 {
		parts = append(parts, "kinds", strings.Join(filter.Kinds, ","))
	}
	if len(filter.TupleIDs) > 0 {
		parts = append(parts, "tuples", strings.Join(filter.TupleIDs, ","))
	}
	if len(filter.AgentIDs) > 0 {
		parts = append(parts, "agents", strings.Join(filter.AgentIDs, ","))
	}
	if len(filter.TraceIDs) > 0 {
		parts = append(parts, "traces", strings.Join(filter.TraceIDs, ","))
	}
	return strings.Join(parts, ":")
}

func EventMatchesFilter(event *agent.Event, subKey string) bool {
	parts := strings.Split(subKey, ":")
	if len(parts) < 2 {
		return true
	}

	for i := 1; i < len(parts)-1; i += 2 {
		filterType := parts[i]
		filterValue := parts[i+1]

		switch filterType {
		case "types":
			found := false
			for _, t := range strings.Split(filterValue, ",") {
				if string(event.Type) == t {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		case "kinds":
			if !containsString(strings.Split(filterValue, ","), event.Kind) {
				return false
			}
		case "tuples":
			if !containsString(strings.Split(filterValue, ","), event.TupleID) {
				return false
			}
		case "agents":
			if !containsString(strings.Split(filterValue, ","), event.AgentID) {
				return false
			}
		case "traces":
			if !containsString(strings.Split(filterValue, ","), event.TraceID) {
				return false
			}
		}
	}
	return true
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

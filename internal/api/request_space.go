package api

import (
	"context"
	"net/http"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type requestContextBinder interface {
	BindContext(ctx context.Context) agent.AgentSpace
}

func (h *Handlers) requestSpace(r *http.Request) agent.AgentSpace {
	if h == nil || h.space == nil {
		return nil
	}
	if r != nil {
		if binder, ok := h.space.(requestContextBinder); ok {
			return binder.BindContext(r.Context())
		}
	}
	return h.space
}

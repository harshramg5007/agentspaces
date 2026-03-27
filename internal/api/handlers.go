package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
	"github.com/urobora-ai/agentspaces/pkg/store"
	postgressharded "github.com/urobora-ai/agentspaces/pkg/store/postgressharded"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

var jsonBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// isTransientError checks if an error is transient (retryable)
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "too many open files") ||
		strings.Contains(errStr, "resource temporarily unavailable") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "no such host") ||
		strings.Contains(errStr, "no hosts available") ||
		strings.Contains(errStr, "no connections") ||
		strings.Contains(errStr, "dial tcp") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "operation timed out") ||
		strings.Contains(errStr, "unavailable") ||
		strings.Contains(errStr, "too many requests") ||
		strings.Contains(errStr, "overloaded") ||
		strings.Contains(errStr, "bootstrapping")
}

// Handlers holds HTTP handlers
type Handlers struct {
	space     agent.AgentSpace
	logger    *zap.Logger
	telemetry telemetryStore
}

type debugInvariantsProvider interface {
	DebugInvariants(ctx context.Context) (map[string]interface{}, error)
}

type healthChecker interface {
	Health(ctx context.Context) error
}

type shardMapInfoProvider interface {
	ShardMapInfo(ctx context.Context) (map[string]interface{}, error)
}

type shardHealthInfoProvider interface {
	ShardHealthInfo(ctx context.Context) ([]postgressharded.ShardHealth, error)
}

type shardQueueStatsProvider interface {
	ShardQueueStats(ctx context.Context, namespaceID string, queue string) (map[string]interface{}, error)
}

const (
	namespaceMetadataKey = "namespace_id"
	queueMetadataKey     = "queue"
)

// NewHandlers creates new HTTP handlers
func NewHandlers(space agent.AgentSpace, logger *zap.Logger) *Handlers {
	var telemetry telemetryStore
	if recorder, ok := space.(telemetryStore); ok {
		telemetry = recorder
	}
	return &Handlers{
		space:     space,
		logger:    logger,
		telemetry: telemetry,
	}
}

// extractIDFromPath extracts the ID from paths like /api/v1/agents/{id}
func extractIDFromPath(path string, prefix string) string {
	// Remove the prefix and extract the ID
	trimmed := strings.TrimPrefix(path, prefix)
	parts := strings.Split(trimmed, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func requestPrefersReturnMinimal(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, preferHeader := range r.Header.Values("Prefer") {
		for _, rawToken := range strings.Split(preferHeader, ",") {
			token := strings.TrimSpace(rawToken)
			if token == "" {
				continue
			}
			parts := strings.SplitN(token, ";", 2)
			directive := strings.ToLower(strings.TrimSpace(parts[0]))
			if !strings.HasPrefix(directive, "return=") {
				continue
			}
			value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(directive, "return=")), "\"")
			if value == "minimal" {
				return true
			}
		}
	}
	return false
}

// CreateAgent handles POST /api/v1/agents
func (h *Handlers) CreateAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var req CreateAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	principal, hasPrincipal := AuthPrincipalFromContext(r.Context())
	namespaceID := strings.TrimSpace(req.NamespaceID)
	if hasPrincipal && principal.NamespaceID != "" {
		if namespaceID != "" && namespaceID != principal.NamespaceID {
			h.writeError(w, http.StatusForbidden, "namespace mismatch for authenticated principal", nil)
			return
		}
		namespaceID = principal.NamespaceID
	}

	if hasPrincipal {
		if ok, reason, err := h.allowNamespaceAgentCreation(principal, namespaceID, req.Metadata, 1); err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to evaluate namespace quotas", err)
			return
		} else if !ok {
			metrics.RecordQuotaRejection(namespaceID, reason)
			h.writeError(w, http.StatusTooManyRequests, "namespace quota exceeded", nil)
			return
		}
	}

	// Create agent
	t := &agent.Agent{
		ID:          uuid.New().String(),
		Kind:        req.Kind,
		Status:      agent.StatusNew,
		Payload:     req.Payload,
		Tags:        req.Tags,
		ParentID:    req.ParentID,
		TraceID:     req.TraceID,
		NamespaceID: namespaceID,
		TTL:         time.Duration(req.TTL) * time.Second,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Metadata:    req.Metadata,
	}

	if t.TraceID == "" {
		t.TraceID = uuid.New().String()
	}

	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	// Store agent ID in metadata for querying
	t.Metadata["id"] = t.ID
	if t.NamespaceID != "" {
		t.Metadata[namespaceMetadataKey] = t.NamespaceID
	}

	// Store agent
	if err := space.Out(t); err != nil {
		if isTransientError(err) {
			h.writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable", err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, "failed to create agent", err)
		return
	}

	h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, req.Obs)

	h.writeJSON(w, http.StatusCreated, t)
}

// GetAgent handles GET /api/v1/agents/{id}
func (h *Handlers) GetAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	id := extractIDFromPath(r.URL.Path, "/api/v1/agents/")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}

	// Direct lookup by ID (O(1) instead of query scan)
	t, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, t) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}

	h.writeJSON(w, http.StatusOK, t)
}

// UpdateAgent handles PUT /api/v1/agents/{id}
func (h *Handlers) UpdateAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	id := extractIDFromPath(r.URL.Path, "/api/v1/agents/")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}

	current, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, current) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}

	var req UpdateAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if req.Status != nil || req.Owner != nil {
		h.writeError(w, http.StatusBadRequest, "status and owner updates must use lease-aware endpoints", nil)
		return
	}

	// Build updates
	updates := make(map[string]interface{})
	if req.Payload != nil {
		updates["payload"] = req.Payload
	}
	if req.Tags != nil {
		updates["tags"] = req.Tags
	}
	if req.Metadata != nil {
		if req.Metadata[namespaceMetadataKey] != "" && req.Metadata[namespaceMetadataKey] != h.effectiveAgentNamespace(current) {
			h.writeError(w, http.StatusForbidden, "namespace_id metadata is immutable", nil)
			return
		}
		if ns := h.effectiveAgentNamespace(current); ns != "" {
			req.Metadata[namespaceMetadataKey] = ns
		}
		updates["metadata"] = req.Metadata
	}

	// Update agent
	var t *agent.Agent
	updateAndGetter, hasUpdateAndGet := space.(agent.UpdateAndGetProvider)
	if hasUpdateAndGet {
		t, err = updateAndGetter.UpdateAndGet(id, updates)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to update agent", err)
			return
		}
		if t == nil {
			t, err = space.Get(id)
			if err != nil {
				h.writeError(w, http.StatusNotFound, "agent not found", err)
				return
			}
		}
	} else {
		if err := space.Update(id, updates); err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to update agent", err)
			return
		}
		// Fallback for stores without UpdateAndGet support.
		t, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
	}

	h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, req.Obs)

	h.writeJSON(w, http.StatusOK, t)
}

// QueryAgents handles POST /api/v1/agents/query
func (h *Handlers) QueryAgents(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var query agent.Query
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid query", err)
		return
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}

	// Set defaults
	if query.Limit == 0 {
		query.Limit = 100
	}

	// Execute query (non-blocking list)
	agents, err := space.Query(&query)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to query agents", err)
		return
	}
	if agents == nil {
		agents = []*agent.Agent{}
	}

	countQuery := query
	countQuery.Limit = 0
	countQuery.Offset = 0
	total, err := h.countByQuery(&countQuery)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to count agents", err)
		return
	}

	response := map[string]interface{}{
		"agents": agents,
		"total":  total,
	}

	h.writeJSON(w, http.StatusOK, response)
}

// AgentStats handles POST /api/v1/agents/stats
func (h *Handlers) AgentStats(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var query agent.Query
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil && !errors.Is(err, io.EOF) {
			h.writeError(w, http.StatusBadRequest, "invalid query", err)
			return
		}
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}

	statusCounter, hasStatusCounter := space.(agent.StatusCounter)
	counter, hasCounter := space.(agent.Counter)
	statuses := []agent.Status{
		agent.StatusNew,
		agent.StatusInProgress,
		agent.StatusCompleted,
		agent.StatusFailed,
	}
	if query.Status != "" {
		statuses = []agent.Status{query.Status}
	}

	counts := make(map[string]int, len(statuses))
	total := 0

	if hasStatusCounter {
		baseQuery := query
		baseQuery.Status = ""
		baseQuery.Limit = 0
		baseQuery.Offset = 0

		grouped, err := statusCounter.CountByStatus(&baseQuery)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to compute stats", err)
			return
		}
		for _, status := range statuses {
			count := grouped[status]
			counts[string(status)] = count
			total += count
		}

		response := map[string]interface{}{
			"counts": counts,
			"total":  total,
		}
		h.writeJSON(w, http.StatusOK, response)
		return
	}

	for _, status := range statuses {
		q := query
		q.Status = status
		var (
			count int
			err   error
		)
		if hasCounter {
			count, err = counter.Count(&q)
		} else {
			count, err = h.countAllWithSpace(space, &q, 1000)
		}
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to compute stats", err)
			return
		}
		counts[string(status)] = count
		total += count
	}

	response := map[string]interface{}{
		"counts": counts,
		"total":  total,
	}

	h.writeJSON(w, http.StatusOK, response)
}

// ReadAgent handles POST /api/v1/agents/read
func (h *Handlers) ReadAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var req ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request", err)
		return
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &req.Query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}

	if req.Blocking {
		if req.Timeout == 0 {
			req.Timeout = 30
		}
		t, err := space.Read(&req.Query, time.Duration(req.Timeout)*time.Second)
		if err != nil {
			if err.Error() == "timeout waiting for agent" {
				h.writeError(w, http.StatusRequestTimeout, "request timeout", err)
			} else {
				h.writeError(w, http.StatusInternalServerError, "failed to read agent", err)
			}
			return
		}
		if t == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.writeJSON(w, http.StatusOK, t)
		return
	}

	query := req.Query
	if query.Limit <= 0 || query.Limit > 1 {
		query.Limit = 1
	}
	agents, err := space.Query(&query)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to read agent", err)
		return
	}
	if len(agents) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.writeJSON(w, http.StatusOK, agents[0])
}

// TakeAgent handles POST /api/v1/agents/take
func (h *Handlers) TakeAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	backendType := h.getBackendType()
	decodeStart := time.Now()
	var req TakeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request", err)
		return
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &req.Query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}

	// Extract agent_id from header for ownership (not filtering)
	agentID := r.Header.Get("X-Agent-ID")
	if agentID != "" {
		req.Query.AgentID = agentID
	}
	metrics.RecordOperationStage("http_take", "decode", backendType, time.Since(decodeStart).Seconds())

	// Take agent (non-blocking)
	storeStart := time.Now()
	t, err := space.Take(&req.Query)
	metrics.RecordOperationStage("http_take", "store", backendType, time.Since(storeStart).Seconds())
	if err != nil {
		h.logger.Warn("take agent failed",
			zap.String("agent_id", req.Query.AgentID),
			zap.String("kind", req.Query.Kind),
			zap.String("status", string(req.Query.Status)),
			zap.String("trace_id", req.Query.TraceID),
			zap.Strings("tags", req.Query.Tags),
			zap.Any("metadata", req.Query.Metadata),
			zap.Int("limit", req.Query.Limit),
			zap.Int("offset", req.Query.Offset),
			zap.Error(err),
		)
		// Classify error type for appropriate HTTP response
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			h.writeError(w, http.StatusGatewayTimeout, "operation timeout", err)
		case errors.Is(err, context.Canceled):
			h.writeError(w, http.StatusGatewayTimeout, "request canceled", err)
		case isTransientError(err):
			h.writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable", err)
		default:
			h.writeError(w, http.StatusInternalServerError, "failed to take agent", err)
		}
		return
	}

	if t == nil {
		encodeStart := time.Now()
		w.WriteHeader(http.StatusNoContent)
		metrics.RecordOperationStage("http_take", "encode", backendType, time.Since(encodeStart).Seconds())
		return
	}

	if req.Obs != nil {
		h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, req.Obs)
	}

	encodeStart := time.Now()
	if requestPrefersReturnMinimal(r) {
		h.writeJSON(w, http.StatusOK, struct {
			ID         string `json:"id"`
			LeaseToken string `json:"lease_token"`
			ShardID    string `json:"shard_id,omitempty"`
		}{ID: t.ID, LeaseToken: t.LeaseToken, ShardID: t.ShardID})
		metrics.RecordOperationStage("http_take", "encode", backendType, time.Since(encodeStart).Seconds())
		return
	}

	h.writeJSON(w, http.StatusOK, t)
	metrics.RecordOperationStage("http_take", "encode", backendType, time.Since(encodeStart).Seconds())
}

// BatchTakeAgent handles POST /api/v1/agents/batch-take
func (h *Handlers) BatchTakeAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var req BatchTakeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request", err)
		return
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &req.Query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if req.Limit > 100 {
		req.Limit = 100
	}

	agentID := r.Header.Get("X-Agent-ID")
	if agentID != "" {
		req.Query.AgentID = agentID
	}

	btp, ok := space.(agent.BatchTakeProvider)
	if !ok {
		h.writeError(w, http.StatusNotImplemented, "batch take not supported by this backend", nil)
		return
	}

	agents, err := btp.BatchTake(&req.Query, req.Limit)
	if err != nil {
		h.logger.Warn("batch take failed", zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, "failed to batch take agents", err)
		return
	}

	if len(agents) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.writeJSON(w, http.StatusOK, agents)
}

// InAgent handles POST /api/v1/agents/in
func (h *Handlers) InAgent(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	var req InRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request", err)
		return
	}
	if err := h.applyPrincipalNamespaceToQuery(r, &req.Query); err != nil {
		h.writeError(w, http.StatusForbidden, err.Error(), nil)
		return
	}

	// Set default timeout
	if req.Timeout == 0 {
		req.Timeout = 30
	}

	// Extract agent_id from header for ownership (not filtering)
	agentID := r.Header.Get("X-Agent-ID")
	if agentID != "" {
		req.Query.AgentID = agentID
	}

	// Take agent (blocking)
	t, err := space.In(&req.Query, time.Duration(req.Timeout)*time.Second)
	if err != nil {
		h.logger.Warn("blocking take agent failed",
			zap.String("agent_id", req.Query.AgentID),
			zap.String("kind", req.Query.Kind),
			zap.String("status", string(req.Query.Status)),
			zap.String("trace_id", req.Query.TraceID),
			zap.Strings("tags", req.Query.Tags),
			zap.Any("metadata", req.Query.Metadata),
			zap.Int("limit", req.Query.Limit),
			zap.Int("offset", req.Query.Offset),
			zap.Int("timeout_sec", req.Timeout),
			zap.Error(err),
		)
		// Classify error type for appropriate HTTP response
		switch {
		case err.Error() == "timeout waiting for agent":
			h.writeError(w, http.StatusRequestTimeout, "request timeout", err)
		case errors.Is(err, context.DeadlineExceeded):
			h.writeError(w, http.StatusRequestTimeout, "request timeout", err)
		case errors.Is(err, context.Canceled):
			h.writeError(w, http.StatusRequestTimeout, "request canceled", err)
		case isTransientError(err):
			h.writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable", err)
		default:
			h.writeError(w, http.StatusInternalServerError, "failed to take agent", err)
		}
		return
	}

	h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, req.Obs)

	h.writeJSON(w, http.StatusOK, t)
}

// CompleteAgent handles POST /api/v1/agents/{id}/complete
func (h *Handlers) CompleteAgent(w http.ResponseWriter, r *http.Request) {
	backendType := h.getBackendType()
	decodeStart := time.Now()
	id := strings.TrimSuffix(extractIDFromPath(r.URL.Path, "/api/v1/agents/"), "/complete")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}
	preferMinimal := requestPrefersReturnMinimal(r)

	var req CompleteAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}
	space := h.requestSpace(r.WithContext(agent.ContextWithShardID(r.Context(), req.ShardID)))

	var current *agent.Agent
	if principal, ok := AuthPrincipalFromContext(r.Context()); ok && strings.TrimSpace(principal.NamespaceID) != "" {
		var err error
		current, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
		if !h.agentVisibleToPrincipal(r, current) {
			h.writeError(w, http.StatusNotFound, "agent not found", nil)
			return
		}
	}

	agentID := r.Header.Get("X-Agent-ID")
	if agentID == "" {
		h.writeError(w, http.StatusBadRequest, "X-Agent-ID header required", nil)
		return
	}

	leaseToken := r.Header.Get("X-Lease-Token")
	if leaseToken == "" {
		h.writeError(w, http.StatusBadRequest, "X-Lease-Token header required", nil)
		return
	}
	metrics.RecordOperationStage("http_complete", "decode", backendType, time.Since(decodeStart).Seconds())

	// Complete agent
	var t *agent.Agent
	var err error
	completeAndGetter, hasCompleteAndGet := space.(agent.CompleteAndGetProvider)
	storeStart := time.Now()
	if preferMinimal {
		err = space.Complete(id, agentID, leaseToken, req.Result)
	} else if hasCompleteAndGet {
		t, err = completeAndGetter.CompleteAndGet(id, agentID, leaseToken, req.Result)
	} else {
		err = space.Complete(id, agentID, leaseToken, req.Result)
	}
	metrics.RecordOperationStage("http_complete", "store", backendType, time.Since(storeStart).Seconds())
	if err != nil {
		errMsg := err.Error()
		// Return appropriate status codes for specific error types
		if strings.Contains(errMsg, "TOKEN_MISMATCH") || strings.Contains(errMsg, "NOT_OWNER") || strings.Contains(errMsg, "lease token mismatch") || strings.Contains(errMsg, "cannot complete") || strings.Contains(errMsg, "complete rejected") || strings.Contains(errMsg, "owned by") {
			h.writeError(w, http.StatusForbidden, "stale or invalid lease token", err)
			return
		}
		if strings.Contains(errMsg, "INVALID_STATUS") || strings.Contains(errMsg, "invalid status") {
			h.writeError(w, http.StatusConflict, "agent not in correct state", err)
			return
		}
		if strings.Contains(errMsg, "AGENT_NOT_FOUND") || strings.Contains(errMsg, "agent not found") {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, "failed to complete agent", err)
		return
	}

	if preferMinimal {
		traceID := ""
		namespaceID := ""
		if current != nil {
			traceID = current.TraceID
			namespaceID = current.NamespaceID
		}
		if req.Obs != nil {
			h.recordObs(r.Context(), id, traceID, namespaceID, req.Obs)
		}
		encodeStart := time.Now()
		w.WriteHeader(http.StatusNoContent)
		metrics.RecordOperationStage("http_complete", "encode", backendType, time.Since(encodeStart).Seconds())
		return
	}

	if !hasCompleteAndGet {
		// Fallback for stores without CompleteAndGet support.
		t, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
	} else if t == nil {
		t, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
	}

	if req.Obs != nil {
		h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, req.Obs)
	}

	encodeStart := time.Now()
	h.writeJSON(w, http.StatusOK, t)
	metrics.RecordOperationStage("http_complete", "encode", backendType, time.Since(encodeStart).Seconds())
}

// CompleteAndOutAgent handles POST /api/v1/agents/{id}/complete-and-out
// This atomically completes a agent and creates new output agents,
// preventing partial workflow state.
func (h *Handlers) CompleteAndOutAgent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(extractIDFromPath(r.URL.Path, "/api/v1/agents/"), "/complete-and-out")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}

	var req CompleteAndOutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}
	space := h.requestSpace(r.WithContext(agent.ContextWithShardID(r.Context(), req.ShardID)))
	current, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, current) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}
	namespaceID := h.effectiveAgentNamespace(current)
	if principal, ok := AuthPrincipalFromContext(r.Context()); ok {
		if okQuota, reason, err := h.allowNamespaceAgentCreation(principal, namespaceID, nil, len(req.Outputs)); err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to evaluate namespace quotas", err)
			return
		} else if !okQuota {
			metrics.RecordQuotaRejection(namespaceID, reason)
			h.writeError(w, http.StatusTooManyRequests, "namespace quota exceeded", nil)
			return
		}
		queueAdditions := make(map[string]int)
		for _, out := range req.Outputs {
			if out.Metadata == nil {
				continue
			}
			queueName := strings.TrimSpace(out.Metadata[queueMetadataKey])
			if queueName == "" {
				continue
			}
			queueAdditions[queueName]++
		}
		for queueName, additional := range queueAdditions {
			if okQuota, reason, err := h.allowNamespaceAgentCreation(principal, namespaceID, map[string]string{
				queueMetadataKey: queueName,
			}, additional); err != nil {
				h.writeError(w, http.StatusInternalServerError, "failed to evaluate namespace quotas", err)
				return
			} else if !okQuota {
				metrics.RecordQuotaRejection(namespaceID, reason)
				h.writeError(w, http.StatusTooManyRequests, "namespace quota exceeded", nil)
				return
			}
		}
	}

	agentID := r.Header.Get("X-Agent-ID")
	if agentID == "" {
		h.writeError(w, http.StatusBadRequest, "X-Agent-ID header required", nil)
		return
	}

	leaseToken := r.Header.Get("X-Lease-Token")
	if leaseToken == "" {
		h.writeError(w, http.StatusBadRequest, "X-Lease-Token header required", nil)
		return
	}

	// Convert CreateAgentRequest to Agent
	outputs := make([]*agent.Agent, 0, len(req.Outputs))
	for _, out := range req.Outputs {
		if out.NamespaceID != "" && out.NamespaceID != namespaceID {
			h.writeError(w, http.StatusForbidden, "output namespace_id must inherit parent namespace", nil)
			return
		}
		t := &agent.Agent{
			Kind:        out.Kind,
			Payload:     out.Payload,
			Tags:        out.Tags,
			ParentID:    out.ParentID,
			TraceID:     out.TraceID,
			NamespaceID: namespaceID,
			Status:      agent.StatusNew,
			Metadata:    out.Metadata,
		}
		if out.TTL > 0 {
			t.TTL = time.Duration(out.TTL) * time.Second
		}
		if t.Metadata == nil {
			t.Metadata = make(map[string]string)
		}
		if namespaceID != "" {
			t.Metadata[namespaceMetadataKey] = namespaceID
		}
		outputs = append(outputs, t)
	}

	// Complete and output atomically
	completed, created, err := space.CompleteAndOut(id, agentID, leaseToken, req.Result, outputs)
	if err != nil {
		// Determine appropriate error code
		errMsg := err.Error()
		if strings.Contains(errMsg, "not found") {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
		} else if strings.Contains(errMsg, "cross_shard_atomic_unsupported") {
			h.writeError(w, http.StatusConflict, "cross-shard CompleteAndOut is not supported", err)
		} else if strings.Contains(errMsg, "TOKEN_MISMATCH") || strings.Contains(errMsg, "NOT_OWNER") || strings.Contains(errMsg, "owned by") || strings.Contains(errMsg, "lease token mismatch") {
			h.writeError(w, http.StatusForbidden, "not authorized to complete agent", err)
		} else if strings.Contains(errMsg, "invalid status") {
			h.writeError(w, http.StatusConflict, "agent in invalid state", err)
		} else {
			h.writeError(w, http.StatusInternalServerError, "failed to complete agent", err)
		}
		return
	}

	h.recordObs(r.Context(), completed.ID, completed.TraceID, completed.NamespaceID, req.Obs)
	for i, outReq := range req.Outputs {
		if outReq.Obs == nil {
			continue
		}
		if i >= len(created) {
			break
		}
		h.recordObs(r.Context(), created[i].ID, created[i].TraceID, created[i].NamespaceID, outReq.Obs)
	}

	h.writeJSON(w, http.StatusOK, CompleteAndOutResponse{
		Completed: completed,
		Created:   created,
	})
}

// ReleaseAgent handles POST /api/v1/agents/{id}/release
func (h *Handlers) ReleaseAgent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(extractIDFromPath(r.URL.Path, "/api/v1/agents/"), "/release")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}

	// Get agent ID from header (required)
	agentID := r.Header.Get("X-Agent-ID")
	if agentID == "" {
		h.writeError(w, http.StatusBadRequest, "X-Agent-ID header required", nil)
		return
	}

	leaseToken := r.Header.Get("X-Lease-Token")
	if leaseToken == "" {
		h.writeError(w, http.StatusBadRequest, "X-Lease-Token header required", nil)
		return
	}

	// Parse optional request body for reason
	var reason string
	var obs *telemetry.Observation
	var shardID string
	if r.Body != nil && r.ContentLength > 0 {
		var req ReleaseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			reason = req.Reason
			obs = req.Obs
			shardID = req.ShardID
		}
		// Ignore decode errors - reason is optional
	}
	space := h.requestSpace(r.WithContext(agent.ContextWithShardID(r.Context(), shardID)))
	current, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, current) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}

	// Release agent (ownership verified by store)
	var t *agent.Agent
	releaseAndGetter, hasReleaseAndGet := space.(agent.ReleaseAndGetProvider)
	if hasReleaseAndGet {
		t, err = releaseAndGetter.ReleaseAndGet(id, agentID, leaseToken, reason)
	} else {
		err = space.Release(id, agentID, leaseToken, reason)
	}
	if err != nil {
		// Check if it's an ownership error
		if strings.Contains(err.Error(), "cannot release") {
			h.writeError(w, http.StatusForbidden, err.Error(), nil)
			return
		}
		h.writeError(w, http.StatusInternalServerError, "failed to release agent", err)
		return
	}

	if !hasReleaseAndGet {
		// Fallback for stores without ReleaseAndGet support.
		t, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
	} else if t == nil {
		t, err = space.Get(id)
		if err != nil {
			h.writeError(w, http.StatusNotFound, "agent not found", err)
			return
		}
	}

	h.recordObs(r.Context(), t.ID, t.TraceID, t.NamespaceID, obs)

	h.writeJSON(w, http.StatusOK, t)
}

// RenewLeaseAgent handles POST /api/v1/agents/{id}/renew
func (h *Handlers) RenewLeaseAgent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(extractIDFromPath(r.URL.Path, "/api/v1/agents/"), "/renew")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing agent ID", nil)
		return
	}

	agentID := r.Header.Get("X-Agent-ID")
	if agentID == "" {
		h.writeError(w, http.StatusBadRequest, "X-Agent-ID header required", nil)
		return
	}

	leaseToken := r.Header.Get("X-Lease-Token")
	if leaseToken == "" {
		h.writeError(w, http.StatusBadRequest, "X-Lease-Token header required", nil)
		return
	}

	var req RenewLeaseRequest
	if r.Body != nil && (r.ContentLength > 0 || r.ContentLength == -1) {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			h.writeError(w, http.StatusBadRequest, "invalid request body", err)
			return
		}
	}
	space := h.requestSpace(r.WithContext(agent.ContextWithShardID(r.Context(), req.ShardID)))
	current, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, current) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}

	leaseDuration := time.Duration(req.LeaseSec) * time.Second
	t, err := space.RenewLease(id, agentID, leaseToken, leaseDuration)
	if err != nil {
		errMsg := err.Error()
		switch {
		case strings.Contains(errMsg, "TOKEN_MISMATCH") || strings.Contains(errMsg, "lease token mismatch") || strings.Contains(errMsg, "NOT_OWNER") || strings.Contains(errMsg, "owned by") || strings.Contains(errMsg, "cannot renew") || strings.Contains(errMsg, "renew rejected"):
			h.writeError(w, http.StatusForbidden, "stale or invalid lease token", err)
		case strings.Contains(errMsg, "INVALID_STATUS") || strings.Contains(errMsg, "invalid status"):
			h.writeError(w, http.StatusConflict, "agent not in correct state", err)
		case strings.Contains(errMsg, "agent not found") || strings.Contains(errMsg, "AGENT_NOT_FOUND"):
			h.writeError(w, http.StatusNotFound, "agent not found", err)
		default:
			h.writeError(w, http.StatusInternalServerError, "failed to renew lease", err)
		}
		return
	}

	h.writeJSON(w, http.StatusOK, t)
}

// GetEvents handles GET /api/v1/events
func (h *Handlers) GetEvents(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	// Parse query parameters
	tupleID := r.URL.Query().Get("tuple_id")
	agentID := r.URL.Query().Get("agent_id")
	eventType := r.URL.Query().Get("type")
	traceID := r.URL.Query().Get("trace_id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	cursor := r.URL.Query().Get("cursor")

	if limit == 0 {
		limit = 100
	}
	if tupleID != "" {
		t, err := space.Get(tupleID)
		if err != nil || !h.agentVisibleToPrincipal(r, t) {
			h.writeError(w, http.StatusNotFound, "agent not found", nil)
			return
		}
	}

	if cursor != "" {
		if _, ok := space.(agent.EventQuerier); !ok {
			h.writeError(w, http.StatusBadRequest, "cursor pagination is not supported by this backend", nil)
			return
		}
	}

	if querier, ok := space.(agent.EventQuerier); ok {
		query := &agent.EventQuery{
			TupleID: tupleID,
			AgentID: agentID,
			TraceID: traceID,
			Limit:   limit,
			Offset:  offset,
			Cursor:  cursor,
		}
		if eventType != "" {
			query.Type = agent.EventType(eventType)
		}

		page, err := querier.QueryEvents(query)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to get events", err)
			return
		}
		if page == nil {
			page = &agent.EventPage{Events: []*agent.Event{}}
		}
		if page.Events == nil {
			page.Events = []*agent.Event{}
		}
		filtered, err := h.filterEventsForPrincipalWithSpace(r, space, page.Events)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to scope events by namespace", err)
			return
		}
		response := map[string]interface{}{
			"events": filtered,
			"total":  len(filtered),
		}
		if page.NextCursor != "" {
			response["next_cursor"] = page.NextCursor
		}
		h.writeJSON(w, http.StatusOK, response)
		return
	}

	// Get events for specific agent
	if tupleID != "" {
		events, err := space.GetEvents(tupleID)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to get events", err)
			return
		}
		if events == nil {
			events = []*agent.Event{}
		}
		events, err = h.filterEventsForPrincipalWithSpace(r, space, events)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to scope events by namespace", err)
			return
		}

		// Apply filters
		filtered := make([]*agent.Event, 0)
		for _, e := range events {
			if agentID != "" && e.AgentID != agentID {
				continue
			}
			if eventType != "" && string(e.Type) != eventType {
				continue
			}
			if traceID != "" && e.TraceID != traceID {
				continue
			}
			filtered = append(filtered, e)
		}

		// Apply pagination
		start := offset
		end := offset + limit
		if start > len(filtered) {
			start = len(filtered)
		}
		if end > len(filtered) {
			end = len(filtered)
		}

		response := map[string]interface{}{
			"events": filtered[start:end],
			"total":  len(filtered),
		}

		h.writeJSON(w, http.StatusOK, response)
		return
	}

	// Get global events when no agent ID specified
	if agentID != "" || eventType != "" || traceID != "" {
		filtered, total, err := h.filterGlobalEvents(space, limit, offset, agentID, eventType, traceID)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to get global events", err)
			return
		}
		filtered, err = h.filterEventsForPrincipalWithSpace(r, space, filtered)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to scope events by namespace", err)
			return
		}
		response := map[string]interface{}{
			"events": filtered,
			"total":  len(filtered),
		}
		_ = total
		h.writeJSON(w, http.StatusOK, response)
		return
	}

	events, err := space.GetGlobalEvents(limit, offset)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get global events", err)
		return
	}
	if events == nil {
		events = []*agent.Event{}
	}
	events, err = h.filterEventsForPrincipalWithSpace(r, space, events)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to scope events by namespace", err)
		return
	}

	response := map[string]interface{}{
		"events": events,
		"total":  len(events),
	}
	h.writeJSON(w, http.StatusOK, response)
}

// IngestTelemetrySpan handles POST /api/v1/telemetry/spans
func (h *Handlers) IngestTelemetrySpan(w http.ResponseWriter, r *http.Request) {
	if h.telemetry == nil {
		h.writeError(w, http.StatusNotImplemented, "telemetry store not available", nil)
		return
	}

	var span telemetry.Span
	if err := json.NewDecoder(r.Body).Decode(&span); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid telemetry span", err)
		return
	}
	if span.TraceID == "" {
		h.writeError(w, http.StatusBadRequest, "trace_id is required", nil)
		return
	}
	if span.TsStart.IsZero() {
		span.TsStart = time.Now().UTC()
	}
	spans, status, message, err := h.scopeTelemetrySpansToPrincipal(r, []telemetry.Span{span})
	if status != 0 {
		h.writeError(w, status, message, err)
		return
	}

	if err := h.telemetry.RecordSpans(r.Context(), spans); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to record telemetry span", err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ingested": 1,
	})
}

// IngestTelemetrySpanBatch handles POST /api/v1/telemetry/spans:batch
func (h *Handlers) IngestTelemetrySpanBatch(w http.ResponseWriter, r *http.Request) {
	if h.telemetry == nil {
		h.writeError(w, http.StatusNotImplemented, "telemetry store not available", nil)
		return
	}

	var batch telemetrySpanBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid telemetry span batch", err)
		return
	}
	if len(batch.Spans) == 0 {
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"ingested": 0,
		})
		return
	}

	now := time.Now().UTC()
	for i := range batch.Spans {
		if batch.Spans[i].TraceID == "" {
			h.writeError(w, http.StatusBadRequest, "trace_id is required for all spans", nil)
			return
		}
		if batch.Spans[i].TsStart.IsZero() {
			batch.Spans[i].TsStart = now
		}
	}
	spans, status, message, err := h.scopeTelemetrySpansToPrincipal(r, batch.Spans)
	if status != 0 {
		h.writeError(w, status, message, err)
		return
	}

	if err := h.telemetry.RecordSpans(r.Context(), spans); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to record telemetry spans", err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ingested": len(spans),
	})
}

// ListTelemetrySpans handles GET /api/v1/telemetry/spans
func (h *Handlers) ListTelemetrySpans(w http.ResponseWriter, r *http.Request) {
	if h.telemetry == nil {
		h.writeError(w, http.StatusNotImplemented, "telemetry store not available", nil)
		return
	}

	traceID := r.URL.Query().Get("trace_id")
	if traceID == "" {
		h.writeError(w, http.StatusBadRequest, "trace_id is required", nil)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if limit == 0 {
		limit = 100
	}
	namespaceID := h.requestPrincipalNamespace(r)

	spans, err := h.telemetry.ListSpans(r.Context(), traceID, namespaceID, limit, offset)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list telemetry spans", err)
		return
	}

	response := map[string]interface{}{
		"spans": spans,
		"total": len(spans),
	}
	h.writeJSON(w, http.StatusOK, response)
}

// GetDAG handles GET /api/v1/dag/{id}
func (h *Handlers) GetDAG(w http.ResponseWriter, r *http.Request) {
	space := h.requestSpace(r)
	id := extractIDFromPath(r.URL.Path, "/api/v1/dag/")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing root agent ID", nil)
		return
	}
	root, err := space.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "agent not found", err)
		return
	}
	if !h.agentVisibleToPrincipal(r, root) {
		h.writeError(w, http.StatusNotFound, "agent not found", nil)
		return
	}

	dag, err := space.GetDAG(id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get DAG", err)
		return
	}

	h.writeJSON(w, http.StatusOK, dag)
}

// HealthCheck handles GET /api/v1/health
func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":  "healthy",
		"ready":   true,
		"backend": h.getBackendType(),
		"version": "1.0.0",
	}

	if checker, ok := h.space.(healthChecker); ok {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := checker.Health(ctx); err != nil {
			h.logger.Warn("health check failed", zap.Error(err))
			response["status"] = "unhealthy"
			response["ready"] = false
			response["message"] = "backend unavailable"
			h.writeJSON(w, http.StatusServiceUnavailable, response)
			return
		}
	}

	h.writeJSON(w, http.StatusOK, response)
}

// GetStats handles GET /api/v1/stats
func (h *Handlers) GetStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	totalCount, err := h.countAgentsFast()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to count agents", err)
		return
	}

	eventCount, err := h.countEventsFast()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to count events", err)
		return
	}

	activeAgentCount, err := h.countActiveAgentsFast()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to count active agents", err)
		return
	}

	response := map[string]interface{}{
		"agents":        totalCount,
		"events":        eventCount,
		"active_agents": activeAgentCount,
		"backend":       h.getBackendType(),
	}
	h.writeJSON(w, http.StatusOK, response)
}

// GetShardMap handles GET /api/v1/admin/shards
func (h *Handlers) GetShardMap(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	provider, ok := h.space.(shardMapInfoProvider)
	if !ok {
		h.writeError(w, http.StatusNotFound, "shard admin endpoints are not available for this backend", nil)
		return
	}
	info, err := provider.ShardMapInfo(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to read shard map", err)
		return
	}
	h.writeJSON(w, http.StatusOK, info)
}

// GetShardHealth handles GET /api/v1/admin/shards/health
func (h *Handlers) GetShardHealth(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	provider, ok := h.space.(shardHealthInfoProvider)
	if !ok {
		h.writeError(w, http.StatusNotFound, "shard admin endpoints are not available for this backend", nil)
		return
	}
	info, err := provider.ShardHealthInfo(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to read shard health", err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"backend": h.getBackendType(),
		"shards":  info,
	})
}

// GetShardQueueStats handles GET /api/v1/admin/shards/queues/stats?namespace_id=...&queue=...
func (h *Handlers) GetShardQueueStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	provider, ok := h.space.(shardQueueStatsProvider)
	if !ok {
		h.writeError(w, http.StatusNotFound, "shard admin endpoints are not available for this backend", nil)
		return
	}
	namespaceID := strings.TrimSpace(r.URL.Query().Get("namespace_id"))
	queue := strings.TrimSpace(r.URL.Query().Get("queue"))
	if namespaceID == "" || queue == "" {
		h.writeError(w, http.StatusBadRequest, "namespace_id and queue are required", nil)
		return
	}
	info, err := provider.ShardQueueStats(r.Context(), namespaceID, queue)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to read shard queue stats", err)
		return
	}
	h.writeJSON(w, http.StatusOK, info)
}

// DebugInvariants handles GET /debug/invariants (dev-only).
func (h *Handlers) DebugInvariants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := map[string]interface{}{
		"backend": h.getBackendType(),
	}

	if provider, ok := h.space.(debugInvariantsProvider); ok {
		invariants, err := provider.DebugInvariants(r.Context())
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to compute invariants", err)
			return
		}
		response["invariants"] = invariants
		h.writeJSON(w, http.StatusOK, response)
		return
	}

	counts, truncated := h.countByStatus(2000)
	response["invariants"] = map[string]interface{}{
		"status_counts":    counts,
		"counts_truncated": truncated,
		"details":          "store does not expose debug invariants",
	}
	h.writeJSON(w, http.StatusOK, response)
}

// Helper methods

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(data); err != nil {
		h.logger.Error("failed to encode response", zap.Error(err))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	w.Write(buf.Bytes())
}

func (h *Handlers) writeError(w http.ResponseWriter, status int, message string, err error) {
	requestID := uuid.New().String()[:8]
	response := ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
		Details: map[string]interface{}{
			"request_id": requestID,
		},
	}
	if err != nil {
		h.logger.Warn("request error",
			zap.String("request_id", requestID),
			zap.Int("status", status),
			zap.String("message", message),
			zap.Error(err))
	}
	h.writeJSON(w, status, response)
}

func (h *Handlers) countByStatus(limit int) (map[string]int, bool) {
	counts := map[string]int{}
	truncated := false
	statuses := []agent.Status{
		agent.StatusNew,
		agent.StatusInProgress,
		agent.StatusCompleted,
		agent.StatusFailed,
	}

	for _, status := range statuses {
		agents, err := h.space.Query(&agent.Query{
			Status: status,
			Limit:  limit,
		})
		if err != nil {
			counts[string(status)] = 0
			continue
		}
		counts[string(status)] = len(agents)
		if len(agents) >= limit {
			truncated = true
		}
	}

	return counts, truncated
}

func (h *Handlers) countAll(query *agent.Query, pageSize int) (int, error) {
	return h.countAllWithSpace(h.space, query, pageSize)
}

func (h *Handlers) countAllWithSpace(space agent.AgentSpace, query *agent.Query, pageSize int) (int, error) {
	if query == nil {
		query = &agent.Query{}
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	total := 0
	offset := 0
	for {
		q := *query
		q.Limit = pageSize
		q.Offset = offset
		agents, err := space.Query(&q)
		if err != nil {
			return total, err
		}
		total += len(agents)
		if len(agents) < pageSize {
			break
		}
		offset += pageSize
	}
	return total, nil
}

func (h *Handlers) countAgentsFast() (int, error) {
	if counter, ok := h.space.(agent.Counter); ok {
		return counter.Count(&agent.Query{})
	}
	return h.countAll(&agent.Query{}, 1000)
}

func (h *Handlers) countEventsFast() (int, error) {
	if querier, ok := h.space.(agent.EventQuerier); ok {
		page, err := querier.QueryEvents(&agent.EventQuery{
			Limit: 1,
		})
		if err != nil {
			return 0, err
		}
		if page == nil {
			return 0, nil
		}
		return page.Total, nil
	}

	const pageSize = 1000
	total := 0
	offset := 0
	for {
		events, err := h.space.GetGlobalEvents(pageSize, offset)
		if err != nil {
			return 0, err
		}
		total += len(events)
		if len(events) < pageSize {
			break
		}
		offset += pageSize
	}

	return total, nil
}

func (h *Handlers) countActiveAgentsFast() (int, error) {
	if statusCounter, ok := h.space.(agent.StatusCounter); ok {
		counts, err := statusCounter.CountByStatus(&agent.Query{})
		if err != nil {
			return 0, err
		}
		if counts[agent.StatusInProgress] == 0 {
			return 0, nil
		}
	}

	const pageSize = 500
	uniqueOwners := make(map[string]struct{})
	offset := 0

	for {
		results, err := h.space.Query(&agent.Query{
			Status: agent.StatusInProgress,
			Limit:  pageSize,
			Offset: offset,
		})
		if err != nil {
			return 0, err
		}

		for _, t := range results {
			if t == nil {
				continue
			}
			if t.Owner != "" {
				uniqueOwners[t.Owner] = struct{}{}
				continue
			}
			if t.Metadata == nil {
				continue
			}
			if agentID := t.Metadata["agent_id"]; agentID != "" {
				uniqueOwners[agentID] = struct{}{}
			}
		}

		if len(results) < pageSize {
			break
		}
		offset += pageSize
	}

	return len(uniqueOwners), nil
}

func (h *Handlers) filterGlobalEvents(space agent.AgentSpace, limit, offset int, agentID, eventType, traceID string) ([]*agent.Event, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	batchSize := limit
	if batchSize < 500 {
		batchSize = 500
	}

	filtered := make([]*agent.Event, 0, limit)
	totalFiltered := 0
	globalOffset := 0

	for {
		events, err := space.GetGlobalEvents(batchSize, globalOffset)
		if err != nil {
			return nil, 0, err
		}
		if len(events) == 0 {
			break
		}

		for _, e := range events {
			if agentID != "" && e.AgentID != agentID {
				continue
			}
			if eventType != "" && string(e.Type) != eventType {
				continue
			}
			if traceID != "" && e.TraceID != traceID {
				continue
			}
			if totalFiltered >= offset && len(filtered) < limit {
				filtered = append(filtered, e)
			}
			totalFiltered++
		}

		globalOffset += len(events)
		if len(events) < batchSize {
			break
		}
	}

	return filtered, totalFiltered, nil
}

func (h *Handlers) effectiveAgentNamespace(t *agent.Agent) string {
	if t == nil {
		return ""
	}
	if ns := strings.TrimSpace(t.NamespaceID); ns != "" {
		return ns
	}
	if t.Metadata != nil {
		return strings.TrimSpace(t.Metadata[namespaceMetadataKey])
	}
	return ""
}

func (h *Handlers) agentVisibleToPrincipal(r *http.Request, t *agent.Agent) bool {
	principal, ok := AuthPrincipalFromContext(r.Context())
	if !ok || strings.TrimSpace(principal.NamespaceID) == "" {
		return true
	}
	return h.effectiveAgentNamespace(t) == strings.TrimSpace(principal.NamespaceID)
}

func (h *Handlers) applyPrincipalNamespaceToQuery(r *http.Request, query *agent.Query) error {
	if query == nil {
		return nil
	}
	principal, ok := AuthPrincipalFromContext(r.Context())
	if !ok || strings.TrimSpace(principal.NamespaceID) == "" {
		return nil
	}

	namespaceID := strings.TrimSpace(principal.NamespaceID)
	query.NamespaceID = namespaceID
	if query.Metadata == nil {
		query.Metadata = make(map[string]string)
	}
	query.Metadata[namespaceMetadataKey] = namespaceID
	return nil
}

func (h *Handlers) countByQuery(query *agent.Query) (int, error) {
	if counter, ok := h.space.(agent.Counter); ok {
		return counter.Count(query)
	}
	return h.countAll(query, 1000)
}

func (h *Handlers) allowNamespaceAgentCreation(principal AuthPrincipal, namespaceID string, metadata map[string]string, additional int) (bool, string, error) {
	if additional <= 0 {
		return true, "", nil
	}

	principalNamespace := strings.TrimSpace(principal.NamespaceID)
	if principalNamespace != "" {
		namespaceID = principalNamespace
	}
	namespaceID = strings.TrimSpace(namespaceID)

	if principalNamespace != "" && namespaceID != principalNamespace {
		return false, "namespace_mismatch", nil
	}
	if namespaceID == "" {
		return true, "", nil
	}

	activeCap := principal.Limits.MaxActiveAgents
	if activeCap > 0 {
		baseMetadata := map[string]string{
			namespaceMetadataKey: namespaceID,
		}

		newCount, err := h.countByQuery(&agent.Query{
			Status:   agent.StatusNew,
			Metadata: baseMetadata,
		})
		if err != nil {
			return false, "", err
		}
		inProgressCount, err := h.countByQuery(&agent.Query{
			Status:   agent.StatusInProgress,
			Metadata: baseMetadata,
		})
		if err != nil {
			return false, "", err
		}

		if newCount+inProgressCount+additional > activeCap {
			return false, "active_agent_quota", nil
		}
	}

	queueCap := principal.Limits.MaxQueueDepth
	if queueCap > 0 && metadata != nil {
		queueName := strings.TrimSpace(metadata[queueMetadataKey])
		if queueName != "" {
			queueCount, err := h.countByQuery(&agent.Query{
				Status: agent.StatusNew,
				Metadata: map[string]string{
					namespaceMetadataKey: namespaceID,
					queueMetadataKey:     queueName,
				},
			})
			if err != nil {
				return false, "", err
			}
			if queueCount+additional > queueCap {
				return false, "queue_depth_quota", nil
			}
		}
	}

	return true, "", nil
}

func (h *Handlers) filterEventsForPrincipalWithSpace(r *http.Request, space agent.AgentSpace, events []*agent.Event) ([]*agent.Event, error) {
	principal, ok := AuthPrincipalFromContext(r.Context())
	if !ok || strings.TrimSpace(principal.NamespaceID) == "" {
		return events, nil
	}
	if len(events) == 0 {
		return []*agent.Event{}, nil
	}

	namespaceID := strings.TrimSpace(principal.NamespaceID)
	visibleByTupleID := make(map[string]bool)
	filtered := make([]*agent.Event, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		tupleID := strings.TrimSpace(event.TupleID)
		if tupleID == "" {
			continue
		}

		visible, cached := visibleByTupleID[tupleID]
		if !cached {
			t, err := space.Get(tupleID)
			visible = err == nil && h.effectiveAgentNamespace(t) == namespaceID
			visibleByTupleID[tupleID] = visible
		}
		if visible {
			filtered = append(filtered, event)
		}
	}

	return filtered, nil
}

func (h *Handlers) requestPrincipalNamespace(r *http.Request) string {
	principal, ok := AuthPrincipalFromContext(r.Context())
	if !ok {
		return ""
	}
	return strings.TrimSpace(principal.NamespaceID)
}

func (h *Handlers) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	principal, ok := AuthPrincipalFromContext(r.Context())
	if !ok {
		h.writeError(w, http.StatusUnauthorized, "admin authentication required", nil)
		return false
	}
	if !principal.Admin {
		h.writeError(w, http.StatusForbidden, "admin access required", nil)
		return false
	}
	return true
}

func (h *Handlers) scopeTelemetrySpansToPrincipal(r *http.Request, spans []telemetry.Span) ([]telemetry.Span, int, string, error) {
	namespaceID := h.requestPrincipalNamespace(r)
	if namespaceID == "" || len(spans) == 0 {
		return spans, 0, "", nil
	}

	space := h.requestSpace(r)
	visibleByTupleID := make(map[string]bool)
	scoped := make([]telemetry.Span, 0, len(spans))
	for _, span := range spans {
		tupleID := strings.TrimSpace(span.TupleID)
		if tupleID != "" {
			visible, cached := visibleByTupleID[tupleID]
			if !cached {
				t, err := space.Get(tupleID)
				visible = err == nil && h.effectiveAgentNamespace(t) == namespaceID
				visibleByTupleID[tupleID] = visible
			}
			if !visible {
				return nil, http.StatusNotFound, "agent not found", nil
			}
		}

		span.NamespaceID = namespaceID
		scoped = append(scoped, span)
	}

	return scoped, 0, "", nil
}

func (h *Handlers) getBackendType() string {
	switch h.space.(type) {
	case *store.PostgresStore:
		return string(store.StoreTypePostgres)
	case *store.PostgresShardedStore:
		return string(store.StoreTypePostgres)
	case *store.SQLiteStore:
		return "sqlite"
	default:
		return "unknown"
	}
}

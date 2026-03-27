package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Operation metrics
	AgentOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_operations_total",
		Help: "Total number of agent space operations",
	}, []string{"operation", "status", "store_type"})

	AgentOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_operation_duration_seconds",
		Help:    "Duration of agent space operations in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "store_type"})

	AgentOperationStageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_operation_stage_duration_seconds",
		Help:    "Duration of agent space operation stages in seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms to ~6.5s
	}, []string{"operation", "stage", "store_type"})

	RouteActivity = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_route_activity_total",
		Help: "Total number of sharded route-cache and route-index activity events",
	}, []string{"activity", "store_type"})

	RouteActivityDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_route_activity_duration_seconds",
		Help:    "Duration of sharded route-index operations in seconds",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 18), // 0.01ms to ~1.3s
	}, []string{"activity", "store_type"})

	// Agent metrics
	AgentsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_agents_total",
		Help: "Total number of agents in the space",
	}, []string{"status", "store_type"})

	AgentsByKind = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_agents_by_kind",
		Help: "Number of agents by kind",
	}, []string{"kind", "store_type"})

	BacklogByKind = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_backlog_total",
		Help: "Number of backlog agents (NEW + IN_PROGRESS) by kind",
	}, []string{"kind", "store_type"})

	// Event metrics
	EventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_events_processed_total",
		Help: "Total number of events processed",
	}, []string{"event_type", "store_type"})

	// Store-specific metrics
	StoreConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_store_connections",
		Help: "Number of active store connections",
	}, []string{"store_type"})

	StoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_store_errors_total",
		Help: "Total number of store errors",
	}, []string{"store_type", "error_type"})

	// Waiter metrics (for blocking operations)
	WaitersActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_waiters_active",
		Help: "Number of active waiters for agents",
	}, []string{"store_type"})

	WaiterWaitTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_waiter_wait_time_seconds",
		Help:    "Time spent waiting for agents",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to 16s
	}, []string{"store_type", "operation"})

	// Subscriber metrics
	SubscribersActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_subscribers_active",
		Help: "Number of active event subscribers",
	}, []string{"store_type"})

	// PostgreSQL specific metrics
	PostgresPoolStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_space_postgres_pool_connections",
		Help: "PostgreSQL connection pool statistics",
	}, []string{"stat"}) // open_connections, in_use, idle

	// HTTP API metrics
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "endpoint", "status_code", "namespace_id"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "endpoint", "namespace_id"})

	HTTPRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_http_request_size_bytes",
		Help:    "Size of HTTP requests in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 6), // 100B to 10MB
	}, []string{"method", "endpoint", "namespace_id"})

	HTTPResponseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_space_http_response_size_bytes",
		Help:    "Size of HTTP responses in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 6), // 100B to 10MB
	}, []string{"method", "endpoint", "namespace_id"})

	QuotaRejections = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_space_quota_rejections_total",
		Help: "Total number of per-namespace quota rejections",
	}, []string{"namespace_id", "reason"})
)

// Helper functions for common metric operations

// RecordOperation records a agent operation with timing
func RecordOperation(operation, storeType string, success bool, duration float64) {
	status := "success"
	if !success {
		status = "failure"
	}
	AgentOperations.WithLabelValues(operation, status, storeType).Inc()
	AgentOperationDuration.WithLabelValues(operation, storeType).Observe(duration)
}

// RecordOperationStage records a per-stage duration for an operation.
func RecordOperationStage(operation, stage, storeType string, duration float64) {
	AgentOperationStageDuration.WithLabelValues(operation, stage, storeType).Observe(duration)
}

// RecordRouteActivity records a sharded route-cache or route-index event.
func RecordRouteActivity(activity, storeType string) {
	RouteActivity.WithLabelValues(activity, storeType).Inc()
}

// RecordRouteActivityDuration records the duration of a sharded route-index operation.
func RecordRouteActivityDuration(activity, storeType string, duration float64) {
	RouteActivityDuration.WithLabelValues(activity, storeType).Observe(duration)
}

// RecordHTTPRequest records an HTTP request with timing
func RecordHTTPRequest(method, endpoint, statusCode, namespaceID string, duration float64, requestSize, responseSize float64) {
	HTTPRequests.WithLabelValues(method, endpoint, statusCode, namespaceID).Inc()
	HTTPRequestDuration.WithLabelValues(method, endpoint, namespaceID).Observe(duration)
	if requestSize > 0 {
		HTTPRequestSize.WithLabelValues(method, endpoint, namespaceID).Observe(requestSize)
	}
	if responseSize > 0 {
		HTTPResponseSize.WithLabelValues(method, endpoint, namespaceID).Observe(responseSize)
	}
}

// RecordStoreError records a store error
func RecordStoreError(storeType, errorType string) {
	StoreErrors.WithLabelValues(storeType, errorType).Inc()
}

// RecordQuotaRejection increments per-namespace quota rejection counters.
func RecordQuotaRejection(namespaceID, reason string) {
	if namespaceID == "" {
		namespaceID = "_global"
	}
	if reason == "" {
		reason = "unknown"
	}
	QuotaRejections.WithLabelValues(namespaceID, reason).Inc()
}

// UpdateAgentMetrics updates agent count metrics
func UpdateAgentMetrics(storeType string, byStatus map[string]int, byKind map[string]int) {
	// Update status metrics
	for status, count := range byStatus {
		AgentsTotal.WithLabelValues(status, storeType).Set(float64(count))
	}

	// Update kind metrics
	for kind, count := range byKind {
		AgentsByKind.WithLabelValues(kind, storeType).Set(float64(count))
	}
}

// UpdateBacklogMetric updates NEW+IN_PROGRESS backlog by kind.
func UpdateBacklogMetric(storeType, kind string, backlog int) {
	BacklogByKind.WithLabelValues(kind, storeType).Set(float64(backlog))
}

package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
)

// metricsResponseWriter wraps http.ResponseWriter to capture status code and response size
type metricsResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    int64
}

func (rw *metricsResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *metricsResponseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.written += int64(n)
	return n, err
}

// MetricsMiddleware records HTTP request metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture metrics
		rw := &metricsResponseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Get request size
		requestSize := float64(r.ContentLength)
		if requestSize < 0 {
			requestSize = 0
		}

		// Normalize endpoint for metrics
		endpoint := normalizeEndpoint(r.URL.Path)

		// Process request
		next.ServeHTTP(rw, r)

		// Record metrics
		duration := time.Since(start).Seconds()
		statusCode := strconv.Itoa(rw.statusCode)
		responseSize := float64(rw.written)
		namespaceID := "_global"
		if principal, ok := AuthPrincipalFromContext(r.Context()); ok && principal.NamespaceID != "" {
			namespaceID = principal.NamespaceID
		}

		metrics.RecordHTTPRequest(r.Method, endpoint, statusCode, namespaceID, duration, requestSize, responseSize)
	})
}

// normalizeEndpoint normalizes URL paths for consistent metric labels
func normalizeEndpoint(path string) string {
	// Remove trailing slashes
	path = strings.TrimSuffix(path, "/")

	// Common patterns to normalize
	patterns := []struct {
		prefix  string
		replace string
	}{
		{"/api/v1/agents/", "/api/v1/agents/:id"},
		{"/api/v1/dag/", "/api/v1/dag/:id"},
		{"/api/v1/events/", "/api/v1/events/:id"},
	}

	for _, p := range patterns {
		if strings.HasPrefix(path, p.prefix) && len(path) > len(p.prefix) {
			// Check if the rest looks like an ID (UUID or similar)
			rest := path[len(p.prefix):]
			if !strings.Contains(rest, "/") {
				return p.replace
			}
		}
	}

	// Special handling for specific endpoints
	switch path {
	case "/api/v1/agents/query":
		return "/api/v1/agents/query"
	case "/api/v1/agents/read":
		return "/api/v1/agents/read"
	case "/api/v1/agents/take":
		return "/api/v1/agents/take"
	case "/api/v1/agents/in":
		return "/api/v1/agents/in"
	}

	// Check for action suffixes
	if strings.HasSuffix(path, "/complete") {
		return "/api/v1/agents/:id/complete"
	}
	if strings.HasSuffix(path, "/release") {
		return "/api/v1/agents/:id/release"
	}

	return path
}

// PrometheusHandler returns the Prometheus metrics handler
func PrometheusHandler() http.Handler {
	return promhttp.Handler()
}

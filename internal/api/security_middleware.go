package api

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urobora-ai/agentspaces/pkg/metrics"
	"go.uber.org/zap"
)

// RateLimitConfig controls fixed-window request limiting.
type RateLimitConfig struct {
	RequestsPerWindow int
	Window            time.Duration
	MutatingOnly      bool
	TrustedProxies    []string
}

type rateLimitEntry struct {
	windowStart time.Time
	count       int
}

// RateLimitMiddleware applies per-client fixed-window request limits.
func RateLimitMiddleware(cfg RateLimitConfig, logger *zap.Logger) func(http.Handler) http.Handler {
	if cfg.RequestsPerWindow <= 0 || cfg.Window <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	_ = logger

	var mu sync.Mutex
	entries := make(map[string]rateLimitEntry)

	// Prune expired entries periodically to prevent unbounded memory growth.
	go func() {
		ticker := time.NewTicker(cfg.Window * 2)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			mu.Lock()
			for key, entry := range entries {
				if now.Sub(entry.windowStart) >= cfg.Window*2 {
					delete(entries, key)
				}
			}
			mu.Unlock()
		}
	}()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.MutatingOnly && !isMutatingMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			effectiveLimit := cfg.RequestsPerWindow
			effectiveWindow := cfg.Window
			namespaceKey := ""
			if principal, ok := AuthPrincipalFromContext(r.Context()); ok {
				namespaceKey = principal.NamespaceID
				if principal.Limits.RateLimitRequests > 0 {
					effectiveLimit = principal.Limits.RateLimitRequests
				}
				if principal.Limits.RateLimitWindow > 0 {
					effectiveWindow = principal.Limits.RateLimitWindow
				}
			}
			if effectiveLimit <= 0 || effectiveWindow <= 0 {
				next.ServeHTTP(w, r)
				return
			}

			now := time.Now()
			clientKey := clientIdentifierWithTrust(r, cfg.TrustedProxies)
			if namespaceKey != "" {
				clientKey = "ns:" + namespaceKey
			}

			mu.Lock()
			entry := entries[clientKey]
			if entry.windowStart.IsZero() || now.Sub(entry.windowStart) >= effectiveWindow {
				entry.windowStart = now
				entry.count = 0
			}

			if entry.count >= effectiveLimit {
				retryAfter := effectiveWindow - now.Sub(entry.windowStart)
				if retryAfter < 0 {
					retryAfter = 0
				}
				entries[clientKey] = entry
				mu.Unlock()
				metrics.RecordQuotaRejection(namespaceKey, "rate_limit")

				retrySeconds := int(retryAfter.Seconds())
				if retrySeconds <= 0 {
					retrySeconds = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}

			entry.count++
			entries[clientKey] = entry
			mu.Unlock()

			next.ServeHTTP(w, r)
		})
	}
}

// MaxBodyBytesMiddleware enforces request body size limits.
func MaxBodyBytesMiddleware(maxBytes int64, mutatingOnly bool) func(http.Handler) http.Handler {
	if maxBytes <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if mutatingOnly && !isMutatingMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}
			effectiveMax := maxBytes
			if principal, ok := AuthPrincipalFromContext(r.Context()); ok && principal.Limits.MaxBodyBytes > 0 {
				effectiveMax = principal.Limits.MaxBodyBytes
			}
			if effectiveMax <= 0 {
				next.ServeHTTP(w, r)
				return
			}
			if r.ContentLength > effectiveMax {
				namespaceID := ""
				if principal, ok := AuthPrincipalFromContext(r.Context()); ok {
					namespaceID = principal.NamespaceID
				}
				metrics.RecordQuotaRejection(namespaceID, "max_body_bytes")
			}
			r.Body = http.MaxBytesReader(w, r.Body, effectiveMax)
			next.ServeHTTP(w, r)
		})
	}
}

// RequestTimeoutMiddleware wraps requests in a timeout handler.
func RequestTimeoutMiddleware(timeout time.Duration, mutatingOnly bool) func(http.Handler) http.Handler {
	if timeout <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	return func(next http.Handler) http.Handler {
		timeoutHandler := http.TimeoutHandler(next, timeout, `{"error":"request timeout"}`)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if mutatingOnly && !isMutatingMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}
			timeoutHandler.ServeHTTP(w, r)
		})
	}
}

// TelemetryModeMiddleware disables telemetry endpoints when mode=disabled.
func TelemetryModeMiddleware(mode string) func(http.Handler) http.Handler {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "disabled" {
		return func(next http.Handler) http.Handler { return next }
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/v1/telemetry/") {
				http.Error(w, "Telemetry endpoints disabled", http.StatusNotFound)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func clientIdentifier(r *http.Request) string {
	return clientIdentifierWithTrust(r, nil)
}

func clientIdentifierWithTrust(r *http.Request, trustedProxies []string) string {
	remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
	if remoteHost == "" {
		remoteHost = r.RemoteAddr
	}

	if len(trustedProxies) > 0 && isTrustedProxy(remoteHost, trustedProxies) {
		forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
		if forwarded != "" {
			// Walk the chain from right to left and return the rightmost
			// untrusted IP. The leftmost entry is client-controlled and
			// can be spoofed to rotate rate-limit buckets.
			parts := strings.Split(forwarded, ",")
			for i := len(parts) - 1; i >= 0; i-- {
				candidate := strings.TrimSpace(parts[i])
				if candidate != "" && !isTrustedProxy(candidate, trustedProxies) {
					return candidate
				}
			}
		}
	}

	if remoteHost != "" {
		return remoteHost
	}
	return "unknown"
}

func isTrustedProxy(ip string, trusted []string) bool {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}
	for _, cidr := range trusted {
		if strings.Contains(cidr, "/") {
			_, network, err := net.ParseCIDR(cidr)
			if err == nil && network.Contains(parsedIP) {
				return true
			}
		} else {
			if net.ParseIP(cidr) != nil && cidr == ip {
				return true
			}
		}
	}
	return false
}
